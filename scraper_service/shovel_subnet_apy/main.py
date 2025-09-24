from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.shovel_base_class import ShovelBaseClass
from shared.substrate import get_substrate_client
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
import logging
import os
from scalecodec.utils.ss58 import ss58_encode
from datetime import datetime, timezone, timedelta

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

last_epoch_blocks = {}
daily_sampled_subnets = {}
next_check_block = 0

DEFAULT_VALIDATOR_HOTKEY = os.environ.get('TAOCOM_VALIDATOR_HOTKEY')
if not DEFAULT_VALIDATOR_HOTKEY:
    raise ValueError("TAOCOM_VALIDATOR_HOTKEY environment variable must be set")

# Format: "netuid1:hotkey1,netuid2:hotkey2" e.g., "1:5Dyi8aj...,3:5Xyz9..."
SUBNET_VALIDATOR_HOTKEYS = {}
subnet_hotkeys_env = os.environ.get('TAOCOM_SUBNET_VALIDATOR_HOTKEYS', '')
if subnet_hotkeys_env:
    try:
        for pair in subnet_hotkeys_env.split(','):
            if pair.strip():
                netuid_str, hotkey = pair.strip().split(':')
                SUBNET_VALIDATOR_HOTKEYS[int(netuid_str)] = hotkey
        if SUBNET_VALIDATOR_HOTKEYS:
            logging.info(f"Loaded subnet-specific validators for subnets: {list(SUBNET_VALIDATOR_HOTKEYS.keys())}")
    except Exception as e:
        logging.error(f"Failed to parse TAOCOM_SUBNET_VALIDATOR_HOTKEYS: {e}")
        logging.error(f"Expected format: 'netuid1:hotkey1,netuid2:hotkey2'")
        raise ValueError(f"Invalid TAOCOM_SUBNET_VALIDATOR_HOTKEYS format: {subnet_hotkeys_env}")

SS58_FORMAT = 42
U64_MAX = (1 << 64) - 1
U16_MAX = (1 << 16) - 1
BLOCK_SECONDS = 12

def decode_account_id(account_id_bytes):
    if hasattr(account_id_bytes, 'value'):
        account_id_bytes = account_id_bytes.value

    if isinstance(account_id_bytes, str):
        return account_id_bytes

    try:
        return ss58_encode(bytes(account_id_bytes).hex(), SS58_FORMAT)
    except Exception as e:
        logging.error(f"Failed to decode account ID: {e}")
        raise


def get_validator_hotkey_for_subnet(subnet_id: int) -> str:
    return SUBNET_VALIDATOR_HOTKEYS.get(subnet_id, DEFAULT_VALIDATOR_HOTKEY)


def get_date_from_timestamp(timestamp) -> str:
    """Convert timestamp to date string in UTC (YYYY-MM-DD)"""
    dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    return dt.strftime("%Y-%m-%d")


class SubnetAPYShovel(ShovelBaseClass):
    table_name = "shovel_subnet_apy"

    def __init__(self, name):
        super().__init__(name)
        # since this shovel starts on Sep11, lets
        # start indexing from Aug11 to get a full
        # 30day APY today
        self.starting_block = 6200000

    def process_block(self, n):
        do_process_block(n, self.table_name)


def do_process_block(n, table_name):
    """
    SAMPLING STRATEGY:
    - Process only ONE epoch per subnet per day
    - Once all subnets are sampled for current day, skip to midnight UTC
    """
    global last_epoch_blocks, daily_sampled_subnets, next_check_block

    if n < next_check_block:
        logging.debug(f"Block {n}: Skipping (already sampled all subnets until block {next_check_block})")
        return

    try:
        try:
            if not table_exists(table_name):
                query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    block_number UInt64 CODEC(Delta, ZSTD),
                    timestamp DateTime CODEC(Delta, ZSTD),
                    netuid UInt16 CODEC(ZSTD),
                    hotkey String CODEC(ZSTD),
                    subnet_stake UInt64 CODEC(Delta, ZSTD),
                    subnet_dividend UInt64 CODEC(Delta, ZSTD),
                    tao_weight Float32 CODEC(ZSTD),
                    root_stake UInt64 CODEC(Delta, ZSTD),
                    epoch_yield Float32 CODEC(ZSTD),
                    passed_filter Bool CODEC(ZSTD)
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (netuid, timestamp)
                """
                get_clickhouse_client().execute(query)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/check table: {str(e)}")

        try:
            substrate = get_substrate_client()
            (block_timestamp, block_hash) = get_block_metadata(n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        current_date = get_date_from_timestamp(block_timestamp)

        if current_date not in daily_sampled_subnets:
            daily_sampled_subnets[current_date] = set()
            dates_to_remove = []
            for date_str in daily_sampled_subnets:
                if len(daily_sampled_subnets) > 35:
                    dates_to_remove.append(date_str)
            for date_str in dates_to_remove[:len(dates_to_remove)-35]:
                del daily_sampled_subnets[date_str]
            logging.info(f"New day started: {current_date}")

        try:
            pending_emissions_result = substrate.query_map(
                module="SubtensorModule",
                storage_function="PendingEmission",
                block_hash=block_hash
            )

            if pending_emissions_result is None:
                logging.debug(f"Block {n}: No pending emissions found")
                return

            all_subnet_ids = set()
            for storage_key, _ in pending_emissions_result:
                subnet_id = storage_key.value if hasattr(storage_key, 'value') else storage_key
                all_subnet_ids.add(subnet_id)

            remaining_subnets = all_subnet_ids - daily_sampled_subnets[current_date]
            if not remaining_subnets:
                current_dt = datetime.fromtimestamp(block_timestamp, tz=timezone.utc)
                next_midnight = current_dt.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

                seconds_to_skip = (next_midnight - current_dt).total_seconds()
                blocks_to_skip = max(1, int(seconds_to_skip / BLOCK_SECONDS))
                next_check_block = n + blocks_to_skip

                logging.info(f"Block {n}: All {len(all_subnet_ids)} subnets sampled for {current_date}")
                logging.info(f"  Skipping to midnight: {next_midnight.strftime('%Y-%m-%d')} (block ~{next_check_block})")
                return

            if n % 100 == 0:
                logging.info(f"Block {n}: Date {current_date}, sampled {len(daily_sampled_subnets[current_date])}/{len(all_subnet_ids)} subnets")

            for storage_key, pending_emission in pending_emissions_result:
                subnet_id = storage_key.value if hasattr(storage_key, 'value') else storage_key

                if subnet_id in daily_sampled_subnets[current_date]:
                    continue

                if subnet_id not in last_epoch_blocks:
                    last_epoch_blocks[subnet_id] = {"block": 0, "emission": pending_emission.value if hasattr(pending_emission, 'value') else pending_emission}
                    logging.debug(f"Block {n}: Initialized tracking for subnet {subnet_id}")
                    continue

                prev_emission = last_epoch_blocks[subnet_id]["emission"]
                curr_emission = pending_emission.value if hasattr(pending_emission, 'value') else pending_emission

                # Is it a new epoch?
                if curr_emission == 0 and prev_emission > 0:
                    logging.info(f"\n{'='*60}")
                    logging.info(f"Block {n}: EPOCH DETECTED for subnet {subnet_id} (Date: {current_date})")
                    logging.info(f"  Emission reset: {prev_emission} → 0")
                    logging.info(f"  Daily sampling: {len(daily_sampled_subnets[current_date])+1}/{len(all_subnet_ids)} subnets sampled")
                    logging.info(f"{'='*60}")

                    process_subnet_epoch(n, block_timestamp, block_hash, subnet_id, table_name, substrate)

                    daily_sampled_subnets[current_date].add(subnet_id)

                    logging.info(f"{'='*60}\n")

                last_epoch_blocks[subnet_id] = {"block": n, "emission": curr_emission}

        except ShovelProcessingError:
            raise
        except Exception as e:
            raise ShovelProcessingError(f"Failed to process epoch data: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        raise
    except Exception as e:
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


def process_subnet_epoch(block_number, block_timestamp, block_hash, subnet_id, table_name, substrate):
    """Process APY data for Validator's validator on this subnet at epoch boundary"""
    try:
        validator_hotkey = get_validator_hotkey_for_subnet(subnet_id)
        logging.info(f"\n>>> Processing epoch for subnet {subnet_id}")
        logging.info(f"    Block: {block_number}, Timestamp: {block_timestamp}")
        logging.info(f"    Validator's hotkey: {validator_hotkey}")

        try:
            children_result = substrate.query(
                module="SubtensorModule",
                storage_function="ChildKeys",
                params=[validator_hotkey, subnet_id],
                block_hash=block_hash
            )

            child_validators = []
            if children_result and children_result.value:
                children_list = children_result.value if hasattr(children_result, 'value') else children_result
                logging.info(f"Subnet {subnet_id}: Validator has {len(children_list)} childkey(s)")
                for proportion, child_hotkey in children_list:
                    child_key = decode_account_id(child_hotkey.value if hasattr(child_hotkey, 'value') else child_hotkey)
                    child_validators.append((proportion, child_key))
                    proportion_pct = (proportion * 100) / U64_MAX
                    logging.info(f"  - Childkey {child_key[:8]}...: {proportion_pct:.2f}% delegation (proportion={proportion})")
            else:
                logging.debug(f"Subnet {subnet_id}: Validator has no childkeys, checking direct registration")
        except Exception as e:
            logging.warning(f"Failed to check child keys for {validator_hotkey[:8]} on subnet {subnet_id}: {str(e)}")
            child_validators = []

        validators_to_check = [validator_hotkey] + [child for _, child in child_validators]

        if not child_validators:
            logging.debug(f"No childkeys found, checking if Validator is registered directly on subnet {subnet_id}")
        else:
            logging.debug(f"Checking Validator and {len(child_validators)} childkey(s) on subnet {subnet_id}")

        try:
            keys_result = substrate.query_map(
                module="SubtensorModule",
                storage_function="Keys",
                params=[subnet_id],
                block_hash=block_hash
            )

            validators_found = set()
            hotkey_count = 0
            for _, account_id in keys_result:
                hotkey_count += 1
                decoded_hotkey = decode_account_id(account_id.value if hasattr(account_id, 'value') else account_id)
                if decoded_hotkey in validators_to_check:
                    validators_found.add(decoded_hotkey)

            logging.debug(f"Subnet {subnet_id}: Found {len(validators_found)} Validator-related validator(s) out of {hotkey_count} total")

            tao_weight_result = substrate.query(
                module="SubtensorModule",
                storage_function="TaoWeight",
                block_hash=block_hash
            )
            if tao_weight_result is None:
                logging.error(f"Failed to get TaoWeight for block {block_hash} - skipping epoch")
                return

            tao_weight_raw = tao_weight_result.value if hasattr(tao_weight_result, 'value') else tao_weight_result
            tao_weight = tao_weight_raw / U64_MAX

            if not validators_found:
                logging.info(f"Subnet {subnet_id}: Validator not participating (neither directly nor via childkeys) - setting APY to 0")
                buffer_insert(table_name, [
                    block_number,
                    block_timestamp,
                    subnet_id,
                    f"'{validator_hotkey}'",
                    0,  # subnet_stake
                    0,  # subnet_dividend
                    tao_weight,
                    0,  # root_stake
                    0.0,  # epoch_yield
                    False  # passed_filter
                ])
                return

            is_direct = validator_hotkey in validators_found
            num_childkeys = len([c for _, c in child_validators if c in validators_found])
            logging.info(f"Subnet {subnet_id}: Validator participates via: direct={is_direct}, childkeys={num_childkeys}")

        except Exception as e:
            logging.warning(f"Failed to check validator existence on subnet {subnet_id}: {str(e)}")
            return

        try:
            total_subnet_stake = 0
            total_root_stake = 0
            total_subnet_dividend = 0

            tao_stake_result = substrate.query(
                module="SubtensorModule",
                storage_function="TotalHotkeyAlpha",
                params=[validator_hotkey, subnet_id],
                block_hash=block_hash
            )
            tao_subnet_stake = tao_stake_result.value if hasattr(tao_stake_result, 'value') else tao_stake_result or 0

            tao_root_result = substrate.query(
                module="SubtensorModule",
                storage_function="TotalHotkeyAlpha",
                params=[validator_hotkey, 0],
                block_hash=block_hash
            )
            tao_root_stake = tao_root_result.value if hasattr(tao_root_result, 'value') else tao_root_result or 0

            logging.debug(f"Validator base stakes - subnet: {tao_subnet_stake}, root: {tao_root_stake}")

            if validator_hotkey in validators_found:
                total_subnet_stake = tao_subnet_stake
                total_root_stake = tao_root_stake

                dividend_result = substrate.query(
                    module="SubtensorModule",
                    storage_function="AlphaDividendsPerSubnet",
                    params=[subnet_id, validator_hotkey],
                    block_hash=block_hash
                )
                direct_dividend = dividend_result.value if hasattr(dividend_result, 'value') else dividend_result or 0
                total_subnet_dividend = direct_dividend

                logging.info(f"Subnet {subnet_id}: Validator DIRECT validation - stake={tao_subnet_stake}, dividend={direct_dividend}")

            for proportion, child_hotkey in child_validators:
                if child_hotkey in validators_found:
                    # When Validator delegates to a childkey:
                    # - The stake is transferred to the child on-chain
                    # - The child's TotalHotkeyAlpha contains the actual working stake
                    # - The child earns dividends based on their stake
                    # - Validator (parent) gets (1 - childkey_take) of those dividends

                    child_subnet_stake_result = substrate.query(
                        module="SubtensorModule",
                        storage_function="TotalHotkeyAlpha",
                        params=[child_hotkey, subnet_id],
                        block_hash=block_hash
                    )
                    child_subnet_stake = child_subnet_stake_result.value if hasattr(child_subnet_stake_result, 'value') else child_subnet_stake_result or 0

                    child_root_stake_result = substrate.query(
                        module="SubtensorModule",
                        storage_function="TotalHotkeyAlpha",
                        params=[child_hotkey, 0],
                        block_hash=block_hash
                    )
                    child_root_stake = child_root_stake_result.value if hasattr(child_root_stake_result, 'value') else child_root_stake_result or 0

                    total_subnet_stake += child_subnet_stake
                    total_root_stake += child_root_stake

                    child_dividend_result = substrate.query(
                        module="SubtensorModule",
                        storage_function="AlphaDividendsPerSubnet",
                        params=[subnet_id, child_hotkey],
                        block_hash=block_hash
                    )
                    child_dividend = child_dividend_result.value if hasattr(child_dividend_result, 'value') else child_dividend_result or 0

                    childkey_take_result = substrate.query(
                        module="SubtensorModule",
                        storage_function="ChildkeyTake",
                        params=[child_hotkey, subnet_id],
                        block_hash=block_hash
                    )
                    childkey_take = childkey_take_result.value if hasattr(childkey_take_result, 'value') else childkey_take_result or 0

                    parent_share = (child_dividend * (U16_MAX - childkey_take)) // U16_MAX
                    total_subnet_dividend += parent_share

                    take_pct = (childkey_take * 100) / U16_MAX
                    parent_pct = 100 - take_pct
                    proportion_pct = (proportion * 100) / U64_MAX
                    logging.info(f"  Childkey {child_hotkey[:8]}: delegation={proportion_pct:.2f}%, stake={child_subnet_stake}, dividend={child_dividend}, Validator gets {parent_pct:.1f}% = {parent_share}")

            subnet_stake = total_subnet_stake
            root_stake = total_root_stake
            subnet_dividend = total_subnet_dividend

            MIN_STAKE_THRESHOLD = 10  # Minimum stake in rao to consider valid

            if subnet_stake < MIN_STAKE_THRESHOLD:
                epoch_yield = 0.0
                logging.warning(f"Subnet {subnet_id}: Stake {subnet_stake} below minimum threshold {MIN_STAKE_THRESHOLD}, setting yield to 0")
            else:
                epoch_yield = float(subnet_dividend) / float(subnet_stake)

            #
            combined_stake = root_stake * tao_weight + subnet_stake
            passed_filter = subnet_stake >= 10 and combined_stake >= 4000

            logging.info(f"Subnet {subnet_id} SUMMARY: stake={subnet_stake}, dividend={subnet_dividend}, yield={epoch_yield:.6f}, passed_filter={passed_filter}")

            buffer_insert(table_name, [
                block_number,
                block_timestamp,
                subnet_id,
                f"'{validator_hotkey}'",
                subnet_stake,
                subnet_dividend,
                tao_weight,
                root_stake,
                epoch_yield,
                passed_filter
            ])

            if subnet_dividend > 0:
                logging.info(f"Subnet {subnet_id}: ✅ Validator earned {subnet_dividend} dividends, epoch yield={epoch_yield:.6f}")
            else:
                logging.info(f"Subnet {subnet_id}: ⚠️ Validator earned 0 dividends (stake={subnet_stake}, root={root_stake}, combined={combined_stake})")

        except Exception as e:
            logging.warning(f"Failed to process Validator validator {validator_hotkey} on subnet {subnet_id}: {str(e)}")

    except Exception as e:
        raise ShovelProcessingError(f"Failed to process subnet epoch for subnet {subnet_id}: {str(e)}")


def main():
    SubnetAPYShovel(name="taocom_validator_apy").start()


if __name__ == "__main__":
    main()
