import logging
from decimal import Decimal, getcontext

from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert
from shared.clickhouse.utils import get_clickhouse_client, table_exists
from shared.shovel_base_class import ShovelBaseClass
from substrate import get_substrate_client
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
from shared.utils import convert_address_to_ss58

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(process)d %(message)s")

BLOCKS_PER_DAY = 7200
FIRST_BLOCK_WITH_NEW_STAKING_MECHANISM = 5004000

# Increase precision for fixed point operations
getcontext().prec = 50

class StakeDailyMapShovel(ShovelBaseClass):
    table_name = "shovel_stake_daily_map"

    def __init__(self, name):
        super().__init__(name)
        self.starting_block = FIRST_BLOCK_WITH_NEW_STAKING_MECHANISM

    def process_block(self, n):
        do_process_block(n, self.table_name)


def do_process_block(n, table_name):
    if n % BLOCKS_PER_DAY != 0:
        return
    try:
        try:
            if not table_exists(table_name):
                query = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    block_number UInt64 CODEC(Delta, ZSTD),
                    timestamp DateTime CODEC(Delta, ZSTD),
                    coldkey String CODEC(ZSTD),
                    hotkey String CODEC(ZSTD),
                    netuid UInt8 CODEC(Delta, ZSTD),
                    stake UInt64 CODEC(Delta, ZSTD),
                    alpha UInt64 CODEC(Delta, ZSTD)
                ) ENGINE = ReplacingMergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (coldkey, hotkey, netuid, timestamp)
                """
                get_clickhouse_client().execute(query)
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to create/check table: {str(e)}")

        try:
            (block_timestamp, block_hash) = get_block_metadata(n)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to get block metadata: {str(e)}")

        try:
            stake_data = fetch_all_stakes_at_block(block_hash)
        except Exception as e:
            raise ShovelProcessingError(f"Failed to fetch stakes from substrate: {str(e)}")

        if not stake_data:
            raise ShovelProcessingError(f"No stake data returned for block {n}")

        try:
            for stake_entry in stake_data:
                buffer_insert(
                    table_name,
                    [n, block_timestamp, f"'{stake_entry['coldkey']}'", f"'{stake_entry['hotkey']}'", stake_entry['netuid'], stake_entry['stake'], stake_entry['alpha']]
                )
        except Exception as e:
            raise DatabaseConnectionError(f"Failed to insert data into buffer: {str(e)}")

    except (DatabaseConnectionError, ShovelProcessingError):
        # Re-raise these exceptions to be handled by the base class
        raise
    except Exception as e:
        raise ShovelProcessingError(f"Unexpected error processing block {n}: {str(e)}")


def _extract_int(value_obj):
    """Extract integer from substrate result (int/str/hex/dict)."""
    if value_obj is None:
        return 0
    if isinstance(value_obj, int):
        return value_obj
    if isinstance(value_obj, str):
        try:
            return int(value_obj, 16) if value_obj.startswith('0x') else int(value_obj)
        except Exception:
            return 0
    if isinstance(value_obj, dict):
        if 'bits' in value_obj:
            return _extract_int(value_obj['bits'])
        if 'value' in value_obj:
            return _extract_int(value_obj['value'])
    try:
        return int(value_obj)
    except Exception:
        return 0


def _fixed128_to_float(bits_int: int) -> float:
    MASK64 = (1 << 64) - 1
    int_part = bits_int >> 64
    frac_part = bits_int & MASK64
    return float(int_part) + float(frac_part) / 2 ** 64


def _query_int(substrate, func, params, block_hash):
    res = substrate.query(
        'SubtensorModule',
        func,
        params,
        block_hash=block_hash
    )
    return _extract_int(res.value if hasattr(res, 'value') else res)


def _query_fixed_float(substrate, func, params, block_hash):
    bits = _query_int(substrate, func, params, block_hash)
    return _fixed128_to_float(bits)


def fetch_all_stakes_at_block(block_hash):
    """Compute TAO stake and Alpha share for all coldkeys using only storage maps (no runtime calls)."""
    try:
        substrate = get_substrate_client()

        # Cache: per (netuid, hotkey) -> (hotkey_alpha_int, total_hotkey_shares_float)
        hotkey_metrics_cache = {}

        # Active subnets
        networks_added = substrate.query_map('SubtensorModule', 'NetworksAdded', block_hash=block_hash)
        netuids = [_extract_int(net[0]) for net in networks_added]

        # Full StakingHotkeys map (coldkey -> Vec<hotkey>)
        staking_entries_q = substrate.query_map(
            module='SubtensorModule',
            storage_function='StakingHotkeys',
            block_hash=block_hash,
            page_size=1000
        )
        staking_entries = list(staking_entries_q)
        if len(staking_entries) == 0:
            raise ShovelProcessingError('No StakingHotkeys data returned')

        all_stakes = []

        # Helper to convert Scale bytes/tuple to SS58 using shared utils
        def to_ss58(raw_addr, addr_type):
            try:
                return convert_address_to_ss58(raw_addr, addr_type)
            except Exception:
                return None

        def get_hotkey_metrics(netuid, hotkey_address):
            key = (netuid, hotkey_address)
            if key in hotkey_metrics_cache:
                return hotkey_metrics_cache[key]
            hotkey_alpha_int = _query_int(substrate, 'TotalHotkeyAlpha', [hotkey_address, netuid], block_hash)
            total_hotkey_shares_float = _query_fixed_float(substrate, 'TotalHotkeyShares', [hotkey_address, netuid], block_hash)
            hotkey_metrics_cache[key] = (hotkey_alpha_int, total_hotkey_shares_float)
            return hotkey_metrics_cache[key]

        for idx, entry in enumerate(staking_entries):
            if idx % 1000 == 0:
                logging.debug(f'Processing StakingHotkeys entry {idx+1}/{len(staking_entries)}')
            try:
                coldkey_raw = entry[0]
                hotkeys_raw_list = entry[1]
                coldkey_ss58 = to_ss58(coldkey_raw, 'coldkey')
                if not coldkey_ss58 or not hotkeys_raw_list:
                    continue
                # Unwrap BittensorScaleType value attribute
                if hasattr(hotkeys_raw_list, 'value'):
                    hotkeys_raw_list = hotkeys_raw_list.value

                for hotkey_raw_container in hotkeys_raw_list:
                    # handle nested tuple ((bytes,),) or direct tuple
                    hotkey_raw = hotkey_raw_container
                    if isinstance(hotkey_raw, (tuple, list)) and len(hotkey_raw) == 1 and isinstance(hotkey_raw[0], (tuple, list)):
                        hotkey_raw = hotkey_raw[0]
                    hotkey_ss58 = to_ss58(hotkey_raw, 'hotkey')
                    if not hotkey_ss58:
                        continue

                    for netuid in netuids:
                        # Alpha share (fixed 128) between this coldkey/hotkey on subnet
                        alpha_share_float = _query_fixed_float(substrate, 'Alpha', [hotkey_ss58, coldkey_ss58, netuid], block_hash)
                        if alpha_share_float == 0:
                            continue  # coldkey not staked to this hotkey in this subnet

                        hotkey_alpha_int, total_hotkey_shares_float = get_hotkey_metrics(netuid, hotkey_ss58)
                        if total_hotkey_shares_float == 0:
                            continue
                        stake_tao = int(alpha_share_float * hotkey_alpha_int / total_hotkey_shares_float)
                        alpha_int = int(alpha_share_float)

                        all_stakes.append({
                            'coldkey': coldkey_ss58,
                            'hotkey': hotkey_ss58,
                            'netuid': netuid,
                            'stake': stake_tao,
                            'alpha': alpha_int
                        })
            except Exception as exc:
                logging.warning(f'Error processing staking entry #{idx}: {exc}')
                continue

        return all_stakes

    except Exception as e:
        raise ShovelProcessingError(f'Error fetching stakes: {str(e)}')


def main():
    StakeDailyMapShovel(name="stake_daily_map").start()


if __name__ == "__main__":
    main()
