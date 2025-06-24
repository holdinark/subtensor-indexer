from time import sleep
from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert, set_debug_mode
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.shovel_base_class import ShovelBaseClass
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
from substrate import get_substrate_client
import logging
from typing import Dict, List, Any
from typing import Union
from scalecodec.utils.ss58 import ss58_encode
import traceback
import os

set_debug_mode(True)

SS58_FORMAT = 42
FIRST_DTAO_BLOCK = int(os.getenv("FIRST_DTAO_BLOCK", "4920351"))

def decode_account_id(account_id_bytes: Union[tuple[int], tuple[tuple[int]]]):
    if isinstance(account_id_bytes, tuple) and isinstance(account_id_bytes[0], tuple):
        account_id_bytes = account_id_bytes[0]
    return ss58_encode(bytes(account_id_bytes).hex(), SS58_FORMAT)

def decode_string(string: Union[str, tuple[int]]):
    if isinstance(string, str):
        return string
    return bytes(string).decode('utf-8')

logging.basicConfig(level=logging.INFO,
                   format="%(asctime)s %(process)d %(message)s")

def create_validators_table(table_name):
    if not table_exists(table_name):
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            block_number UInt64,
            timestamp DateTime,
            name String,
            address String,
            image Nullable(String),
            description Nullable(String),
            owner Nullable(String),
            url Nullable(String),
            nominators UInt64,
            daily_return Float64,
            registrations Array(UInt64),
            validator_permits Array(UInt64),
            subnet_hotkey_alpha Map(UInt64, Float64)
        ) ENGINE = ReplacingMergeTree()
        ORDER BY (block_number, address)
        """

        get_clickhouse_client().execute(query)

def get_subnet_uids(substrate, block_hash: str) -> List[int]:
    try:
        result = substrate.runtime_call(
            api="SubnetInfoRuntimeApi",
            method="get_subnets_info",
            params=[],
            block_hash=block_hash
        )
        subnet_info = result.value

        return [info['netuid'] for info in subnet_info if 'netuid' in info]
    except Exception as e:
        logging.error(f"Failed to get subnet UIDs: {str(e)}")
        return []

def get_active_validators(substrate, block_hash: str, delegate_info) -> List[str]:
    try:
        return [decode_account_id(delegate['delegate_ss58']) for delegate in delegate_info if 'delegate_ss58' in delegate]
    except Exception as e:
        logging.error(f"Failed to get active validators: {str(e)}")
        return []

def get_child_keys_for_validator(substrate, validator_address: str, net_uid: int, block_hash: str) -> List[tuple]:
    """Get child keys for a validator in a specific subnet."""
    try:
        result = substrate.query("SubtensorModule", "ParentKeys", [validator_address, net_uid], block_hash=block_hash)
        if not result or not result.value:
            return []

        child_keys = []
        for encoded_child in result.value:
            try:
                child_take = encoded_child[0]
                child_hotkey = decode_account_id(encoded_child[1])
                child_keys.append((child_hotkey, child_take))
            except Exception as e:
                logging.error(f"Failed to decode child key: {str(e)}")
                continue

        return child_keys
    except Exception as e:
        logging.error(f"Failed to get child keys for {validator_address} in subnet {net_uid}: {str(e)}")
        return []

def is_registered_in_subnet(substrate, net_uid: int, address: str, block_hash: str) -> bool:
    try:
        result = substrate.query("SubtensorModule", "Uids", [net_uid, address], block_hash=block_hash)
        return result is not None
    except Exception as e:
        logging.error(f"Failed to check subnet registration for {address} in subnet {net_uid}: {str(e)}")
        return False

def get_total_hotkey_alpha(substrate, address: str, net_uid: int, block_hash: str) -> float:
    try:
        result = substrate.query("SubtensorModule", "TotalHotkeyAlpha", [address, net_uid], block_hash=block_hash)
        return float(result.value) if result.value else 0.0
    except Exception as e:
        logging.error(f"Failed to get total hotkey alpha for {address} in subnet {net_uid}: {str(e)}")
        return 0.0

def fetch_validator_info(substrate, address: str, block_hash: str, delegate_info) -> Dict[str, Any]:
    try:
        chain_info = next((d for d in delegate_info if decode_account_id(d['delegate_ss58']) == address), None)

        if not chain_info:
            return {
                "name": address,
                "image": None,
                "description": None,
                "owner": None,
                "url": None
            }

        owner = decode_account_id(chain_info.get('owner_ss58'))

        identity = substrate.query("SubtensorModule", "IdentitiesV2", [owner], block_hash=block_hash)

        return {
            "name": decode_string(identity.get('name', address)) if identity else address,
            "image": decode_string( identity.get('image', '')) if identity else None,
            "description": decode_string(identity.get('description', '')) if identity else None,
            "owner": owner,
            "url": decode_string(identity.get('url', '')) if identity else None
        }
    except Exception as e:
        logging.error(f"Failed to fetch validator info for {address}: {str(e)}")
        return {
            "name": address,
            "image": None,
            "description": None,
            "owner": None,
            "url": None
        }

def fetch_validator_stats(substrate, address: str, block_hash: str, delegate_info) -> Dict[str, Any]:
    try:
        info = next((d for d in delegate_info if decode_account_id(d['delegate_ss58']) == address), None)

        if not info:
            return {
                "nominators": 0,
                "daily_return": 0.0,
                "registrations": [],
                "validator_permits": [],
                "subnet_hotkey_alpha": {}
            }

        return_per_1000 = int(info['return_per_1000'], 16) if isinstance(info['return_per_1000'], str) else info['return_per_1000']

        subnet_uids = get_subnet_uids(substrate, block_hash)
        subnet_hotkey_alpha = {}

        for net_uid in subnet_uids:
            if is_registered_in_subnet(substrate, net_uid, address, block_hash):
                logging.info(f"Found validator {address} in subnet {net_uid}")
                alpha = get_total_hotkey_alpha(substrate, address, net_uid, block_hash)
                if alpha > 0:
                    subnet_hotkey_alpha[net_uid] = alpha
                else:
                    subnet_hotkey_alpha[net_uid] = 0
            else:
                logging.info(f"Validator {address} not found in subnet {net_uid}")

        return {
            "nominators": len(info.get('nominators', [])),
            "daily_return": info.get('total_daily_return', 0.0),
            "registrations": info.get('registrations', []),
            "validator_permits": info.get('validator_permits', []),
            "subnet_hotkey_alpha": subnet_hotkey_alpha
        }
    except Exception as e:
        logging.error(f"Failed to fetch validator stats for {address}: {str(e)}")
        return {
            "nominators": 0,
            "daily_return": 0.0,
            "registrations": [],
            "validator_permits": [],
            "subnet_hotkey_alpha": {}
        }

def fetch_validator_stats_for_child(substrate, child_address: str, parent_address: str, block_hash: str, delegate_info) -> Dict[str, Any]:
    """Fetch stats for a child key validator."""
    try:
        # Get parent's info
        parent_info = next((d for d in delegate_info if decode_account_id(d['delegate_ss58']) == parent_address), None)

        if not parent_info:
            return {
                "nominators": 0,
                "daily_return": 0.0,
                "registrations": [],
                "validator_permits": [],
                "subnet_hotkey_alpha": {}
            }

        subnet_uids = get_subnet_uids(substrate, block_hash)
        subnet_hotkey_alpha = {}
        registrations = []

        for net_uid in subnet_uids:
            # First check if parent is registered in this subnet
            if is_registered_in_subnet(substrate, net_uid, parent_address, block_hash):
                # Then check if child is a child key of this parent in this subnet
                child_keys = get_child_keys_for_validator(substrate, parent_address, net_uid, block_hash)
                if any(child_hotkey == child_address for child_hotkey, _ in child_keys):
                    registrations.append(net_uid)
                    alpha = get_total_hotkey_alpha(substrate, child_address, net_uid, block_hash)
                    if alpha > 0:
                        subnet_hotkey_alpha[net_uid] = alpha
                    else:
                        subnet_hotkey_alpha[net_uid] = 0
                    logging.info(f"Child key {child_address} registered through parent {parent_address} in subnet {net_uid}")

        return {
            "nominators": 0,  # Child keys don't have direct nominators
            "daily_return": 0.0,  # Child keys don't have direct returns
            "registrations": registrations,
            "validator_permits": parent_info.get('validator_permits', []),
            "subnet_hotkey_alpha": subnet_hotkey_alpha
        }
    except Exception as e:
        logging.error(f"Failed to fetch validator stats for child {child_address}: {str(e)}")
        return {
            "nominators": 0,
            "daily_return": 0.0,
            "registrations": [],
            "validator_permits": [],
            "subnet_hotkey_alpha": {}
        }

class ValidatorsShovel(ShovelBaseClass):
    table_name = "shovel_validators"

    def __init__(self, name):
        super().__init__(name)
        self.starting_block = FIRST_DTAO_BLOCK

    def process_block(self, n):
        # if n % 7200 != 0:
        #     logging.info(f"Skipping block {n}")
        #     return
        try:
            logging.info(f"Processing block {n}")
            substrate = get_substrate_client()
            logging.info("Got substrate client")

            (block_timestamp, block_hash) = get_block_metadata(n)
            logging.info(f"Got block metadata: timestamp={block_timestamp}, hash={block_hash}")

            create_validators_table(self.table_name)
            logging.info("Ensured validators table exists")

            logging.info("Fetching delegate info...")
            delegate_info = substrate.runtime_call(
                api="DelegateInfoRuntimeApi",
                method="get_delegates",
                params=[],
                block_hash=block_hash
            ).value
            logging.info(f"Got delegate info with {len(delegate_info)} entries")

            validators = get_active_validators(substrate, block_hash, delegate_info)
            logging.info(f"Found {len(validators)} active validators")

            # Get subnet UIDs for querying child keys
            subnet_uids = get_subnet_uids(substrate, block_hash)
            logging.info(f"Found {len(subnet_uids)} subnets")

            # Discover child keys for each validator
            child_key_validators = []
            for validator_address in validators:
                validator_info = next((d for d in delegate_info if decode_account_id(d['delegate_ss58']) == validator_address), None)
                if validator_info and any(validator_info.get('validator_permits', [])):
                    for net_uid in subnet_uids:
                        child_keys = get_child_keys_for_validator(substrate, validator_address, net_uid, block_hash)
                        for child_hotkey, child_take in child_keys:
                            child_key_validators.append((child_hotkey, validator_address))
                            logging.info(f"Found child key {child_hotkey} for parent {validator_address} in subnet {net_uid}")
                            if len(child_key_validators) >= 20:
                                break
                        if len(child_key_validators) >= 20:
                            break
                if len(child_key_validators) >= 20:
                    break

            logging.info(f"Found {len(child_key_validators)} child key validators")

            # Combine all validators to process
            all_validators_to_process = [(addr, None) for addr in validators] + child_key_validators
            logging.info(f"Total validators to process: {len(all_validators_to_process)}")

            successful_inserts = 0
            for idx, (validator_address, parent_address) in enumerate(all_validators_to_process, 1):
                try:
                    logging.info(f"Processing validator {idx}/{len(all_validators_to_process)}: {validator_address}")

                    if parent_address:
                        info = fetch_validator_info(substrate, validator_address, block_hash, delegate_info)
                        logging.info(f"-----> Got parent validator info for child {validator_address}: name={info['name']}, owner={info['owner']}")

                        stats = fetch_validator_stats_for_child(substrate, validator_address, parent_address, block_hash, delegate_info)
                        logging.info(f"Got child validator stats for {validator_address}: registrations={stats['registrations']}")
                    else:
                        # This is a regular validator
                        info = fetch_validator_info(substrate, validator_address, block_hash, delegate_info)
                        logging.info(f"Got validator info for {validator_address}: name={info['name']}, owner={info['owner']}")

                        stats = fetch_validator_stats(substrate, validator_address, block_hash, delegate_info)
                        logging.info(f"Got validator stats for {validator_address}: nominators={stats['nominators']}, registrations={stats['registrations']}")

                    def escape_string(s):
                        if s is None:
                            return 'NULL'
                        return f"'{s.replace("'", "''")}'"

                    values = [
                        n,
                        block_timestamp,
                        escape_string(info['name']),
                        escape_string(validator_address),
                        escape_string(info['image']),
                        escape_string(info['description']),
                        escape_string(info['owner']),
                        escape_string(info['url']),
                        stats["nominators"],
                        stats["daily_return"],
                        f"[{','.join(str(x) for x in stats['registrations'])}]",
                        f"[{','.join(str(x) for x in stats['validator_permits'])}]",
                        f"{{{','.join(f'{k}:{v}' for k,v in stats['subnet_hotkey_alpha'].items())}}}" if stats['subnet_hotkey_alpha'] else '{}'
                    ]

                    buffer_insert(self.table_name, values)
                    successful_inserts += 1
                    logging.info(f"Successfully processed validator {validator_address} ({idx}/{len(all_validators_to_process)})")

                except Exception as e:
                    logging.error(f"Error processing validator {validator_address} ({idx}/{len(all_validators_to_process)}): {str(e)}")
                    continue

            logging.info(f"Block {n} summary:")
            logging.info(f"- Total validators: {len(all_validators_to_process)}")
            logging.info(f"- Direct validators: {len(validators)}")
            logging.info(f"- Child key validators: {len(child_key_validators)}")
            logging.info(f"- Successful inserts: {successful_inserts}")
            logging.info(f"- Failed inserts: {len(all_validators_to_process) - successful_inserts}")

            logging.info(f"Mannually updating shovel_checkpoints for block {n}")

            buffer_insert(
                "shovel_checkpoints",
                [f"'{self.name}'", n],
            )
            logging.info(f"Done, processing another block")

        except DatabaseConnectionError as e:
            logging.error(f"Database connection error in block {n}: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Failed to process block {n}: {str(e)}")
            logging.error("Stack trace:")
            logging.error(traceback.format_exc())
            raise ShovelProcessingError(f"Failed to process block {n}: {str(e)}")

def main():
    ValidatorsShovel(name="validators").start()

if __name__ == "__main__":
    main()
