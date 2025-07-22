from shared.block_metadata import get_block_metadata
from shared.clickhouse.batch_insert import buffer_insert, set_debug_mode
from shared.clickhouse.utils import (
    get_clickhouse_client,
    table_exists,
)
from shared.shovel_base_class import ShovelBaseClass
from shared.exceptions import DatabaseConnectionError, ShovelProcessingError
from substrate import get_substrate_client, reconnect_substrate
import logging
from typing import Dict, List, Any, Tuple
from typing import Union
from scalecodec.utils.ss58 import ss58_encode
import traceback
from collections import defaultdict
import os

set_debug_mode(True)

SS58_FORMAT = 42
FIRST_DTAO_BLOCK = int(os.getenv("FIRST_DTAO_BLOCK", "4920351"))

def decode_account_id(account_id_bytes: Union[tuple[int], tuple[tuple[int]]]):
    if hasattr(account_id_bytes, 'value'):
        account_id_bytes = account_id_bytes.value

    if isinstance(account_id_bytes, tuple) and len(account_id_bytes) == 1 and isinstance(account_id_bytes[0], (tuple, list)):
        account_id_bytes = account_id_bytes[0]

    if isinstance(account_id_bytes, list) and len(account_id_bytes) == 1 and isinstance(account_id_bytes[0], tuple):
        account_id_bytes = account_id_bytes[0]

    if isinstance(account_id_bytes, str):
        return account_id_bytes

    try:
        return ss58_encode(bytes(account_id_bytes).hex(), SS58_FORMAT)
    except Exception as e:
        logging.error(f"Failed to decode account ID: {e}, type: {type(account_id_bytes)}, value: {account_id_bytes}")
        raise

def decode_string(string: Union[str, tuple[int]]):
    if isinstance(string, str):
        return string
    return bytes(string).decode('utf-8')

logging.basicConfig(level=logging.INFO,
                   format="%(asctime)s %(process)d %(levelname)s %(name)s %(message)s",
                   force=True)

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

def get_registered_validators_in_subnet(substrate, net_uid: int, block_hash: str) -> List[str]:
    """Get all validators registered in a specific subnet."""
    try:
        result = substrate.query_map(
            module="SubtensorModule",
            storage_function="Keys",
            params=[net_uid],
            block_hash=block_hash
        )

        validators = []
        for storage_key, value in result:
            if value:
                try:
                    if hasattr(value, 'value'):
                        account_data = value.value
                    else:
                        account_data = value

                    hotkey = decode_account_id(account_data)
                    validators.append(hotkey)
                except Exception as e:
                    logging.error(f"Failed to decode validator hotkey: {e}")
                    continue

        logging.debug(f"Found {len(validators)} validators registered in subnet {net_uid}")
        return validators

    except Exception as e:
        logging.error(f"Failed to get registered validators for subnet {net_uid}: {str(e)}")
        return []

def get_parent_keys_for_validator(substrate, validator_address: str, net_uid: int, block_hash: str) -> List[Tuple[str, int]]:
    """Get parent keys for a validator (child) in a specific subnet."""
    try:
        result = substrate.query("SubtensorModule", "ParentKeys", [validator_address, net_uid], block_hash=block_hash)
        if not result or not result.value:
            return []

        parent_keys = []
        for encoded_parent in result.value:
            try:
                parent_take = encoded_parent[0]
                parent_hotkey = decode_account_id(encoded_parent[1])
                parent_keys.append((parent_hotkey, parent_take))
            except Exception as e:
                logging.error(f"Failed to decode parent key: {str(e)}")
                continue

        return parent_keys
    except Exception as e:
        logging.error(f"Failed to get parent keys for {validator_address} in subnet {net_uid}: {str(e)}")
        return []

def get_child_keys_for_validator(substrate, validator_address: str, net_uid: int, block_hash: str) -> List[tuple]:
    """Get child keys for a validator in a specific subnet."""
    try:
        result = substrate.query("SubtensorModule", "ChildKeys", [validator_address, net_uid], block_hash=block_hash)
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
        return result is not None and result.value is not None
    except Exception as e:
        logging.error(f"Failed to check subnet registration for {address} in subnet {net_uid}: {str(e)}")
        return False

def get_total_hotkey_alpha(substrate, address: str, net_uid: int, block_hash: str) -> float:
    try:
        result = substrate.query("SubtensorModule", "TotalHotkeyAlpha", [address, net_uid], block_hash=block_hash)
        return float(result.value) if result and result.value else 0.0
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

        owner = decode_account_id(chain_info.get('owner_ss58', chain_info['delegate_ss58']))

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

def fetch_all_validators_data_new(substrate, block_hash: str, block_timestamp, block_number: int, table_name: str):
    """Fetch all validators data using the new logic (from registered validators to parents) and insert directly."""
    logging.info(f"=== fetch_all_validators_data_new called for block {block_number} ===")
    logging.info("Fetching delegate info...")
    try:
        delegate_info = substrate.runtime_call(
            api="DelegateInfoRuntimeApi",
            method="get_delegates",
            params=[],
            block_hash=block_hash
        ).value
        logging.info(f"Got delegate info with {len(delegate_info)} entries")
    except Exception as e:
        logging.error(f"Failed to fetch delegate info: {e}")
        raise

    delegate_map = {}
    for delegate in delegate_info:
        if 'delegate_ss58' in delegate:
            address = decode_account_id(delegate['delegate_ss58'])
            delegate_map[address] = delegate

    subnet_uids = get_subnet_uids(substrate, block_hash)
    logging.info(f"Found {len(subnet_uids)} subnets to process")

    all_validators = {}  # address -> validator data
    parents_with_child_access = defaultdict(set)  # parent -> set of subnets they can access via children
    child_relationships = []  # List of (child, parent, subnet, take) tuples

    for net_uid in subnet_uids:
        logging.info(f"Processing subnet {net_uid}...")
        registered_validators = get_registered_validators_in_subnet(substrate, net_uid, block_hash)

        for validator_address in registered_validators:
            if validator_address not in all_validators:
                all_validators[validator_address] = {
                    "address": validator_address,
                    "subnets": set(),
                    "subnet_hotkey_alpha": {},
                    "is_child": False,
                    "parent_addresses": set()
                }

            all_validators[validator_address]["subnets"].add(net_uid)

            alpha = get_total_hotkey_alpha(substrate, validator_address, net_uid, block_hash)
            all_validators[validator_address]["subnet_hotkey_alpha"][net_uid] = alpha

            parent_keys = get_parent_keys_for_validator(substrate, validator_address, net_uid, block_hash)

            if parent_keys:
                all_validators[validator_address]["is_child"] = True
                for parent_address, parent_take in parent_keys:
                    all_validators[validator_address]["parent_addresses"].add(parent_address)
                    parents_with_child_access[parent_address].add(net_uid)
                    child_relationships.append((validator_address, parent_address, net_uid, parent_take))
                    logging.debug(f"Found child {validator_address} -> parent {parent_address} in subnet {net_uid}")

    for parent_address, child_subnets in parents_with_child_access.items():
        if parent_address not in all_validators:
            all_validators[parent_address] = {
                "address": parent_address,
                "subnets": set(),  # Not directly registered
                "subnet_hotkey_alpha": {},
                "is_child": False,
                "parent_addresses": set()
            }

    successful_inserts = 0
    total_validators = 0
    regular_validators = 0
    child_key_validators = 0

    for address, val_data in all_validators.items():
        if address not in delegate_map:
            continue

        total_validators += 1
        delegate = delegate_map[address]

        info = fetch_validator_info(substrate, address, block_hash, delegate_info)

        registrations = list(val_data["subnets"])
        if address in parents_with_child_access:
            child_access_subnets = list(parents_with_child_access[address])
            registrations = list(set(registrations + child_access_subnets))

        if val_data["is_child"]:
            child_key_validators += 1
        else:
            regular_validators += 1

        validator_data = {
            "block_number": block_number,
            "timestamp": block_timestamp,
            "name": info['name'],
            "address": address,
            "image": info['image'],
            "description": info['description'],
            "owner": info['owner'],
            "url": info['url'],
            "nominators": len(delegate.get('nominators', [])),
            "daily_return": delegate.get('total_daily_return', 0.0),
            "registrations": sorted(registrations),  # All subnets this validator can access
            "validator_permits": delegate.get('validator_permits', []),
            "subnet_hotkey_alpha": val_data["subnet_hotkey_alpha"],
            "is_child_key": val_data["is_child"],
            "parent_addresses": list(val_data["parent_addresses"])
        }

        if insert_validator(table_name, validator_data):
            successful_inserts += 1

        if address in ["5G3wMP3g3d775hauwmAZioYFVZYnvw6eY46wkFy8hEWD5KP3", "5HdTZQ6UXD7MWcRsMeExVwqAKKo4UwomUd662HvtXiZXkxmv"]:
            logging.info(f"Validator {address}: direct_subnets={val_data['subnets']}, child_access={parents_with_child_access.get(address, set())}, total_registrations={registrations}")

    # Log final statistics
    parents_with_children = len(parents_with_child_access)

    logging.info(f"Processed {total_validators} total validators:")
    logging.info(f"- Regular validators: {regular_validators}")
    logging.info(f"- Child key validators: {child_key_validators}")
    logging.info(f"- Parents with child access: {parents_with_children}")
    logging.info(f"- Successful inserts: {successful_inserts}")

    if successful_inserts < total_validators:
        logging.warning(f"Only {successful_inserts}/{total_validators} validators were successfully inserted")

    return successful_inserts


def insert_validator(table_name: str, validator_data: Dict[str, Any]) -> bool:
    """Insert a single validator into ClickHouse. Returns True if successful."""
    try:
        def escape_string(s):
            if s is None:
                return 'NULL'
            return f"'{s.replace("'", "''")}'"

        values = [
            validator_data["block_number"],
            validator_data["timestamp"],
            escape_string(validator_data['name']),
            escape_string(validator_data['address']),
            escape_string(validator_data['image']),
            escape_string(validator_data['description']),
            escape_string(validator_data['owner']),
            escape_string(validator_data['url']),
            validator_data["nominators"],
            validator_data["daily_return"],
            f"[{','.join(str(x) for x in validator_data['registrations'])}]",
            f"[{','.join(str(x) for x in validator_data['validator_permits'])}]",
            f"{{{','.join(f'{k}:{v}' for k,v in validator_data['subnet_hotkey_alpha'].items())}}}" if validator_data['subnet_hotkey_alpha'] else '{}'
        ]

        buffer_insert(table_name, values)
        return True

    except Exception as e:
        logging.error(f"Error inserting validator {validator_data['address']}: {str(e)}")
        return False


class ValidatorsShovel(ShovelBaseClass):
    table_name = "shovel_validators"

    def __init__(self, name):
        logging.info(f"Initializing ValidatorsShovel with name: {name}")
        super().__init__(name)
        self.starting_block = FIRST_DTAO_BLOCK
        logging.info(f"ValidatorsShovel initialized with starting_block: {self.starting_block}")

    def process_block(self, n):
        if n % 10 == 0:
            logging.info(f"process_block called for block {n}")

        if n % 3600 != 0:
            return

        logging.info(f"=== STARTING TO PROCESS BLOCK {n} ===")
        try:
            reconnect_substrate()
            logging.info(f"Processing block {n}")
            substrate = get_substrate_client()
            logging.info("Got substrate client")

            (block_timestamp, block_hash) = get_block_metadata(n)

            logging.info(f"Got block metadata: timestamp={block_timestamp}, hash={block_hash}")

            logging.info("About to create/check validators table...")
            create_validators_table(self.table_name)
            logging.info("Ensured validators table exists - ClickHouse connection successful!")

            successful_inserts = fetch_all_validators_data_new(substrate, block_hash, block_timestamp, n, self.table_name)

            logging.info(f"Successfully processed block {n}")

            if successful_inserts > 0:
                logging.info(f"Added {successful_inserts} validators to buffer. Buffer will auto-flush based on size/time thresholds.")

        except DatabaseConnectionError as e:
            logging.error(f"Database connection error in block {n}: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error processing block {n}: {str(e)}")
            logging.error(traceback.format_exc())
            raise ShovelProcessingError(f"Failed to process block {n}: {str(e)}")

    def cleanup(self):
        """Cleanup method called when the shovel is stopped."""
        logging.info("Cleaning up ValidatorsShovel...")
        pass


def main():
    print("Starting ValidatorsShovel...")
    print(f"Current log level: {logging.getLogger().level}")
    logging.warning("TEST: This is a warning message")
    logging.info("TEST: This is an info message")
    logging.debug("TEST: This is a debug message")

    try:
        logging.info("Creating ValidatorsShovel instance...")
        shovel = ValidatorsShovel(name="validators")
        logging.info(f"ValidatorsShovel created with starting_block: {shovel.starting_block}")
        logging.info("Starting shovel...")
        shovel.start()
    except Exception as e:
        logging.error(f"Error in main: {e}", exc_info=True)
        print(f"ERROR: {e}")
        raise

if __name__ == "__main__":
    main()
