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
from typing import Dict, List, Any, Tuple
from typing import Union
from scalecodec.utils.ss58 import ss58_encode
import traceback
from collections import defaultdict

set_debug_mode(True)

SS58_FORMAT = 42
FIRST_DTAO_BLOCK = 6000000 #int(os.getenv("FIRST_DTAO_BLOCK", "4920351"))

def decode_account_id(account_id_bytes: Union[tuple[int], tuple[tuple[int]]]):
    # Handle BittensorScaleType objects
    if hasattr(account_id_bytes, 'value'):
        account_id_bytes = account_id_bytes.value

    # Handle nested tuples
    if isinstance(account_id_bytes, tuple) and len(account_id_bytes) == 1 and isinstance(account_id_bytes[0], (tuple, list)):
        account_id_bytes = account_id_bytes[0]

    # Handle list of tuples
    if isinstance(account_id_bytes, list) and len(account_id_bytes) == 1 and isinstance(account_id_bytes[0], tuple):
        account_id_bytes = account_id_bytes[0]

    # If it's already a string (SS58 address), return it
    if isinstance(account_id_bytes, str):
        return account_id_bytes

    # Convert to bytes and encode
    try:
        return ss58_encode(bytes(account_id_bytes).hex(), SS58_FORMAT)
    except Exception as e:
        # Log more details about the error
        logging.error(f"Failed to decode account ID: {e}, type: {type(account_id_bytes)}, value: {account_id_bytes}")
        raise

def decode_string(string: Union[str, tuple[int]]):
    if isinstance(string, str):
        return string
    return bytes(string).decode('utf-8')

logging.basicConfig(level=logging.DEBUG,
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

        return [3] #info['netuid'] for info in subnet_info if 'netuid' in info]
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
                    # Handle different value formats
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

def fetch_all_validators_data_new(substrate, block_hash: str, block_timestamp, block_number: int):
    """Fetch all validators data using the new logic (from registered validators to parents)."""
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

    # Create delegate map for easy lookup
    delegate_map = {}
    for delegate in delegate_info:
        if 'delegate_ss58' in delegate:
            address = decode_account_id(delegate['delegate_ss58'])
            delegate_map[address] = delegate

    subnet_uids = get_subnet_uids(substrate, block_hash)
    logging.info(f"Found {len(subnet_uids)} subnets to process")

    # Track all validators and their data
    all_validators = {}  # address -> validator data
    parents_with_child_access = defaultdict(set)  # parent -> set of subnets they can access via children
    child_relationships = []  # List of (child, parent, subnet, take) tuples

    # For each subnet, get registered validators and check for parents
    for net_uid in subnet_uids:
        logging.info(f"Processing subnet {net_uid}...")
        registered_validators = get_registered_validators_in_subnet(substrate, net_uid, block_hash)

        for validator_address in registered_validators:
            # Initialize validator entry if not exists
            if validator_address not in all_validators:
                all_validators[validator_address] = {
                    "address": validator_address,
                    "subnets": set(),
                    "subnet_hotkey_alpha": {},
                    "is_child": False,
                    "parent_addresses": set()
                }

            all_validators[validator_address]["subnets"].add(net_uid)

            # Get alpha for this validator
            alpha = get_total_hotkey_alpha(substrate, validator_address, net_uid, block_hash)
            all_validators[validator_address]["subnet_hotkey_alpha"][net_uid] = alpha

            # Check if this validator has parents (making it a child key)
            parent_keys = get_parent_keys_for_validator(substrate, validator_address, net_uid, block_hash)

            if parent_keys:
                all_validators[validator_address]["is_child"] = True
                for parent_address, parent_take in parent_keys:
                    all_validators[validator_address]["parent_addresses"].add(parent_address)
                    parents_with_child_access[parent_address].add(net_uid)
                    child_relationships.append((validator_address, parent_address, net_uid, parent_take))
                    logging.debug(f"Found child {validator_address} -> parent {parent_address} in subnet {net_uid}")

    # Add parent validators that only have child access
    for parent_address, child_subnets in parents_with_child_access.items():
        if parent_address not in all_validators:
            all_validators[parent_address] = {
                "address": parent_address,
                "subnets": set(),  # Not directly registered
                "subnet_hotkey_alpha": {},
                "is_child": False,
                "parent_addresses": set()
            }

    # Build final validator data list
    validators_data = []

    for address, val_data in all_validators.items():
        if address not in delegate_map:
            # Skip validators not in delegate info
            continue

        delegate = delegate_map[address]

        # Fetch validator info
        info = fetch_validator_info(substrate, address, block_hash, delegate_info)

        # Calculate registrations (direct registrations + child access)
        registrations = list(val_data["subnets"])
        if address in parents_with_child_access:
            # Add subnets accessible via children
            child_access_subnets = list(parents_with_child_access[address])
            # Merge with direct registrations
            registrations = list(set(registrations + child_access_subnets))

        # For validators that are children, we keep their own registrations
        # For parent validators, we include both direct and child-accessible subnets

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

        validators_data.append(validator_data)

        # Log summary for important validators
        if address in ["5G3wMP3g3d775hauwmAZioYFVZYnvw6eY46wkFy8hEWD5KP3", "5HdTZQ6UXD7MWcRsMeExVwqAKKo4UwomUd662HvtXiZXkxmv"]:
            logging.info(f"Validator {address}: direct_subnets={val_data['subnets']}, child_access={parents_with_child_access.get(address, set())}, total_registrations={registrations}")

    # Count statistics
    regular_validators = len([v for v in validators_data if not v['is_child_key']])
    child_key_validators = len([v for v in validators_data if v['is_child_key']])
    parents_with_children = len(parents_with_child_access)

    logging.info(f"Processed {len(validators_data)} total validators:")
    logging.info(f"- Regular validators: {regular_validators}")
    logging.info(f"- Child key validators: {child_key_validators}")
    logging.info(f"- Parents with child access: {parents_with_children}")

    return validators_data


class ValidatorsShovel(ShovelBaseClass):
    table_name = "shovel_validators"

    def __init__(self, name):
        logging.info(f"Initializing ValidatorsShovel with name: {name}")
        super().__init__(name)
        self.starting_block = FIRST_DTAO_BLOCK
        logging.info(f"ValidatorsShovel initialized with starting_block: {self.starting_block}")

    def process_block(self, n):
        # Log every block to see if we're getting called
        if n % 10 == 0:
            logging.info(f"process_block called for block {n}")

        if n % 100 != 0:
            return

        logging.info(f"=== STARTING TO PROCESS BLOCK {n} ===")
        try:
            logging.info(f"Processing block {n}")
            substrate = get_substrate_client()
            logging.info("Got substrate client")

            (block_timestamp, block_hash) = get_block_metadata(n)
            logging.info(f"Got block metadata: timestamp={block_timestamp}, hash={block_hash}")

            logging.info("About to create/check validators table...")
            create_validators_table(self.table_name)
            logging.info("Ensured validators table exists - ClickHouse connection successful!")

            # Use the new logic
            validators_data = fetch_all_validators_data_new(substrate, block_hash, block_timestamp, n)

            successful_inserts = 0
            for validator_data in validators_data:
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

                    buffer_insert(self.table_name, values)
                    successful_inserts += 1

                except Exception as e:
                    logging.error(f"Error inserting validator {validator_data['address']}: {str(e)}")
                    continue

            # Count regular vs child key validators
            regular_validators = len([v for v in validators_data if not v['is_child_key']])
            child_key_validators = len([v for v in validators_data if v['is_child_key']])

            logging.info(f"Block {n} summary:")
            logging.info(f"- Total validators: {len(validators_data)}")
            logging.info(f"- Regular validators: {regular_validators}")
            logging.info(f"- Child key validators: {child_key_validators}")
            logging.info(f"- Successful inserts: {successful_inserts}")

            if successful_inserts < len(validators_data):
                logging.warning(f"Only {successful_inserts}/{len(validators_data)} validators were successfully inserted")

            logging.info(f"Successfully processed block {n}")
            
            # Log buffer status
            if successful_inserts > 0:
                logging.info(f"Added {successful_inserts} validators to buffer. Buffer will auto-flush based on size/time thresholds.")

        except DatabaseConnectionError as e:
            logging.error(f"Database connection error in block {n}: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Error processing block {n}: {str(e)}")
            logging.error(traceback.format_exc())
            # Try to reconnect substrate on JSON decode errors
            if "JSONDecodeError" in str(e) or "websocket" in str(e).lower():
                logging.info("Attempting to reconnect substrate due to connection error...")
                from substrate import reconnect_substrate
                reconnect_substrate()
            raise ShovelProcessingError(f"Failed to process block {n}: {str(e)}")

    def cleanup(self):
        """Cleanup method called when the shovel is stopped."""
        logging.info("Cleaning up ValidatorsShovel...")
        # Any cleanup logic here
        pass


def main():
    print("Starting ValidatorsShovel...")
    print("działam..............................................")
    print("działam..............................................")
    print("działam..............................................")
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
