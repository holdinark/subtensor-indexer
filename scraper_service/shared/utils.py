import logging
from substrateinterface.utils.ss58 import ss58_encode

def convert_address_to_ss58(address_key, context="address"):
    """
    Convert an address key (string or binary) to SS58 format.

    Args:
        address_key: The address in various formats (string, tuple, bytes)
        context: Context for error logging (e.g., "coldkey", "hotkey")

    Returns:
        str: SS58 formatted address

    Raises:
        Exception: If conversion fails
    """
    try:
        if isinstance(address_key, str):
            # Already a string (SS58 address)
            return address_key
        elif isinstance(address_key, tuple) and len(address_key) > 0:
            # Binary format: ((bytes,),) or similar tuple structure
            if isinstance(address_key[0], tuple) and len(address_key[0]) > 0:
                # Structure: ((byte1, byte2, ..., byte32),)
                address_bytes = bytes(address_key[0])
                return ss58_encode(address_bytes, ss58_format=42)
            else:
                # Simple tuple of bytes
                address_bytes = bytes(address_key)
                return ss58_encode(address_bytes, ss58_format=42)
        else:
            # Try to convert directly to bytes
            address_bytes = bytes(address_key)
            return ss58_encode(address_bytes, ss58_format=42)
    except Exception as e:
        logging.warning(f"Error converting {context} to SS58: {e}")
        logging.warning(f"{context} type: {type(address_key)}, value: {address_key}")
        raise
