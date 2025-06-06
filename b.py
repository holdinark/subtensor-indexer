from async_substrate_interface import SubstrateInterface
import asyncio

node = SubstrateInterface(url="wss://entrypoint-finney.opentensor.ai:443")

def fixed_point_128_to_float_from_hex_string(bits_hex):
    """Convert fixed point 128 bits to float from hex string"""
    bits = int(bits_hex, 16) if isinstance(bits_hex, str) else int(bits_hex)

    MASK_64 = (1 << 64) - 1

    int_part = bits >> 64
    frac_part = bits & MASK_64

    return int_part + frac_part / (2 ** 64)

def get_stake(api, netuid, coldkey, hotkey):
    """Get stake information for a given hotkey/coldkey pair"""
    # Get alpha share
    alpha_share_128 = api.query(
        module="SubtensorModule",
        storage_function="Alpha",
        params=[hotkey, coldkey, netuid],
        block_hash=None
    )
    alpha_share = fixed_point_128_to_float_from_hex_string(alpha_share_128['bits'])

    # Get hotkey alpha
    hotkey_alpha_result = api.query(
        module="SubtensorModule",
        storage_function="TotalHotkeyAlpha",
        params=[hotkey, netuid],
        block_hash=None
    )
    hotkey_alpha = hotkey_alpha_result.value

    # Get total hotkey shares
    total_hotkey_shares_128 = api.query(
        module="SubtensorModule",
        storage_function="TotalHotkeyShares",
        params=[hotkey, netuid],
        block_hash=None
    )
    total_hotkey_shares = fixed_point_128_to_float_from_hex_string(total_hotkey_shares_128['bits'])

    stake = 0
    if total_hotkey_shares != 0:
        stake = int(alpha_share * hotkey_alpha / total_hotkey_shares)

    return stake

# Example usage
def main():
    print("Starting...")

    netuid = 3
    coldkey = "5CcH9xVPJY2Nc9Wovft6Lr2qoQzefEEJfqgVhSrXdAx2ixGr"
    hotkey = "5F4tQyWrhfGVcNhoqeiNsR6KjD4wMZ2kfhLj4oHYuyHbZAc3"

    stake = get_stake(node, netuid, coldkey, hotkey)
    print(f"Stake: {stake}")
    print("Done")

if __name__ == "__main__":
    main()
