from async_substrate_interface import SubstrateInterface
import json
from datetime import datetime
from substrateinterface.utils.ss58 import ss58_encode

def main():
    substrate = SubstrateInterface(url="wss://archive.chain.opentensor.ai:443/")

    print("Fetching all coldkeys from System.Account...")

    # Fetch all accounts (which includes coldkeys)
    raw_accounts = substrate.query_map(
        module='System',
        storage_function='Account',
        page_size=1000
    )

    if not raw_accounts:
        print("No account data returned from substrate")
        return

            # Extract addresses from the specific format: [((bytes,),), account_data]
    all_coldkeys = []

    for account in raw_accounts:
        try:
            # Structure: [((byte1, byte2, ..., byte32),), account_data]
            address_key = account[0]  # This is ((bytes,),)
            address_bytes_tuple = address_key[0]  # This is (bytes,)
            address_bytes = bytes(address_bytes_tuple)  # Convert tuple to bytes
            address_id = ss58_encode(address_bytes, ss58_format=42)  # Encode as SS58

            all_coldkeys.append(address_id)

        except Exception as e:
            print(f"Error processing account: {e}")
            continue

    print(f"Found {len(all_coldkeys)} total accounts/coldkeys")

    results = []

    for i, coldkey in enumerate(all_coldkeys):
        print(f"Processing coldkey {i+1}/{len(all_coldkeys)}: {coldkey}")

        try:
            # Use StakeInfoRuntimeApi to get stake info for this coldkey
            stake_info = substrate.runtime_call(
                api="StakeInfoRuntimeApi",
                method="get_stake_info_for_coldkey",
                params=[coldkey]
            )

            result_entry = {
                'coldkey': coldkey,
                'stake_info': stake_info.value if hasattr(stake_info, 'value') else stake_info,
                'processed_at': datetime.now().isoformat()
            }

            results.append(result_entry)
            print(f"  ✓ Successfully got stake info for {coldkey}")

        except Exception as e:
            print(f"  ✗ Error getting stake info for {coldkey}: {e}")

            error_entry = {
                'coldkey': coldkey,
                'error': str(e),
                'processed_at': datetime.now().isoformat()
            }
            results.append(error_entry)

    # Save results to JSON file
    filename = f"stake_info_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    output_data = {
        'timestamp': datetime.now().isoformat(),
        'total_coldkeys_found': len(all_coldkeys),
        'coldkeys_processed': len(all_coldkeys),
        'successful_results': len([r for r in results if 'stake_info' in r]),
        'errors': len([r for r in results if 'error' in r]),
        'results': results
    }

    with open(filename, 'w') as f:
        json.dump(output_data, f, indent=2)

    print(f"\nResults saved to {filename}")
    print(f"Total coldkeys found: {len(all_coldkeys)}")
    print(f"Coldkeys processed: {len(all_coldkeys)}")
    print(f"Successful: {output_data['successful_results']}")
    print(f"Errors: {output_data['errors']}")

if __name__ == "__main__":
    main()
