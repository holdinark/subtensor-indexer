# TAO.com Validator APY Shovel

This shovel tracks the APY (Annual Percentage Yield) for TAO.com's validators across different subnets.

## Overview

The shovel monitors epoch boundaries on each subnet and calculates the yield earned by TAO.com's validator at each epoch. This data is then used to calculate APY for display on the frontend.

## Configuration

### Environment Variables

- `TAOCOM_VALIDATOR_HOTKEY`: The default validator hotkey used by TAO.com (required)
- `TAOCOM_SUBNET_VALIDATOR_HOTKEYS`: Optional comma-separated list of subnet-specific validators in format `netuid1:hotkey1,netuid2:hotkey2` 
  - Example: `"1:5Dyi8aj...,3:5Xyz9abc..."`
  - Subnets not listed here will use the default `TAOCOM_VALIDATOR_HOTKEY`
- `SUBSTRATE_ARCHIVE_NODE_URL`: The substrate node URL (inherited from base config)

## How It Works

1. **Epoch Detection**: The shovel monitors `PendingEmission` storage for each subnet. When it resets from a positive value to 0, an epoch has occurred.

2. **Validator Check**: For each epoch, the shovel checks if TAO.com's validator exists on that subnet by querying the `Keys` storage.

3. **Data Collection**: If the validator exists, the shovel collects:
   - Subnet stake (`TotalHotkeyAlpha` on the subnet)
   - Root stake (`TotalHotkeyAlpha` on root network)
   - Dividends earned (`AlphaDividendsPerSubnet`)
   - TAO weight (`TaoWeight`)

4. **Yield Calculation**: Epoch yield is calculated as `dividends / stake`

5. **Filtering**: A stake filter is applied (minimum 10 TAO subnet stake and 4000 combined stake)

## Table Structure

```sql
shovel_taocom_validator_apy:
- block_number: Block where epoch occurred
- timestamp: Time of epoch
- netuid: Subnet ID
- hotkey: Validator hotkey (for tracking different validators per subnet)
- subnet_stake: Validator's stake on the subnet
- subnet_dividend: Dividends earned this epoch
- tao_weight: Weight factor between root and subnet
- root_stake: Validator's stake on root network
- epoch_yield: Calculated yield (dividend/stake)
- passed_filter: Whether validator had sufficient stake
```

## Backend Queries

The `backend_queries.py` file provides functions to:
- Calculate 30-day APY for a specific subnet
- Get APY across all subnets
- Track historical APY for charting
- Get portfolio summary across all subnets

## Materialized View

A materialized view `shovel_taocom_apy_30d` aggregates the last 30 days of data for fast APY queries.

## Usage

```python
# Start the shovel
python main.py

# Query APY for subnet 1
from backend_queries import get_subnet_apy_30d
apy_data = get_subnet_apy_30d(clickhouse_client, netuid=1)
```