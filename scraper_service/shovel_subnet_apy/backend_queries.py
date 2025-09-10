"""
Backend queries for calculating TAO.com validator APY across subnets.
These queries calculate APY based on TAO.com's validator performance on each subnet.
"""

from typing import Optional, Dict, List
from datetime import datetime, timedelta


def get_subnet_apy_30d(clickhouse_client, netuid: int) -> Dict:
    """
    Calculate 30-day APY for TAO.com's validator on a specific subnet.
    Uses the sampling approach from apy.md for fast calculation.
    """
    query = f"""
    SELECT 
        any(hotkey) as hotkey,
        count() as epoch_count,
        avg(epoch_yield) as avg_epoch_yield,
        sum(subnet_dividend) as total_dividends,
        avg(subnet_stake) as avg_stake,
        countIf(subnet_dividend > 0) as active_epochs,
        max(timestamp) as last_update,
        min(timestamp) as first_update
    FROM shovel_taocom_validator_apy
    WHERE netuid = {netuid}
        AND timestamp >= now() - INTERVAL 30 DAY
        AND passed_filter = true
    """
    
    result = clickhouse_client.execute(query)
    if not result or result[0][1] == 0:  # No epochs found (check epoch_count)
        return {
            "netuid": netuid,
            "apy": 0,
            "epoch_count": 0,
            "total_dividends": 0,
            "last_update": None
        }
    
    row = result[0]
    hotkey = row[0]
    epoch_count = row[1]
    avg_epoch_yield = row[2]
    total_dividends = row[3]
    avg_stake = row[4]
    active_epochs = row[5]
    last_update = row[6]
    first_update = row[7]
    
    # Calculate APY using compound interest formula
    # Assuming tempo of 360 blocks (most common)
    tempo = 360
    epochs_per_year = (365 * 24 * 3600 / 12) / tempo  # ~700 epochs per year
    
    # Compound the average yield over a year
    apy = ((1 + avg_epoch_yield) ** epochs_per_year - 1) * 100 if avg_epoch_yield > 0 else 0
    
    return {
        "netuid": netuid,
        "hotkey": hotkey,
        "apy": apy,
        "epoch_count": epoch_count,
        "active_epochs": active_epochs,
        "total_dividends": total_dividends,
        "avg_stake": avg_stake,
        "last_update": last_update,
        "data_days": (last_update - first_update).days if last_update and first_update else 0,
        "activity_rate": active_epochs / epoch_count if epoch_count > 0 else 0
    }


def get_all_subnets_apy(clickhouse_client) -> List[Dict]:
    """
    Get TAO.com validator APY for all subnets where it operates.
    Uses materialized view for ultra-fast queries.
    """
    query = """
    SELECT 
        netuid,
        anyMerge(validator_hotkey_state) as hotkey,
        avgMerge(avg_yield_state) as avg_epoch_yield,
        sumMerge(total_divs_state) as total_dividends,
        avgMerge(avg_stake_state) as avg_stake,
        countMerge(epoch_count_state) as epoch_count,
        countIfMerge(active_epoch_count_state) as active_epochs,
        maxMerge(last_update_state) as last_update
    FROM shovel_taocom_apy_30d
    GROUP BY netuid
    HAVING epoch_count > 10  -- Minimum epochs for reliable APY
    ORDER BY avg_epoch_yield DESC  -- Best performing subnets first
    """
    
    results = clickhouse_client.execute(query)
    subnets = []
    
    tempo = 360
    epochs_per_year = (365 * 24 * 3600 / 12) / tempo
    
    for row in results:
        netuid = row[0]
        hotkey = row[1]
        avg_epoch_yield = row[2]
        total_dividends = row[3]
        avg_stake = row[4]
        epoch_count = row[5]
        active_epochs = row[6]
        last_update = row[7]
        
        # Calculate APY
        apy = ((1 + avg_epoch_yield) ** epochs_per_year - 1) * 100 if avg_epoch_yield > 0 else 0
        
        subnets.append({
            "netuid": netuid,
            "hotkey": hotkey,
            "apy": apy,
            "total_dividends_30d": total_dividends,
            "avg_stake": avg_stake,
            "epoch_count": epoch_count,
            "active_epochs": active_epochs,
            "activity_rate": active_epochs / epoch_count if epoch_count > 0 else 0,
            "last_update": last_update
        })
    
    return subnets


def get_subnet_apy_history(clickhouse_client, netuid: int, days: int = 30) -> List[Dict]:
    """
    Get historical TAO.com validator APY on a subnet for charting.
    Returns daily APY calculations for the specified period.
    """
    query = f"""
    SELECT 
        toDate(timestamp) as date,
        avg(epoch_yield) as avg_epoch_yield,
        count() as epoch_count,
        sum(subnet_dividend) as daily_dividends,
        avg(subnet_stake) as avg_stake,
        countIf(subnet_dividend > 0) as active_epochs
    FROM shovel_taocom_validator_apy
    WHERE netuid = {netuid}
        AND timestamp >= now() - INTERVAL {days} DAY
        AND passed_filter = true
    GROUP BY date
    ORDER BY date DESC
    """
    
    results = clickhouse_client.execute(query)
    history = []
    
    tempo = 360
    epochs_per_year = (365 * 24 * 3600 / 12) / tempo
    
    for row in results:
        date = row[0]
        avg_epoch_yield = row[1]
        epoch_count = row[2]
        daily_dividends = row[3]
        avg_stake = row[4]
        active_epochs = row[5]
        
        # Calculate APY for this day
        apy = ((1 + avg_epoch_yield) ** epochs_per_year - 1) * 100 if avg_epoch_yield > 0 else 0
        
        history.append({
            "date": date.isoformat(),
            "apy": apy,
            "epoch_count": epoch_count,
            "active_epochs": active_epochs,
            "daily_dividends": daily_dividends,
            "avg_stake": avg_stake,
            "activity_rate": active_epochs / epoch_count if epoch_count > 0 else 0
        })
    
    return history


def get_taocom_portfolio_summary(clickhouse_client, period_days: int = 30) -> Dict:
    """
    Get a summary of TAO.com validator's overall portfolio performance across all subnets.
    """
    query = f"""
    SELECT 
        count(DISTINCT netuid) as subnet_count,
        sum(subnet_dividend) as total_dividends,
        avg(subnet_stake) as avg_stake_per_subnet,
        sum(subnet_stake) as total_stake,
        avg(epoch_yield) as avg_epoch_yield,
        countIf(subnet_dividend > 0) as active_epochs,
        count() as total_epochs
    FROM shovel_taocom_validator_apy
    WHERE timestamp >= now() - INTERVAL {period_days} DAY
        AND passed_filter = true
    """
    
    result = clickhouse_client.execute(query)
    if not result or result[0][0] == 0:
        return {
            "subnet_count": 0,
            "total_dividends": 0,
            "total_stake": 0,
            "portfolio_apy": 0,
            "activity_rate": 0
        }
    
    row = result[0]
    subnet_count = row[0]
    total_dividends = row[1]
    avg_stake_per_subnet = row[2]
    total_stake = row[3]
    avg_epoch_yield = row[4]
    active_epochs = row[5]
    total_epochs = row[6]
    
    # Calculate portfolio APY
    tempo = 360
    epochs_per_year = (365 * 24 * 3600 / 12) / tempo
    portfolio_apy = ((1 + avg_epoch_yield) ** epochs_per_year - 1) * 100 if avg_epoch_yield > 0 else 0
    
    return {
        "subnet_count": subnet_count,
        "total_dividends": total_dividends,
        "total_stake": total_stake,
        "avg_stake_per_subnet": avg_stake_per_subnet,
        "portfolio_apy": portfolio_apy,
        "active_epochs": active_epochs,
        "total_epochs": total_epochs,
        "activity_rate": active_epochs / total_epochs if total_epochs > 0 else 0,
        "period_days": period_days
    }


def get_best_performing_subnets(clickhouse_client, limit: int = 10) -> List[Dict]:
    """
    Get the best performing subnets for TAO.com validator based on APY.
    """
    query = f"""
    SELECT 
        netuid,
        any(hotkey) as hotkey,
        avg(epoch_yield) as avg_epoch_yield,
        sum(subnet_dividend) as total_dividends,
        avg(subnet_stake) as avg_stake,
        count() as epoch_count,
        countIf(subnet_dividend > 0) as active_epochs
    FROM shovel_taocom_validator_apy
    WHERE timestamp >= now() - INTERVAL 30 DAY
        AND passed_filter = true
    GROUP BY netuid
    HAVING epoch_count > 10
    ORDER BY avg_epoch_yield DESC
    LIMIT {limit}
    """
    
    results = clickhouse_client.execute(query)
    subnets = []
    
    tempo = 360
    epochs_per_year = (365 * 24 * 3600 / 12) / tempo
    
    for row in results:
        netuid = row[0]
        hotkey = row[1]
        avg_epoch_yield = row[2]
        total_dividends = row[3]
        avg_stake = row[4]
        epoch_count = row[5]
        active_epochs = row[6]
        
        # Calculate APY
        apy = ((1 + avg_epoch_yield) ** epochs_per_year - 1) * 100 if avg_epoch_yield > 0 else 0
        
        subnets.append({
            "netuid": netuid,
            "hotkey": hotkey,
            "apy": apy,
            "total_dividends_30d": total_dividends,
            "avg_stake": avg_stake,
            "epoch_count": epoch_count,
            "active_epochs": active_epochs,
            "activity_rate": active_epochs / epoch_count if epoch_count > 0 else 0,
            "roi": total_dividends / avg_stake if avg_stake > 0 else 0
        })
    
    return subnets