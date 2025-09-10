-- Materialized view for fast 30-day TAO.com validator APY calculations
-- This view aggregates TAO.com's validator performance across subnets for the last 30 days

CREATE MATERIALIZED VIEW IF NOT EXISTS shovel_taocom_apy_30d
ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(now())
ORDER BY netuid
POPULATE AS
SELECT 
    netuid,
    anyState(hotkey) as validator_hotkey_state,  -- The hotkey used for this subnet
    -- Aggregation states for fast queries
    avgState(epoch_yield) as avg_yield_state,
    sumState(subnet_dividend) as total_divs_state,
    avgState(subnet_stake) as avg_stake_state,
    countState() as epoch_count_state,
    countIfState(subnet_dividend > 0) as active_epoch_count_state,
    maxState(timestamp) as last_update_state,
    minState(timestamp) as first_update_state
FROM shovel_taocom_validator_apy
WHERE timestamp >= now() - INTERVAL 30 DAY
    AND passed_filter = true  -- Only count epochs where validator had sufficient stake
GROUP BY netuid;

-- Create a refresh schedule (run daily at 2 AM)
-- This needs to be set up as a cron job or ClickHouse scheduled query
-- Example query to refresh:
-- INSERT INTO shovel_subnet_apy_30d
-- SELECT ... (same as above)
-- WHERE timestamp >= now() - INTERVAL 30 DAY;