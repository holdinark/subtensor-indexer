# OpenTelemetry Integration for Shovels

This document explains how OpenTelemetry (OTel) **tracing**, **logging**, and **metrics** have been integrated into the shovel system.

## Overview

The telemetry system provides automatic **traces**, **logs**, and **metrics** for all shovels that inherit from `ShovelBaseClass`. Each shovel can emit telemetry under its own service name without requiring individual setup.

## How It Works

### 1. Automatic Initialization

When a shovel is created, the `ShovelBaseClass` automatically:

- Initializes OpenTelemetry tracing, logging, and metrics
- Uses environment variables for configuration
- Falls back to shovel name as service name if `OTEL_SERVICE_NAME` is not set

### 2. Environment Variables

The system uses these environment variables (already configured in your deployment):

```bash
OTEL_EXPORTER_OTLP_ENDPOINT="https://clickstack.178.156.178.234.sslip.io"
OTEL_EXPORTER_OTLP_HEADERS="Authorization=Bearer <your-token>"
OTEL_SERVICE_NAME="shovel-stake-map-test-copy"  # Optional, defaults to shovel name
OTEL_RESOURCE_ATTRIBUTES="key1=value1,key2=value2"  # Optional
```

### 3. Automatic Telemetry

The base class automatically creates:

#### 🔍 **Traces**

- **`shovel.start`** - The entire shovel lifecycle
- **`shovel.initialization`** - Substrate and buffer setup
- **`shovel.processing_cycle`** - Each iteration of block processing
- **`shovel.process_block_batch`** - Processing multiple blocks
- **`shovel.process_block`** - Individual block processing
- **`shovel.checkpoint_update`** - Database checkpoint updates
- **`shovel.get_checkpoint`** - Checkpoint retrieval
- **Error spans** - Database errors, processing errors, fatal errors
- **Retry spans** - Connection retry attempts

#### 📊 **Metrics**

- **`shovel_blocks_processed_total`** - Counter of successfully processed blocks
- **`shovel_blocks_failed_total`** - Counter of failed blocks
- **`shovel_database_errors_total`** - Counter of database errors
- **`shovel_records_inserted_total`** - Counter of records inserted
- **`shovel_block_processing_duration_seconds`** - Histogram of block processing times
- **`shovel_batch_processing_duration_seconds`** - Histogram of batch processing times
- **`shovel_current_block_number`** - Gauge of current block being processed
- **`shovel_blocks_behind`** - Gauge of how many blocks behind chain head

#### 📝 **Logs**

- All Python `logging` calls automatically correlated with traces
- Structured logging with trace/span context

## Usage Patterns

### 1. Basic Usage (No Changes Required)

Existing shovels automatically get full telemetry:

```python
class MyExistingShovel(ShovelBaseClass):
    def process_block(self, n):
        # Automatically gets:
        # ✅ Tracing: "shovel.process_block" span
        # ✅ Metrics: blocks_processed, processing_duration, etc.
        # ✅ Logging: All logging.info/error calls correlated
        pass

def main():
    MyExistingShovel(name="my-shovel").start()
```

### 2. Custom Metrics

#### Basic Custom Metrics

```python
from shared.telemetry import get_metrics

class MyShovel(ShovelBaseClass):
    def __init__(self, name):
        super().__init__(name)
        self.metrics = get_metrics()

        # Create custom metrics
        self.my_counter = self.metrics.create_custom_counter(
            "shovel_custom_operations_total",
            "Custom operations performed by shovel"
        )

        self.processing_histogram = self.metrics.create_custom_histogram(
            "shovel_custom_processing_duration_seconds",
            "Time for custom processing operations"
        )

    def process_block(self, n):
        # Use built-in metrics automatically tracked

        # Add custom metrics
        self.my_counter.add(1, {"operation": "special_processing"})

        # Time custom operations
        start_time = time.time()
        self.do_custom_processing(n)
        duration = time.time() - start_time

        self.processing_histogram.record(duration, {
            "operation_type": "analysis",
            "block_number": n
        })
```

#### Advanced Metrics Usage

```python
from shared.telemetry import get_metrics

class AdvancedShovel(ShovelBaseClass):
    def __init__(self, name):
        super().__init__(name)
        self.metrics = get_metrics()

        # Custom counters
        self.stake_events = self.metrics.create_custom_counter(
            "shovel_stake_events_processed_total",
            "Total stake events processed"
        )

        self.subnet_updates = self.metrics.create_custom_counter(
            "shovel_subnet_updates_total",
            "Total subnet updates processed"
        )

        # Custom gauges
        self.active_hotkeys = self.metrics.create_custom_gauge(
            "shovel_active_hotkeys_current",
            "Number of currently active hotkeys"
        )

        # Custom histograms
        self.query_duration = self.metrics.create_custom_histogram(
            "shovel_substrate_query_duration_seconds",
            "Duration of substrate queries"
        )

    def process_block(self, n):
        # Track stake events
        stake_events = self.get_stake_events(n)
        self.stake_events.add(len(stake_events), {
            "block_number": n,
            "event_type": "stake_change"
        })

        # Track active hotkeys
        active_count = self.count_active_hotkeys(n)
        self.active_hotkeys.set(active_count, {"block_number": n})

        # Track query performance
        query_start = time.time()
        data = self.query_substrate(n)
        query_duration = time.time() - query_start

        self.query_duration.record(query_duration, {
            "query_type": "block_data",
            "success": str(data is not None).lower()
        })
```

### 3. Decorator Patterns with Metrics

```python
from shared.telemetry import trace, get_metrics
import time

def timed_operation(metric_name):
    """Decorator that traces and times operations."""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            metrics = get_metrics()
            timer = metrics.create_custom_histogram(
                f"shovel_{metric_name}_duration_seconds",
                f"Duration of {metric_name} operations"
            )

            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                duration = time.time() - start_time
                timer.record(duration, {"status": "success"})
                return result
            except Exception as e:
                duration = time.time() - start_time
                timer.record(duration, {"status": "error", "error_type": type(e).__name__})
                raise
        return wrapper
    return decorator

class MetricsShovel(ShovelBaseClass):
    @trace("shovel.fetch_substrate_data")
    @timed_operation("substrate_fetch")
    def fetch_substrate_data(self, block_number):
        # Automatically gets tracing + timing metrics
        return get_substrate_client().get_block(block_number)

    @timed_operation("data_analysis")
    def analyze_data(self, data):
        # Custom timing for analysis operations
        return self.complex_analysis(data)
```

### 4. Business Metrics Examples

```python
class StakeShovel(ShovelBaseClass):
    def __init__(self, name):
        super().__init__(name)
        self.metrics = get_metrics()

        # Business-specific metrics
        self.total_stake = self.metrics.create_custom_gauge(
            "shovel_total_stake_amount",
            "Total stake amount across all hotkeys",
            unit="tao"
        )

        self.stake_changes = self.metrics.create_custom_counter(
            "shovel_stake_changes_total",
            "Total number of stake changes"
        )

        self.large_stakes = self.metrics.create_custom_counter(
            "shovel_large_stakes_total",
            "Stakes above threshold"
        )

    def process_block(self, n):
        stakes_data = self.get_stakes_for_block(n)

        # Track total stake
        total = sum(stake.amount for stake in stakes_data)
        self.total_stake.set(total, {"block_number": n})

        # Track stake changes
        for stake in stakes_data:
            self.stake_changes.add(1, {
                "change_type": stake.change_type,
                "subnet_id": stake.subnet_id
            })

            # Track large stakes
            if stake.amount > 1000:  # Example threshold
                self.large_stakes.add(1, {
                    "subnet_id": stake.subnet_id,
                    "stake_range": self.get_stake_range(stake.amount)
                })
```

## Built-in Metrics Details

### 📈 **Counters** (always increasing)

```python
# Automatically tracked
shovel_blocks_processed_total{shovel_name="my-shovel", status="success"}
shovel_blocks_failed_total{shovel_name="my-shovel", error_type="database"}
shovel_database_errors_total{shovel_name="my-shovel", error_type="connection"}
shovel_records_inserted_total{shovel_name="my-shovel", tables_count="3"}
```

### ⏱️ **Histograms** (distribution of values)

```python
# Processing time distributions
shovel_block_processing_duration_seconds{shovel_name="my-shovel", status="success"}
shovel_batch_processing_duration_seconds{shovel_name="my-shovel", block_count="50"}
```

### 📊 **Gauges** (current state)

```python
# Current values
shovel_current_block_number{shovel_name="my-shovel"} = 1234567
shovel_blocks_behind{shovel_name="my-shovel"} = 5
```

## Deployment

### Automatic Enablement

The telemetry system is automatically enabled when:

1. Environment variables are properly set
2. OpenTelemetry dependencies are installed (already in requirements.txt)
3. No additional deployment changes needed

### Metrics Export

- **Frequency**: Metrics exported every 30 seconds
- **Endpoint**: `https://clickstack.178.156.178.234.sslip.io/v1/metrics`
- **Format**: OTLP (OpenTelemetry Protocol)

## Viewing Telemetry

Access your observability at: `https://clickstack.178.156.178.234.sslip.io`

### 🔍 **Traces**

- See detailed execution flow
- Debug performance bottlenecks
- Track error propagation

### 📊 **Metrics**

- Monitor shovel performance
- Set up alerts on failures
- Track processing rates
- Capacity planning

### 📝 **Logs**

- Correlated with traces
- Structured searchable logs
- Error context

## Dashboard Examples

### Key Metrics to Monitor

```promql
# Processing rate
rate(shovel_blocks_processed_total[5m])

# Error rate
rate(shovel_blocks_failed_total[5m]) / rate(shovel_blocks_processed_total[5m])

# Processing latency
histogram_quantile(0.95, shovel_block_processing_duration_seconds)

# Blocks behind
shovel_blocks_behind

# Database throughput
rate(shovel_records_inserted_total[5m])
```

### Alerting Rules

```promql
# Alert if shovel stops processing
increase(shovel_blocks_processed_total[10m]) == 0

# Alert if error rate > 5%
(rate(shovel_blocks_failed_total[5m]) / rate(shovel_blocks_processed_total[5m])) > 0.05

# Alert if too far behind
shovel_blocks_behind > 100

# Alert if processing too slow
histogram_quantile(0.95, shovel_block_processing_duration_seconds) > 30
```

## Best Practices

### 1. Metric Naming

```python
# Good: Descriptive, consistent
"shovel_stake_events_processed_total"
"shovel_substrate_query_duration_seconds"
"shovel_active_validators_current"

# Avoid: Generic, unclear
"events"
"duration"
"count"
```

### 2. Labels Strategy

```python
# Good: Relevant business context
{"shovel_name": "stake-map", "subnet_id": "1", "operation": "stake_add"}

# Avoid: High cardinality
{"hotkey": "5F3sa2TJAWMqDhXG6jhV4N8ko9SxwGy8TpaNS1repo5EYjQX"}  # Too many unique values
```

### 3. Performance

```python
# Efficient: Use attributes for filtering
self.metrics.increment_blocks_processed(1, {"status": "success"})

# Less efficient: Create many separate metrics
# Don't create separate metrics for each status
```

This comprehensive telemetry system gives you production-grade observability with minimal code changes! 🚀
