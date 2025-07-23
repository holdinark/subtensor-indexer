import os
import logging
import functools
import inspect
from opentelemetry import trace as ot_trace, metrics as ot_metrics
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.instrumentation.logging import LoggingInstrumentor
from opentelemetry.trace import Status, StatusCode

logger = logging.getLogger(__name__)

class TelemetryManager:
    """Manages OpenTelemetry initialization and configuration for shovels."""

    def __init__(self):
        self.tracer = None
        self.meter = None
        self.initialized = False

    def initialize(self, service_name=None, fallback_name="unknown-shovel"):
        """
        Initialize OpenTelemetry tracing and metrics.

        Args:
            service_name: Override service name (falls back to env var, then fallback_name)
            fallback_name: Default service name if none provided
        """
        if self.initialized:
            logger.debug("Telemetry already initialized")
            return self.tracer, self.meter

        try:
            # Determine service name from priority: parameter > env var > fallback
            final_service_name = (
                service_name or
                os.getenv('OTEL_SERVICE_NAME') or
                fallback_name
            )

            # Get configuration from environment variables
            otlp_endpoint = os.getenv('OTEL_EXPORTER_OTLP_ENDPOINT')
            otlp_headers_raw = os.getenv('OTEL_EXPORTER_OTLP_HEADERS', '')
            resource_attributes = os.getenv('OTEL_RESOURCE_ATTRIBUTES', '')

            if not otlp_endpoint:
                logger.warning("OTEL_EXPORTER_OTLP_ENDPOINT not set, skipping telemetry initialization")
                return None, None

            # Parse headers - expecting format like "Authorization=Bearer token"
            headers = {}
            if otlp_headers_raw:
                for header in otlp_headers_raw.split(','):
                    if '=' in header:
                        key, value = header.split('=', 1)
                        headers[key.strip()] = value.strip()

            # Create resource with service name and additional attributes
            resource_dict = {
                "service.name": final_service_name,
                "service.version": "1.0.0",
            }

            # Parse additional resource attributes from env var
            if resource_attributes:
                for attr in resource_attributes.split(','):
                    if '=' in attr:
                        key, value = attr.split('=', 1)
                        resource_dict[key.strip()] = value.strip()

            resource = Resource.create(resource_dict)

            # Set up tracer provider
            ot_trace.set_tracer_provider(TracerProvider(resource=resource))

            # Create OTLP trace exporter
            otlp_trace_exporter = OTLPSpanExporter(
                endpoint=f"{otlp_endpoint.rstrip('/')}/v1/traces",
                headers=headers
            )

            # Add span processor
            span_processor = BatchSpanProcessor(otlp_trace_exporter)
            ot_trace.get_tracer_provider().add_span_processor(span_processor)

            # Set up metrics provider
            otlp_metric_exporter = OTLPMetricExporter(
                endpoint=f"{otlp_endpoint.rstrip('/')}/v1/metrics",
                headers=headers
            )

            metric_reader = PeriodicExportingMetricReader(
                exporter=otlp_metric_exporter,
                export_interval_millis=30000,  # Export metrics every 30 seconds
            )

            ot_metrics.set_meter_provider(
                MeterProvider(resource=resource, metric_readers=[metric_reader])
            )

            # Get tracer and meter for this service
            self.tracer = ot_trace.get_tracer(final_service_name)
            self.meter = ot_metrics.get_meter(final_service_name)

            # Enable logging instrumentation
            LoggingInstrumentor().instrument(set_logging_format=True)

            self.initialized = True
            logger.info(f"OpenTelemetry initialized for service: {final_service_name}")
            logger.info(f"Tracing endpoint: {otlp_endpoint}/v1/traces")
            logger.info(f"Metrics endpoint: {otlp_endpoint}/v1/metrics")

            return self.tracer, self.meter

        except Exception as e:
            logger.error(f"Failed to initialize OpenTelemetry: {str(e)}")
            return None, None

    def get_tracer(self):
        """Get the current tracer instance."""
        return self.tracer if self.initialized else None

    def get_meter(self):
        """Get the current meter instance."""
        return self.meter if self.initialized else None

# Global telemetry manager instance
telemetry_manager = TelemetryManager()

def init_telemetry(service_name=None, fallback_name="unknown-shovel"):
    """Convenience function to initialize telemetry."""
    return telemetry_manager.initialize(service_name, fallback_name)

def get_tracer():
    """Convenience function to get tracer."""
    return telemetry_manager.get_tracer()

def get_meter():
    """Convenience function to get meter."""
    return telemetry_manager.get_meter()

# Helper to get current active span
def current_span():
    """Return the currently active span from OpenTelemetry context."""
    return ot_trace.get_current_span()


# =============================================================================
# METRICS - Custom Metric Helpers
# =============================================================================

class ShovelMetrics:
    """Helper class for common shovel metrics."""

    def __init__(self):
        self._meter = None
        self._counters = {}
        self._gauges = {}
        self._histograms = {}
        self._initialized = False

    def _ensure_initialized(self):
        """Ensure metrics are initialized."""
        if not self._initialized:
            self._meter = get_meter()
            if self._meter:
                self._init_common_metrics()
                self._initialized = True

    def _init_common_metrics(self):
        """Initialize common shovel metrics."""
        # Counters
        self._counters['blocks_processed'] = self._meter.create_counter(
            name="shovel_blocks_processed_total",
            description="Total number of blocks processed by shovel",
            unit="blocks"
        )

        self._counters['blocks_failed'] = self._meter.create_counter(
            name="shovel_blocks_failed_total",
            description="Total number of blocks that failed processing",
            unit="blocks"
        )

        self._counters['database_errors'] = self._meter.create_counter(
            name="shovel_database_errors_total",
            description="Total number of database errors",
            unit="errors"
        )

        self._counters['records_inserted'] = self._meter.create_counter(
            name="shovel_records_inserted_total",
            description="Total number of records inserted into database",
            unit="records"
        )

        # Histograms (for latency/duration measurements)
        self._histograms['block_processing_duration'] = self._meter.create_histogram(
            name="shovel_block_processing_duration_seconds",
            description="Time taken to process a single block",
            unit="s"
        )

        self._histograms['batch_processing_duration'] = self._meter.create_histogram(
            name="shovel_batch_processing_duration_seconds",
            description="Time taken to process a batch of blocks",
            unit="s"
        )

        # Gauges (for current state)
        self._gauges['current_block'] = self._meter.create_gauge(
            name="shovel_current_block_number",
            description="Current block number being processed",
            unit="block_number"
        )

        self._gauges['blocks_behind'] = self._meter.create_gauge(
            name="shovel_blocks_behind",
            description="Number of blocks behind the chain head",
            unit="blocks"
        )

    def increment_blocks_processed(self, count=1, **attributes):
        """Increment blocks processed counter."""
        self._ensure_initialized()
        if 'blocks_processed' in self._counters:
            self._counters['blocks_processed'].add(count, attributes)

    def increment_blocks_failed(self, count=1, **attributes):
        """Increment blocks failed counter."""
        self._ensure_initialized()
        if 'blocks_failed' in self._counters:
            self._counters['blocks_failed'].add(count, attributes)

    def increment_database_errors(self, count=1, **attributes):
        """Increment database errors counter."""
        self._ensure_initialized()
        if 'database_errors' in self._counters:
            self._counters['database_errors'].add(count, attributes)

    def increment_records_inserted(self, count, **attributes):
        """Increment records inserted counter."""
        self._ensure_initialized()
        if 'records_inserted' in self._counters:
            self._counters['records_inserted'].add(count, attributes)

    def record_block_processing_time(self, duration_seconds, **attributes):
        """Record block processing duration."""
        self._ensure_initialized()
        if 'block_processing_duration' in self._histograms:
            self._histograms['block_processing_duration'].record(duration_seconds, attributes)

    def record_batch_processing_time(self, duration_seconds, **attributes):
        """Record batch processing duration."""
        self._ensure_initialized()
        if 'batch_processing_duration' in self._histograms:
            self._histograms['batch_processing_duration'].record(duration_seconds, attributes)

    def set_current_block(self, block_number, **attributes):
        """Set current block number gauge."""
        self._ensure_initialized()
        if 'current_block' in self._gauges:
            self._gauges['current_block'].set(block_number, attributes)

    def set_blocks_behind(self, blocks_behind, **attributes):
        """Set blocks behind gauge."""
        self._ensure_initialized()
        if 'blocks_behind' in self._gauges:
            self._gauges['blocks_behind'].set(blocks_behind, attributes)

    def create_custom_counter(self, name, description, unit="1"):
        """Create a custom counter metric."""
        self._ensure_initialized()
        if self._meter and name not in self._counters:
            self._counters[name] = self._meter.create_counter(
                name=name,
                description=description,
                unit=unit
            )
        return self._counters.get(name)

    def create_custom_histogram(self, name, description, unit="1"):
        """Create a custom histogram metric."""
        self._ensure_initialized()
        if self._meter and name not in self._histograms:
            self._histograms[name] = self._meter.create_histogram(
                name=name,
                description=description,
                unit=unit
            )
        return self._histograms.get(name)

    def create_custom_gauge(self, name, description, unit="1"):
        """Create a custom gauge metric."""
        self._ensure_initialized()
        if self._meter and name not in self._gauges:
            self._gauges[name] = self._meter.create_gauge(
                name=name,
                description=description,
                unit=unit
            )
        return self._gauges.get(name)

# Global metrics instance
shovel_metrics = ShovelMetrics()

def get_metrics():
    """Get the global shovel metrics instance."""
    return shovel_metrics


# =============================================================================
# DECORATORS - Common OpenTelemetry Patterns
# =============================================================================

def trace(span_name=None, attributes=None, record_exception=True):
    """
    Decorator to automatically trace function calls.

    Args:
        span_name: Custom span name (defaults to module.class.function_name)
        attributes: Dict of additional attributes to add to span
        record_exception: Whether to record exceptions in the span

    Example:
        @trace()
        def my_function(x, y):
            return x + y

        @trace("custom.operation", {"operation_type": "analysis"})
        def analyze_data(data):
            return processed_data
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracer()
            if not tracer:
                return func(*args, **kwargs)

            # Generate span name
            if span_name:
                full_span_name = span_name
            else:
                module_name = func.__module__.split('.')[-1] if func.__module__ else "unknown"
                class_name = ""
                if args and hasattr(args[0], '__class__'):
                    class_name = f"{args[0].__class__.__name__}."
                full_span_name = f"{module_name}.{class_name}{func.__name__}"

            # Prepare attributes
            span_attributes = {
                "function.name": func.__name__,
                "function.module": func.__module__ or "unknown",
            }

            # Add function signature info
            try:
                sig = inspect.signature(func)
                span_attributes["function.args_count"] = len(args)
                span_attributes["function.kwargs_count"] = len(kwargs)

                # Add parameter names (but not values for security)
                param_names = list(sig.parameters.keys())
                if param_names:
                    span_attributes["function.parameters"] = ",".join(param_names)
            except Exception:
                pass  # Ignore signature inspection errors

            # Add custom attributes
            if attributes:
                span_attributes.update(attributes)

            with tracer.start_as_current_span(full_span_name, attributes=span_attributes) as span:
                try:
                    result = func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    if record_exception:
                        span.record_exception(e)
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise

        return wrapper
    return decorator


def trace_method(span_name=None, attributes=None, include_args=False):
    """
    Decorator specifically for class methods with enhanced context.

    Args:
        span_name: Custom span name (defaults to class.method_name)
        attributes: Dict of additional attributes
        include_args: Whether to include argument values (be careful with sensitive data)

    Example:
        class MyClass:
            @trace_method("shovel.process_block")
            def process_block(self, block_number):
                return self._do_processing(block_number)
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            tracer = get_tracer()
            if not tracer:
                return func(self, *args, **kwargs)

            # Generate span name
            if span_name:
                full_span_name = span_name
            else:
                class_name = self.__class__.__name__
                full_span_name = f"{class_name}.{func.__name__}"

            # Prepare attributes
            span_attributes = {
                "method.name": func.__name__,
                "method.class": self.__class__.__name__,
                "method.module": func.__module__ or "unknown",
            }

            # Add instance info if available
            if hasattr(self, 'name'):
                span_attributes["instance.name"] = self.name

            # Include arguments if requested
            if include_args and args:
                for i, arg in enumerate(args):
                    # Only include simple types to avoid sensitive data
                    if isinstance(arg, (str, int, float, bool)):
                        span_attributes[f"arg.{i}"] = str(arg)

            if include_args and kwargs:
                for key, value in kwargs.items():
                    if isinstance(value, (str, int, float, bool)):
                        span_attributes[f"kwarg.{key}"] = str(value)

            # Add custom attributes
            if attributes:
                span_attributes.update(attributes)

            with tracer.start_as_current_span(full_span_name, attributes=span_attributes) as span:
                try:
                    result = func(self, *args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise

        return wrapper
    return decorator


def trace_async(span_name=None, attributes=None):
    """
    Decorator for async functions.

    Example:
        @trace_async("async.operation")
        async def fetch_data(url):
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    return await response.json()
    """
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            tracer = get_tracer()
            if not tracer:
                return await func(*args, **kwargs)

            # Generate span name
            if span_name:
                full_span_name = span_name
            else:
                module_name = func.__module__.split('.')[-1] if func.__module__ else "unknown"
                full_span_name = f"{module_name}.{func.__name__}"

            span_attributes = {
                "function.name": func.__name__,
                "function.module": func.__module__ or "unknown",
                "function.type": "async",
            }

            if attributes:
                span_attributes.update(attributes)

            with tracer.start_as_current_span(full_span_name, attributes=span_attributes) as span:
                try:
                    result = await func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    return result
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    raise

        return wrapper
    return decorator


def trace_block_processing(include_block_details=True):
    """
    Specialized decorator for block processing functions.

    Example:
        @trace_block_processing()
        def process_block(self, block_number):
            # Processing logic
            pass
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            tracer = get_tracer()
            if not tracer:
                return func(*args, **kwargs)

            # Extract block number from arguments
            block_number = None
            if args and len(args) > 1:  # Assuming first arg is self, second is block_number
                if isinstance(args[1], int):
                    block_number = args[1]
            elif 'block_number' in kwargs:
                block_number = kwargs['block_number']
            elif 'n' in kwargs:
                block_number = kwargs['n']

            span_attributes = {
                "operation.type": "block_processing",
                "function.name": func.__name__,
            }

            if block_number is not None:
                span_attributes["block.number"] = block_number

            if include_block_details and block_number:
                span_attributes["block.processing_stage"] = "main"

            span_name = f"shovel.block.{func.__name__}"

            with tracer.start_as_current_span(span_name, attributes=span_attributes) as span:
                try:
                    result = func(*args, **kwargs)
                    span.set_status(Status(StatusCode.OK))
                    if block_number:
                        span.set_attribute("block.processed_successfully", True)
                    return result
                except Exception as e:
                    span.record_exception(e)
                    span.set_status(Status(StatusCode.ERROR, str(e)))
                    if block_number:
                        span.set_attribute("block.processed_successfully", False)
                        span.set_attribute("block.error_type", type(e).__name__)
                    raise

        return wrapper
    return decorator
