"""OpenTelemetry setup that degrades to no-ops when extras are missing.

Local-first defaults:
* Disabled unless ``OTEL_ENABLED=true``.
* No collector required. ``OTEL_EXPORTER=console`` prints spans to stderr.
* ``OTEL_EXPORTER=otlp`` plus ``OTEL_EXPORTER_OTLP_ENDPOINT`` ships to a local
  collector (Jaeger / Grafana Tempo / OTel Collector). Never required for tests.

Install extras: ``pip install -e '.[otel]'``.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from realtime_market_stream.config.settings import Settings, get_settings

logger = logging.getLogger(__name__)

_PROVIDER: Any = None
_CONFIGURED = False


def otel_available() -> bool:
    """True when the optional OpenTelemetry SDK is importable."""
    try:
        import opentelemetry  # noqa: F401
    except ImportError:
        return False
    return True


def is_configured() -> bool:
    """True after a successful :func:`configure_tracing` call."""
    return _CONFIGURED


def reset_tracing_state() -> None:
    """Test helper: drop the cached provider so configure can run again."""
    global _PROVIDER, _CONFIGURED
    _PROVIDER = None
    _CONFIGURED = False


def configure_tracing(settings: Settings | None = None) -> bool:
    """Install a TracerProvider when OTEL is enabled and extras are present.

    Returns True when a real provider was installed. Safe to call more than
    once; subsequent calls are no-ops until :func:`reset_tracing_state`.
    """
    global _PROVIDER, _CONFIGURED
    if _CONFIGURED:
        return _PROVIDER is not None

    resolved = settings or get_settings()
    otel = resolved.otel
    if not otel.enabled:
        logger.debug("OpenTelemetry disabled (OTEL_ENABLED=false)")
        _CONFIGURED = True
        return False
    if not otel_available():
        logger.warning(
            "OTEL_ENABLED=true but OpenTelemetry extras are missing. "
            "Install with: pip install -e '.[otel]'"
        )
        _CONFIGURED = True
        return False

    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

    resource = Resource.create(
        {
            "service.name": otel.service_name,
            "service.namespace": "realtime-market-stream",
            "deployment.environment": str(resolved.app_env),
        }
    )
    ratio = max(0.0, min(1.0, otel.sample_ratio))
    provider = TracerProvider(
        resource=resource,
        sampler=ParentBased(TraceIdRatioBased(ratio)),
    )

    exporter_name = otel.exporter
    if exporter_name == "otlp":
        endpoint = otel.endpoint.strip()
        if not endpoint:
            logger.warning(
                "OTEL_EXPORTER=otlp but OTEL_EXPORTER_OTLP_ENDPOINT is empty; using console"
            )
            provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
        else:
            try:
                from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                    OTLPSpanExporter,
                )

                provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
            except ImportError:
                logger.warning("OTLP exporter missing; falling back to console")
                provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    elif exporter_name == "none":
        logger.debug("OpenTelemetry exporter=none; spans stay in-process")
    else:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))

    trace.set_tracer_provider(provider)
    _PROVIDER = provider
    _CONFIGURED = True
    logger.info(
        "OpenTelemetry tracing enabled service=%s exporter=%s",
        otel.service_name,
        exporter_name,
    )
    return True


def shutdown_tracing() -> None:
    """Flush and shutdown the SDK provider if one was installed."""
    global _PROVIDER, _CONFIGURED
    if _PROVIDER is not None:
        try:
            _PROVIDER.shutdown()
        except Exception:  # noqa: BLE001
            logger.debug("tracer provider shutdown failed", exc_info=True)
    _PROVIDER = None
    _CONFIGURED = False


def get_tracer(name: str = "realtime_market_stream") -> Any:
    """Return a real or no-op tracer."""
    if not otel_available():
        return _NoopTracer()
    from opentelemetry import trace

    return trace.get_tracer(name)


@contextmanager
def start_span(name: str, **attributes: Any) -> Iterator[Any]:
    """Create a span; attributes are set when the SDK is present."""
    tracer = get_tracer()
    with tracer.start_as_current_span(name) as span:
        setter = getattr(span, "set_attribute", None)
        if callable(setter):
            for key, value in attributes.items():
                if value is None:
                    continue
                try:
                    setter(key, value)
                except Exception:  # noqa: BLE001
                    logger.debug("span attribute %s rejected", key, exc_info=True)
        yield span


def instrument_fastapi(app: Any, settings: Settings | None = None) -> bool:
    """Attach FastAPI instrumentation when extras + OTEL_ENABLED are on."""
    resolved = settings or get_settings()
    if not resolved.otel.enabled or not otel_available():
        return False
    configure_tracing(resolved)
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
    except ImportError:
        logger.debug("opentelemetry-instrumentation-fastapi not installed")
        return False
    try:
        FastAPIInstrumentor.instrument_app(app)
    except Exception:  # noqa: BLE001
        logger.debug("FastAPI instrumentation failed", exp_info=True)
        return False
    return True


class _NoopSpan:
    def set_attribute(self, *_args: Any, **_kwargs: Any) -> None:
        return None

    def __enter__(self) -> _NoopSpan:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


class _NoopTracer:
    def start_as_current_span(self, _name: str, **_kwargs: Any) -> _NoopSpan:
        return _NoopSpan()
