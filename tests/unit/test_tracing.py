"""OpenTelemetry helpers work without the optional SDK installed."""

from __future__ import annotations

from realtime_market_stream.config.settings import AppEnv, Settings
from realtime_market_stream.observability.tracing import (
    configure_tracing,
    instrument_fastapi,
    is_configured,
    reset_tracing_state,
    shutdown_tracing,
    start_span,
)


def test_configure_tracing_disabled_is_noop() -> None:
    reset_tracing_state()
    settings = Settings(app_env=AppEnv.LOCAL)
    assert settings.otel.enabled is False
    assert configure_tracing(settings) is False
    assert is_configured() is True
    shutdown_tracing()
    assert is_configured() is False


def test_start_span_is_safe_without_sdk() -> None:
    reset_tracing_state()
    with start_span("unit.test", symbol="AAPL") as span:
        assert span is not None
        span.set_attribute("ignored", 1)
    shutdown_tracing()


def test_instrument_fastapi_skips_when_disabled() -> None:
    reset_tracing_state()
    settings = Settings(app_env=AppEnv.LOCAL)
    assert instrument_fastapi(object(), settings) is False
    shutdown_tracing()


def test_otel_env_flags(monkeypatch: object) -> None:
    from pytest import MonkeyPatch

    assert isinstance(monkeypatch, MonkeyPatch)
    monkeypatch.setenv("OTEL_ENABLED", "true")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "rms-test")
    monkeypatch.setenv("OTEL_EXPORTER", "None")
    monkeypatch.setenv("OTEL_SAMPLE_RATIO", "0.25")
    settings = Settings(_env_file=None)
    assert settings.otel.enabled is True
    assert settings.otel.service_name == "rms-test"
    assert settings.otel.exporter == "none"
    assert settings.otel.sample_ratio == 0.25
