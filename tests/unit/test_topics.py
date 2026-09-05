"""Unit tests for topic catalog (no broker required)."""

from realtime_market_stream.config.settings import KafkaSettings
from realtime_market_stream.ingestion.topics import (
    TopicSpec,
    default_topic_specs,
    ensure_topics,
)


def test_default_specs_use_settings_names() -> None:
    kafka = KafkaSettings(
        topic_raw_ticks="raw-ticks",
        topic_enriched_ticks="enriched-ticks",
        topic_alerts="alerts",
        topic_dlq="dlq",
    )
    names = [spec.name for spec in default_topic_specs(kafka)]
    assert names == ["raw-ticks", "enriched-ticks", "alerts", "dlq"]


def test_default_specs_respect_custom_names() -> None:
    kafka = KafkaSettings(
        topic_raw_ticks="custom-raw",
        topic_enriched_ticks="custom-enriched",
        topic_alerts="custom-alerts",
        topic_dlq="custom-dlq",
    )
    names = {spec.name for spec in default_topic_specs(kafka)}
    assert names == {"custom-raw", "custom-enriched", "custom-alerts", "custom-dlq"}


def test_topic_spec_config() -> None:
    spec = TopicSpec(name="raw-ticks", retention_ms=1000, cleanup_policy="delete")
    cfg = spec.config()
    assert cfg["retention.ms"] == "1000"
    assert cfg["cleanup.policy"] == "delete"


def test_ensure_topics_dry_run() -> None:
    specs = [TopicSpec(name="raw-ticks"), TopicSpec(name="dlq")]
    results = ensure_topics(specs, dry_run=True)
    assert results == {"raw-ticks": "dry-run", "dlq": "dry-run"}
