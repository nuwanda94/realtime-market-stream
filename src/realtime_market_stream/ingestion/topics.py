"""Canonical Redpanda/Kafka topics and idempotent create helpers.

Topics are named from :class:`KafkaSettings` so local and Snowflake profiles
share the same catalog. Creation is idempotent: existing topics are skipped.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING

from realtime_market_stream.config.settings import KafkaSettings, get_settings

if TYPE_CHECKING:
    from kafka.admin import KafkaAdminClient

logger = logging.getLogger(__name__)

DEFAULT_PARTITIONS = 3
DEFAULT_REPLICATION = 1
DEFAULT_RETENTION_MS = 7 * 24 * 60 * 60 * 1000  # 7 days


@dataclass(frozen=True, slots=True)
class TopicSpec:
    """Declarative topic definition."""

    name: str
    partitions: int = DEFAULT_PARTITIONS
    replication_factor: int = DEFAULT_REPLICATION
    retention_ms: int = DEFAULT_RETENTION_MS
    cleanup_policy: str = "delete"

    def config(self) -> dict[str, str]:
        return {
            "retention.ms": str(self.retention_ms),
            "cleanup.policy": self.cleanup_policy,
        }


def default_topic_specs(kafka: KafkaSettings | None = None) -> list[TopicSpec]:
    """Return the four pipeline topics: raw, enriched, alerts, dlq."""
    settings = kafka or get_settings().kafka
    return [
        TopicSpec(name=settings.topic_raw_ticks),
        TopicSpec(name=settings.topic_enriched_ticks),
        TopicSpec(name=settings.topic_alerts),
        TopicSpec(name=settings.topic_dlq),
    ]


def _admin_client(bootstrap_servers: str) -> KafkaAdminClient:
    from kafka.admin import KafkaAdminClient

    return KafkaAdminClient(
        bootstrap_servers=bootstrap_servers,
        client_id="rms-topic-admin",
        request_timeout_ms=15_000,
        api_version_auto_timeout_ms=15_000,
    )


def ensure_topics(
    specs: list[TopicSpec] | None = None,
    *,
    bootstrap_servers: str | None = None,
    dry_run: bool = False,
) -> dict[str, str]:
    """Create missing topics. Returns ``{topic_name: created|exists|dry-run}``.

    Raises ``ImportError`` if ``kafka-python`` is not installed, or connection
    errors from the broker when ``dry_run`` is false.
    """
    kafka = get_settings().kafka
    specs = specs or default_topic_specs(kafka)
    servers = bootstrap_servers or kafka.bootstrap_servers

    if dry_run:
        return {spec.name: "dry-run" for spec in specs}

    from kafka.admin import NewTopic
    from kafka.errors import TopicAlreadyExistsError

    admin = _admin_client(servers)
    try:
        existing = set(admin.list_topics())
        results: dict[str, str] = {}
        to_create: list[NewTopic] = []
        for spec in specs:
            if spec.name in existing:
                results[spec.name] = "exists"
                logger.info("topic already exists: %s", spec.name)
            else:
                to_create.append(
                    NewTopic(
                        name=spec.name,
                        num_partitions=spec.partitions,
                        replication_factor=spec.replication_factor,
                        topic_configs=spec.config(),
                    )
                )
        if to_create:
            try:
                admin.create_topics(to_create, validate_only=False)
            except TopicAlreadyExistsError:
                # Race with another creator (e.g. compose init + this script).
                pass
            existing_after = set(admin.list_topics())
            for spec in to_create:
                results[spec.name] = "created" if spec.name in existing_after else "exists"
                logger.info("topic %s: %s", spec.name, results[spec.name])
        return results
    finally:
        admin.close()
