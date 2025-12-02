"""Shared Kafka helpers for JSON serialization and topic names."""

from __future__ import annotations

import json
import os
from typing import Any, Iterable

from kafka import KafkaConsumer, KafkaProducer

TOPIC_TRANSACTIONS = "fraud_transactions"
TOPIC_ALERTS = "fraud_alerts"


def get_bootstrap_servers(cli_value: str | None = None) -> str:
    """Resolve Kafka bootstrap servers from CLI override or environment."""

    return cli_value or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


def json_serializer(value: Any) -> bytes:
    return json.dumps(value).encode("utf-8")


def json_deserializer(data: bytes) -> Any:
    return json.loads(data.decode("utf-8"))


def build_producer(bootstrap_servers: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=json_serializer,
    )


def build_consumer(
    topics: Iterable[str],
    bootstrap_servers: str,
    group_id: str,
) -> KafkaConsumer:
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=json_deserializer,
    )
    return consumer
