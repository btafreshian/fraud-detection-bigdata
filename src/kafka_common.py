"""Shared Kafka utilities for the streaming demo."""

from __future__ import annotations

import json
import os
from typing import Any, Dict, Iterable

from kafka import KafkaConsumer, KafkaProducer

ENV_BOOTSTRAP: str = "KAFKA_BOOTSTRAP_SERVERS"
DEFAULT_BOOTSTRAP: str = "localhost:9092"
TRANSACTIONS_TOPIC: str = "fraud_transactions"
ALERTS_TOPIC: str = "fraud_alerts"


def get_bootstrap_server(cli_value: str | None = None) -> str:
    """Resolve the Kafka bootstrap server from CLI arg, env var, or default."""

    if cli_value:
        return cli_value
    env_value = os.getenv(ENV_BOOTSTRAP)
    return env_value if env_value else DEFAULT_BOOTSTRAP


def json_serializer(value: Dict[str, Any]) -> bytes:
    return json.dumps(value).encode("utf-8")


def json_deserializer(value: bytes) -> Dict[str, Any]:
    return json.loads(value.decode("utf-8"))


def build_producer(bootstrap_server: str) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_server,
        value_serializer=json_serializer,
    )


def build_consumer(
    bootstrap_server: str,
    group_id: str,
    topics: Iterable[str],
    *,
    auto_offset_reset: str = "latest",
) -> KafkaConsumer:
    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_server,
        group_id=group_id,
        value_deserializer=json_deserializer,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
    )
    return consumer
