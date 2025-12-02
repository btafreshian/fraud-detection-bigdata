"""Kafka consumer that scores transactions and emits fraud alerts."""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path
from typing import Dict, Tuple
from uuid import uuid4

import pandas as pd
from kafka.errors import NoBrokersAvailable

from .ignite_client import insert_alert, insert_transaction
from .kafka_common import (
    TOPIC_ALERTS,
    TOPIC_TRANSACTIONS,
    build_consumer,
    build_producer,
    get_bootstrap_servers,
)
from .preprocess import FEATURE_COLS
from .schema import from_dict, to_row_dict
from .score import load_model

DEFAULT_MODEL_PATH = Path("artifacts/model.joblib")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Consume transactions and emit fraud alerts.")
    parser.add_argument(
        "--bootstrap",
        type=str,
        help="Kafka bootstrap servers (default: localhost:9092 or env KAFKA_BOOTSTRAP_SERVERS).",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.7,
        help="Fraud probability threshold for raising an alert (default: 0.7).",
    )
    parser.add_argument(
        "--group-id",
        type=str,
        default="fraud-consumer",
        help="Kafka consumer group id (default: fraud-consumer).",
    )
    parser.add_argument(
        "--model-path",
        type=Path,
        default=DEFAULT_MODEL_PATH,
        help="Path to trained sklearn pipeline (default: artifacts/model.joblib).",
    )
    return parser.parse_args()


def _evaluate_rules(tx_row: Dict[str, object]) -> Tuple[bool, str | None]:
    tx_type = str(tx_row.get("type", "")).upper()
    amount = float(tx_row.get("amount", 0) or 0)
    oldbalanceOrg = float(tx_row.get("oldbalanceOrg", 0) or 0)
    newbalanceDest = float(tx_row.get("newbalanceDest", 0) or 0)

    if amount > 1_000_000:
        return True, "amount > 1,000,000"

    if tx_type == "TRANSFER" and oldbalanceOrg == 0 and newbalanceDest == 0:
        return True, "TRANSFER with zero origin and destination balances"

    if tx_type == "CASH_OUT" and oldbalanceOrg == 0 and newbalanceDest == 0:
        return True, "CASH_OUT with zero origin and destination balances"

    return False, None


def _score_transaction(model, tx_row: Dict[str, object]) -> float:
    df = pd.DataFrame([tx_row])
    proba = model.predict_proba(df[FEATURE_COLS])[:, 1][0]
    return float(proba)


def _format_alert_console(tx_row: Dict[str, object], prob: float, rule_reason: str | None) -> str:
    lines = [
        f"[ALERT] tx_id={tx_row.get('tx_id')} prob={prob:.4f} rule={bool(rule_reason)}",
        "  details:",
        f"    type={tx_row.get('type')} amount={tx_row.get('amount')}",
        f"    nameOrig={tx_row.get('nameOrig')} nameDest={tx_row.get('nameDest')}",
        f"    oldbalanceOrg={tx_row.get('oldbalanceOrg')} newbalanceDest={tx_row.get('newbalanceDest')}",
    ]
    if rule_reason:
        lines.append(f"    rule_reason={rule_reason}")
    return "\n".join(lines)


def main() -> None:
    args = parse_args()
    bootstrap = get_bootstrap_servers(args.bootstrap)

    try:
        model = load_model(args.model_path)
    except FileNotFoundError:
        print(
            f"Model not found at {args.model_path}. Train one first with `python -m src.train`.",
        )
        sys.exit(1)

    try:
        consumer = build_consumer(
            topics=[TOPIC_TRANSACTIONS],
            bootstrap_servers=bootstrap,
            group_id=args.group_id,
        )
    except NoBrokersAvailable:
        print(
            f"Unable to connect to Kafka at {bootstrap}. Ensure Docker stack is running and Kafka is accessible.",
        )
        sys.exit(1)

    alert_producer = build_producer(bootstrap)

    print(
        f"Consuming transactions from '{TOPIC_TRANSACTIONS}' with threshold {args.threshold}..."
    )
    try:
        for message in consumer:
            raw = message.value
            try:
                tx = from_dict(raw)
            except Exception as exc:  # pragma: no cover - defensive for malformed messages
                print(f"Skipping malformed message: {exc}")
                continue

            tx_row = to_row_dict(tx)
            insert_transaction(tx_row)

            prob = _score_transaction(model, tx_row)
            rule_hit, rule_reason = _evaluate_rules(tx_row)

            alert_needed = prob >= args.threshold or rule_hit
            if not alert_needed:
                continue

            reasons = []
            if prob >= args.threshold:
                reasons.append(f"ml_prob={prob:.3f}>=threshold={args.threshold}")
            if rule_reason:
                reasons.append(rule_reason)

            alert = {
                "alert_id": str(uuid4()),
                "tx_id": tx_row.get("tx_id"),
                "ts": int(time.time() * 1000),
                "ml_prob": prob,
                "rule_hit": int(rule_hit),
                "reason": "; ".join(reasons),
                "amount": tx_row.get("amount"),
                "type": tx_row.get("type"),
                "nameOrig": tx_row.get("nameOrig"),
                "nameDest": tx_row.get("nameDest"),
            }

            insert_alert(alert)
            alert_producer.send(TOPIC_ALERTS, alert)
            alert_producer.flush()

            print(_format_alert_console(tx_row, prob, rule_reason))

    except KeyboardInterrupt:  # pragma: no cover - manual interruption
        print("Stopping consumer.")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
