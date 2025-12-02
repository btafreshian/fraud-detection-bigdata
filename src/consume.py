"""Kafka consumer that scores transactions and emits alerts."""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Dict, Tuple
from uuid import uuid4

import joblib
import pandas as pd

from kafka.errors import KafkaError, NoBrokersAvailable

from .cli_utils import error_exit, format_exception, info
from . import schema
from .ignite_client import insert_alert, insert_transaction
from .kafka_common import (
    ALERTS_TOPIC,
    TRANSACTIONS_TOPIC,
    build_consumer,
    build_producer,
    get_bootstrap_server,
)
from .preprocess import FEATURE_COLS

MODEL_PATH = Path("artifacts/model.joblib")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume transactions from Kafka, score them, and emit fraud alerts.",
    )
    parser.add_argument(
        "--bootstrap",
        type=str,
        default=None,
        help="Kafka bootstrap server (overrides env KAFKA_BOOTSTRAP_SERVERS).",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.7,
        help="Probability threshold for flagging fraud (default: 0.7).",
    )
    parser.add_argument(
        "--group-id",
        type=str,
        default="fraud-consumer",
        help="Kafka consumer group id (default: fraud-consumer).",
    )
    return parser.parse_args()


def load_pipeline(model_path: Path) -> object:
    if not model_path.exists():
        error_exit(
            f"Model artifact not found at {model_path}. "
            "Run `python -m src.train` to train and save the pipeline before consuming."
        )
    return joblib.load(model_path)


def evaluate_rules(tx: schema.Transaction) -> Tuple[bool, list[str]]:
    reasons: list[str] = []
    if tx.amount > 1_000_000:
        reasons.append("amount > 1,000,000")
    if tx.type == "TRANSFER" and tx.oldbalanceOrg == 0 and tx.newbalanceDest == 0:
        reasons.append("TRANSFER with zero balances")
    if tx.type == "CASH_OUT" and tx.oldbalanceOrg == 0 and tx.newbalanceDest == 0:
        reasons.append("CASH_OUT with zero balances")
    return (len(reasons) > 0, reasons)


def score_transaction(pipeline, tx_row: Dict[str, object]) -> float:
    features = pd.DataFrame([{col: tx_row.get(col) for col in FEATURE_COLS}])
    proba = pipeline.predict_proba(features)[0][1]
    return float(proba)


def build_alert(
    tx: schema.Transaction,
    prob: float,
    threshold: float,
    rule_hit: bool,
    rule_reasons: list[str],
) -> Dict[str, object]:
    reasons = []
    if prob >= threshold:
        reasons.append(f"ml_prob {prob:.3f} >= threshold {threshold}")
    reasons.extend(rule_reasons)

    return {
        "alert_id": str(uuid4()),
        "tx_id": tx.tx_id,
        "ts": int(time.time() * 1000),
        "ml_prob": prob,
        "rule_hit": int(rule_hit),
        "reason": "; ".join(reasons) if reasons else "ml trigger",
        "amount": tx.amount,
        "type": tx.type,
        "nameOrig": tx.nameOrig,
        "nameDest": tx.nameDest,
    }


def print_alert(alert: Dict[str, object]) -> None:
    print(
        f"[ALERT] tx_id={alert['tx_id']} prob={alert['ml_prob']:.3f} "
        f"rule_hit={alert['rule_hit']}"
    )
    pretty = {
        "alert_id": alert["alert_id"],
        "tx_id": alert["tx_id"],
        "ml_prob": f"{alert['ml_prob']:.3f}",
        "rule_hit": alert["rule_hit"],
        "reason": alert["reason"],
        "amount": alert["amount"],
        "type": alert["type"],
        "nameOrig": alert["nameOrig"],
        "nameDest": alert["nameDest"],
    }
    print(json.dumps(pretty, indent=2))


def main() -> None:
    args = parse_args()
    bootstrap_server = get_bootstrap_server(args.bootstrap)
    pipeline = load_pipeline(MODEL_PATH)

    try:
        consumer = build_consumer(
            bootstrap_server,
            args.group_id,
            [TRANSACTIONS_TOPIC],
            auto_offset_reset="earliest",
        )
        producer = build_producer(bootstrap_server)
    except (KafkaError, NoBrokersAvailable) as exc:  # pragma: no cover - network
        error_exit(
            f"Unable to connect to Kafka at {bootstrap_server}. Start the stack with `make up`."
        )

    info(
        "Consuming transactions from topic "
        f"'{TRANSACTIONS_TOPIC}' with group '{args.group_id}' using {bootstrap_server}."
    )
    info(f"Alerts will be published to topic '{ALERTS_TOPIC}'. Press Ctrl+C to exit.")

    try:
        for message in consumer:
            tx_data = message.value
            tx = schema.from_dict(tx_data)
            tx_row = schema.to_row_dict(tx)

            insert_transaction(tx_row)
            prob = score_transaction(pipeline, tx_row)
            rule_hit, rule_reasons = evaluate_rules(tx)

            if prob >= args.threshold or rule_hit:
                alert = build_alert(tx, prob, args.threshold, rule_hit, rule_reasons)
                insert_alert(alert)
                producer.send(ALERTS_TOPIC, alert)
                producer.flush()
                print_alert(alert)
    except KeyboardInterrupt:
        info("Stopping consumer...")
    except ConnectionError as exc:  # pragma: no cover - ignite down
        error_exit("Unable to reach Apache Ignite while consuming. Ensure it is running.")
    except (KafkaError, NoBrokersAvailable) as exc:  # pragma: no cover - kafka down
        error_exit(f"Kafka consumer error: {format_exception(exc)}")
    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    main()
