"""Publish transactions to Kafka for streaming fraud detection."""

from __future__ import annotations

import argparse
import random
import time
from pathlib import Path
from typing import Iterable, List

import pandas as pd

from .kafka_common import (
    TOPIC_TRANSACTIONS,
    build_producer,
    get_bootstrap_servers,
)
from .schema import Transaction, from_dict, to_row_dict


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish transactions to Kafka.")
    parser.add_argument(
        "--csv",
        type=Path,
        help="Optional CSV file containing transaction data to publish.",
    )
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="Generate synthetic transactions instead of reading a CSV (default).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Number of transactions to publish (default: 100).",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Optional sleep in milliseconds between messages.",
    )
    parser.add_argument(
        "--bootstrap",
        type=str,
        help="Kafka bootstrap servers (default: localhost:9092 or env KAFKA_BOOTSTRAP_SERVERS).",
    )
    return parser.parse_args()


def _load_csv_transactions(csv_path: Path, limit: int | None) -> List[dict]:
    df = pd.read_csv(csv_path, nrows=limit if limit and limit > 0 else None)
    records: List[dict] = []
    for record in df.to_dict(orient="records"):
        tx = from_dict(record)
        records.append(to_row_dict(tx))
    return records


def _generate_synthetic_transactions(limit: int) -> Iterable[dict]:
    random.seed()
    tx_types = ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]

    for step in range(limit):
        tx_type = random.choice(tx_types)
        base_amount = random.lognormvariate(5, 1)  # skewed towards higher values occasionally

        rnd = random.random()
        if rnd < 0.02:
            amount = random.uniform(1_100_000, 2_000_000)  # guarantee rule hit on amount
        elif rnd < 0.12:
            amount = base_amount * 20  # spike to create potential rule hits
        else:
            amount = base_amount

        if random.random() < 0.05:
            # Construct transactions that hit the zero-balance rule
            oldbalanceOrg = 0.0
            newbalanceDest = 0.0
        else:
            oldbalanceOrg = abs(random.normalvariate(1_000.0, 2_500.0))
            newbalanceDest = abs(random.normalvariate(1_000.0, 2_500.0))

        tx = Transaction(
            tx_id="",
            step=step,
            type=tx_type,
            amount=round(amount, 2),
            nameOrig=f"C{random.randint(100000, 999999)}",
            oldbalanceOrg=round(oldbalanceOrg, 2),
            newbalanceOrig=max(0.0, round(oldbalanceOrg - amount, 2)),
            nameDest=f"C{random.randint(100000, 999999)}",
            oldbalanceDest=round(abs(random.normalvariate(1_000.0, 2_500.0)), 2),
            newbalanceDest=round(newbalanceDest, 2),
            isFraud=0,
            isFlaggedFraud=0,
        )
        yield to_row_dict(tx)


def main() -> None:
    args = parse_args()
    bootstrap = get_bootstrap_servers(args.bootstrap)

    if args.csv:
        records = _load_csv_transactions(args.csv, args.limit)
        mode = f"CSV {args.csv}"
    else:
        records = list(_generate_synthetic_transactions(args.limit))
        mode = "synthetic"

    producer = build_producer(bootstrap)
    sleep_seconds = max(args.sleep_ms, 0) / 1000.0

    print(f"Publishing {len(records)} {mode} transactions to topic '{TOPIC_TRANSACTIONS}'...")
    for idx, record in enumerate(records, start=1):
        producer.send(TOPIC_TRANSACTIONS, record)
        if sleep_seconds:
            time.sleep(sleep_seconds)
        if idx % 50 == 0:
            print(f"  sent {idx} messages...")

    producer.flush()
    print("Finished publishing transactions.")


if __name__ == "__main__":
    main()
