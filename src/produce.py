"""Kafka producer for streaming transactions into the fraud demo."""

from __future__ import annotations

import argparse
import random
import time
from pathlib import Path
from typing import Dict, Iterable, Iterator, Optional

import pandas as pd

from . import schema
from .kafka_common import TRANSACTIONS_TOPIC, build_producer, get_bootstrap_server


def _random_choice(values: list[str]) -> str:
    return random.choice(values)


def synthetic_transactions(limit: int) -> Iterator[Dict[str, object]]:
    """Generate synthetic transactions with occasional rule hits."""

    type_options = ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"]
    for i in range(limit):
        tx_type = _random_choice(type_options)

        amount = float(random.uniform(10.0, 200_000.0))
        oldbalanceOrg = float(random.uniform(0.0, 300_000.0))
        newbalanceOrig = max(0.0, oldbalanceOrg - amount)
        oldbalanceDest = float(random.uniform(0.0, 300_000.0))
        newbalanceDest = oldbalanceDest + amount

        # Occasionally emit extreme or rule-triggering cases
        if random.random() < 0.1:
            amount = float(random.uniform(1_100_000.0, 5_000_000.0))
            oldbalanceOrg = 0.0
            newbalanceOrig = 0.0
            oldbalanceDest = 0.0
            newbalanceDest = 0.0
            tx_type = _random_choice(["TRANSFER", "CASH_OUT", "PAYMENT"])

        tx_dict = schema.to_row_dict(
            schema.from_dict(
                {
                    "tx_id": None,
                    "step": i,
                    "type": tx_type,
                    "amount": amount,
                    "nameOrig": f"C{i:010d}",
                    "oldbalanceOrg": oldbalanceOrg,
                    "newbalanceOrig": newbalanceOrig,
                    "nameDest": f"M{i:010d}",
                    "oldbalanceDest": oldbalanceDest,
                    "newbalanceDest": newbalanceDest,
                }
            )
        )
        yield tx_dict


def csv_transactions(path: Path, limit: Optional[int]) -> Iterator[Dict[str, object]]:
    df = pd.read_csv(path)
    if limit:
        df = df.head(limit)
    for _, row in df.iterrows():
        yield schema.to_row_dict(schema.from_dict(row.to_dict()))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish transactions to Kafka for the fraud streaming demo.",
    )
    parser.add_argument("--csv", type=Path, help="Path to a CSV file of transactions")
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="Generate synthetic transactions when no CSV is provided.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Maximum number of transactions to publish (default: 100).",
    )
    parser.add_argument(
        "--sleep-ms",
        type=int,
        default=0,
        help="Optional sleep between messages in milliseconds.",
    )
    parser.add_argument(
        "--bootstrap",
        type=str,
        default=None,
        help="Kafka bootstrap server (overrides env KAFKA_BOOTSTRAP_SERVERS).",
    )
    return parser.parse_args()


def validate_source(args: argparse.Namespace) -> None:
    if args.csv and args.synthetic:
        raise SystemExit("Specify either --csv or --synthetic, not both.")
    if not args.csv and not args.synthetic:
        print("No CSV provided; defaulting to synthetic transaction generation.")


def iter_transactions(args: argparse.Namespace) -> Iterable[Dict[str, object]]:
    if args.csv:
        return csv_transactions(args.csv, args.limit)
    return synthetic_transactions(args.limit)


def main() -> None:
    args = parse_args()
    validate_source(args)

    bootstrap_server = get_bootstrap_server(args.bootstrap)
    producer = build_producer(bootstrap_server)
    print(f"Kafka producer connected to {bootstrap_server}; publishing to {TRANSACTIONS_TOPIC}.")

    sleep_seconds = args.sleep_ms / 1000.0 if args.sleep_ms else 0.0
    count = 0

    try:
        for tx in iter_transactions(args):
            producer.send(TRANSACTIONS_TOPIC, tx)
            count += 1
            if count % 10 == 0:
                producer.flush()
            if sleep_seconds:
                time.sleep(sleep_seconds)
        producer.flush()
    finally:
        producer.close()

    print(f"Published {count} transactions to topic '{TRANSACTIONS_TOPIC}'.")


if __name__ == "__main__":
    main()
