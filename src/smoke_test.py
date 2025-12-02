"""Minimal smoke test to verify Ignite connectivity and inserts."""

from __future__ import annotations

import argparse
from uuid import uuid4

from src import config
from src.ignite_client import fetch_transactions, insert_transaction


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Insert and fetch a single transaction for validation")
    parser.add_argument("--host", default=config.IGNITE_HOST, help="Ignite host (default: 127.0.0.1)")
    parser.add_argument(
        "--port",
        type=int,
        default=config.IGNITE_PORT,
        help=f"Ignite thin client port (default: {config.IGNITE_PORT})",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    tx_id = str(uuid4())
    sample = {
        "tx_id": tx_id,
        "step": 1,
        "type": "PAYMENT",
        "amount": 42.5,
        "nameOrig": "C123456789",
        "oldbalanceOrg": 100.0,
        "newbalanceOrig": 57.5,
        "nameDest": "M123456789",
        "oldbalanceDest": 0.0,
        "newbalanceDest": 42.5,
        "isFraud": 0,
        "isFlaggedFraud": 0,
    }

    inserted_id = insert_transaction(sample, host=args.host, port=args.port)
    print(f"Inserted transaction with tx_id={inserted_id} into {args.host}:{args.port}")

    rows = fetch_transactions(limit=1, host=args.host, port=args.port)
    print("First row from database:")
    print(rows[0] if rows else "No rows returned")


if __name__ == "__main__":
    main()
