"""Minimal smoke test to verify Ignite connectivity and inserts."""

from __future__ import annotations

from uuid import uuid4

from src.ignite_client import fetch_transactions, insert_transaction


def main() -> None:
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

    inserted_id = insert_transaction(sample)
    print(f"Inserted transaction with tx_id={inserted_id}")

    rows = fetch_transactions(limit=1)
    print("First row from database:")
    print(rows[0] if rows else "No rows returned")


if __name__ == "__main__":
    main()
