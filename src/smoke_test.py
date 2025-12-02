"""Simple connectivity and CRUD smoke test for Apache Ignite."""

from __future__ import annotations

from uuid import uuid4

from .cli_utils import error_exit, info
from .ignite_client import fetch_transaction_by_id, insert_transaction
from .schema import Transaction, to_row_dict


def main() -> None:
    tx = Transaction(tx_id=str(uuid4()), amount=123.45, type="PAYMENT", step=1)
    info(f"Inserting transaction {tx.tx_id}...")
    try:
        insert_transaction(to_row_dict(tx))
    except ConnectionError:
        error_exit(
            "Unable to connect to Apache Ignite. Start the docker compose stack with `make up`."
        )

    info("Fetching it back...")
    fetched = fetch_transaction_by_id(tx.tx_id)
    if fetched is None:
        error_exit("Smoke test failed: inserted transaction not found.")

    info("Retrieved row:")
    print(fetched)


if __name__ == "__main__":
    main()

