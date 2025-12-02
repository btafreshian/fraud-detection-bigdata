"""Thin client helpers for Apache Ignite."""

from __future__ import annotations

from contextlib import contextmanager, suppress
from typing import Any, Dict, Iterable, List

from pyignite import Client

from src import config
from src.schema import to_row_dict


TRANSACTION_COLUMNS = [
    "tx_id",
    "step",
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
    "isFraud",
    "isFlaggedFraud",
]


@contextmanager
def get_client():
    """Context manager that yields a connected Ignite client."""
    client = Client()
    try:
        client.connect(config.IGNITE_HOST, config.IGNITE_PORT)
        yield client
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f"Unable to connect to Apache Ignite at {config.IGNITE_HOST}:{config.IGNITE_PORT}"
        ) from exc
    finally:
        with suppress(Exception):
            client.close()


def _insert_statement() -> str:
    placeholders = ", ".join(["?"] * len(TRANSACTION_COLUMNS))
    columns = ", ".join(TRANSACTION_COLUMNS)
    return f"INSERT INTO {config.TRANSACTIONS_TABLE} ({columns}) VALUES ({placeholders})"


def insert_transaction(tx: Dict[str, Any]) -> str:
    """Insert a single transaction row and return its tx_id."""
    row = to_row_dict(tx)
    statement = _insert_statement()

    if not row.get("tx_id"):
        raise ValueError("tx_id is required for insert_transaction")

    with get_client() as client:
        client.sql(statement, query_args=[row[col] for col in TRANSACTION_COLUMNS])
    return row["tx_id"]


def insert_transactions(rows: Iterable[Dict[str, Any]]) -> List[str]:
    """Insert multiple transactions in one client session."""
    row_dicts = [to_row_dict(row) for row in rows]
    for row in row_dicts:
        if not row.get("tx_id"):
            raise ValueError("Each transaction must include tx_id")

    if not row_dicts:
        return []

    statement = _insert_statement()
    inserted_ids: List[str] = []
    with get_client() as client:
        for row in row_dicts:
            client.sql(statement, query_args=[row[col] for col in TRANSACTION_COLUMNS])
            inserted_ids.append(row["tx_id"])
    return inserted_ids


def fetch_transactions(limit: int = config.DEFAULT_FETCH_LIMIT) -> List[Dict[str, Any]]:
    """Fetch transactions up to the provided limit."""
    statement = f"SELECT {', '.join(TRANSACTION_COLUMNS)} FROM {config.TRANSACTIONS_TABLE} LIMIT ?"
    with get_client() as client:
        cursor = client.sql(statement, query_args=[limit])
        rows = cursor.fetchall()
    return [dict(zip(TRANSACTION_COLUMNS, row)) for row in rows]
