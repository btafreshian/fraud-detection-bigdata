"""Lightweight helper for interacting with Apache Ignite via thin client."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional

from pyignite import Client

from .config import (
    DEFAULT_FETCH_LIMIT,
    IGNITE_HOST,
    IGNITE_PORT,
    TRANSACTION_COLUMNS,
    TRANSACTIONS_TABLE,
)

INSERT_SQL = (
    f"INSERT INTO {TRANSACTIONS_TABLE} ("
    + ", ".join(TRANSACTION_COLUMNS)
    + ") VALUES ("
    + ", ".join(["?"] * len(TRANSACTION_COLUMNS))
    + ")"
)

SELECT_SQL = f"SELECT {', '.join(TRANSACTION_COLUMNS)} FROM {TRANSACTIONS_TABLE} LIMIT ?"
SELECT_ALL_SQL = f"SELECT {', '.join(TRANSACTION_COLUMNS)} FROM {TRANSACTIONS_TABLE}"
SELECT_RECENT_SQL = (
    f"SELECT {', '.join(TRANSACTION_COLUMNS)} FROM {TRANSACTIONS_TABLE} "
    "ORDER BY step DESC LIMIT ?"
)
SELECT_BY_ID_SQL = (
    f"SELECT {', '.join(TRANSACTION_COLUMNS)} FROM {TRANSACTIONS_TABLE} WHERE tx_id = ?"
)


@contextmanager
def get_client():
    client = Client()
    try:
        client.connect(IGNITE_HOST, IGNITE_PORT)
        yield client
    except Exception as exc:  # pragma: no cover - thin client connection errors
        raise ConnectionError(
            f"Unable to connect to Apache Ignite at {IGNITE_HOST}:{IGNITE_PORT}. "
            "Ensure the Docker Compose stack is running."
        ) from exc
    finally:
        client.close()


def insert_transaction(tx_row: Dict[str, Any]) -> str:
    tx_id = tx_row.get("tx_id")
    with get_client() as client:
        client.sql(INSERT_SQL, query_args=[tx_row[col] for col in TRANSACTION_COLUMNS])
    return str(tx_id)


def insert_transactions(rows: Iterable[Dict[str, Any]]) -> List[str]:
    inserted: List[str] = []
    with get_client() as client:
        for row in rows:
            client.sql(INSERT_SQL, query_args=[row[col] for col in TRANSACTION_COLUMNS])
            inserted.append(str(row.get("tx_id")))
    return inserted


def fetch_transactions(limit: int = DEFAULT_FETCH_LIMIT) -> List[Dict[str, Any]]:
    with get_client() as client:
        result = client.sql(SELECT_SQL, query_args=[limit])
        return [dict(zip(TRANSACTION_COLUMNS, row)) for row in result]


def fetch_transaction_by_id(tx_id: str) -> Optional[Dict[str, Any]]:
    with get_client() as client:
        result = client.sql(SELECT_BY_ID_SQL, query_args=[tx_id])
        rows = [dict(zip(TRANSACTION_COLUMNS, row)) for row in result]
        return rows[0] if rows else None


def fetch_training_frame(limit: int | None = None) -> List[Dict[str, Any]]:
    """Retrieve rows for model training.

    Args:
        limit: Optional maximum number of rows to fetch. If None, fetch all rows.

    Returns:
        List of row dictionaries covering all transaction columns.
    """

    with get_client() as client:
        if limit is None:
            result = client.sql(SELECT_ALL_SQL)
        else:
            result = client.sql(SELECT_SQL, query_args=[limit])
        return [dict(zip(TRANSACTION_COLUMNS, row)) for row in result]


def fetch_recent_transactions(limit: int = DEFAULT_FETCH_LIMIT) -> List[Dict[str, Any]]:
    """Fetch the most recent transactions ordered by step descending."""

    with get_client() as client:
        result = client.sql(SELECT_RECENT_SQL, query_args=[limit])
        return [dict(zip(TRANSACTION_COLUMNS, row)) for row in result]

