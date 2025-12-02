"""Lightweight helper for interacting with Apache Ignite via thin client."""

from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Dict, Iterable, List, Optional

from pyignite import Client

from .config import (
    DEFAULT_FETCH_LIMIT,
    FRAUD_ALERTS_TABLE,
    FRAUD_ALERT_COLUMNS,
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

SELECT_BASE_SQL = f"SELECT {', '.join(TRANSACTION_COLUMNS)} FROM {TRANSACTIONS_TABLE}"
SELECT_BY_ID_SQL = (
    f"SELECT {', '.join(TRANSACTION_COLUMNS)} FROM {TRANSACTIONS_TABLE} WHERE tx_id = ?"
)

SUSPICIOUS_COLUMNS = TRANSACTION_COLUMNS + ["rule_hit"]

SUSPICIOUS_SQL = f"""
SELECT
    {', '.join(TRANSACTION_COLUMNS)},
    CASE
        WHEN amount > ?
            OR (type = ? AND oldbalanceOrg = 0 AND newbalanceDest = 0)
            OR (type = ? AND oldbalanceOrg = 0 AND newbalanceDest = 0)
        THEN 1 ELSE 0
    END AS rule_hit
FROM {TRANSACTIONS_TABLE}
WHERE amount > ?
    OR (type = ? AND oldbalanceOrg = 0 AND newbalanceDest = 0)
    OR (type = ? AND oldbalanceOrg = 0 AND newbalanceDest = 0)
ORDER BY amount DESC
LIMIT ?
"""

INSERT_ALERT_SQL = (
    f"INSERT INTO {FRAUD_ALERTS_TABLE} ("
    + ", ".join(FRAUD_ALERT_COLUMNS)
    + ") VALUES ("
    + ", ".join(["?"] * len(FRAUD_ALERT_COLUMNS))
    + ")"
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


def insert_alert(alert_row: Dict[str, Any]) -> str:
    alert_id = alert_row.get("alert_id")
    with get_client() as client:
        client.sql(INSERT_ALERT_SQL, query_args=[alert_row[col] for col in FRAUD_ALERT_COLUMNS])
    return str(alert_id)


def _fetch_rows(limit: int | None, order_by: str | None = None) -> List[Dict[str, Any]]:
    query = SELECT_BASE_SQL
    if order_by:
        query += f" ORDER BY {order_by}"
    args: list[Any] = []
    if limit is not None:
        query += " LIMIT ?"
        args.append(limit)

    with get_client() as client:
        result = client.sql(query, query_args=args)
        return [dict(zip(TRANSACTION_COLUMNS, row)) for row in result]


def fetch_transactions(limit: int = DEFAULT_FETCH_LIMIT) -> List[Dict[str, Any]]:
    return _fetch_rows(limit=limit)


def fetch_recent_transactions(limit: int = DEFAULT_FETCH_LIMIT) -> List[Dict[str, Any]]:
    return _fetch_rows(limit=limit, order_by="step DESC")


def fetch_training_frame(limit: int | None = None) -> List[Dict[str, Any]]:
    """Fetch transactions for model training."""

    return _fetch_rows(limit=limit, order_by="step")


def fetch_transaction_by_id(tx_id: str) -> Optional[Dict[str, Any]]:
    with get_client() as client:
        result = client.sql(SELECT_BY_ID_SQL, query_args=[tx_id])
        rows = [dict(zip(TRANSACTION_COLUMNS, row)) for row in result]
        return rows[0] if rows else None


def query_suspicious(limit: int, min_amount: float) -> List[Dict[str, Any]]:
    """Return transactions that match the SQL rules for suspicious behavior."""

    args: list[Any] = [
        min_amount,
        "TRANSFER",
        "CASH_OUT",
        min_amount,
        "TRANSFER",
        "CASH_OUT",
        limit,
    ]

    with get_client() as client:
        result = client.sql(SUSPICIOUS_SQL, query_args=args)
        return [dict(zip(SUSPICIOUS_COLUMNS, row)) for row in result]

