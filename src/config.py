"""Configuration constants for the fraud detection prototype."""

from __future__ import annotations

IGNITE_HOST: str = "127.0.0.1"
IGNITE_PORT: int = 10800

TRANSACTIONS_TABLE: str = "Transactions"
TRANSACTION_COLUMNS: list[str] = [
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

FRAUD_ALERTS_TABLE: str = "FraudAlerts"
FRAUD_ALERT_COLUMNS: list[str] = [
    "alert_id",
    "tx_id",
    "ts",
    "ml_prob",
    "rule_hit",
    "reason",
    "amount",
    "type",
    "nameOrig",
    "nameDest",
]

DEFAULT_FETCH_LIMIT: int = 5
DEFAULT_BATCH_SIZE: int = 1000
