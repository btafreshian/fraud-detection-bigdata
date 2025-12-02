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

DEFAULT_FETCH_LIMIT: int = 5
DEFAULT_BATCH_SIZE: int = 1000

MODEL_ARTIFACT_PATH: str = "artifacts/model.joblib"
