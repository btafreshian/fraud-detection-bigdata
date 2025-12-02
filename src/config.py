"""Configuration constants for the Apache Ignite client utilities."""

IGNITE_HOST: str = "127.0.0.1"
IGNITE_PORT: int = 10800

TRANSACTIONS_TABLE: str = "Transactions"
FRAUD_ALERTS_TABLE: str = "FraudAlerts"

DEFAULT_BATCH_SIZE: int = 500
DEFAULT_FETCH_LIMIT: int = 5
