"""Schema definitions and helpers for Ignite transactions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping


NUMERIC_DEFAULTS = {
    "step": 0,
    "amount": 0.0,
    "oldbalanceOrg": 0.0,
    "newbalanceOrig": 0.0,
    "oldbalanceDest": 0.0,
    "newbalanceDest": 0.0,
    "isFraud": 0,
    "isFlaggedFraud": 0,
}

STRING_DEFAULTS = {
    "tx_id": "",
    "type": "",
    "nameOrig": "",
    "nameDest": "",
}


@dataclass
class Transaction:
    tx_id: str
    step: int = 0
    type: str = ""
    amount: float = 0.0
    nameOrig: str = ""
    oldbalanceOrg: float = 0.0
    newbalanceOrig: float = 0.0
    nameDest: str = ""
    oldbalanceDest: float = 0.0
    newbalanceDest: float = 0.0
    isFraud: int = 0
    isFlaggedFraud: int = 0

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "Transaction":
        """Create a Transaction from a dictionary with sensible defaults."""

        def int_field(key: str) -> int:
            try:
                return int(data.get(key, NUMERIC_DEFAULTS.get(key, 0)))
            except (TypeError, ValueError):
                return NUMERIC_DEFAULTS.get(key, 0)

        def float_field(key: str) -> float:
            try:
                return float(data.get(key, NUMERIC_DEFAULTS.get(key, 0.0)))
            except (TypeError, ValueError):
                return float(NUMERIC_DEFAULTS.get(key, 0.0))

        def str_field(key: str) -> str:
            value = data.get(key, STRING_DEFAULTS.get(key, ""))
            return "" if value is None else str(value)

        return cls(
            tx_id=str_field("tx_id"),
            step=int_field("step"),
            type=str_field("type"),
            amount=float_field("amount"),
            nameOrig=str_field("nameOrig"),
            oldbalanceOrg=float_field("oldbalanceOrg"),
            newbalanceOrig=float_field("newbalanceOrig"),
            nameDest=str_field("nameDest"),
            oldbalanceDest=float_field("oldbalanceDest"),
            newbalanceDest=float_field("newbalanceDest"),
            isFraud=int_field("isFraud"),
            isFlaggedFraud=int_field("isFlaggedFraud"),
        )


def to_row_dict(tx: Mapping[str, Any] | Transaction) -> Dict[str, Any]:
    """Normalize a transaction-like object into a dict suitable for inserts."""
    if isinstance(tx, Transaction):
        tx_obj = tx
    else:
        tx_obj = Transaction.from_dict(tx)

    return {
        "tx_id": tx_obj.tx_id,
        "step": tx_obj.step,
        "type": tx_obj.type,
        "amount": tx_obj.amount,
        "nameOrig": tx_obj.nameOrig,
        "oldbalanceOrg": tx_obj.oldbalanceOrg,
        "newbalanceOrig": tx_obj.newbalanceOrig,
        "nameDest": tx_obj.nameDest,
        "oldbalanceDest": tx_obj.oldbalanceDest,
        "newbalanceDest": tx_obj.newbalanceDest,
        "isFraud": tx_obj.isFraud,
        "isFlaggedFraud": tx_obj.isFlaggedFraud,
    }
