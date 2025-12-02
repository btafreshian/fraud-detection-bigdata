"""Python-side representation of the Transactions schema."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict
from uuid import uuid4

from .config import TRANSACTION_COLUMNS


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


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_str(value: Any, default: str = "") -> str:
    if value is None:
        return default
    return str(value)


def from_dict(data: Dict[str, Any]) -> Transaction:
    """Create a :class:`Transaction` from a mapping with safe defaults."""

    tx_id = _safe_str(data.get("tx_id")) or str(uuid4())

    return Transaction(
        tx_id=tx_id,
        step=_safe_int(data.get("step")),
        type=_safe_str(data.get("type")),
        amount=_safe_float(data.get("amount")),
        nameOrig=_safe_str(data.get("nameOrig")),
        oldbalanceOrg=_safe_float(data.get("oldbalanceOrg")),
        newbalanceOrig=_safe_float(data.get("newbalanceOrig")),
        nameDest=_safe_str(data.get("nameDest")),
        oldbalanceDest=_safe_float(data.get("oldbalanceDest")),
        newbalanceDest=_safe_float(data.get("newbalanceDest")),
        isFraud=_safe_int(data.get("isFraud")),
        isFlaggedFraud=_safe_int(data.get("isFlaggedFraud")),
    )


def to_row_dict(tx: Transaction) -> Dict[str, Any]:
    """Convert a :class:`Transaction` into a dict ordered by schema columns."""

    return {column: getattr(tx, column) for column in TRANSACTION_COLUMNS}

