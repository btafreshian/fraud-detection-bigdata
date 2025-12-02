"""Lightweight regression tests for schema parsing and rule evaluation."""

from __future__ import annotations

from src import schema
from src.consume import evaluate_rules


def test_from_dict_safe_defaults() -> None:
    tx = schema.from_dict({})
    assert tx.tx_id
    assert tx.amount == 0.0
    assert tx.step == 0
    assert tx.isFraud == 0


def test_rule_triggers_on_large_amount() -> None:
    tx = schema.Transaction(
        tx_id="example",
        amount=2_000_000.0,
        type="PAYMENT",
        oldbalanceOrg=10.0,
        newbalanceDest=0.0,
    )
    hit, reasons = evaluate_rules(tx)
    assert hit
    assert any("amount" in reason for reason in reasons)


def test_rule_triggers_on_zero_balance_transfer() -> None:
    tx = schema.Transaction(
        tx_id="example2",
        amount=10.0,
        type="TRANSFER",
        oldbalanceOrg=0.0,
        newbalanceDest=0.0,
    )
    hit, reasons = evaluate_rules(tx)
    assert hit
    assert any("TRANSFER" in reason for reason in reasons)
