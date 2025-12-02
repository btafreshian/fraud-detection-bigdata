"""Hybrid rule + ML scorer for suspicious transactions in Ignite."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, List

import joblib
import pandas as pd
from tabulate import tabulate

from .ignite_client import query_suspicious
from .preprocess import FEATURE_COLS

DEFAULT_LIMIT = 20
DEFAULT_MIN_AMOUNT = 1_000_000.0
MODEL_PATH = Path("artifacts/model.joblib")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a hybrid rule/ML risk report from Ignite transactions.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Maximum number of suspicious rows to fetch (default: {DEFAULT_LIMIT})",
    )
    parser.add_argument(
        "--min-amount",
        type=float,
        default=DEFAULT_MIN_AMOUNT,
        help=(
            "Override the minimum amount rule threshold (amount > min_amount). "
            f"Default: {int(DEFAULT_MIN_AMOUNT)}"
        ),
    )
    return parser.parse_args()


def _load_model() -> Any:
    if not MODEL_PATH.exists():
        print("Saved model artifacts/model.joblib not found. Run python -m src.train first.")
        raise SystemExit(1)
    return joblib.load(MODEL_PATH)


def _format_rows(df: pd.DataFrame, limit: int) -> str:
    display_cols: List[str] = [
        "tx_id",
        "rule_hit",
        "ml_prob",
        "final_risk",
        "amount",
        "type",
        "step",
        "nameOrig",
        "nameDest",
        "oldbalanceOrg",
        "newbalanceOrig",
        "oldbalanceDest",
        "newbalanceDest",
    ]

    df = df[display_cols].head(limit)
    return tabulate(
        df,
        headers="keys",
        tablefmt="github",
        floatfmt=".4f",
        showindex=False,
    )


def main() -> None:
    args = parse_args()
    model = _load_model()

    rows = query_suspicious(limit=args.limit, min_amount=args.min_amount)
    if not rows:
        print("No suspicious transactions found for the supplied criteria.")
        return

    df = pd.DataFrame(rows)
    df["rule_hit"] = df["rule_hit"].astype(bool)

    features = df[FEATURE_COLS]
    ml_probs = model.predict_proba(features)[:, 1]
    df["ml_prob"] = ml_probs

    df["final_risk"] = [
        max(prob, 0.95 if rule else 0.0) for prob, rule in zip(df["ml_prob"], df["rule_hit"])
    ]

    df = df.sort_values(by=["final_risk", "amount"], ascending=[False, False])

    print(_format_rows(df, args.limit))


if __name__ == "__main__":
    main()
