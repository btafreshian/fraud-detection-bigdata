"""Score transactions using a trained fraud detection pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import joblib
import pandas as pd

from .ignite_client import fetch_recent_transactions
from .preprocess import FEATURE_COLS

MODEL_PATH = Path("artifacts/model.joblib")


def load_model(model_path: Path = MODEL_PATH):
    if not model_path.exists():
        raise FileNotFoundError(
            f"Model not found at {model_path}. Train one with `python -m src.train`."
        )
    return joblib.load(model_path)


def score_transactions(rows: List[dict]) -> List[float]:
    if not rows:
        return []

    model = load_model()
    df = pd.DataFrame(rows)
    if df.empty:
        return []

    probabilities = model.predict_proba(df[FEATURE_COLS])[:, 1]
    return probabilities.tolist()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Score recent transactions from Ignite.")
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of recent transactions to score (default: 10)",
    )
    parser.add_argument(
        "--model-path",
        type=Path,
        default=MODEL_PATH,
        help="Path to a trained sklearn pipeline (default: artifacts/model.joblib)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        model = load_model(args.model_path)
    except FileNotFoundError as exc:
        print(exc)
        return

    rows = fetch_recent_transactions(limit=args.limit)
    if not rows:
        print(
            "No transactions available in Ignite to score. Load data with src.load_csv_to_ignite first."
        )
        return

    df = pd.DataFrame(rows)
    probs = model.predict_proba(df[FEATURE_COLS])[:, 1]

    for tx, prob in zip(rows, probs):
        print(f"tx_id={tx.get('tx_id')}: fraud_prob={prob:.6f}")


if __name__ == "__main__":
    main()
