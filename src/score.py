"""Score transactions using a persisted fraud detection pipeline."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import joblib

from .config import MODEL_ARTIFACT_PATH
from .ignite_client import fetch_recent_transactions
from .preprocess import FEATURE_COLS, ensure_dataframe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Score transactions using the trained fraud model.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of recent transactions to score (default: 10).",
    )
    return parser.parse_args()


def load_model(path: str = MODEL_ARTIFACT_PATH):
    artifact_path = Path(path)
    if not artifact_path.exists():
        raise SystemExit(
            f"Model artifact not found at {artifact_path}. Run `python -m src.train` first."
        )
    return joblib.load(artifact_path)


def score_transactions(rows: List[dict]) -> List[float]:
    model = load_model()
    frame = ensure_dataframe(rows)
    if frame.empty:
        return []
    probabilities = model.predict_proba(frame[FEATURE_COLS])[:, 1]
    return probabilities.tolist()


def main() -> None:
    args = parse_args()
    rows = fetch_recent_transactions(limit=args.limit)
    if not rows:
        raise SystemExit(
            "No transactions available to score. Load data with src.load_csv_to_ignite first."
        )

    scores = score_transactions(rows)
    for row, score in zip(rows, scores):
        tx_id = row.get("tx_id", "<unknown>")
        print(f"tx_id={tx_id} fraud_probability={score:.4f}")


if __name__ == "__main__":
    main()
