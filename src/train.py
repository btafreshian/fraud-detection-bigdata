"""Train a fraud detection model using data from Apache Ignite."""

from __future__ import annotations

import argparse
from pathlib import Path

import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    classification_report,
    confusion_matrix,
    precision_recall_fscore_support,
)
from sklearn.model_selection import train_test_split

from .config import MODEL_ARTIFACT_PATH
from .ignite_client import fetch_training_frame
from .preprocess import FEATURE_COLS, LABEL_COL, build_pipeline, ensure_dataframe


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train a fraud detection model from Ignite transactions.",
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.2,
        help="Proportion of the dataset to include in the test split (default: 0.2).",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed for reproducibility (default: 42).",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional maximum number of rows to pull from Ignite.",
    )
    return parser.parse_args()


def load_training_data(max_rows: int | None) -> pd.DataFrame:
    rows = fetch_training_frame(limit=max_rows)
    frame = ensure_dataframe(rows)

    if frame.empty:
        raise SystemExit(
            "No transactions found in Ignite. Load data with src.load_csv_to_ignite first."
        )

    if LABEL_COL not in frame or frame[LABEL_COL].isna().all():
        raise SystemExit(
            "No labeled data found in Ignite (isFraud is null). "
            "Load data with labels before training."
        )

    frame[LABEL_COL] = frame[LABEL_COL].astype(float)
    frame = frame.dropna(subset=[LABEL_COL])

    if frame.empty:
        raise SystemExit(
            "All labels were null after filtering. Provide labeled data before training."
        )

    return frame


def evaluate_model(y_true: pd.Series, y_pred: np.ndarray, y_proba: np.ndarray) -> None:
    cm = confusion_matrix(y_true, y_pred)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_true, y_pred, average="binary", zero_division=0
    )

    print("Confusion matrix:")
    print(cm)
    print()
    print("Classification report:")
    print(classification_report(y_true, y_pred, zero_division=0))

    try:
        pr_auc = average_precision_score(y_true, y_proba)
        print(f"PR-AUC: {pr_auc:.4f}")
    except Exception:
        print("PR-AUC could not be computed (check probabilities).")

    print(f"Precision: {precision:.4f}")
    print(f"Recall:    {recall:.4f}")
    print(f"F1-score:  {f1:.4f}")


def save_model(pipeline, path: str) -> None:
    artifact_path = Path(path)
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(pipeline, artifact_path)
    print(f"Saved model to {artifact_path}")


def main() -> None:
    args = parse_args()
    pipeline = build_pipeline(random_state=args.random_state)

    frame = load_training_data(args.max_rows)
    X = frame[FEATURE_COLS]
    y = frame[LABEL_COL]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_size, random_state=args.random_state, stratify=y
    )

    if X_train.empty or X_test.empty:
        raise SystemExit("Not enough data to split into train/test. Load more rows.")

    pipeline.fit(X_train, y_train)
    y_pred = pipeline.predict(X_test)
    y_proba = pipeline.predict_proba(X_test)[:, 1]

    evaluate_model(y_test, y_pred, y_proba)
    save_model(pipeline, MODEL_ARTIFACT_PATH)


if __name__ == "__main__":
    main()
