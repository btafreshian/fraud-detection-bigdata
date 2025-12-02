"""Train a fraud detection model using transactions stored in Apache Ignite."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import List

import joblib
import pandas as pd
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    precision_recall_fscore_support,
)
from sklearn.model_selection import train_test_split

from .ignite_client import fetch_training_frame
from .preprocess import FEATURE_COLS, LABEL_COL, build_pipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train a fraud detection model.")
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.2,
        help="Fraction of data to reserve for testing (default: 0.2)",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed used for splits and model reproducibility (default: 42)",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional cap on rows fetched from Ignite for training.",
    )
    return parser.parse_args()


def _load_dataframe(limit: int | None) -> pd.DataFrame | None:
    rows: List[dict] = fetch_training_frame(limit=limit)
    if not rows:
        print(
            "No transactions available in Ignite. Load data with src.load_csv_to_ignite first."
        )
        return None

    df = pd.DataFrame(rows)
    if LABEL_COL not in df.columns:
        print(f"Label column '{LABEL_COL}' not found in fetched data.")
        return None

    df = df.dropna(subset=[LABEL_COL])
    if df.empty:
        print(
            "No labeled transactions found (isFraud is null). Load labeled data before training."
        )
        return None

    labels = pd.to_numeric(df[LABEL_COL], errors="coerce")
    valid_mask = labels.notna()
    df = df.loc[valid_mask].copy()
    df[LABEL_COL] = labels.loc[valid_mask].astype(int)

    if df[LABEL_COL].nunique() < 2:
        print(
            "Training requires at least two label classes. "
            "Ensure the dataset includes both fraudulent and non-fraudulent rows."
        )
        return None

    return df


def evaluate_model(model, X_test: pd.DataFrame, y_test: pd.Series) -> None:
    y_pred = model.predict(X_test)
    y_prob = model.predict_proba(X_test)[:, 1]

    cm = confusion_matrix(y_test, y_pred)
    precision, recall, f1, _ = precision_recall_fscore_support(
        y_test, y_pred, average="binary", zero_division=0
    )
    pr_auc = average_precision_score(y_test, y_prob)

    print("Confusion matrix:")
    print(cm)
    print()
    print(f"Precision: {precision:.4f}")
    print(f"Recall: {recall:.4f}")
    print(f"F1 score: {f1:.4f}")
    print(f"PR-AUC: {pr_auc:.4f}")


def main() -> None:
    args = parse_args()
    df = _load_dataframe(limit=args.max_rows)
    if df is None:
        return

    X = df[FEATURE_COLS]
    y = df[LABEL_COL]

    try:
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=args.test_size,
            random_state=args.random_state,
            stratify=y,
        )
    except ValueError as exc:
        print(
            "Unable to split data for training/testing. "
            "Try reducing --test-size or ensuring both classes have enough samples."
        )
        print(exc)
        return

    pipeline = build_pipeline(random_state=args.random_state)
    pipeline.fit(X_train, y_train)

    evaluate_model(pipeline, X_test, y_test)

    artifacts_dir = Path("artifacts")
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    model_path = artifacts_dir / "model.joblib"
    joblib.dump(pipeline, model_path)
    print(f"Saved trained pipeline to {model_path}")


if __name__ == "__main__":
    main()
