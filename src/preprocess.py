"""Preprocessing utilities and model pipeline for fraud detection."""

from __future__ import annotations

from typing import List

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

FEATURE_COLS: List[str] = [
    "type",
    "step",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
]
CAT_COLS: List[str] = ["type"]
NUM_COLS: List[str] = [
    "step",
    "amount",
    "oldbalanceOrg",
    "newbalanceOrig",
    "oldbalanceDest",
    "newbalanceDest",
]
LABEL_COL: str = "isFraud"


def build_pipeline(random_state: int = 42) -> Pipeline:
    """Construct the preprocessing + model pipeline."""

    categorical = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            (
                "onehot",
                OneHotEncoder(handle_unknown="ignore", sparse_output=False),
            ),
        ]
    )

    numeric = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", categorical, CAT_COLS),
            ("num", numeric, NUM_COLS),
        ]
    )

    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=None,
        n_jobs=-1,
        random_state=random_state,
    )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )

    return pipeline


def ensure_dataframe(rows: List[dict]) -> pd.DataFrame:
    """Create a DataFrame with the required feature columns in order."""

    if not rows:
        return pd.DataFrame(columns=FEATURE_COLS)

    frame = pd.DataFrame(rows)
    for col in FEATURE_COLS:
        if col not in frame:
            frame[col] = np.nan
    return frame[FEATURE_COLS + ([LABEL_COL] if LABEL_COL in frame.columns else [])]
