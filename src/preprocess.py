"""Feature definitions and preprocessing pipeline for fraud detection."""

from __future__ import annotations

from typing import List

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
LABEL_COL = "isFraud"


def build_pipeline(random_state: int = 42) -> Pipeline:
    """Construct the sklearn Pipeline used for both training and scoring."""

    numeric_transformer = Pipeline(
        steps=[("imputer", SimpleImputer(strategy="median"))]
    )

    categorical_transformer = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="most_frequent")),
            (
                "onehot",
                OneHotEncoder(handle_unknown="ignore"),
            ),
        ]
    )

    preprocessor = ColumnTransformer(
        transformers=[
            ("categorical", categorical_transformer, CAT_COLS),
            ("numeric", numeric_transformer, NUM_COLS),
        ],
        remainder="drop",
    )

    model = RandomForestClassifier(
        n_estimators=200,
        random_state=random_state,
        n_jobs=-1,
    )

    return Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )
