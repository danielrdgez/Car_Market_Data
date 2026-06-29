"""
Train a panel-style vehicle depreciation model from SQLite price history.

The script reads historical listing prices, joins static vehicle attributes,
engineers time-series features, trains a regression model with a time-based
holdout set, and saves model artifacts to MODELS_OUTPUT.
"""

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

try:
    from xgboost import XGBRegressor
except ImportError:  # pragma: no cover - depends on local environment
    XGBRegressor = None

try:
    from lightgbm import LGBMRegressor
except ImportError:  # pragma: no cover - depends on local environment
    LGBMRegressor = None


RANDOM_STATE = 42
SPLIT_DATE = "2026-05-15"
MODEL_FILENAME = "depreciation_xgboost_model.joblib"
FEATURES_FILENAME = "model_features.json"

CATEGORICAL_FEATURES = [
    "nhtsa_Make",
    "nhtsa_Model",
    "nhtsa_BodyClass",
    "nhtsa_DriveType",
    "nhtsa_FuelTypePrimary",
    "vehicleTitle",
]

NUMERIC_FEATURES = [
    "mileage",
    "days_since_first_seen",
    "month",
    "week_of_year",
]

FEATURE_COLUMNS = NUMERIC_FEATURES + CATEGORICAL_FEATURES


def parse_args() -> argparse.Namespace:
    """Parse command-line options for repeatable training runs."""
    # This gets the 'ML' folder where the script lives
    script_dir = Path(__file__).resolve().parent
    # This goes up one level to the main 'Car-Price-Data-Visualization-Learning' folder
    project_root = script_dir.parent

    default_db_path = project_root / "CAR_DATA_OUTPUT" / "CAR_DATA_CLEANED.db"

    parser = argparse.ArgumentParser(
        description="Train a vehicle depreciation forecasting model."
    )
    parser.add_argument(
        "--db-path",
        default=str(default_db_path),
        help=(
            "Path to the SQLite database. Defaults to "
            "../CAR_DATA_OUTPUT/CAR_DATA_CLEANED.db."
        ),
    )
    parser.add_argument(
        "--split-date",
        default=SPLIT_DATE,
        help="Rows before this date train the model; rows on/after test it.",
    )
    return parser.parse_args()


def ensure_output_dir() -> Path:
    """Create MODELS_OUTPUT in the main project directory if needed."""
    # Find the main project root (one level up from the script's directory)
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent

    # Set the output directory to the main project's MODELS_OUTPUT
    output_dir = project_root / "MODELS_OUTPUT"

    if not output_dir.exists():
        os.makedirs(output_dir, exist_ok=True)

    return output_dir


def load_vehicle_history(db_path: Path) -> pd.DataFrame:
    """
    Load price history joined to the latest listing row and NHTSA attributes.

    The listings table may contain multiple snapshots per VIN. The CTE keeps
    the latest listing snapshot for vehicleTitle so joining on VIN does not
    multiply historical price rows.
    """
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    query = """
    WITH latest_listings AS (
        SELECT
            vin,
            vehicleTitle,
            ROW_NUMBER() OVER (
                PARTITION BY vin
                ORDER BY loaddate DESC
            ) AS row_num
        FROM listings
    )
    SELECT
        ph.vin,
        ph.history_date,
        ph.price,
        ph.mileage,
        ll.vehicleTitle,
        n.nhtsa_Make,
        n.nhtsa_Model,
        n.nhtsa_BodyClass,
        n.nhtsa_DriveType,
        n.nhtsa_FuelTypePrimary
    FROM price_history AS ph
    INNER JOIN latest_listings AS ll
        ON ph.vin = ll.vin
        AND ll.row_num = 1
    LEFT JOIN nhtsa_enrichment AS n
        ON ph.vin = n.vin
    """

    with sqlite3.connect(str(db_path)) as conn:
        df = pd.read_sql(query, conn)

    df["history_date"] = pd.to_datetime(df["history_date"], errors="coerce")
    return df


def clean_and_engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """Clean raw rows and create panel time features per VIN."""
    if df.empty:
        raise ValueError("No rows were returned from the database query.")

    df = df.copy()
    df = df.dropna(subset=["vin", "history_date"])
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["mileage"] = pd.to_numeric(df["mileage"], errors="coerce")

    df = df[df["price"].notna() & (df["price"] > 0)]
    df = df.sort_values(["vin", "history_date"]).reset_index(drop=True)

    # Mileage can be missing in intermittent snapshots; fill within each VIN.
    df["mileage"] = df.groupby("vin", observed=True)["mileage"].ffill()
    df["mileage"] = df.groupby("vin", observed=True)["mileage"].bfill()
    df = df.dropna(subset=["mileage"])

    df["year"] = df["history_date"].dt.year.astype("int16")
    df["month"] = df["history_date"].dt.month.astype("int8")
    df["week_of_year"] = df["history_date"].dt.isocalendar().week.astype("int16")

    first_seen = df.groupby("vin", observed=True)["history_date"].transform("min")
    df["days_since_first_seen"] = (df["history_date"] - first_seen).dt.days
    df["days_since_first_seen"] = df["days_since_first_seen"].astype("int32")

    for column in CATEGORICAL_FEATURES:
        df[column] = df[column].fillna("UNKNOWN").astype("string")

    for column in NUMERIC_FEATURES:
        df[column] = pd.to_numeric(df[column], errors="coerce")

    df = df.dropna(subset=FEATURE_COLUMNS + ["price"])
    return df


def build_model_pipeline() -> tuple[Pipeline, str]:
    """
    Build a preprocessing and model pipeline.

    OrdinalEncoder provides stable categorical handling for both XGBoost and
    the LightGBM fallback. Unknown inference-time categories are encoded as -1.
    """
    numeric_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="median")),
        ]
    )
    categorical_pipeline = Pipeline(
        steps=[
            ("imputer", SimpleImputer(strategy="constant", fill_value="UNKNOWN")),
            (
                "encoder",
                OrdinalEncoder(
                    handle_unknown="use_encoded_value",
                    unknown_value=-1,
                    encoded_missing_value=-1,
                ),
            ),
        ]
    )
    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", numeric_pipeline, NUMERIC_FEATURES),
            ("categorical", categorical_pipeline, CATEGORICAL_FEATURES),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    if XGBRegressor is not None:
        model = XGBRegressor(
            objective="reg:squarederror",
            n_estimators=700,
            learning_rate=0.05,
            max_depth=8,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_lambda=1.0,
            tree_method="hist",
            random_state=RANDOM_STATE,
            n_jobs=-1,
        )
        model_name = "xgboost"
    elif LGBMRegressor is not None:
        model = LGBMRegressor(
            objective="regression",
            n_estimators=700,
            learning_rate=0.05,
            num_leaves=63,
            subsample=0.85,
            colsample_bytree=0.85,
            random_state=RANDOM_STATE,
            n_jobs=-1,
            verbose=-1,
        )
        model_name = "lightgbm"
    else:
        raise ImportError(
            "Install either xgboost or lightgbm before running this script."
        )

    pipeline = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("model", model),
        ]
    )
    return pipeline, model_name


def time_based_split(
    df: pd.DataFrame,
    split_date: str,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame, pd.Series]:
    """Split data by history_date to respect the forecasting timeline."""
    split_timestamp = pd.Timestamp(split_date)

    train_df = df[df["history_date"] < split_timestamp].copy()
    test_df = df[df["history_date"] >= split_timestamp].copy()

    if train_df.empty:
        raise ValueError(f"No training rows found before split date {split_date}.")
    if test_df.empty:
        raise ValueError(f"No test rows found on or after split date {split_date}.")

    X_train = train_df[FEATURE_COLUMNS]
    y_train = train_df["price"]
    X_test = test_df[FEATURE_COLUMNS]
    y_test = test_df["price"]

    return X_train, y_train, X_test, y_test


def evaluate_model(model: Pipeline, X_test: pd.DataFrame, y_test: pd.Series) -> dict:
    """Score model predictions with MAE and RMSE."""
    predictions = model.predict(X_test)
    mae = mean_absolute_error(y_test, predictions)
    rmse = float(np.sqrt(mean_squared_error(y_test, predictions)))

    metrics = {
        "mae": float(mae),
        "rmse": rmse,
        "test_rows": int(X_test.shape[0]),
    }

    print(f"Test MAE:  ${mae:,.2f}")
    print(f"Test RMSE: ${rmse:,.2f}")
    return metrics


def write_feature_metadata(
    output_dir: Path,
    db_path: Path,
    model_name: str,
    split_date: str,
    metrics: dict[str, Any],
) -> None:
    """Save feature and training metadata needed for inference."""
    metadata = {
        "target": "price",
        "date_column": "history_date",
        "split_date": split_date,
        "model_type": model_name,
        "database_path": str(db_path),
        "feature_columns": FEATURE_COLUMNS,
        "numeric_features": NUMERIC_FEATURES,
        "categorical_features": CATEGORICAL_FEATURES,
        "categorical_unknown_value": -1,
        "metrics": metrics,
    }

    features_path = output_dir / FEATURES_FILENAME
    with features_path.open("w", encoding="utf-8") as file:
        json.dump(metadata, file, indent=2)

    print(f"Saved feature metadata to {features_path}")


def main() -> None:
    """Run extraction, preprocessing, training, evaluation, and export."""
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    output_dir = ensure_output_dir()

    print(f"Loading vehicle history from {db_path}")
    raw_df = load_vehicle_history(db_path)
    print(f"Loaded {raw_df.shape[0]:,} raw history rows")

    model_df = clean_and_engineer_features(raw_df)
    print(f"Prepared {model_df.shape[0]:,} modeling rows")

    X_train, y_train, X_test, y_test = time_based_split(
        model_df,
        split_date=args.split_date,
    )
    print(f"Training rows: {X_train.shape[0]:,}")
    print(f"Testing rows:  {X_test.shape[0]:,}")

    model, model_name = build_model_pipeline()
    print(f"Training {model_name} depreciation model")
    model.fit(X_train, y_train)

    metrics = evaluate_model(model, X_test, y_test)

    model_path = output_dir / MODEL_FILENAME
    joblib.dump(model, model_path)
    print(f"Saved trained model to {model_path}")

    write_feature_metadata(
        output_dir=output_dir,
        db_path=db_path,
        model_name=model_name,
        split_date=args.split_date,
        metrics=metrics,
    )


if __name__ == "__main__":
    main()
