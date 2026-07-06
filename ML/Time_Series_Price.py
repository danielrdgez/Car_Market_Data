"""
Train make/model/year/trim cohort depreciation forecasts from price history.

The model grain is a weekly cohort, not an individual VIN. Cohorts are built
from NHTSA make, model, model year, and a best-effort trim token parsed from
the latest listing title. The script trains global forecasting models across
all cohorts so sparse cohorts can borrow signal from richer ones.
"""

from __future__ import annotations

import argparse
import gc
import json
import re
import sqlite3
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyRegressor
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.impute import SimpleImputer
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import ParameterSampler
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

warnings.filterwarnings(
    "ignore",
    message="X does not have valid feature names, but LGBMRegressor was fitted with feature names",
    category=UserWarning,
)

try:
    from lightgbm import LGBMRegressor
except ImportError:  # pragma: no cover - depends on local environment
    LGBMRegressor = None


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "CAR_DATA_OUTPUT" / "CAR_DATA_CLEANED.db"
OUTPUT_DIR = BASE_DIR / "MODELS_OUTPUT"

RANDOM_STATE = 42
DEFAULT_SAMPLE_SIZE = 250_000
DEFAULT_TUNING_SAMPLE_SIZE = 200_000
DEFAULT_TUNING_CANDIDATES = 8
DEFAULT_HORIZONS = [30, 90, 180, 365]
DEFAULT_MIN_WEEKLY_VINS = 1
DEFAULT_MIN_COHORT_WEEKS = 3
DEFAULT_MAX_PRICE = 250_000
MIN_MODEL_ROWS = 50
MIN_TEMPORAL_TRAIN_ROWS = 2
MIN_TEMPORAL_TEST_ROWS = 2

COHORT_COLUMNS = ["make", "model", "model_year", "trim_proxy"]

KNOWN_TRIMS = [
    "PLATINUM",
    "LIMITED",
    "PREMIUM",
    "SEL",
    "SE",
    "LE",
    "XLE",
    "XSE",
    "SR5",
    "TRD PRO",
    "TRD OFF ROAD",
    "TRD SPORT",
    "SPORT",
    "TOURING",
    "EX L",
    "EX",
    "LX",
    "SI",
    "RTL E",
    "RTL",
    "LTZ",
    "LT",
    "LS",
    "RST",
    "Z71",
    "HIGH COUNTRY",
    "DENALI",
    "SLT",
    "SLE",
    "LARIAT",
    "KING RANCH",
    "RAPTOR",
    "XL",
    "XLT",
    "BIG HORN",
    "LARAMIE",
    "LONGHORN",
    "REBEL",
    "WILLYS",
    "SAHARA",
    "RUBICON",
    "OVERLAND",
    "ALTITUDE",
    "SXT",
    "GT",
    "R T",
    "SRT",
    "SV",
    "SR",
    "SL",
    "PRO 4X",
    "2LT",
    "1LT",
    "3LT",
    "2SS",
    "1SS",
    "BASE",
]

TITLE_STOPWORDS = {
    "NEW",
    "USED",
    "CERTIFIED",
    "CPO",
    "SEDAN",
    "COUPE",
    "SUV",
    "TRUCK",
    "VAN",
    "WAGON",
    "AWD",
    "FWD",
    "RWD",
    "4WD",
    "AUTO",
    "AUTOMATIC",
    "MANUAL",
    "GAS",
    "HYBRID",
    "ELECTRIC",
    "DIESEL",
}

CATEGORICAL_FEATURES = [
    "make",
    "model",
    "model_year",
    "trim_proxy",
    "body_class",
    "drive_type",
    "fuel_type",
    "electrification_level",
    "dominant_seller_type",
    "dominant_source_name",
]

NUMERIC_FEATURES = [
    "median_price",
    "avg_price",
    "price_p25",
    "price_p75",
    "avg_mileage",
    "median_mileage",
    "avg_vehicle_age_months",
    "avg_miles_per_year",
    "volume",
    "unique_vins",
    "price_down_rate",
    "month",
    "quarter",
    "week_of_year",
    "cohort_week_number",
    "cohort_age_weeks",
    "cohort_first_median_price",
    "price_index_vs_cohort_first",
    "cumulative_depreciation_pct",
    "lag_median_price_1",
    "lag_median_price_2",
    "lag_price_index_1",
    "rolling_median_price_4",
    "rolling_avg_mileage_4",
    "rolling_volume_4",
    "rolling_depreciation_pct_4",
    "market_median_price",
    "market_price_index",
    "market_weekly_volume",
    "sentiment_score",
    "sentiment_comment_count",
    "sentiment_video_count",
    "engine_hp",
    "engine_cylinders",
    "total_recalls",
    "total_complaints",
]

RESEARCH_REFERENCES = [
    {
        "title": "Principles and Algorithms for Forecasting Groups of Time Series: Locality and Globality",
        "url": "https://arxiv.org/abs/2008.00444",
        "reason": "Supports global models across related series, especially when many cohorts are short or sparse.",
    },
    {
        "title": "Global Models for Time Series Forecasting: A Simulation Study",
        "url": "https://arxiv.org/abs/2012.12485",
        "reason": "Shows global LGBM-style models can be competitive with short, heterogeneous time series.",
    },
    {
        "title": "ProbSAINT: Probabilistic Tabular Regression for Used Car Pricing",
        "url": "https://arxiv.org/abs/2403.03812",
        "reason": "Motivates treating vehicle pricing as dynamic tabular prediction with uncertainty-aware evaluation.",
    },
    {
        "title": "Manheim Used Vehicle Value Index Summary Methodology",
        "url": "https://site.manheim.com/wp-content/uploads/sites/2/2024/02/Used-Vehicle-Summary-Methodology.pdf",
        "reason": "Motivates controls for mileage, market mix, outliers, and seasonality in used-vehicle price indices.",
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train cohort-level vehicle depreciation forecasts."
    )
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to CAR_DATA_CLEANED.db.")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR), help="Directory for model artifacts.")
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help=(
            "Rows used to choose recently observed VINs, then all available history for "
            "those VINs is loaded. Use 0 for full history."
        ),
    )
    parser.add_argument(
        "--horizons",
        default="30,90,180,365",
        help="Comma-separated forecast horizons in days.",
    )
    parser.add_argument(
        "--split-date",
        default=None,
        help="Validation cutoff by cohort week. Defaults to the 80th percentile week.",
    )
    parser.add_argument(
        "--min-weekly-vins",
        type=int,
        default=DEFAULT_MIN_WEEKLY_VINS,
        help="Minimum distinct VINs required in a cohort-week.",
    )
    parser.add_argument(
        "--min-cohort-weeks",
        type=int,
        default=DEFAULT_MIN_COHORT_WEEKS,
        help="Minimum retained weeks required per cohort.",
    )
    parser.add_argument(
        "--max-price",
        type=int,
        default=DEFAULT_MAX_PRICE,
        help="Drop extreme listing prices above this value; use 0 to disable.",
    )
    return parser.parse_args()


def _basic_limit_clause(sample_size: int | None) -> str:
    if sample_size and sample_size > 0:
        return f"LIMIT {int(sample_size)}"
    return ""


def _sample_history_cte(sample_size: int | None) -> str:
    if sample_size and sample_size > 0:
        sample_size = int(sample_size)
        return f"""
        recent_vins AS (
            SELECT DISTINCT vin
            FROM (
                SELECT vin
                FROM price_history
                WHERE price IS NOT NULL
                  AND price > 0
                  AND vin IS NOT NULL
                ORDER BY history_date DESC, rowid DESC
                LIMIT {sample_size}
            )
        ),
        sample_history AS (
            SELECT ph.vin, ph.history_date, ph.mileage, ph.price, ph.trend
            FROM price_history AS ph
            INNER JOIN recent_vins AS rv
                ON ph.vin = rv.vin
            WHERE ph.price IS NOT NULL
              AND ph.price > 0
        )
        """
    return """
        sample_history AS (
            SELECT vin, history_date, mileage, price, trend
            FROM price_history
            WHERE price IS NOT NULL
              AND price > 0
        )
    """


def normalize_label(value: Any, fallback: str = "UNKNOWN") -> str:
    if value is None or pd.isna(value):
        return fallback
    text = re.sub(r"\s+", " ", str(value).strip().upper())
    return text if text else fallback


def normalize_title(value: Any) -> str:
    text = normalize_label(value, "")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def parse_trim_proxy(title: Any, make: Any, model: Any, model_year: Any) -> str:
    """Extract a useful trim-like token from listing title text when possible."""
    normalized_title = normalize_title(title)
    if not normalized_title:
        return "UNKNOWN_TRIM"

    padded = f" {normalized_title} "
    for trim in KNOWN_TRIMS:
        if f" {trim} " in padded:
            return trim.replace(" ", "_")

    tokens = normalized_title.split()
    remove_tokens = set(normalize_title(make).split())
    remove_tokens.update(normalize_title(model).split())
    if model_year is not None and not pd.isna(model_year):
        remove_tokens.add(str(int(float(model_year))))

    candidates = [
        token
        for token in tokens
        if token not in remove_tokens
        and token not in TITLE_STOPWORDS
        and not token.isdigit()
        and len(token) > 1
    ]
    if not candidates:
        return "UNKNOWN_TRIM"
    return "_".join(candidates[:2])[:40]


def mode_or_unknown(values: pd.Series) -> str:
    cleaned = values.dropna().astype(str)
    cleaned = cleaned[cleaned.str.strip().ne("")]
    if cleaned.empty:
        return "UNKNOWN"
    return normalize_label(cleaned.mode().iloc[0])


def load_history_frame(db_path: Path, sample_size: int | None) -> pd.DataFrame:
    """Load recent price history with static VIN attributes for cohorting."""
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    query = f"""
        WITH RECURSIVE
        {_sample_history_cte(sample_size)},
        history_vins AS (
            SELECT DISTINCT vin
            FROM sample_history
        ),
        latest_listing AS (
            SELECT *
            FROM (
                SELECT
                    l.vin,
                    l.title,
                    l.vehicleTitle,
                    l.vehicleTitleDesc,
                    l.sellerType,
                    l.sourceName,
                    l.listingType,
                    ROW_NUMBER() OVER (
                        PARTITION BY l.vin
                        ORDER BY l.loaddate DESC, l.date DESC, l.rowid DESC
                    ) AS row_num
                FROM listings AS l
                INNER JOIN history_vins AS hv
                    ON l.vin = hv.vin
            )
            WHERE row_num = 1
        )
        SELECT
            ph.vin,
            ph.history_date,
            ph.mileage,
            ph.price,
            ph.trend,
            ll.title,
            ll.vehicleTitle,
            ll.vehicleTitleDesc,
            ll.sellerType,
            ll.sourceName,
            ll.listingType,
            n.nhtsa_Make,
            n.nhtsa_Model,
            n.nhtsa_ModelYear,
            n.nhtsa_BodyClass,
            n.nhtsa_DriveType,
            n.nhtsa_FuelTypePrimary,
            n.nhtsa_ElectrificationLevel,
            n.nhtsa_EngineHP,
            n.nhtsa_EngineCylinders,
            n.nhtsa_total_recalls,
            n.nhtsa_total_complaints,
            vs.sentiment_score,
            vs.comment_count AS sentiment_comment_count,
            vs.video_count AS sentiment_video_count
        FROM sample_history AS ph
        LEFT JOIN latest_listing AS ll
            ON ph.vin = ll.vin
        LEFT JOIN nhtsa_enrichment AS n
            ON ph.vin = n.vin
        LEFT JOIN vehicle_sentiment AS vs
            ON LOWER(vs.make) = LOWER(n.nhtsa_Make)
           AND LOWER(vs.model) = LOWER(n.nhtsa_Model)
           AND vs.year = n.nhtsa_ModelYear
    """
    with sqlite3.connect(str(db_path)) as conn:
        return pd.read_sql_query(query, conn)


def clean_history_frame(df: pd.DataFrame, max_price: int | None) -> pd.DataFrame:
    if df.empty:
        raise ValueError("No usable rows were returned from price_history.")

    history = df.copy()
    history["history_date"] = pd.to_datetime(history["history_date"], errors="coerce")
    history["price"] = pd.to_numeric(history["price"], errors="coerce")
    history["mileage"] = pd.to_numeric(history["mileage"], errors="coerce")
    history["model_year"] = pd.to_numeric(history["nhtsa_ModelYear"], errors="coerce")

    history = history.dropna(subset=["vin", "history_date", "price", "model_year"])
    history = history[history["price"].between(1_000, max_price or np.inf)]
    history = history.sort_values(["vin", "history_date"]).reset_index(drop=True)

    history["mileage"] = history.groupby("vin", observed=True)["mileage"].ffill()
    history["mileage"] = history.groupby("vin", observed=True)["mileage"].bfill()
    history = history.dropna(subset=["mileage"])
    history = history[history["mileage"].ge(0)]

    history["make"] = history["nhtsa_Make"].map(normalize_label)
    history["model"] = history["nhtsa_Model"].map(normalize_label)
    history["model_year"] = history["model_year"].astype("int16")
    history = history[history["make"].ne("UNKNOWN") & history["model"].ne("UNKNOWN")]

    title_source = (
        history["title"].fillna("")
        + " "
        + history["vehicleTitle"].fillna("")
        + " "
        + history["vehicleTitleDesc"].fillna("")
    )
    history["trim_proxy"] = [
        parse_trim_proxy(title, make, model, year)
        for title, make, model, year in zip(
            title_source,
            history["make"],
            history["model"],
            history["model_year"],
        )
    ]

    history["body_class"] = history["nhtsa_BodyClass"].map(normalize_label)
    history["drive_type"] = history["nhtsa_DriveType"].map(normalize_label)
    history["fuel_type"] = history["nhtsa_FuelTypePrimary"].map(normalize_label)
    history["electrification_level"] = history["nhtsa_ElectrificationLevel"].map(normalize_label)
    history["seller_type"] = history["sellerType"].map(normalize_label)
    history["source_name"] = history["sourceName"].map(normalize_label)

    history["engine_hp"] = pd.to_numeric(history["nhtsa_EngineHP"], errors="coerce")
    history["engine_cylinders"] = pd.to_numeric(history["nhtsa_EngineCylinders"], errors="coerce")
    history["total_recalls"] = pd.to_numeric(history["nhtsa_total_recalls"], errors="coerce").fillna(0)
    history["total_complaints"] = pd.to_numeric(history["nhtsa_total_complaints"], errors="coerce").fillna(0)
    history["sentiment_score"] = pd.to_numeric(history["sentiment_score"], errors="coerce")
    history["sentiment_comment_count"] = pd.to_numeric(
        history["sentiment_comment_count"], errors="coerce"
    ).fillna(0)
    history["sentiment_video_count"] = pd.to_numeric(
        history["sentiment_video_count"], errors="coerce"
    ).fillna(0)

    model_year_start = pd.to_datetime(history["model_year"].astype(str) + "-01-01")
    history["vehicle_age_months"] = (
        (history["history_date"] - model_year_start).dt.days / 30.4375
    ).clip(lower=0)
    history["miles_per_year"] = history["mileage"] / (history["vehicle_age_months"] / 12).clip(lower=0.25)
    history["price_down_signal"] = (
        history["trend"].fillna("").astype(str).str.lower().str.contains("down|drop|decrease")
    ).astype("int8")
    history["week_start"] = history["history_date"].dt.to_period("W").dt.start_time
    return history


def build_cohort_weekly_frame(
    history: pd.DataFrame,
    horizons: list[int],
    min_weekly_vins: int,
    min_cohort_weeks: int,
) -> pd.DataFrame:
    group_cols = COHORT_COLUMNS + ["week_start"]

    weekly = (
        history.groupby(group_cols, dropna=False)
        .agg(
            median_price=("price", "median"),
            avg_price=("price", "mean"),
            price_p25=("price", lambda s: s.quantile(0.25)),
            price_p75=("price", lambda s: s.quantile(0.75)),
            avg_mileage=("mileage", "mean"),
            median_mileage=("mileage", "median"),
            avg_vehicle_age_months=("vehicle_age_months", "mean"),
            avg_miles_per_year=("miles_per_year", "mean"),
            volume=("vin", "count"),
            unique_vins=("vin", "nunique"),
            price_down_rate=("price_down_signal", "mean"),
            body_class=("body_class", mode_or_unknown),
            drive_type=("drive_type", mode_or_unknown),
            fuel_type=("fuel_type", mode_or_unknown),
            electrification_level=("electrification_level", mode_or_unknown),
            dominant_seller_type=("seller_type", mode_or_unknown),
            dominant_source_name=("source_name", mode_or_unknown),
            sentiment_score=("sentiment_score", "mean"),
            sentiment_comment_count=("sentiment_comment_count", "mean"),
            sentiment_video_count=("sentiment_video_count", "mean"),
            engine_hp=("engine_hp", "median"),
            engine_cylinders=("engine_cylinders", "median"),
            total_recalls=("total_recalls", "median"),
            total_complaints=("total_complaints", "median"),
        )
        .reset_index()
    )

    weekly = weekly[weekly["unique_vins"].ge(min_weekly_vins)].copy()
    cohort_sizes = weekly.groupby(COHORT_COLUMNS, dropna=False)["week_start"].transform("nunique")
    weekly = weekly[cohort_sizes.ge(min_cohort_weeks)].copy()
    if weekly.empty:
        raise ValueError(
            "No cohort-week rows survived support filters. Lower --min-weekly-vins "
            "or --min-cohort-weeks, or increase --sample-size."
        )

    weekly = weekly.sort_values(COHORT_COLUMNS + ["week_start"]).reset_index(drop=True)
    market = (
        weekly.groupby("week_start", as_index=False)
        .agg(
            market_median_price=("median_price", "median"),
            market_weekly_volume=("volume", "sum"),
        )
        .sort_values("week_start")
    )
    market["market_price_index"] = (
        market["market_median_price"] / market["market_median_price"].iloc[0]
    )
    weekly = weekly.merge(market, on="week_start", how="left")

    group = weekly.groupby(COHORT_COLUMNS, dropna=False, group_keys=False)
    weekly["month"] = weekly["week_start"].dt.month.astype("int16")
    weekly["quarter"] = weekly["week_start"].dt.quarter.astype("int16")
    weekly["week_of_year"] = weekly["week_start"].dt.isocalendar().week.astype("int16")
    weekly["cohort_week_number"] = group.cumcount().astype("int16")
    first_week = group["week_start"].transform("min")
    weekly["cohort_age_weeks"] = ((weekly["week_start"] - first_week).dt.days / 7).astype("int16")
    weekly["cohort_first_median_price"] = group["median_price"].transform("first")
    weekly["price_index_vs_cohort_first"] = (
        weekly["median_price"] / weekly["cohort_first_median_price"].replace(0, np.nan)
    )
    weekly["cumulative_depreciation_pct"] = weekly["price_index_vs_cohort_first"] - 1

    weekly["lag_median_price_1"] = group["median_price"].shift(1)
    weekly["lag_median_price_2"] = group["median_price"].shift(2)
    weekly["lag_price_index_1"] = group["price_index_vs_cohort_first"].shift(1)
    weekly["rolling_median_price_4"] = group["median_price"].transform(
        lambda s: s.shift(1).rolling(4, min_periods=1).median()
    )
    weekly["rolling_avg_mileage_4"] = group["avg_mileage"].transform(
        lambda s: s.shift(1).rolling(4, min_periods=1).mean()
    )
    weekly["rolling_volume_4"] = group["volume"].transform(
        lambda s: s.shift(1).rolling(4, min_periods=1).mean()
    )
    weekly["rolling_depreciation_pct_4"] = group["median_price"].transform(
        lambda s: s.pct_change().shift(1).rolling(4, min_periods=1).mean()
    )

    for horizon in horizons:
        periods = max(1, int(np.ceil(horizon / 7)))
        future_price = group["median_price"].shift(-periods)
        future_week = group["week_start"].shift(-periods)
        weekly[f"target_week_{horizon}d"] = future_week
        weekly[f"target_median_price_{horizon}d"] = future_price
        weekly[f"target_depreciation_pct_{horizon}d"] = (
            future_price / weekly["median_price"].replace(0, np.nan) - 1
        )

    return weekly


def build_regression_pipeline(
    X: pd.DataFrame,
    y: pd.Series | None = None,
    regressor_params: dict[str, Any] | None = None,
) -> tuple[Pipeline, str]:
    numeric_features = [column for column in NUMERIC_FEATURES if column in X.columns]
    categorical_features = [column for column in CATEGORICAL_FEATURES if column in X.columns]

    preprocessor = ColumnTransformer(
        transformers=[
            ("numeric", Pipeline([("imputer", SimpleImputer(strategy="median"))]), numeric_features),
            (
                "categorical",
                Pipeline(
                    [
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
                ),
                categorical_features,
            ),
        ],
        remainder="drop",
        verbose_feature_names_out=False,
    )

    target_is_constant = y is not None and pd.Series(y).nunique(dropna=True) < 2
    if X.shape[0] < MIN_TEMPORAL_TRAIN_ROWS or target_is_constant:
        regressor = DummyRegressor(strategy="mean")
        model_name = "constant_baseline"
    elif LGBMRegressor is not None:
        params = {
            "objective": "regression",
            "n_estimators": 650,
            "learning_rate": 0.04,
            "num_leaves": 63,
            "min_child_samples": 35,
            "subsample": 0.85,
            "colsample_bytree": 0.85,
            "reg_lambda": 1.0,
            "random_state": RANDOM_STATE,
            "n_jobs": -1,
            "verbose": -1,
        }
        params.update(regressor_params or {})
        regressor = LGBMRegressor(**params)
        model_name = "lightgbm"
    else:
        params = {
            "learning_rate": 0.05,
            "max_iter": 450,
            "l2_regularization": 0.05,
            "random_state": RANDOM_STATE,
        }
        params.update(regressor_params or {})
        regressor = HistGradientBoostingRegressor(**params)
        model_name = "hist_gradient_boosting"

    return Pipeline([("preprocessor", preprocessor), ("model", regressor)]), model_name


def choose_cutoff(weekly: pd.DataFrame, split_date: str | None) -> pd.Timestamp:
    if split_date:
        return pd.Timestamp(split_date)
    return pd.to_datetime(weekly["week_start"]).quantile(0.8)


def split_for_horizon(
    model_df: pd.DataFrame,
    horizon: int,
    cutoff: pd.Timestamp,
    allow_auto_cutoff: bool,
    min_train_rows: int = MIN_TEMPORAL_TRAIN_ROWS,
    min_test_rows: int = MIN_TEMPORAL_TEST_ROWS,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    target_week = pd.to_datetime(model_df[f"target_week_{horizon}d"])
    week_start = pd.to_datetime(model_df["week_start"])
    candidate_cutoffs = [cutoff]
    if allow_auto_cutoff:
        candidate_cutoffs.extend(
            week_start.quantile(q) for q in [0.85, 0.75, 0.65, 0.55, 0.45, 0.35, 0.25, 0.15]
        )
        candidate_cutoffs.extend(pd.to_datetime(model_df["week_start"].drop_duplicates().sort_values()))

    for candidate in candidate_cutoffs:
        candidate = pd.Timestamp(candidate)
        train = model_df[target_week < candidate].copy()
        test = model_df[week_start >= candidate].copy()
        if train.shape[0] >= min_train_rows and test.shape[0] >= min_test_rows:
            return train, test, candidate

    raise ValueError(
        f"Horizon {horizon}d has no leakage-safe temporal validation split with at least "
        f"{min_train_rows} training rows and {min_test_rows} test rows. "
        f"Complete rows: {model_df.shape[0]:,}; source weeks: "
        f"{week_start.min().date()} to {week_start.max().date()}; target weeks: "
        f"{target_week.min().date()} to {target_week.max().date()}."
    )


def stratified_depreciation_tuning_sample_positions(
    frame: pd.DataFrame,
    max_rows: int = DEFAULT_TUNING_SAMPLE_SIZE,
) -> np.ndarray:
    """Return deterministic row positions for representative cohort tuning."""
    frame = frame.reset_index(drop=True)
    row_count = int(frame.shape[0])
    if max_rows <= 0 or row_count <= max_rows:
        return np.arange(row_count)

    strata_cols = [c for c in [*COHORT_COLUMNS, "quarter"] if c in frame.columns]
    if not strata_cols:
        return np.sort(
            np.random.default_rng(RANDOM_STATE).choice(row_count, size=max_rows, replace=False)
        )

    strata = frame[strata_cols].astype("string").fillna("UNKNOWN").agg("|".join, axis=1)
    rng = np.random.default_rng(RANDOM_STATE)
    sample_fraction = max_rows / row_count
    selected: list[np.ndarray] = []

    for positions in strata.groupby(strata, sort=False).groups.values():
        stratum_positions = np.asarray(list(positions), dtype=int)
        take = min(stratum_positions.size, max(1, int(round(stratum_positions.size * sample_fraction))))
        selected.append(rng.choice(stratum_positions, size=take, replace=False))

    sampled = np.unique(np.concatenate(selected)) if selected else np.array([], dtype=int)
    if sampled.size > max_rows:
        sampled = rng.choice(sampled, size=max_rows, replace=False)
    elif sampled.size < max_rows:
        remaining = np.setdiff1d(np.arange(row_count), sampled, assume_unique=False)
        fill_count = min(max_rows - sampled.size, remaining.size)
        if fill_count:
            sampled = np.concatenate([sampled, rng.choice(remaining, size=fill_count, replace=False)])
    return np.sort(sampled.astype(int))


def _depreciation_param_candidates(model_name: str) -> list[dict[str, Any]]:
    if model_name == "lightgbm":
        grid = {
            "n_estimators": [400, 650, 900],
            "learning_rate": [0.025, 0.04, 0.06],
            "num_leaves": [31, 63, 127],
            "min_child_samples": [25, 35, 60],
            "subsample": [0.75, 0.85, 0.95],
            "colsample_bytree": [0.75, 0.85, 0.95],
            "reg_lambda": [0.5, 1.0, 2.0],
        }
    elif model_name == "hist_gradient_boosting":
        grid = {
            "max_iter": [300, 450, 650],
            "learning_rate": [0.03, 0.05, 0.08],
            "max_leaf_nodes": [31, 63],
            "min_samples_leaf": [20, 35, 60],
            "l2_regularization": [0.0, 0.05, 0.2],
        }
    else:
        return []

    return list(
        ParameterSampler(
            grid,
            n_iter=min(DEFAULT_TUNING_CANDIDATES, int(np.prod([len(v) for v in grid.values()]))),
            random_state=RANDOM_STATE,
        )
    )


def tune_depreciation_hyperparameters(
    train: pd.DataFrame,
    feature_columns: list[str],
    target_col: str,
    horizon: int,
    model_name: str,
    max_rows: int = DEFAULT_TUNING_SAMPLE_SIZE,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Tune on a bounded representative sample using an inner temporal holdout."""
    candidates = _depreciation_param_candidates(model_name)
    metadata: dict[str, Any] = {
        "enabled": bool(candidates),
        "max_tuning_rows": int(max_rows),
        "sampling_strategy": "stratified by make, model, model year, trim proxy, and quarter",
        "inner_validation": "temporal holdout within the outer training window",
    }
    if not candidates:
        metadata["reason"] = f"no tuning grid for {model_name}"
        return {}, metadata

    positions = stratified_depreciation_tuning_sample_positions(train, max_rows=max_rows)
    tuning_df = train.iloc[positions].sort_values("week_start").reset_index(drop=True)
    metadata["sampled_rows"] = int(tuning_df.shape[0])

    try:
        inner_cutoff = pd.to_datetime(tuning_df["week_start"]).quantile(0.8)
        inner_train, inner_valid, cutoff = split_for_horizon(
            model_df=tuning_df,
            horizon=horizon,
            cutoff=inner_cutoff,
            allow_auto_cutoff=True,
        )
    except ValueError as exc:
        metadata.update({"enabled": False, "reason": str(exc)})
        return {}, metadata

    best_params: dict[str, Any] = {}
    best_mae = float("inf")
    for params in candidates:
        candidate_model, _ = build_regression_pipeline(
            inner_train[feature_columns],
            inner_train[target_col],
            regressor_params=params,
        )
        candidate_model.fit(inner_train[feature_columns], inner_train[target_col])
        predictions = candidate_model.predict(inner_valid[feature_columns])
        mae = float(mean_absolute_error(inner_valid[target_col], predictions))
        if mae < best_mae:
            best_mae = mae
            best_params = params

    metadata.update(
        {
            "candidate_count": int(len(candidates)),
            "inner_split_date": str(pd.Timestamp(cutoff).date()),
            "inner_train_rows": int(inner_train.shape[0]),
            "inner_validation_rows": int(inner_valid.shape[0]),
            "best_depreciation_mae_pct_points": float(best_mae * 100),
            "best_params": best_params,
            "final_fit": "best hyperparameters are refit on the full outer training split and final complete frame",
        }
    )
    return best_params, metadata


def evaluate_predictions(
    y_true_dep: pd.Series,
    pred_dep: np.ndarray,
    current_price: pd.Series,
    future_price: pd.Series,
) -> dict[str, float]:
    pred_future_price = current_price.to_numpy(dtype="float64") * (1 + pred_dep)
    baseline_future_price = current_price.to_numpy(dtype="float64")
    future_price_values = future_price.to_numpy(dtype="float64")
    model_abs_error = np.abs(future_price_values - pred_future_price)
    baseline_abs_error = np.abs(future_price_values - baseline_future_price)
    model_mae = float(mean_absolute_error(future_price, pred_future_price))
    baseline_mae = float(mean_absolute_error(future_price, baseline_future_price))
    future_price_denominator = float(np.abs(future_price_values).sum())
    return {
        "depreciation_mae_pct_points": float(mean_absolute_error(y_true_dep, pred_dep) * 100),
        "depreciation_rmse_pct_points": float(np.sqrt(mean_squared_error(y_true_dep, pred_dep)) * 100),
        "depreciation_bias_pct_points": float((pred_dep - y_true_dep.to_numpy(dtype="float64")).mean() * 100),
        "depreciation_r2": float(r2_score(y_true_dep, pred_dep)) if len(y_true_dep) >= 2 else float("nan"),
        "future_price_mae": model_mae,
        "future_price_rmse": float(np.sqrt(mean_squared_error(future_price, pred_future_price))),
        "future_price_wape": float(model_abs_error.sum() / future_price_denominator)
        if future_price_denominator
        else float("nan"),
        "future_price_bias": float((pred_future_price - future_price_values).mean()),
        "naive_no_change_future_price_mae": baseline_mae,
        "naive_no_change_future_price_wape": float(baseline_abs_error.sum() / future_price_denominator)
        if future_price_denominator
        else float("nan"),
        "future_price_mae_skill_vs_naive": float(1 - (model_mae / baseline_mae))
        if baseline_mae
        else float("nan"),
    }


def forecast_latest_cohorts(
    latest_features: pd.DataFrame,
    model: Pipeline,
    feature_columns: list[str],
    horizon: int,
) -> pd.DataFrame:
    forecast_frame = latest_features.copy()
    pred_dep = model.predict(forecast_frame[feature_columns])
    observed_price = forecast_frame["median_price"].to_numpy(dtype="float64")
    predicted_price = np.clip(observed_price * (1 + pred_dep), a_min=0, a_max=None)
    observed_week = pd.to_datetime(forecast_frame["week_start"])

    output = forecast_frame[
        COHORT_COLUMNS + ["week_start", "median_price", "unique_vins", "volume"]
    ].copy()
    output = output.rename(
        columns={
            "week_start": "observed_week_start",
            "median_price": "observed_median_price",
        }
    )
    output["forecast_horizon_days"] = int(horizon)
    output["forecast_date"] = observed_week + pd.to_timedelta(int(horizon), unit="D")
    output["predicted_depreciation_pct"] = pred_dep
    output["predicted_median_price"] = predicted_price
    output["forecast_method"] = f"direct_{int(horizon)}d_model"
    return output


def forecast_latest_cohorts_recursive(
    latest_features: pd.DataFrame,
    model: Pipeline,
    feature_columns: list[str],
    step_horizon: int,
    forecast_days: int,
) -> pd.DataFrame:
    working = latest_features.copy()
    observed_week = pd.to_datetime(latest_features["week_start"])
    elapsed_days = 0
    frames: list[pd.DataFrame] = []

    while elapsed_days < forecast_days:
        remaining_days = min(step_horizon, forecast_days - elapsed_days)
        pred_step_dep = model.predict(working[feature_columns])
        if remaining_days != step_horizon:
            pred_dep = np.power(1 + pred_step_dep, remaining_days / step_horizon) - 1
        else:
            pred_dep = pred_step_dep

        current_price = working["median_price"].to_numpy(dtype="float64")
        predicted_price = np.clip(current_price * (1 + pred_dep), a_min=0, a_max=None)
        elapsed_days += remaining_days

        output = latest_features[
            COHORT_COLUMNS + ["week_start", "median_price", "unique_vins", "volume"]
        ].copy()
        output = output.rename(
            columns={
                "week_start": "observed_week_start",
                "median_price": "observed_median_price",
            }
        )
        output["forecast_horizon_days"] = int(elapsed_days)
        output["forecast_date"] = observed_week + pd.to_timedelta(int(elapsed_days), unit="D")
        output["predicted_depreciation_pct"] = (
            predicted_price / output["observed_median_price"].replace(0, np.nan) - 1
        )
        output["predicted_median_price"] = predicted_price
        output["forecast_method"] = f"recursive_{int(step_horizon)}d_model"
        frames.append(output)

        working["median_price"] = predicted_price
        for column in ["avg_price", "price_p25", "price_p75", "lag_median_price_1", "rolling_median_price_4"]:
            if column in working.columns:
                working[column] = predicted_price
        if "price_index_vs_cohort_first" in working.columns:
            working["price_index_vs_cohort_first"] = (
                working["median_price"] / working["cohort_first_median_price"].replace(0, np.nan)
            )
        if "cumulative_depreciation_pct" in working.columns:
            working["cumulative_depreciation_pct"] = working["price_index_vs_cohort_first"] - 1
        working["week_start"] = observed_week + pd.to_timedelta(int(elapsed_days), unit="D")
        working["month"] = working["week_start"].dt.month.astype("int16")
        working["quarter"] = working["week_start"].dt.quarter.astype("int16")
        working["week_of_year"] = working["week_start"].dt.isocalendar().week.astype("int16")
        working["cohort_age_weeks"] = (
            working["cohort_age_weeks"].astype("float64") + remaining_days / 7
        ).round().astype("int16")

    return pd.concat(frames, ignore_index=True)


def train_cohort_models(
    db_path: Path,
    output_dir: Path,
    sample_size: int | None,
    horizons: list[int],
    split_date: str | None,
    min_weekly_vins: int,
    min_cohort_weeks: int,
    max_price: int | None,
) -> dict[str, Any]:
    raw = load_history_frame(db_path, sample_size)
    history = clean_history_frame(raw, max_price=max_price)
    weekly = build_cohort_weekly_frame(
        history,
        horizons=horizons,
        min_weekly_vins=min_weekly_vins,
        min_cohort_weeks=min_cohort_weeks,
    )

    feature_columns = [
        column
        for column in CATEGORICAL_FEATURES + NUMERIC_FEATURES
        if column in weekly.columns
    ]
    cutoff = choose_cutoff(weekly, split_date)
    report: dict[str, Any] = {
        "task": "cohort_depreciation",
        "modeling_approach": (
            "Global supervised time-series model across make/model/year/trim cohorts, "
            "trained to predict future depreciation percentage."
        ),
        "rows_loaded": int(raw.shape[0]),
        "history_rows_after_cleaning": int(history.shape[0]),
        "cohort_week_rows": int(weekly.shape[0]),
        "cohorts": int(weekly[COHORT_COLUMNS].drop_duplicates().shape[0]),
        "split_date": str(cutoff.date()),
        "min_weekly_vins": int(min_weekly_vins),
        "min_cohort_weeks": int(min_cohort_weeks),
        "minimum_model_rows": MIN_MODEL_ROWS,
        "minimum_temporal_train_rows": MIN_TEMPORAL_TRAIN_ROWS,
        "minimum_temporal_test_rows": MIN_TEMPORAL_TEST_ROWS,
        "feature_columns": feature_columns,
        "models": {},
    }

    latest_features = (
        weekly.sort_values("week_start")
        .groupby(COHORT_COLUMNS, dropna=False)
        .tail(1)
        .sort_values(["make", "model", "model_year", "trim_proxy"])
    )
    latest_features[
        COHORT_COLUMNS + ["week_start", "median_price", "unique_vins", "volume"]
    ].to_csv(output_dir / "cohort_latest_observations.csv", index=False)
    weekly.to_csv(output_dir / "cohort_weekly_training_frame.csv", index=False)

    fitted_horizon_models: dict[int, Pipeline] = {}
    for horizon in horizons:
        target_col = f"target_depreciation_pct_{horizon}d"
        future_price_col = f"target_median_price_{horizon}d"
        model_df = weekly.dropna(subset=feature_columns + [target_col, future_price_col]).copy()
        if model_df.shape[0] < MIN_MODEL_ROWS:
            report["models"][target_col] = {
                "skipped": f"fewer than {MIN_MODEL_ROWS} complete cohort-week rows",
                "complete_rows": int(model_df.shape[0]),
            }
            continue

        validation_warning = None
        try:
            train, test, model_cutoff = split_for_horizon(
                model_df=model_df,
                horizon=horizon,
                cutoff=cutoff,
                allow_auto_cutoff=split_date is None,
            )
        except ValueError as exc:
            validation_warning = str(exc)
            train = model_df
            test = pd.DataFrame()
            model_cutoff = pd.NaT

        X_train = train[feature_columns]
        y_train = train[target_col]

        _, model_name = build_regression_pipeline(X_train, y_train)
        tuned_params, tuning_metadata = tune_depreciation_hyperparameters(
            train=train,
            feature_columns=feature_columns,
            target_col=target_col,
            horizon=horizon,
            model_name=model_name,
        )
        model, model_name = build_regression_pipeline(X_train, y_train, tuned_params)
        model.fit(X_train, y_train)
        metrics: dict[str, float] = {}
        if not test.empty:
            X_test = test[feature_columns]
            y_test = test[target_col]
            pred_dep = model.predict(X_test)
            metrics = evaluate_predictions(
                y_true_dep=y_test,
                pred_dep=pred_dep,
                current_price=test["median_price"],
                future_price=test[future_price_col],
            )

        final_model, final_model_name = build_regression_pipeline(
            model_df[feature_columns],
            model_df[target_col],
            tuned_params,
        )
        final_model.fit(model_df[feature_columns], model_df[target_col])

        artifact_name = f"Cohort_Depreciation_{horizon}d.joblib"
        joblib.dump(final_model, output_dir / artifact_name)
        fitted_horizon_models[int(horizon)] = final_model
        report["models"][target_col] = {
            "artifact": artifact_name,
            "validation_model_type": model_name,
            "artifact_model_type": final_model_name,
            "target": target_col,
            "future_price_column": future_price_col,
            "split_date": str(model_cutoff.date()) if not pd.isna(model_cutoff) else None,
            "train_rows": int(train.shape[0]),
            "test_rows": int(test.shape[0]),
            "train_cohorts": int(train[COHORT_COLUMNS].drop_duplicates().shape[0]),
            "test_cohorts": int(test[COHORT_COLUMNS].drop_duplicates().shape[0]) if not test.empty else 0,
            "complete_rows": int(model_df.shape[0]),
            "artifact_training_rows": int(model_df.shape[0]),
            "tuning": tuning_metadata,
            "validation_warning": validation_warning,
            "metrics": metrics,
        }

    future_forecast_frames: list[pd.DataFrame] = []
    for horizon, model in fitted_horizon_models.items():
        future_forecast_frames.append(
            forecast_latest_cohorts(
                latest_features=latest_features,
                model=model,
                feature_columns=feature_columns,
                horizon=horizon,
            )
        )

    max_forecast_days = max([365, *[int(horizon) for horizon in horizons]])
    if fitted_horizon_models and max_forecast_days not in fitted_horizon_models:
        recursive_horizon = min(fitted_horizon_models)
        future_forecast_frames.append(
            forecast_latest_cohorts_recursive(
                latest_features=latest_features,
                model=fitted_horizon_models[recursive_horizon],
                feature_columns=feature_columns,
                step_horizon=recursive_horizon,
                forecast_days=max_forecast_days,
            )
        )

    if future_forecast_frames:
        future_forecasts = pd.concat(future_forecast_frames, ignore_index=True)
        future_forecasts = future_forecasts.sort_values(
            ["forecast_horizon_days", "forecast_method", "make", "model", "model_year", "trim_proxy"]
        )
        forecast_path = output_dir / "cohort_future_forecasts.csv"
        future_forecasts.to_csv(forecast_path, index=False)
        report["future_forecast_output"] = forecast_path.name
        report["future_forecast_rows"] = int(future_forecasts.shape[0])
        report["last_observed_week"] = str(
            pd.to_datetime(future_forecasts["observed_week_start"]).max().date()
        )
        report["max_forecast_date"] = str(pd.to_datetime(future_forecasts["forecast_date"]).max().date())
    else:
        report["future_forecast_output"] = None
        report["future_forecast_rows"] = 0
        report["last_observed_week"] = str(pd.to_datetime(weekly["week_start"]).max().date())
        report["max_forecast_date"] = None

    return report


def write_reports(output_dir: Path, report: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "cohort_depreciation_model_report.json").write_text(
        json.dumps(report, indent=2, default=str),
        encoding="utf-8",
    )
    (output_dir / "cohort_model_features.json").write_text(
        json.dumps(
            {
                "cohort_columns": COHORT_COLUMNS,
                "categorical_features": CATEGORICAL_FEATURES,
                "numeric_features": NUMERIC_FEATURES,
                "feature_columns": report["feature_columns"],
                "target_family": "target_depreciation_pct_{horizon}d",
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    lines = [
        "# Cohort Depreciation Model Report",
        "",
        f"Generated: {report['generated_at']}",
        f"Database: {report['database_path']}",
        f"Rows loaded: {report['rows_loaded']:,}",
        f"Clean history rows: {report['history_rows_after_cleaning']:,}",
        f"Cohort-week rows: {report['cohort_week_rows']:,}",
        f"Cohorts: {report['cohorts']:,}",
        f"Split date: {report['split_date']}",
        f"Future forecast output: {report.get('future_forecast_output') or 'none'}",
        f"Max forecast date: {report.get('max_forecast_date') or 'none'}",
        "",
        "## Design",
        "",
        "- Cohort grain: make, model, model year, and trim proxy.",
        "- Forecast target: future median-price depreciation percentage from the current cohort week.",
        "- Model family: global gradient boosted regression across all retained cohort time series.",
        "- Hyperparameter search: representative bounded cohort-week sample with an inner temporal holdout, followed by refit on the full training frame.",
        "- Baseline: no-change future price, reported beside model future-price MAE.",
        "- Safeguard: temporal validation requires at least two train and two test rows; otherwise validation is skipped and documented.",
        "",
        "## Model Results",
        "",
    ]
    for target, model_report in report["models"].items():
        if "skipped" in model_report:
            lines.append(f"- {target}: skipped ({model_report['skipped']})")
            continue
        if not model_report["metrics"]:
            lines.append(
                "- {target}: trained on {rows:,} complete rows; validation skipped ({warning})".format(
                    target=target,
                    rows=model_report["artifact_training_rows"],
                    warning=model_report["validation_warning"],
                )
            )
            continue
        metrics = model_report["metrics"]
        lines.append(
            "- {target}: dep MAE {dep_mae:.2f} pct pts, future price MAE ${price_mae:,.0f} "
            "(naive ${naive_mae:,.0f}, skill {skill:.1%}), WAPE {wape:.1%}, R2 {r2:.4f}, "
            "train {train_rows:,}, test {test_rows:,}, artifact {artifact_model}".format(
                target=target,
                dep_mae=metrics["depreciation_mae_pct_points"],
                price_mae=metrics["future_price_mae"],
                naive_mae=metrics["naive_no_change_future_price_mae"],
                skill=metrics["future_price_mae_skill_vs_naive"],
                wape=metrics["future_price_wape"],
                r2=metrics["depreciation_r2"],
                train_rows=model_report["train_rows"],
                test_rows=model_report["test_rows"],
                artifact_model=model_report["artifact_model_type"],
            )
        )
    lines.extend(
        [
            "",
            "## Research Notes",
            "",
            "- Used-car valuation is a hedonic pricing problem: vehicle attributes, mileage, age, and market conditions all affect observed price.",
            "- Global forecasting is a good fit because there are many related cohort series, many of them short or sparse.",
            "- Simulation studies of global forecasting show LGBM-style models can be competitive on short, heterogeneous series.",
            "- The Manheim index methodology reinforces the need to control for mileage, mix, outliers, and seasonality in used-vehicle price indices.",
        ]
    )
    (output_dir / "cohort_depreciation_model_report.md").write_text(
        "\n".join(lines),
        encoding="utf-8",
    )


def run_cohort_forecast(
    db_path: Path,
    output_dir: Path,
    sample_size: int | None = DEFAULT_SAMPLE_SIZE,
    horizons: list[int] | None = None,
    split_date: str | None = None,
    min_weekly_vins: int = DEFAULT_MIN_WEEKLY_VINS,
    min_cohort_weeks: int = DEFAULT_MIN_COHORT_WEEKS,
    max_price: int | None = DEFAULT_MAX_PRICE,
) -> dict[str, Any]:
    horizons = horizons or DEFAULT_HORIZONS
    output_dir.mkdir(parents=True, exist_ok=True)
    report = train_cohort_models(
        db_path=db_path,
        output_dir=output_dir,
        sample_size=sample_size,
        horizons=horizons,
        split_date=split_date,
        min_weekly_vins=min_weekly_vins,
        min_cohort_weeks=min_cohort_weeks,
        max_price=max_price,
    )
    report.update(
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "database_path": str(db_path),
            "sample_size": int(sample_size or 0),
            "horizons": horizons,
            "sample_strategy": "recent_vins_full_history" if sample_size else "full_price_history",
            "research_sources": RESEARCH_REFERENCES,
        }
    )
    write_reports(output_dir, report)
    return report


def load_gap_frame(db_path: Path, sample_size: int | None) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Compatibility helper retained for existing gap validation tests."""
    limit = _basic_limit_clause(sample_size)
    sample_cte = f"""
        WITH sample_price AS (
            SELECT vin, history_date, mileage, price, trend
            FROM price_history
            WHERE price IS NOT NULL AND price > 0
            {limit}
        ),
        sample_vins AS (
            SELECT DISTINCT vin FROM sample_price
        )
    """
    conn = sqlite3.connect(str(db_path))
    try:
        price_history = pd.read_sql_query(f"{sample_cte} SELECT * FROM sample_price", conn)
        listing_history = pd.read_sql_query(
            f"""
            {sample_cte}
            SELECT lh.vin, lh.history_date, lh.mileage, lh.price
            FROM listing_history AS lh
            INNER JOIN sample_vins AS sv USING(vin)
            WHERE lh.price IS NOT NULL AND lh.price > 0
            """,
            conn,
        )
        nhtsa = pd.read_sql_query(
            f"""
            {sample_cte}
            SELECT vin, nhtsa_Make, nhtsa_Model, nhtsa_ModelYear, nhtsa_BodyClass,
                   nhtsa_DriveType, nhtsa_FuelTypePrimary
            FROM nhtsa_enrichment AS n
            INNER JOIN sample_vins AS sv USING(vin)
            """,
            conn,
        )
    finally:
        conn.close()
        gc.collect()
    if price_history.empty:
        return pd.DataFrame(), {"overlap_rows": 0}

    price_history["history_date"] = pd.to_datetime(price_history["history_date"], errors="coerce")
    listing_history["history_date"] = pd.to_datetime(listing_history["history_date"], errors="coerce")
    price_history = price_history.dropna(subset=["vin", "history_date", "price"]).sort_values(
        ["vin", "history_date"]
    )
    listing_history = listing_history.dropna(subset=["vin", "history_date", "price"]).sort_values(
        ["vin", "history_date"]
    )

    same_day = price_history.merge(
        listing_history,
        on=["vin", "history_date"],
        how="inner",
        suffixes=("_observed", "_listing"),
    )
    validation = {
        "price_history_rows": int(price_history.shape[0]),
        "listing_history_rows_for_price_vins": int(listing_history.shape[0]),
        "same_day_overlap_rows": int(same_day.shape[0]),
        "same_day_same_price_rate": float((same_day["price_observed"] == same_day["price_listing"]).mean())
        if not same_day.empty
        else 0.0,
    }

    price_asof = price_history.rename(columns={"history_date": "observed_date"}).sort_values("observed_date")
    listing_asof = listing_history.rename(columns={"history_date": "listing_date"}).sort_values("listing_date")
    matched = pd.merge_asof(
        price_asof,
        listing_asof,
        by="vin",
        left_on="observed_date",
        right_on="listing_date",
        direction="backward",
        tolerance=pd.Timedelta(days=7),
        suffixes=("_observed", "_listing"),
    )
    matched = matched.dropna(subset=["price_listing"]).copy()
    matched = matched.merge(nhtsa, on="vin", how="left")
    matched["absolute_gap"] = matched["price_listing"] - matched["price_observed"]
    matched["percent_gap"] = matched["absolute_gap"] / matched["price_listing"].replace(0, np.nan)
    matched["days_between_listing_and_observed"] = (
        matched["observed_date"] - matched["listing_date"]
    ).dt.days
    validation["matched_gap_rows"] = int(matched.shape[0])
    validation["gap_label"] = (
        "duplicate_like_price_trajectory"
        if validation["same_day_overlap_rows"] > 0 and validation["same_day_same_price_rate"] >= 0.95
        else "listing_vs_observed_spread"
    )
    return matched, validation


def main() -> None:
    args = parse_args()
    horizons = [int(value.strip()) for value in args.horizons.split(",") if value.strip()]
    report = run_cohort_forecast(
        db_path=Path(args.db_path).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        sample_size=args.sample_size,
        horizons=horizons,
        split_date=args.split_date,
        min_weekly_vins=args.min_weekly_vins,
        min_cohort_weeks=args.min_cohort_weeks,
        max_price=args.max_price or None,
    )
    print(f"Saved report to {Path(args.output_dir) / 'cohort_depreciation_model_report.json'}")
    for target, model_report in report["models"].items():
        if "skipped" in model_report:
            print(f"{target}: skipped ({model_report['skipped']})")
        else:
            metrics = model_report["metrics"]
            print(
                f"{target}: dep MAE {metrics['depreciation_mae_pct_points']:.2f} pct pts, "
                f"future price MAE ${metrics['future_price_mae']:,.0f}"
            )


if __name__ == "__main__":
    main()
