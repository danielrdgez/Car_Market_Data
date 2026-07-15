"""
Train canonical make/model/year/trim cohort depreciation forecasts from price history.

The model grain is a monthly cohort, not an individual VIN. Cohorts are built
from the VIN-keyed canonical vehicle identity selected from listing titles.
The script trains global forecasting models across all cohorts so sparse or
newer cohorts can borrow signal from richer related cohorts.
"""

from __future__ import annotations

import argparse
import contextlib
import gc
import io
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

try:
    from statsmodels.tsa.statespace.sarimax import SARIMAX
except ImportError:  # pragma: no cover - optional time-series dependency
    SARIMAX = None

try:
    from prophet import Prophet
except ImportError:  # pragma: no cover - optional time-series dependency
    Prophet = None

try:
    import timesfm
except ImportError:  # pragma: no cover - optional foundation-model dependency
    timesfm = None


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "CAR_DATA_OUTPUT" / "CAR_DATA_CLEANED.db"
OUTPUT_DIR = BASE_DIR / "MODELS_OUTPUT"

RANDOM_STATE = 42
DEFAULT_SAMPLE_SIZE = 250_000
DEFAULT_TUNING_SAMPLE_SIZE = 200_000
DEFAULT_TUNING_CANDIDATES = 8
DEFAULT_TARGET_MONTHS = [1]
DEFAULT_FORECAST_MONTHS = 60
DEFAULT_MIN_MONTHLY_VINS = 1
DEFAULT_MIN_COHORT_MONTHS = 3
DEFAULT_MAX_PRICE = 0
DEFAULT_TIME_SERIES_MODELS = ["global_ml", "sarimax", "prophet", "timesfm"]
MIN_ROLLING_HISTORY_MONTHS = 6
DEFAULT_MAX_LOCAL_MODEL_COHORTS = 250
DEFAULT_TIMESFM_MODEL_ID = "google/timesfm-2.5-200m-pytorch"
MIN_MODEL_ROWS = 50
MIN_TEMPORAL_TRAIN_ROWS = 2
MIN_TEMPORAL_TEST_ROWS = 2
MIN_LOCAL_MODEL_MONTHS = 6
MIN_SEASONAL_MODEL_MONTHS = 18
MAX_FEATURE_IMPORTANCE_ROWS = 40
FEATURE_IMPORTANCE_REPORT_ROWS = 30
TIMESFM_BATCH_SIZE = 32

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
    "cohort_month_number",
    "cohort_age_months",
    "cohort_first_median_price",
    "price_index_vs_cohort_first",
    "cumulative_depreciation_pct",
    "lag_median_price_1",
    "lag_median_price_2",
    "lag_price_index_1",
    "rolling_median_price_3m",
    "rolling_avg_mileage_3m",
    "rolling_volume_3m",
    "rolling_depreciation_pct_3m",
    "market_median_price",
    "market_price_index",
    "market_monthly_volume",
    "sentiment_score",
    "sentiment_comment_count",
    "sentiment_video_count",
    "engine_hp",
    "engine_cylinders",
    "total_recalls",
    "total_complaints",
]

PRICE_LEAKAGE_FEATURE_COLUMNS = {
    "nhtsa_BasePrice",
    "nhtsa_BasePrice_source",
}

RESEARCH_REFERENCES = [
    {
        "title": "Forecasting: Principles and Practice — Time series cross-validation",
        "url": "https://otexts.com/fpp3/tscv.html",
        "reason": "Supports rolling forecasting-origin evaluation with training data restricted to observations available at each origin.",
    },
    {
        "title": "Out-of-sample tests of forecasting accuracy: An analysis and review",
        "url": "https://doi.org/10.1016/S0169-2070(00)00065-0",
        "reason": "Supports multiple rolling origins and explicit out-of-sample backtest design.",
    },
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
    {
        "title": "statsmodels SARIMAX API",
        "url": "https://www.statsmodels.org/stable/generated/statsmodels.tsa.statespace.sarimax.SARIMAX.html",
        "reason": "Defines the seasonal ARIMA with exogenous-regression implementation used as a statistical local baseline.",
    },
    {
        "title": "Prophet Quick Start",
        "url": "https://facebook.github.io/prophet/docs/quick_start.html",
        "reason": "Defines Prophet's ds/y monthly forecasting interface used for interpretable cohort-level forecasts.",
    },
    {
        "title": "Google Research TimesFM",
        "url": "https://github.com/google-research/timesfm",
        "reason": "Defines TimesFM 2.5 package usage and pretrained checkpoint loading for zero-shot foundation-model forecasts.",
    },
    {
        "title": "Hugging Face google/timesfm-2.5-200m-pytorch",
        "url": "https://huggingface.co/google/timesfm-2.5-200m-pytorch",
        "reason": "Records the default TimesFM checkpoint used by the optional foundation-model benchmark.",
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
        "--target-months",
        default=",".join(str(value) for value in DEFAULT_TARGET_MONTHS),
        help=(
            "Comma-separated direct training targets in months. The default one-month "
            "target is recursively rolled forward for the full forecast window."
        ),
    )
    parser.add_argument(
        "--forecast-months",
        type=int,
        default=DEFAULT_FORECAST_MONTHS,
        help="Number of future monthly points to emit for each latest cohort.",
    )
    parser.add_argument(
        "--split-date",
        default=None,
        help="Validation cutoff by cohort month. Defaults to the 80th percentile month.",
    )
    parser.add_argument(
        "--min-monthly-vins",
        type=int,
        default=DEFAULT_MIN_MONTHLY_VINS,
        help="Minimum distinct VINs required in a cohort-month.",
    )
    parser.add_argument(
        "--min-cohort-months",
        type=int,
        default=DEFAULT_MIN_COHORT_MONTHS,
        help="Minimum retained months required per cohort.",
    )
    parser.add_argument(
        "--max-price",
        type=int,
        default=DEFAULT_MAX_PRICE,
        help=(
            "Drop prices above this value only for explicit sensitivity runs. "
            "The default 0 disables the global cap so high-value cohorts keep "
            "their full observed history."
        ),
    )
    parser.add_argument(
        "--time-series-models",
        default=",".join(DEFAULT_TIME_SERIES_MODELS),
        help=(
            "Comma-separated model families to run. Supported values: "
            "global_ml, sarimax, prophet, timesfm. Use global_ml only for the "
            "legacy global supervised model."
        ),
    )
    parser.add_argument(
        "--max-local-model-cohorts",
        type=int,
        default=DEFAULT_MAX_LOCAL_MODEL_COHORTS,
        help=(
            "Maximum cohorts fit by local/statistical models in a default run. "
            "Use 0 to run all eligible cohorts; the cap keeps iteration bounded."
        ),
    )
    parser.add_argument(
        "--timesfm-model-id",
        default=DEFAULT_TIMESFM_MODEL_ID,
        help="Hugging Face model ID used by the TimesFM forecaster.",
    )
    parser.add_argument(
        "--min-rolling-history-months",
        type=int,
        default=MIN_ROLLING_HISTORY_MONTHS,
        help="Minimum canonical-cohort months before a rolling backtest origin.",
    )
    return parser.parse_args()


def parse_model_families(value: str | list[str] | None) -> list[str]:
    supported = set(DEFAULT_TIME_SERIES_MODELS)
    if value is None:
        return DEFAULT_TIME_SERIES_MODELS.copy()
    if isinstance(value, str):
        tokens = [token.strip().lower() for token in value.split(",") if token.strip()]
    else:
        tokens = [str(token).strip().lower() for token in value if str(token).strip()]
    if not tokens:
        return DEFAULT_TIME_SERIES_MODELS.copy()
    unknown = sorted(set(tokens) - supported)
    if unknown:
        raise ValueError(f"Unsupported time-series model family: {', '.join(unknown)}")
    return list(dict.fromkeys(tokens))


def _basic_limit_clause(sample_size: int | None) -> str:
    if sample_size and sample_size > 0:
        return f"LIMIT {int(sample_size)}"
    return ""


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    if not _table_exists(conn, table):
        return set()
    return {row[1] for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()}


def _select_or_null(columns: set[str], table_alias: str, column: str, output_alias: str | None = None) -> str:
    alias = output_alias or column
    if column in columns:
        return f"{table_alias}.{column} AS {alias}"
    return f"NULL AS {alias}"


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

    conn = sqlite3.connect(str(db_path))
    try:
        listing_cols = _table_columns(conn, "listings")
        nhtsa_cols = _table_columns(conn, "nhtsa_enrichment")
        has_sentiment = _table_exists(conn, "vehicle_sentiment")
        has_vehicle_identity = _table_exists(conn, "vehicle_identity")

        latest_listing_columns = [
            "l.vin AS vin",
            _select_or_null(listing_cols, "l", "title"),
            _select_or_null(listing_cols, "l", "vehicleTitle"),
            _select_or_null(listing_cols, "l", "vehicleTitleDesc"),
            _select_or_null(listing_cols, "l", "title_trim"),
            _select_or_null(listing_cols, "l", "trim_combined"),
            _select_or_null(listing_cols, "l", "trim_source"),
            _select_or_null(listing_cols, "l", "canonical_make"),
            _select_or_null(listing_cols, "l", "canonical_model"),
            _select_or_null(listing_cols, "l", "canonical_year"),
            _select_or_null(listing_cols, "l", "canonical_trim"),
            _select_or_null(listing_cols, "l", "canonical_trim_source"),
            _select_or_null(listing_cols, "l", "canonical_match_confidence"),
            _select_or_null(listing_cols, "l", "epa_vehicle_id"),
            _select_or_null(listing_cols, "l", "normalization_version"),
            _select_or_null(listing_cols, "l", "nhtsa_year_agrees"),
            _select_or_null(listing_cols, "l", "nhtsa_make_agrees"),
            _select_or_null(listing_cols, "l", "nhtsa_model_agrees"),
            _select_or_null(listing_cols, "l", "nhtsa_trim_agrees"),
            _select_or_null(listing_cols, "l", "sellerType"),
            _select_or_null(listing_cols, "l", "sourceName"),
            _select_or_null(listing_cols, "l", "listingType"),
        ]
        latest_listing_order = [
            f"l.{column} DESC" for column in ["loaddate", "date"] if column in listing_cols
        ]
        latest_listing_order.append("l.rowid DESC")
        nhtsa_select_columns = [
            _select_or_null(nhtsa_cols, "n", "nhtsa_Make"),
            _select_or_null(nhtsa_cols, "n", "nhtsa_Model"),
            _select_or_null(nhtsa_cols, "n", "nhtsa_ModelYear"),
            _select_or_null(nhtsa_cols, "n", "nhtsa_Trim"),
            _select_or_null(nhtsa_cols, "n", "nhtsa_Trim2"),
            _select_or_null(nhtsa_cols, "n", "nhtsa_trim_combined"),
            _select_or_null(nhtsa_cols, "n", "nhtsa_BodyClass"),
            _select_or_null(nhtsa_cols, "n", "nhtsa_DriveType"),
            _select_or_null(nhtsa_cols, "n", "nhtsa_FuelTypePrimary"),
            _select_or_null(nhtsa_cols, "n", "nhtsa_ElectrificationLevel"),
            _select_or_null(nhtsa_cols, "n", "nhtsa_EngineHP"),
            _select_or_null(nhtsa_cols, "n", "nhtsa_EngineCylinders"),
            _select_or_null(nhtsa_cols, "n", "nhtsa_total_recalls"),
            _select_or_null(nhtsa_cols, "n", "nhtsa_total_complaints"),
        ]
        sentiment_select_columns = (
            [
                "vs.sentiment_score AS sentiment_score",
                "vs.comment_count AS sentiment_comment_count",
                "vs.video_count AS sentiment_video_count",
            ]
            if has_sentiment
            else [
                "NULL AS sentiment_score",
                "NULL AS sentiment_comment_count",
                "NULL AS sentiment_video_count",
            ]
        )
        sentiment_join = (
            """
        LEFT JOIN vehicle_sentiment AS vs
            ON LOWER(vs.make) = LOWER(n.nhtsa_Make)
           AND LOWER(vs.model) = LOWER(n.nhtsa_Model)
           AND vs.year = n.nhtsa_ModelYear
            """
            if has_sentiment
            else ""
        )
        identity_select_columns = (
            [
                "vi.canonical_make AS canonical_make",
                "vi.canonical_model AS canonical_model",
                "vi.canonical_year AS canonical_year",
                "vi.canonical_trim AS canonical_trim",
                "vi.canonical_trim_source AS canonical_trim_source",
                "vi.canonical_match_confidence AS canonical_match_confidence",
                "vi.epa_vehicle_id AS epa_vehicle_id",
                "vi.normalization_version AS normalization_version",
                "vi.nhtsa_year_agrees AS nhtsa_year_agrees",
                "vi.nhtsa_make_agrees AS nhtsa_make_agrees",
                "vi.nhtsa_model_agrees AS nhtsa_model_agrees",
                "vi.nhtsa_trim_agrees AS nhtsa_trim_agrees",
            ]
            if has_vehicle_identity
            else [
                "ll.canonical_make AS canonical_make",
                "ll.canonical_model AS canonical_model",
                "ll.canonical_year AS canonical_year",
                "ll.canonical_trim AS canonical_trim",
                "ll.canonical_trim_source AS canonical_trim_source",
                "ll.canonical_match_confidence AS canonical_match_confidence",
                "ll.epa_vehicle_id AS epa_vehicle_id",
                "ll.normalization_version AS normalization_version",
                "ll.nhtsa_year_agrees AS nhtsa_year_agrees",
                "ll.nhtsa_make_agrees AS nhtsa_make_agrees",
                "ll.nhtsa_model_agrees AS nhtsa_model_agrees",
                "ll.nhtsa_trim_agrees AS nhtsa_trim_agrees",
            ]
        )
        identity_join = (
            "LEFT JOIN vehicle_identity AS vi ON ph.vin = vi.vin"
            if has_vehicle_identity
            else ""
        )

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
                        {', '.join(latest_listing_columns)},
                        ROW_NUMBER() OVER (
                            PARTITION BY l.vin
                            ORDER BY {', '.join(latest_listing_order)}
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
                ll.title_trim,
                ll.trim_combined,
                ll.trim_source,
                ll.sellerType,
                ll.sourceName,
                ll.listingType,
                {', '.join(identity_select_columns)},
                {', '.join(nhtsa_select_columns + sentiment_select_columns)}
            FROM sample_history AS ph
            LEFT JOIN latest_listing AS ll
                ON ph.vin = ll.vin
            LEFT JOIN nhtsa_enrichment AS n
                ON ph.vin = n.vin
            {identity_join}
            {sentiment_join}
        """
        return pd.read_sql_query(query, conn)
    finally:
        conn.close()


def clean_history_frame(df: pd.DataFrame, max_price: int | None) -> pd.DataFrame:
    if df.empty:
        raise ValueError("No usable rows were returned from price_history.")

    history = df.copy()
    history["history_date"] = pd.to_datetime(history["history_date"], errors="coerce")
    history["price"] = pd.to_numeric(history["price"], errors="coerce")
    history["mileage"] = pd.to_numeric(history["mileage"], errors="coerce")
    required_identity = {"canonical_make", "canonical_model", "canonical_year", "canonical_trim"}
    missing_identity = sorted(required_identity - set(history.columns))
    if missing_identity:
        raise ValueError("Cleaned database lacks canonical identity columns: " + ", ".join(missing_identity))
    history["model_year"] = pd.to_numeric(history["canonical_year"], errors="coerce")

    history = history.dropna(subset=["vin", "history_date", "price", "model_year"])
    history = history[history["price"].between(1_000, max_price or np.inf)]
    history = history.sort_values(["vin", "history_date"]).reset_index(drop=True)

    history["mileage"] = history.groupby("vin", observed=True)["mileage"].ffill()
    history["mileage"] = history.groupby("vin", observed=True)["mileage"].bfill()
    history = history.dropna(subset=["mileage"])
    history = history[history["mileage"].ge(0)]

    history["make"] = history["canonical_make"].map(normalize_label)
    history["model"] = history["canonical_model"].map(normalize_label)
    history["model_year"] = history["model_year"].astype("int16")
    history = history[history["make"].ne("UNKNOWN") & history["model"].ne("UNKNOWN")]

    history["trim_proxy"] = history["canonical_trim"].map(
        lambda value: normalize_label(value, "UNKNOWN_TRIM").replace(" ", "_")
    )
    expected_trim = history["canonical_trim"].map(
        lambda value: normalize_label(value, "UNKNOWN_TRIM").replace(" ", "_")
    )
    if not history["trim_proxy"].equals(expected_trim):
        raise AssertionError("Time-series cohorts must use canonical_trim only")

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
    history["month_start"] = history["history_date"].dt.to_period("M").dt.start_time
    return history


def build_cohort_monthly_frame(
    history: pd.DataFrame,
    target_months: list[int],
    min_monthly_vins: int,
    min_cohort_months: int,
) -> pd.DataFrame:
    group_cols = COHORT_COLUMNS + ["month_start"]

    monthly = (
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

    monthly = monthly[monthly["unique_vins"].ge(min_monthly_vins)].copy()
    cohort_sizes = monthly.groupby(COHORT_COLUMNS, dropna=False)["month_start"].transform("nunique")
    monthly = monthly[cohort_sizes.ge(min_cohort_months)].copy()
    if monthly.empty:
        raise ValueError(
            "No cohort-month rows survived support filters. Lower --min-monthly-vins "
            "or --min-cohort-months, or increase --sample-size."
        )

    monthly = monthly.sort_values(COHORT_COLUMNS + ["month_start"]).reset_index(drop=True)
    market = (
        monthly.groupby("month_start", as_index=False)
        .agg(
            market_median_price=("median_price", "median"),
            market_monthly_volume=("volume", "sum"),
        )
        .sort_values("month_start")
    )
    market["market_price_index"] = (
        market["market_median_price"] / market["market_median_price"].iloc[0]
    )
    monthly = monthly.merge(market, on="month_start", how="left")

    group = monthly.groupby(COHORT_COLUMNS, dropna=False, group_keys=False)
    monthly["month"] = monthly["month_start"].dt.month.astype("int16")
    monthly["quarter"] = monthly["month_start"].dt.quarter.astype("int16")
    monthly["cohort_month_number"] = group.cumcount().astype("int16")
    first_month = group["month_start"].transform("min")
    monthly["cohort_age_months"] = (
        (monthly["month_start"].dt.year - first_month.dt.year) * 12
        + (monthly["month_start"].dt.month - first_month.dt.month)
    ).astype("int16")
    monthly["cohort_first_median_price"] = group["median_price"].transform("first")
    monthly["price_index_vs_cohort_first"] = (
        monthly["median_price"] / monthly["cohort_first_median_price"].replace(0, np.nan)
    )
    monthly["cumulative_depreciation_pct"] = monthly["price_index_vs_cohort_first"] - 1

    monthly["lag_median_price_1"] = group["median_price"].shift(1)
    monthly["lag_median_price_2"] = group["median_price"].shift(2)
    monthly["lag_price_index_1"] = group["price_index_vs_cohort_first"].shift(1)
    monthly["rolling_median_price_3m"] = group["median_price"].transform(
        lambda s: s.shift(1).rolling(3, min_periods=1).median()
    )
    monthly["rolling_avg_mileage_3m"] = group["avg_mileage"].transform(
        lambda s: s.shift(1).rolling(3, min_periods=1).mean()
    )
    monthly["rolling_volume_3m"] = group["volume"].transform(
        lambda s: s.shift(1).rolling(3, min_periods=1).mean()
    )
    monthly["rolling_depreciation_pct_3m"] = group["median_price"].transform(
        lambda s: s.pct_change().shift(1).rolling(3, min_periods=1).mean()
    )

    for horizon in target_months:
        periods = max(1, int(horizon))
        future_price = group["median_price"].shift(-periods)
        future_month = group["month_start"].shift(-periods)
        monthly[f"target_month_start_{horizon}m"] = future_month
        monthly[f"target_median_price_{horizon}m"] = future_price
        monthly[f"target_depreciation_pct_{horizon}m"] = (
            future_price / monthly["median_price"].replace(0, np.nan) - 1
        )

    return monthly


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


def choose_cutoff(monthly: pd.DataFrame, split_date: str | None) -> pd.Timestamp:
    if split_date:
        return pd.Timestamp(split_date)
    return pd.to_datetime(monthly["month_start"]).quantile(0.8)


def split_for_horizon(
    model_df: pd.DataFrame,
    horizon: int,
    cutoff: pd.Timestamp,
    allow_auto_cutoff: bool,
    min_train_rows: int = MIN_TEMPORAL_TRAIN_ROWS,
    min_test_rows: int = MIN_TEMPORAL_TEST_ROWS,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.Timestamp]:
    target_month = pd.to_datetime(model_df[f"target_month_start_{horizon}m"])
    month_start = pd.to_datetime(model_df["month_start"])
    candidate_cutoffs = [cutoff]
    if allow_auto_cutoff:
        candidate_cutoffs.extend(
            month_start.quantile(q) for q in [0.85, 0.75, 0.65, 0.55, 0.45, 0.35, 0.25, 0.15]
        )
        candidate_cutoffs.extend(pd.to_datetime(model_df["month_start"].drop_duplicates().sort_values()))

    for candidate in candidate_cutoffs:
        candidate = pd.Timestamp(candidate)
        train = model_df[target_month < candidate].copy()
        test = model_df[month_start >= candidate].copy()
        if train.shape[0] >= min_train_rows and test.shape[0] >= min_test_rows:
            return train, test, candidate

    raise ValueError(
        f"Horizon {horizon}m has no leakage-safe temporal validation split with at least "
        f"{min_train_rows} training rows and {min_test_rows} test rows. "
        f"Complete rows: {model_df.shape[0]:,}; source months: "
        f"{month_start.min().date()} to {month_start.max().date()}; target months: "
        f"{target_month.min().date()} to {target_month.max().date()}."
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
    tuning_df = train.iloc[positions].sort_values("month_start").reset_index(drop=True)
    metadata["sampled_rows"] = int(tuning_df.shape[0])

    try:
        inner_cutoff = pd.to_datetime(tuning_df["month_start"]).quantile(0.8)
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


def _cohort_id_frame(frame: pd.DataFrame) -> pd.Series:
    return frame[COHORT_COLUMNS].astype("string").fillna("UNKNOWN").agg("|".join, axis=1)


def _cohort_key_from_row(row: pd.Series) -> tuple[Any, ...]:
    return tuple(row[column] for column in COHORT_COLUMNS)


def _cohort_price_series(group: pd.DataFrame) -> pd.Series:
    series = (
        group.sort_values("month_start")
        .set_index("month_start")["median_price"]
        .astype("float64")
    )
    series.index = pd.to_datetime(series.index)
    series = series[~series.index.duplicated(keep="last")]
    series = series.asfreq("MS")
    series = series.interpolate(limit_direction="both")
    return series.dropna()


def _forecast_output_from_predictions(
    cohort_row: pd.Series,
    predictions: np.ndarray,
    forecast_method: str,
    model_family: str,
) -> pd.DataFrame:
    predictions = np.clip(np.asarray(predictions, dtype="float64"), a_min=0, a_max=None)
    observed_month = pd.Timestamp(cohort_row["month_start"])
    observed_price = float(cohort_row["median_price"])
    output = pd.DataFrame([cohort_row[COHORT_COLUMNS + ["month_start", "median_price", "unique_vins", "volume"]].to_dict()] * len(predictions))
    output = output.rename(
        columns={
            "month_start": "observed_month_start",
            "median_price": "observed_median_price",
        }
    )
    output["forecast_month"] = np.arange(1, len(predictions) + 1, dtype=int)
    output["forecast_date"] = [
        observed_month + pd.DateOffset(months=int(month)) for month in output["forecast_month"]
    ]
    output["predicted_median_price"] = predictions
    output["predicted_depreciation_pct"] = (
        output["predicted_median_price"] / observed_price - 1 if observed_price else np.nan
    )
    output["forecast_method"] = forecast_method
    output["model_family"] = model_family
    output["cohort_id"] = _cohort_id_frame(output)
    return output


def supervised_backtest_rows(
    test: pd.DataFrame,
    pred_dep: np.ndarray,
    horizon: int,
    target_col: str,
    future_price_col: str,
    model_key: str,
    model_family: str,
    forecast_method: str,
) -> pd.DataFrame:
    rows = test[COHORT_COLUMNS + ["month_start", "median_price", "unique_vins", "volume"]].copy()
    rows = rows.rename(
        columns={
            "month_start": "origin_month_start",
            "median_price": "observed_median_price",
        }
    )
    rows["forecast_month"] = int(horizon)
    rows["forecast_date"] = pd.to_datetime(rows["origin_month_start"]) + pd.DateOffset(months=int(horizon))
    rows["actual_median_price"] = pd.to_numeric(test[future_price_col], errors="coerce").to_numpy(dtype="float64")
    rows["actual_depreciation_pct"] = pd.to_numeric(test[target_col], errors="coerce").to_numpy(dtype="float64")
    rows["predicted_depreciation_pct"] = np.asarray(pred_dep, dtype="float64")
    rows["predicted_median_price"] = (
        rows["observed_median_price"].to_numpy(dtype="float64") * (1 + rows["predicted_depreciation_pct"])
    )
    rows["model_key"] = model_key
    rows["model_family"] = model_family
    rows["forecast_method"] = forecast_method
    rows["cohort_id"] = _cohort_id_frame(rows)
    rows["absolute_error"] = (rows["actual_median_price"] - rows["predicted_median_price"]).abs()
    rows["naive_absolute_error"] = (rows["actual_median_price"] - rows["observed_median_price"]).abs()
    return rows


def eligible_rolling_origins(
    monthly: pd.DataFrame,
    horizon: int,
    min_history_months: int = MIN_ROLLING_HISTORY_MONTHS,
) -> list[pd.Timestamp]:
    """Return origins with prior canonical-cohort history and observable targets.

    The origin itself is an observed month. Eligibility is calculated per cohort,
    then reduced to the distinct origins used to refit global models.
    """
    target_col = f"target_median_price_{int(horizon)}m"
    required = set(COHORT_COLUMNS + ["month_start", target_col])
    if not required.issubset(monthly.columns):
        return []
    ranked = monthly.sort_values(COHORT_COLUMNS + ["month_start"]).copy()
    ranked["_history_months"] = ranked.groupby(COHORT_COLUMNS, dropna=False).cumcount() + 1
    eligible = ranked[
        ranked["_history_months"].ge(int(min_history_months))
        & ranked[target_col].notna()
    ]
    return sorted(pd.to_datetime(eligible["month_start"], errors="coerce").dropna().unique())


def rolling_origin_metadata(
    rows: pd.DataFrame,
    origin: pd.Timestamp,
    train_start: pd.Timestamp,
    min_history_months: int,
) -> pd.DataFrame:
    """Append auditable rolling-origin metadata without changing legacy columns."""
    if rows.empty:
        return rows
    rows = rows.copy()
    rows["backtest_scheme"] = "expanding_rolling_origin"
    rows["rolling_origin_month"] = pd.Timestamp(origin)
    rows["train_window_start"] = pd.Timestamp(train_start)
    rows["train_window_end"] = pd.Timestamp(origin)
    rows["minimum_history_months"] = int(min_history_months)
    return rows


def rolling_origin_global_backtest(
    model_df: pd.DataFrame,
    feature_columns: list[str],
    target_col: str,
    future_price_col: str,
    horizon: int,
    tuned_params: dict[str, Any] | None,
    min_history_months: int = MIN_ROLLING_HISTORY_MONTHS,
) -> pd.DataFrame:
    """Refit the global model at each eligible origin without future leakage."""
    rows: list[pd.DataFrame] = []
    origins = eligible_rolling_origins(model_df, horizon, min_history_months)
    if not origins:
        return pd.DataFrame()
    first_month = pd.to_datetime(model_df["month_start"], errors="coerce").min()
    for origin in origins:
        origin = pd.Timestamp(origin)
        train_end = origin - pd.DateOffset(months=int(horizon))
        train = model_df[pd.to_datetime(model_df["month_start"], errors="coerce") <= train_end]
        test = model_df[pd.to_datetime(model_df["month_start"], errors="coerce").eq(origin)].copy()
        if train.shape[0] < MIN_TEMPORAL_TRAIN_ROWS or test.empty:
            continue
        # A cohort must have six observed months at the origin, not merely six
        # rows elsewhere in the global panel.
        prior = model_df[pd.to_datetime(model_df["month_start"], errors="coerce") <= origin]
        history_counts = prior.groupby(COHORT_COLUMNS, dropna=False).size().rename("_history_months")
        test = test.join(history_counts, on=COHORT_COLUMNS)
        test = test[test["_history_months"].ge(int(min_history_months))].drop(columns="_history_months")
        if test.empty:
            continue
        model, _ = build_regression_pipeline(train[feature_columns], train[target_col], tuned_params)
        model.fit(train[feature_columns], train[target_col])
        pred_dep = model.predict(test[feature_columns])
        scored = supervised_backtest_rows(
            test=test,
            pred_dep=pred_dep,
            horizon=horizon,
            target_col=target_col,
            future_price_col=future_price_col,
            model_key=target_col,
            model_family="global_ml",
            forecast_method="recursive_global_ml_model",
        )
        rows.append(rolling_origin_metadata(scored, origin, first_month, min_history_months))
    return pd.concat(rows, ignore_index=True) if rows else pd.DataFrame()


def local_backtest_rows(
    cohort_row: pd.Series,
    origin_month: pd.Timestamp,
    observed_price: float,
    predictions: np.ndarray,
    actual_by_month: dict[pd.Timestamp, float],
    target_months: list[int],
    model_key: str,
    model_family: str,
    forecast_method: str,
) -> pd.DataFrame:
    rows = []
    predictions = np.asarray(predictions, dtype="float64")
    for horizon in target_months:
        if horizon <= 0 or horizon > len(predictions):
            continue
        forecast_date = origin_month + pd.DateOffset(months=int(horizon))
        actual_price = actual_by_month.get(pd.Timestamp(forecast_date))
        if actual_price is None or pd.isna(actual_price):
            continue
        predicted_price = float(np.clip(predictions[horizon - 1], a_min=0, a_max=None))
        actual_dep = actual_price / observed_price - 1 if observed_price else np.nan
        pred_dep = predicted_price / observed_price - 1 if observed_price else np.nan
        row = cohort_row[COHORT_COLUMNS + ["unique_vins", "volume"]].to_dict()
        row.update(
            {
                "origin_month_start": origin_month,
                "observed_median_price": observed_price,
                "forecast_month": int(horizon),
                "forecast_date": forecast_date,
                "actual_median_price": float(actual_price),
                "actual_depreciation_pct": actual_dep,
                "predicted_median_price": predicted_price,
                "predicted_depreciation_pct": pred_dep,
                "model_key": model_key,
                "model_family": model_family,
                "forecast_method": forecast_method,
            }
        )
        rows.append(row)
    out = pd.DataFrame(rows)
    if out.empty:
        return out
    out["cohort_id"] = _cohort_id_frame(out)
    out["absolute_error"] = (out["actual_median_price"] - out["predicted_median_price"]).abs()
    out["naive_absolute_error"] = (out["actual_median_price"] - out["observed_median_price"]).abs()
    return out


def metrics_from_backtest_rows(rows: pd.DataFrame) -> dict[str, float]:
    rows = rows.dropna(
        subset=[
            "actual_median_price",
            "predicted_median_price",
            "observed_median_price",
            "actual_depreciation_pct",
            "predicted_depreciation_pct",
        ]
    )
    if rows.empty:
        return {}
    actual_price = rows["actual_median_price"].to_numpy(dtype="float64")
    predicted_price = rows["predicted_median_price"].to_numpy(dtype="float64")
    observed_price = rows["observed_median_price"].to_numpy(dtype="float64")
    actual_dep = rows["actual_depreciation_pct"].to_numpy(dtype="float64")
    pred_dep = rows["predicted_depreciation_pct"].to_numpy(dtype="float64")
    model_abs_error = np.abs(actual_price - predicted_price)
    baseline_abs_error = np.abs(actual_price - observed_price)
    model_mae = float(model_abs_error.mean())
    baseline_mae = float(baseline_abs_error.mean())
    denominator = float(np.abs(actual_price).sum())
    return {
        "backtest_rows": int(rows.shape[0]),
        "backtest_cohorts": int(rows[COHORT_COLUMNS].drop_duplicates().shape[0]),
        "depreciation_mae_pct_points": float(np.mean(np.abs(actual_dep - pred_dep)) * 100),
        "depreciation_rmse_pct_points": float(np.sqrt(np.mean((actual_dep - pred_dep) ** 2)) * 100),
        "depreciation_bias_pct_points": float(np.mean(pred_dep - actual_dep) * 100),
        "depreciation_r2": float(r2_score(actual_dep, pred_dep)) if len(actual_dep) >= 2 else float("nan"),
        "future_price_mae": model_mae,
        "future_price_rmse": float(np.sqrt(np.mean((actual_price - predicted_price) ** 2))),
        "future_price_wape": float(model_abs_error.sum() / denominator) if denominator else float("nan"),
        "future_price_bias": float(np.mean(predicted_price - actual_price)),
        "naive_no_change_future_price_mae": baseline_mae,
        "naive_no_change_future_price_wape": float(baseline_abs_error.sum() / denominator)
        if denominator
        else float("nan"),
        "future_price_mae_skill_vs_naive": float(1 - (model_mae / baseline_mae))
        if baseline_mae
        else float("nan"),
    }


def backtesting_kpi_frame(backtest_rows: pd.DataFrame) -> pd.DataFrame:
    if backtest_rows.empty:
        return pd.DataFrame()
    records: list[dict[str, Any]] = []
    summary_groups = ["model_key", "model_family", "forecast_method", "forecast_month"]
    cohort_groups = summary_groups + COHORT_COLUMNS + ["cohort_id"]
    for scope, group_cols in [("model_horizon", summary_groups), ("cohort_horizon", cohort_groups)]:
        for group_values, group_df in backtest_rows.groupby(group_cols, dropna=False):
            if not isinstance(group_values, tuple):
                group_values = (group_values,)
            row = {"kpi_scope": scope, **dict(zip(group_cols, group_values))}
            row.update(metrics_from_backtest_rows(group_df))
            records.append(row)
    return pd.DataFrame(records)


def extract_pipeline_feature_importance(model: Pipeline, top_n: int = MAX_FEATURE_IMPORTANCE_ROWS) -> list[dict[str, Any]]:
    """Return model-native feature importances for fitted cohort pipelines."""
    if "preprocessor" not in model.named_steps or "model" not in model.named_steps:
        return []

    estimator = model.named_steps["model"]
    if not hasattr(estimator, "feature_importances_"):
        return []

    try:
        feature_names = list(model.named_steps["preprocessor"].get_feature_names_out())
    except Exception:
        feature_names = [f"feature_{idx}" for idx in range(len(estimator.feature_importances_))]

    weights = np.ravel(estimator.feature_importances_).astype("float64")
    if len(feature_names) != len(weights):
        feature_names = [f"feature_{idx}" for idx in range(len(weights))]

    rows = [
        {
            "feature": str(feature),
            "importance": float(weight),
            "abs_weight": float(abs(weight)),
            "weight_type": "feature_importance",
        }
        for feature, weight in zip(feature_names, weights)
    ]
    return sorted(rows, key=lambda row: row["abs_weight"], reverse=True)[:top_n]


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
    observed_month = pd.to_datetime(forecast_frame["month_start"])

    output = forecast_frame[
        COHORT_COLUMNS + ["month_start", "median_price", "unique_vins", "volume"]
    ].copy()
    output = output.rename(
        columns={
            "month_start": "observed_month_start",
            "median_price": "observed_median_price",
        }
    )
    output["forecast_month"] = int(horizon)
    output["forecast_date"] = observed_month + pd.DateOffset(months=int(horizon))
    output["predicted_depreciation_pct"] = pred_dep
    output["predicted_median_price"] = predicted_price
    output["forecast_method"] = f"direct_{int(horizon)}m_model"
    output["model_family"] = "global_ml"
    output["cohort_id"] = _cohort_id_frame(output)
    return output


def forecast_latest_cohorts_recursive(
    latest_features: pd.DataFrame,
    model: Pipeline,
    feature_columns: list[str],
    step_months: int,
    forecast_months: int,
) -> pd.DataFrame:
    working = latest_features.copy()
    observed_month = pd.to_datetime(latest_features["month_start"])
    elapsed_months = 0
    frames: list[pd.DataFrame] = []

    while elapsed_months < forecast_months:
        remaining_months = min(step_months, forecast_months - elapsed_months)
        pred_step_dep = model.predict(working[feature_columns])
        if remaining_months != step_months:
            pred_dep = np.power(1 + pred_step_dep, remaining_months / step_months) - 1
        else:
            pred_dep = pred_step_dep

        current_price = working["median_price"].to_numpy(dtype="float64")
        predicted_price = np.clip(current_price * (1 + pred_dep), a_min=0, a_max=None)
        elapsed_months += remaining_months

        output = latest_features[
            COHORT_COLUMNS + ["month_start", "median_price", "unique_vins", "volume"]
        ].copy()
        output = output.rename(
            columns={
                "month_start": "observed_month_start",
                "median_price": "observed_median_price",
            }
        )
        output["forecast_month"] = int(elapsed_months)
        output["forecast_date"] = observed_month + pd.DateOffset(months=int(elapsed_months))
        output["predicted_depreciation_pct"] = (
            predicted_price / output["observed_median_price"].replace(0, np.nan) - 1
        )
        output["predicted_median_price"] = predicted_price
        output["forecast_method"] = "recursive_global_ml_model"
        output["model_family"] = "global_ml"
        output["cohort_id"] = _cohort_id_frame(output)
        frames.append(output)

        working["median_price"] = predicted_price
        for column in ["avg_price", "price_p25", "price_p75", "lag_median_price_1", "rolling_median_price_3m"]:
            if column in working.columns:
                working[column] = predicted_price
        if "price_index_vs_cohort_first" in working.columns:
            working["price_index_vs_cohort_first"] = (
                working["median_price"] / working["cohort_first_median_price"].replace(0, np.nan)
            )
        if "cumulative_depreciation_pct" in working.columns:
            working["cumulative_depreciation_pct"] = working["price_index_vs_cohort_first"] - 1
        working["month_start"] = observed_month + pd.DateOffset(months=int(elapsed_months))
        working["month"] = working["month_start"].dt.month.astype("int16")
        working["quarter"] = working["month_start"].dt.quarter.astype("int16")
        working["cohort_age_months"] = (
            working["cohort_age_months"].astype("float64") + remaining_months
        ).round().astype("int16")
        working["cohort_month_number"] = (
            working["cohort_month_number"].astype("float64") + remaining_months
        ).round().astype("int16")

    return pd.concat(frames, ignore_index=True)


def forecast_sarimax_series(series: pd.Series, periods: int) -> np.ndarray:
    if SARIMAX is None:
        raise ImportError("statsmodels is not installed; install statsmodels to run SARIMAX forecasts.")
    if series.shape[0] < MIN_LOCAL_MODEL_MONTHS:
        raise ValueError(f"SARIMAX requires at least {MIN_LOCAL_MODEL_MONTHS} monthly observations.")

    seasonal_order = (0, 1, 1, 12) if series.shape[0] >= MIN_SEASONAL_MODEL_MONTHS else (0, 0, 0, 0)
    order = (1, 1, 1) if series.shape[0] >= 8 else (0, 1, 1)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        model = SARIMAX(
            series,
            order=order,
            seasonal_order=seasonal_order,
            enforce_stationarity=False,
            enforce_invertibility=False,
        )
        fitted = model.fit(disp=False)
        forecast = fitted.forecast(steps=int(periods))
    return np.asarray(forecast, dtype="float64")


def forecast_prophet_series(series: pd.Series, periods: int) -> np.ndarray:
    if Prophet is None:
        raise ImportError("prophet is not installed; install prophet to run Prophet forecasts.")
    if series.shape[0] < MIN_LOCAL_MODEL_MONTHS:
        raise ValueError(f"Prophet requires at least {MIN_LOCAL_MODEL_MONTHS} monthly observations.")

    train_df = pd.DataFrame({"ds": series.index, "y": series.to_numpy(dtype="float64")})
    model = Prophet(
        daily_seasonality=False,
        weekly_seasonality=False,
        yearly_seasonality=series.shape[0] >= MIN_SEASONAL_MODEL_MONTHS,
        interval_width=0.8,
    )
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        model.fit(train_df)
    future = pd.DataFrame(
        {
            "ds": [
                pd.Timestamp(series.index.max()) + pd.DateOffset(months=month)
                for month in range(1, int(periods) + 1)
            ]
        }
    )
    forecast = model.predict(future)
    return forecast["yhat"].to_numpy(dtype="float64")


def build_timesfm_model(model_id: str, max_horizon: int) -> Any:
    if timesfm is None:
        raise ImportError("timesfm is not installed; install timesfm[torch] to run TimesFM forecasts.")
    try:
        import torch
    except ImportError as exc:  # pragma: no cover - optional backend
        raise ImportError("torch is required for the TimesFM PyTorch checkpoint.") from exc

    if hasattr(torch, "set_float32_matmul_precision"):
        torch.set_float32_matmul_precision("high")
    model_class = getattr(timesfm, "TimesFM_2p5_200M_torch", None)
    forecast_config = getattr(timesfm, "ForecastConfig", None)
    if model_class is None or forecast_config is None:
        raise RuntimeError(
            "The installed timesfm package does not expose the TimesFM 2.5 PyTorch API. "
            "Install timesfm[torch]>=2.0.2."
        )

    model = model_class.from_pretrained(model_id)
    model.compile(
        forecast_config(
            max_context=1024,
            max_horizon=max(1, int(max_horizon)),
            normalize_inputs=True,
            use_continuous_quantile_head=True,
            force_flip_invariance=True,
            infer_is_positive=True,
            fix_quantile_crossing=True,
        )
    )
    return model


def forecast_timesfm_batch(model: Any, series_list: list[pd.Series], periods: int) -> list[np.ndarray]:
    inputs = [series.to_numpy(dtype="float32") for series in series_list]
    point_forecast, _ = model.forecast(horizon=int(periods), inputs=inputs)
    return [np.asarray(row, dtype="float64") for row in point_forecast]


def select_local_model_cohorts(monthly: pd.DataFrame, max_cohorts: int) -> pd.DataFrame:
    summary = (
        monthly.groupby(COHORT_COLUMNS, dropna=False)
        .agg(
            cohort_months=("month_start", "nunique"),
            cohort_volume=("volume", "sum"),
            latest_month=("month_start", "max"),
            latest_unique_vins=("unique_vins", "last"),
        )
        .reset_index()
    )
    summary = summary[summary["cohort_months"].ge(MIN_LOCAL_MODEL_MONTHS)].copy()
    summary = summary.sort_values(
        ["cohort_months", "cohort_volume", "latest_month", "latest_unique_vins"],
        ascending=[False, False, False, False],
    )
    if max_cohorts and max_cohorts > 0:
        summary = summary.head(int(max_cohorts))
    return summary


def run_local_forecaster(
    model_family: str,
    monthly: pd.DataFrame,
    latest_features: pd.DataFrame,
    target_months: list[int],
    forecast_months: int,
    cutoff: pd.Timestamp,
    max_local_model_cohorts: int,
    timesfm_model_id: str,
) -> tuple[list[pd.DataFrame], list[pd.DataFrame], dict[str, Any]]:
    forecast_method = f"{model_family}_local_model"
    selected = select_local_model_cohorts(monthly, max_local_model_cohorts)
    report: dict[str, Any] = {
        "model_family": model_family,
        "forecast_method": forecast_method,
        "requested_cohorts": int(selected.shape[0]),
        "min_series_months": MIN_LOCAL_MODEL_MONTHS,
        "max_local_model_cohorts": int(max_local_model_cohorts or 0),
        "per_cohort_model": True,
    }
    if selected.empty:
        report["skipped"] = f"no cohorts have at least {MIN_LOCAL_MODEL_MONTHS} monthly observations"
        return [], [], report
    if model_family == "sarimax" and SARIMAX is None:
        report["skipped"] = "statsmodels is not installed"
        return [], [], report
    if model_family == "prophet" and Prophet is None:
        report["skipped"] = "prophet is not installed"
        return [], [], report
    if model_family == "timesfm" and timesfm is None:
        report["skipped"] = "timesfm is not installed"
        report["timesfm_model_id"] = timesfm_model_id
        return [], [], report

    selected_keys = {tuple(row) for row in selected[COHORT_COLUMNS].itertuples(index=False, name=None)}
    latest_lookup = {
        _cohort_key_from_row(row): row for _, row in latest_features.iterrows()
        if _cohort_key_from_row(row) in selected_keys
    }
    grouped = [
        (key, group.copy())
        for key, group in monthly.groupby(COHORT_COLUMNS, dropna=False)
        if key in selected_keys and key in latest_lookup
    ]

    future_frames: list[pd.DataFrame] = []
    backtest_frames: list[pd.DataFrame] = []
    errors: list[str] = []
    fitted_count = 0
    max_target_month = max(target_months) if target_months else 1
    forecast_steps = max(int(forecast_months), int(max_target_month))
    timesfm_model = None
    if model_family == "timesfm":
        report["timesfm_model_id"] = timesfm_model_id
        try:
            timesfm_model = build_timesfm_model(timesfm_model_id, forecast_steps)
        except Exception as exc:
            report["skipped"] = str(exc)
            return [], [], report

    for key, group in grouped:
        try:
            series = _cohort_price_series(group)
            if series.shape[0] < MIN_LOCAL_MODEL_MONTHS:
                continue
            latest_row = latest_lookup[key]
            actual_lookup = {pd.Timestamp(index): float(value) for index, value in series.items()}

            if model_family == "sarimax":
                future_pred = forecast_sarimax_series(series, forecast_steps)
                backtest_pred = np.array([])
            elif model_family == "prophet":
                future_pred = forecast_prophet_series(series, forecast_steps)
                backtest_pred = np.array([])
            else:
                future_pred = forecast_timesfm_batch(timesfm_model, [series], forecast_steps)[0]
                backtest_pred = np.array([])

            future_frames.append(
                _forecast_output_from_predictions(
                    cohort_row=latest_row,
                    predictions=future_pred[:forecast_months],
                    forecast_method=forecast_method,
                    model_family=model_family,
                )
            )
            for origin_position in range(MIN_LOCAL_MODEL_MONTHS - 1, len(series)):
                train_series = series.iloc[: origin_position + 1]
                origin_month = pd.Timestamp(train_series.index.max())
                if not any(pd.Timestamp(origin_month + pd.DateOffset(months=h)) in actual_lookup for h in target_months):
                    continue
                if model_family == "sarimax":
                    backtest_pred = forecast_sarimax_series(train_series, max_target_month)
                elif model_family == "prophet":
                    backtest_pred = forecast_prophet_series(train_series, max_target_month)
                else:
                    backtest_pred = forecast_timesfm_batch(timesfm_model, [train_series], max_target_month)[0]
                origin_row = group[group["month_start"].eq(origin_month)].iloc[-1]
                backtest = local_backtest_rows(
                    cohort_row=origin_row,
                    origin_month=origin_month,
                    observed_price=float(train_series.iloc[-1]),
                    predictions=backtest_pred,
                    actual_by_month=actual_lookup,
                    target_months=target_months,
                    model_key=f"{model_family}_local",
                    model_family=model_family,
                    forecast_method=forecast_method,
                )
                if not backtest.empty:
                    backtest_frames.append(
                        rolling_origin_metadata(
                            backtest,
                            origin_month,
                            pd.Timestamp(train_series.index.min()),
                            MIN_ROLLING_HISTORY_MONTHS,
                        )
                    )
            fitted_count += 1
        except Exception as exc:  # pragma: no cover - defensive per-cohort guardrail
            if len(errors) < 12:
                errors.append(f"{'|'.join(map(str, key))}: {exc}")

    report.update(
        {
            "fitted_cohorts": int(fitted_count),
            "forecast_rows": int(sum(frame.shape[0] for frame in future_frames)),
            "backtest_rows": int(sum(frame.shape[0] for frame in backtest_frames)),
            "error_examples": errors,
        }
    )
    if fitted_count == 0 and "skipped" not in report:
        report["skipped"] = "all eligible cohorts failed to fit"
    return future_frames, backtest_frames, report


def train_cohort_models(
    db_path: Path,
    output_dir: Path,
    sample_size: int | None,
    target_months: list[int],
    forecast_months: int,
    split_date: str | None,
    min_monthly_vins: int,
    min_cohort_months: int,
    max_price: int | None,
    time_series_models: list[str],
    max_local_model_cohorts: int,
    timesfm_model_id: str,
    min_rolling_history_months: int = MIN_ROLLING_HISTORY_MONTHS,
) -> dict[str, Any]:
    raw = load_history_frame(db_path, sample_size)
    history = clean_history_frame(raw, max_price=max_price)
    monthly = build_cohort_monthly_frame(
        history,
        target_months=target_months,
        min_monthly_vins=min_monthly_vins,
        min_cohort_months=min_cohort_months,
    )

    feature_columns = [
        column
        for column in CATEGORICAL_FEATURES + NUMERIC_FEATURES
        if column in monthly.columns and column not in PRICE_LEAKAGE_FEATURE_COLUMNS
    ]
    cutoff = choose_cutoff(monthly, split_date)
    report: dict[str, Any] = {
        "task": "cohort_depreciation",
        "modeling_approach": (
            "Global supervised monthly time-series model across make/model/year/trim cohorts, "
            "trained to predict one-step monthly depreciation and recursively forecast 60 months."
        ),
        "rows_loaded": int(raw.shape[0]),
        "history_rows_after_cleaning": int(history.shape[0]),
        "cohort_month_rows": int(monthly.shape[0]),
        "cohort_week_rows": int(monthly.shape[0]),
        "cohorts": int(monthly[COHORT_COLUMNS].drop_duplicates().shape[0]),
        "split_date": str(cutoff.date()),
        "forecast_months": int(forecast_months),
        "requested_model_families": time_series_models,
        "max_local_model_cohorts": int(max_local_model_cohorts or 0),
        "timesfm_model_id": timesfm_model_id,
        "min_monthly_vins": int(min_monthly_vins),
        "min_cohort_months": int(min_cohort_months),
        "max_price": int(max_price or 0),
        "minimum_model_rows": MIN_MODEL_ROWS,
        "minimum_temporal_train_rows": MIN_TEMPORAL_TRAIN_ROWS,
        "minimum_temporal_test_rows": MIN_TEMPORAL_TEST_ROWS,
        "backtesting": {
            "scheme": "expanding_rolling_origin",
            "minimum_history_months": int(min_rolling_history_months),
            "training_rule": "Each origin fits only rows whose target month is observed by that origin.",
        },
        "feature_columns": feature_columns,
        "excluded_price_leakage_columns": sorted(PRICE_LEAKAGE_FEATURE_COLUMNS),
        "identity_normalization": {
            "versions": {
                str(key): int(value)
                for key, value in raw.get("normalization_version", pd.Series(dtype="string"))
                .fillna("NULL")
                .astype("string")
                .value_counts()
                .items()
            },
            "trim_coverage_rate": float(history["trim_proxy"].ne("UNKNOWN_TRIM").mean()),
            "source_distribution": {
                str(key): int(value)
                for key, value in raw.get("canonical_trim_source", pd.Series(dtype="string"))
                .fillna("NULL").astype("string").value_counts().items()
            },
            "confidence_distribution": {
                str(key): int(value)
                for key, value in raw.get("canonical_match_confidence", pd.Series(dtype="string"))
                .fillna("NULL").astype("string").value_counts().items()
            },
            "epa_match_rate": float(
                raw.get("epa_vehicle_id", pd.Series(index=raw.index, dtype="object")).notna().mean()
            ),
            "nhtsa_disagreement": {
                column: {
                    "observed_rows": int(pd.to_numeric(raw[column], errors="coerce").notna().sum()),
                    "disagreement_rate": float(
                        pd.to_numeric(raw[column], errors="coerce").dropna().eq(0).mean()
                    )
                    if pd.to_numeric(raw[column], errors="coerce").notna().any()
                    else None,
                }
                for column in [
                    "nhtsa_year_agrees",
                    "nhtsa_make_agrees",
                    "nhtsa_model_agrees",
                    "nhtsa_trim_agrees",
                ]
                if column in raw.columns
            },
            "cohort_identity_source": "vehicle_identity VIN consensus with canonical listing fallback",
            "identity_precedence": "NHTSA make/model anchors; title-derived trim; EPA validation/standardization",
        },
        "price_feature_policy": (
            "Current-month cohort price summaries are allowed because they are observed at "
            "the forecast origin; future target price columns are excluded from model inputs."
        ),
        "models": {},
        "local_model_runs": {},
    }

    latest_features = (
        monthly.sort_values("month_start")
        .groupby(COHORT_COLUMNS, dropna=False)
        .tail(1)
        .sort_values(["make", "model", "model_year", "trim_proxy"])
    )
    latest_features[
        COHORT_COLUMNS + ["month_start", "median_price", "unique_vins", "volume"]
    ].to_csv(output_dir / "cohort_latest_observations.csv", index=False)
    monthly.to_csv(output_dir / "cohort_monthly_training_frame.csv", index=False)
    monthly.to_csv(output_dir / "cohort_weekly_training_frame.csv", index=False)

    fitted_horizon_models: dict[int, Pipeline] = {}
    backtest_frames: list[pd.DataFrame] = []
    future_forecast_frames: list[pd.DataFrame] = []
    if "global_ml" in time_series_models:
        for horizon in target_months:
            target_col = f"target_depreciation_pct_{horizon}m"
            future_price_col = f"target_median_price_{horizon}m"
            model_df = monthly.dropna(subset=feature_columns + [target_col, future_price_col]).copy()
            if model_df.shape[0] < MIN_MODEL_ROWS:
                report["models"][target_col] = {
                    "model_family": "global_ml",
                    "forecast_method": "recursive_global_ml_model",
                    "skipped": f"fewer than {MIN_MODEL_ROWS} complete cohort-month rows",
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
            rolling_rows = rolling_origin_global_backtest(
                model_df=model_df,
                feature_columns=feature_columns,
                target_col=target_col,
                future_price_col=future_price_col,
                horizon=horizon,
                tuned_params=tuned_params,
                min_history_months=min_rolling_history_months,
            )
            metrics = metrics_from_backtest_rows(rolling_rows)
            if not rolling_rows.empty:
                backtest_frames.append(rolling_rows)

            final_model, final_model_name = build_regression_pipeline(
                model_df[feature_columns],
                model_df[target_col],
                tuned_params,
            )
            final_model.fit(model_df[feature_columns], model_df[target_col])

            artifact_name = f"Cohort_Depreciation_{horizon}m.joblib"
            joblib.dump(final_model, output_dir / artifact_name)
            fitted_horizon_models[int(horizon)] = final_model
            feature_importance = extract_pipeline_feature_importance(final_model)
            report["models"][target_col] = {
                "model_family": "global_ml",
                "forecast_method": "recursive_global_ml_model",
                "artifact": artifact_name,
                "validation_model_type": model_name,
                "artifact_model_type": final_model_name,
                "target": target_col,
                "future_price_column": future_price_col,
                "split_date": str(model_cutoff.date()) if not pd.isna(model_cutoff) else None,
                "train_rows": int(train.shape[0]),
                "test_rows": int(rolling_rows.shape[0]),
                "train_cohorts": int(train[COHORT_COLUMNS].drop_duplicates().shape[0]),
                "test_cohorts": int(rolling_rows[COHORT_COLUMNS].drop_duplicates().shape[0]) if not rolling_rows.empty else 0,
                "complete_rows": int(model_df.shape[0]),
                "artifact_training_rows": int(model_df.shape[0]),
                "tuning": tuning_metadata,
                "validation_warning": validation_warning,
                "metrics": metrics,
                "feature_importance": feature_importance,
            }
    else:
        report["local_model_runs"]["global_ml"] = {"skipped": "not requested"}

    for model_family in [family for family in time_series_models if family != "global_ml"]:
        local_future_frames, local_backtest_frames, local_report = run_local_forecaster(
            model_family=model_family,
            monthly=monthly,
            latest_features=latest_features,
            target_months=target_months,
            forecast_months=forecast_months,
            cutoff=cutoff,
            max_local_model_cohorts=max_local_model_cohorts,
            timesfm_model_id=timesfm_model_id,
        )
        report["local_model_runs"][model_family] = local_report
        future_forecast_frames.extend(local_future_frames)
        backtest_frames.extend(local_backtest_frames)
        local_backtests = pd.concat(local_backtest_frames, ignore_index=True) if local_backtest_frames else pd.DataFrame()
        for horizon in target_months:
            target_col = f"target_depreciation_pct_{horizon}m"
            model_key = f"{model_family}_{target_col}"
            horizon_rows = (
                local_backtests[local_backtests["forecast_month"].eq(int(horizon))]
                if not local_backtests.empty
                else pd.DataFrame()
            )
            metrics = metrics_from_backtest_rows(horizon_rows)
            report["models"][model_key] = {
                "model_family": model_family,
                "forecast_method": local_report.get("forecast_method"),
                "target": target_col,
                "future_price_column": f"target_median_price_{horizon}m",
                "artifact": None,
                "validation_model_type": model_family,
                "artifact_model_type": model_family,
                "split_date": report["split_date"],
                "train_rows": None,
                "test_rows": int(horizon_rows.shape[0]),
                "train_cohorts": local_report.get("fitted_cohorts", 0),
                "test_cohorts": int(horizon_rows[COHORT_COLUMNS].drop_duplicates().shape[0])
                if not horizon_rows.empty
                else 0,
                "complete_rows": local_report.get("requested_cohorts", 0),
                "artifact_training_rows": None,
                "tuning": {"enabled": False, "reason": "local forecaster uses fixed default settings"},
                "validation_warning": None if metrics else local_report.get("skipped") or "no backtest rows available",
                "metrics": metrics,
                "feature_importance": [],
            }
            if local_report.get("skipped"):
                report["models"][model_key]["skipped"] = local_report["skipped"]

    backtest_results = pd.concat(backtest_frames, ignore_index=True) if backtest_frames else pd.DataFrame()
    if not backtest_results.empty:
        backtest_results = backtest_results.sort_values(
            ["forecast_month", "model_family", "forecast_method", "make", "model", "model_year", "trim_proxy"]
        )
        backtest_path = output_dir / "cohort_backtesting_results.csv"
        backtest_results.to_csv(backtest_path, index=False)
        kpis = backtesting_kpi_frame(backtest_results)
        kpi_path = output_dir / "cohort_backtesting_kpis.csv"
        kpis.to_csv(kpi_path, index=False)
        report["backtest_output"] = backtest_path.name
        report["backtest_kpi_output"] = kpi_path.name
        report["backtest_rows"] = int(backtest_results.shape[0])
        report["backtest_kpi_rows"] = int(kpis.shape[0])
    else:
        report["backtest_output"] = None
        report["backtest_kpi_output"] = None
        report["backtest_rows"] = 0
        report["backtest_kpi_rows"] = 0

    if fitted_horizon_models:
        recursive_horizon = min(fitted_horizon_models)
        future_forecast_frames.append(
            forecast_latest_cohorts_recursive(
                latest_features=latest_features,
                model=fitted_horizon_models[recursive_horizon],
                feature_columns=feature_columns,
                step_months=recursive_horizon,
                forecast_months=forecast_months,
            )
        )

    if future_forecast_frames:
        future_forecasts = pd.concat(future_forecast_frames, ignore_index=True)
        future_forecasts = future_forecasts.sort_values(
            ["forecast_month", "forecast_method", "make", "model", "model_year", "trim_proxy"]
        )
        forecast_path = output_dir / "cohort_future_forecasts.csv"
        future_forecasts.to_csv(forecast_path, index=False)
        report["future_forecast_output"] = forecast_path.name
        report["future_forecast_rows"] = int(future_forecasts.shape[0])
        report["last_observed_month"] = str(
            pd.to_datetime(future_forecasts["observed_month_start"]).max().date()
        )
        report["max_forecast_date"] = str(pd.to_datetime(future_forecasts["forecast_date"]).max().date())
    else:
        report["future_forecast_output"] = None
        report["future_forecast_rows"] = 0
        report["last_observed_month"] = str(pd.to_datetime(monthly["month_start"]).max().date())
        report["max_forecast_date"] = None

    return report


def write_reports(output_dir: Path, report: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "cohort_depreciation_model_report.json").write_text(
        json.dumps(report, indent=2, default=str),
        encoding="utf-8",
    )
    importance_rows = [
        {"target": target, "rank": rank, **row}
        for target, model_report in report.get("models", {}).items()
        for rank, row in enumerate(model_report.get("feature_importance", []), start=1)
    ]
    if importance_rows:
        pd.DataFrame(importance_rows).to_csv(
            output_dir / "cohort_depreciation_feature_importance.csv",
            index=False,
        )
    (output_dir / "cohort_model_features.json").write_text(
        json.dumps(
            {
                "cohort_columns": COHORT_COLUMNS,
                "categorical_features": CATEGORICAL_FEATURES,
                "numeric_features": NUMERIC_FEATURES,
                "feature_columns": report["feature_columns"],
                "excluded_price_leakage_columns": report.get("excluded_price_leakage_columns", []),
                "price_feature_policy": report.get("price_feature_policy"),
                "target_family": "target_depreciation_pct_{horizon}m",
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
        f"Cohort-month rows: {report['cohort_month_rows']:,}",
        f"Cohorts: {report['cohorts']:,}",
        f"Split date: {report['split_date']}",
        f"Forecast months: {report['forecast_months']}",
        f"Requested model families: {', '.join(report.get('requested_model_families', []))}",
        f"Future forecast output: {report.get('future_forecast_output') or 'none'}",
        f"Backtest output: {report.get('backtest_output') or 'none'}",
        f"Backtest KPI output: {report.get('backtest_kpi_output') or 'none'}",
        f"Max forecast date: {report.get('max_forecast_date') or 'none'}",
        "",
        "## Design",
        "",
        "- Cohort grain: make, model, model year, and trim proxy.",
        "- Forecast target: future median-price depreciation percentage from the current cohort month.",
        "- Price features: current-month cohort price summaries are forecast-origin inputs; future target price columns are never model inputs.",
        "- Forecast output: month-by-month median-price paths for each requested time-series model family.",
        "- Model families: global supervised gradient boosting, SARIMAX, Prophet, and TimesFM when requested and installed.",
        "- Hyperparameter search: global ML uses a representative bounded cohort-month sample with an inner temporal holdout, followed by refit on the full training frame.",
        "- Local model guardrail: SARIMAX, Prophet, and TimesFM use cohort-level histories with a default cohort cap for bounded iteration; use --max-local-model-cohorts 0 for all eligible cohorts.",
        "- Baseline: no-change future price, reported beside model future-price MAE.",
        "- Leakage control: NHTSA base-price fields are excluded from model inputs.",
        "- Safeguard: temporal validation requires at least two train and two test rows; otherwise validation is skipped and documented.",
        "",
        "## Model Results",
        "",
    ]
    for target, model_report in report["models"].items():
        if "skipped" in model_report:
            lines.append(f"- {target}: skipped ({model_report['skipped']})")
            continue
        if not model_report.get("metrics"):
            lines.append(
                "- {target}: trained on {rows:,} complete rows; validation skipped ({warning})".format(
                    target=target,
                    rows=model_report.get("artifact_training_rows") or model_report.get("complete_rows") or 0,
                    warning=model_report["validation_warning"],
                )
            )
            continue
        metrics = model_report["metrics"]
        train_rows = model_report.get("train_rows")
        test_rows = model_report.get("test_rows")
        train_label = f"{int(train_rows):,}" if pd.notna(train_rows) else "n/a"
        test_label = f"{int(test_rows):,}" if pd.notna(test_rows) else "n/a"
        lines.append(
            "- {target} [{family}]: dep MAE {dep_mae:.2f} pct pts, future price MAE ${price_mae:,.0f} "
            "(naive ${naive_mae:,.0f}, skill {skill:.1%}), WAPE {wape:.1%}, R2 {r2:.4f}, "
            "train {train_rows}, test {test_rows}, model {artifact_model}".format(
                target=target,
                family=model_report.get("model_family", "global_ml"),
                dep_mae=metrics["depreciation_mae_pct_points"],
                price_mae=metrics["future_price_mae"],
                naive_mae=metrics["naive_no_change_future_price_mae"],
                skill=metrics["future_price_mae_skill_vs_naive"],
                wape=metrics["future_price_wape"],
                r2=metrics["depreciation_r2"],
                train_rows=train_label,
                test_rows=test_label,
                artifact_model=model_report["artifact_model_type"],
            )
        )
        importance = model_report.get("feature_importance", [])
        if importance:
            top_features = ", ".join(row["feature"] for row in importance[:6])
            lines.append(f"  - Top model-native importance: {top_features}")
            lines.append("  - Top 30 model-native importance:")
            for row in importance[:FEATURE_IMPORTANCE_REPORT_ROWS]:
                lines.append(
                    "    - {feature}: {importance:.4g} ({weight_type})".format(
                        feature=row["feature"],
                        importance=row["importance"],
                        weight_type=row["weight_type"],
                    )
                )
    lines.extend(
        [
            "",
            "## Research Notes",
            "",
            "- Used-car valuation is a hedonic pricing problem: vehicle attributes, mileage, age, and market conditions all affect observed price.",
            "- Global forecasting is a good fit because there are many related cohort series, many of them short or sparse.",
            "- Sparse/new cohorts borrow signal through the global model and cohort descriptors; exact VIN-level local forecasts are intentionally avoided.",
            "- SARIMAX and Prophet are included as interpretable local cohort baselines where each cohort has enough monthly observations.",
            "- TimesFM is included as a zero-shot foundation-model benchmark using the configured Hugging Face checkpoint when the optional package and backend are installed.",
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
    target_months: list[int] | None = None,
    forecast_months: int = DEFAULT_FORECAST_MONTHS,
    split_date: str | None = None,
    min_monthly_vins: int = DEFAULT_MIN_MONTHLY_VINS,
    min_cohort_months: int = DEFAULT_MIN_COHORT_MONTHS,
    max_price: int | None = DEFAULT_MAX_PRICE,
    horizons: list[int] | None = None,
    time_series_models: list[str] | None = None,
    max_local_model_cohorts: int = DEFAULT_MAX_LOCAL_MODEL_COHORTS,
    timesfm_model_id: str = DEFAULT_TIMESFM_MODEL_ID,
    min_rolling_history_months: int = MIN_ROLLING_HISTORY_MONTHS,
) -> dict[str, Any]:
    if target_months is None:
        target_months = horizons or DEFAULT_TARGET_MONTHS
    if time_series_models is None:
        time_series_models = DEFAULT_TIME_SERIES_MODELS.copy()
    output_dir.mkdir(parents=True, exist_ok=True)
    report = train_cohort_models(
        db_path=db_path,
        output_dir=output_dir,
        sample_size=sample_size,
        target_months=target_months,
        forecast_months=forecast_months,
        split_date=split_date,
        min_monthly_vins=min_monthly_vins,
        min_cohort_months=min_cohort_months,
        max_price=max_price,
        time_series_models=time_series_models,
        max_local_model_cohorts=max_local_model_cohorts,
        timesfm_model_id=timesfm_model_id,
        min_rolling_history_months=min_rolling_history_months,
    )
    report.update(
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "database_path": str(db_path),
            "sample_size": int(sample_size or 0),
            "target_months": target_months,
            "forecast_months": int(forecast_months),
            "sample_strategy": "recent_vins_full_history" if sample_size else "full_price_history",
            "time_series_models": time_series_models,
            "min_rolling_history_months": int(min_rolling_history_months),
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
    target_months = [int(value.strip()) for value in args.target_months.split(",") if value.strip()]
    time_series_models = parse_model_families(args.time_series_models)
    report = run_cohort_forecast(
        db_path=Path(args.db_path).expanduser().resolve(),
        output_dir=Path(args.output_dir).expanduser().resolve(),
        sample_size=args.sample_size,
        target_months=target_months,
        forecast_months=args.forecast_months,
        split_date=args.split_date,
        min_monthly_vins=args.min_monthly_vins,
        min_cohort_months=args.min_cohort_months,
        max_price=args.max_price or None,
        time_series_models=time_series_models,
        max_local_model_cohorts=args.max_local_model_cohorts,
        timesfm_model_id=args.timesfm_model_id,
        min_rolling_history_months=args.min_rolling_history_months,
    )
    print(f"Saved report to {Path(args.output_dir) / 'cohort_depreciation_model_report.json'}")
    for target, model_report in report["models"].items():
        if "skipped" in model_report:
            print(f"{target}: skipped ({model_report['skipped']})")
        else:
            metrics = model_report["metrics"]
            if metrics:
                print(
                    f"{target}: dep MAE {metrics['depreciation_mae_pct_points']:.2f} pct pts, "
                    f"future price MAE ${metrics['future_price_mae']:,.0f}"
                )
            else:
                print(f"{target}: trained; validation metrics unavailable")


if __name__ == "__main__":
    main()
