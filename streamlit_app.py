"""Streamlit dashboard for the automotive market capstone."""

from __future__ import annotations

import json
import math
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("CAR_DATA_DB_PATH", BASE_DIR / "CAR_DATA_OUTPUT" / "CAR_DATA_CLEANED.db"))
SENTIMENT_DB_PATH = Path(
    os.environ.get("CAR_SENTIMENT_DB_PATH", BASE_DIR / "CAR_DATA_OUTPUT" / "CAR_YOUTUBE_COMMENTS.db")
)
MODELS_DIR = BASE_DIR / "MODELS_OUTPUT"
CURRENT_REPORT_PATH = MODELS_DIR / "model_report.json"
COHORT_REPORT_PATH = MODELS_DIR / "cohort_depreciation_model_report.json"
FORECAST_PATH = MODELS_DIR / "cohort_future_forecasts.csv"
BACKTEST_RESULTS_PATH = MODELS_DIR / "cohort_backtesting_results.csv"
BACKTEST_KPI_PATH = MODELS_DIR / "cohort_backtesting_kpis.csv"
TRIM_FEATURE_CANDIDATES = ["canonical_trim"]

if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

from ML.Price_ML_Models import (  # noqa: E402
    HighValueRoutedRegressor,
    engineer_current_price_features,
    make_feature_matrix,
    to_float32,
)
from ML.Time_Series_Price import normalize_label  # noqa: E402

# Models were trained by direct script execution, so some joblib artifacts may
# look for custom objects on __main__ when Streamlit unpickles them.
import __main__  # noqa: E402

setattr(__main__, "HighValueRoutedRegressor", HighValueRoutedRegressor)
setattr(__main__, "to_float32", to_float32)


st.set_page_config(
    page_title="Automotive Market Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
    <style>
    .block-container {padding-top: 1.4rem; padding-bottom: 2rem;}
    div[data-testid="stMetric"] {
        background: #f8fafc;
        border: 1px solid #e2e8f0;
        border-radius: 8px;
        padding: 0.75rem 0.85rem;
    }
    div[data-testid="stDataFrame"] {border: 1px solid #e2e8f0; border-radius: 8px;}
    </style>
    """,
    unsafe_allow_html=True,
)


def fmt_currency(value: Any, decimals: int = 0) -> str:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return "n/a"
    return f"${number:,.{decimals}f}"


def fmt_number(value: Any, decimals: int = 0) -> str:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return "n/a"
    return f"{number:,.{decimals}f}"


def fmt_percent(value: Any, decimals: int = 1) -> str:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return "n/a"
    return f"{number:.{decimals}%}"


def safe_float(value: Any) -> float | None:
    number = pd.to_numeric(value, errors="coerce")
    if pd.isna(number):
        return None
    return float(number)


def combine_transmission(style: Any, speeds: Any) -> str | None:
    style_text = str(style).strip() if pd.notna(style) and str(style).strip() else ""
    speed_value = pd.to_numeric(speeds, errors="coerce")
    speed_text = f"{int(speed_value)} speed" if pd.notna(speed_value) and speed_value > 0 else ""
    parts = [part for part in [style_text, speed_text] if part]
    return " / ".join(parts) if parts else None


def normalize_trim_proxy(value: Any) -> str:
    return normalize_label(value, "UNKNOWN_TRIM").replace(" ", "_")


def read_sql(query: str, params: tuple[Any, ...] = ()) -> pd.DataFrame:
    with sqlite3.connect(DB_PATH) as conn:
        return pd.read_sql_query(query, conn, params=params)


def canonical_sentiment_entity(make: str, model: str, year: int, trim: str | None = None) -> str:
    parts = [str(int(year)), str(make).replace("_", " "), str(model).replace("_", " ")]
    if trim and trim != "UNKNOWN_TRIM":
        parts.append(str(trim).replace("_", " "))
    return " ".join(parts)


@st.cache_data(show_spinner=False)
def load_cohort_sentiment(make: str, model: str, year: int, trim: str | None) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    """Load exact title-derived entity sentiment, then an explicitly labeled fallback."""
    if not SENTIMENT_DB_PATH.exists():
        return pd.DataFrame(), pd.DataFrame(), "unavailable"
    exact_entity = canonical_sentiment_entity(make, model, year, trim)
    model_entity = canonical_sentiment_entity(make, model, year)
    try:
        with sqlite3.connect(SENTIMENT_DB_PATH) as conn:
            entity_exists = conn.execute(
                "SELECT 1 FROM youtube_comments_scored WHERE Vehicle_Entity = ? COLLATE NOCASE LIMIT 1",
                (exact_entity,),
            ).fetchone()
            if entity_exists:
                entity_clause, entity_params, match_label = "Vehicle_Entity = ? COLLATE NOCASE", (exact_entity,), "exact canonical cohort"
            else:
                entity_clause, entity_params, match_label = "Vehicle_Entity LIKE ? COLLATE NOCASE", (f"{model_entity}%",), "make/model/year fallback"
            comments = pd.read_sql_query(
                f"""
                SELECT Vehicle_Entity, text, video_title, published_at, like_count, comment_weight,
                       reliability_sentiment, value_sentiment, performance_sentiment, comfort_sentiment
                FROM youtube_comments_scored
                WHERE {entity_clause} AND text IS NOT NULL AND TRIM(text) <> ''
                ORDER BY comment_weight DESC, datetime(published_at) DESC
                LIMIT 5
                """,
                conn,
                params=entity_params,
            )
            index = pd.read_sql_query(
                f"SELECT * FROM vehicle_sentiment_index WHERE Vehicle_Entity {'=' if entity_exists else 'LIKE'} ? COLLATE NOCASE ORDER BY Sample_Size DESC LIMIT 1",
                conn,
                params=(exact_entity if entity_exists else f"{model_entity}%",),
            )
    except sqlite3.Error:
        return pd.DataFrame(), pd.DataFrame(), "unavailable"
    return index, comments, match_label if not comments.empty else "no matching sentiment"


@st.cache_data(show_spinner=False)
def table_columns(table: str) -> list[str]:
    with sqlite3.connect(DB_PATH) as conn:
        return [row[1] for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]


def select_or_null(columns: list[str] | set[str], table_alias: str, column: str) -> str:
    if column in columns:
        return f"{table_alias}.{column} AS {column}"
    return f"NULL AS {column}"


@st.cache_data(show_spinner=False)
def has_canonical_schema() -> bool:
    required = {
        "canonical_title",
        "canonical_year",
        "canonical_make",
        "canonical_model",
        "canonical_trim",
        "normalization_version",
    }
    return required.issubset(set(table_columns("listings"))) and bool(table_columns("vehicle_identity"))


@st.cache_data(show_spinner=False)
def database_normalization_versions() -> list[str]:
    if not has_canonical_schema():
        return []
    values = read_sql(
        "SELECT DISTINCT normalization_version FROM vehicle_identity "
        "WHERE normalization_version IS NOT NULL ORDER BY normalization_version"
    )
    return values["normalization_version"].dropna().astype(str).tolist()


@st.cache_data(show_spinner=False)
def normalization_quality_metrics() -> dict[str, Any]:
    if not has_canonical_schema():
        return {}
    row = read_sql(
        """
        SELECT
            COUNT(*) AS vehicles,
            SUM(CASE WHEN canonical_trim IS NOT NULL AND canonical_trim <> 'UNKNOWN_TRIM' THEN 1 ELSE 0 END) AS resolved,
            SUM(CASE WHEN canonical_trim = 'UNKNOWN_TRIM' THEN 1 ELSE 0 END) AS unresolved,
            SUM(CASE WHEN epa_vehicle_id IS NOT NULL THEN 1 ELSE 0 END) AS epa_matched,
            SUM(CASE WHEN nhtsa_make_agrees = 0 OR nhtsa_model_agrees = 0 THEN 1 ELSE 0 END) AS nhtsa_identity_disagreements
        FROM vehicle_identity
        """
    ).iloc[0]
    total = max(int(row.get("vehicles") or 0), 1)
    return {
        "vehicles": int(row.get("vehicles") or 0),
        "trim_coverage": float((row.get("resolved") or 0) / total),
        "unresolved": int(row.get("unresolved") or 0),
        "epa_match_rate": float((row.get("epa_matched") or 0) / total),
        "nhtsa_identity_disagreements": int(row.get("nhtsa_identity_disagreements") or 0),
    }


def report_versions(report: dict[str, Any]) -> set[str]:
    versions = report.get("identity_normalization", {}).get("versions", {})
    return {str(value) for value in versions if value and str(value) != "NULL"}


def normalization_versions_match(*reports: dict[str, Any]) -> bool:
    database_versions = set(database_normalization_versions())
    return bool(database_versions) and all(report_versions(report) == database_versions for report in reports)


@st.cache_data(show_spinner=False)
def load_json(path_text: str) -> dict[str, Any]:
    path = Path(path_text)
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


@st.cache_data(show_spinner=False)
def get_makes() -> list[str]:
    query = """
        SELECT DISTINCT canonical_make AS make
        FROM vehicle_identity
        WHERE canonical_make IS NOT NULL AND TRIM(canonical_make) <> ''
        ORDER BY canonical_make
    """
    return read_sql(query)["make"].dropna().astype(str).tolist()


@st.cache_data(show_spinner=False)
def get_models(make: str | None) -> list[str]:
    params: tuple[Any, ...] = ()
    where = "canonical_model IS NOT NULL AND TRIM(canonical_model) <> ''"
    if make:
        where += " AND canonical_make = ?"
        params = (make,)
    query = f"""
        SELECT DISTINCT canonical_model AS model
        FROM vehicle_identity
        WHERE {where}
        ORDER BY canonical_model
    """
    return read_sql(query, params)["model"].dropna().astype(str).tolist()


@st.cache_data(show_spinner=False)
def get_years(make: str | None, model: str | None) -> list[int]:
    conditions = ["canonical_year IS NOT NULL"]
    params: list[Any] = []
    if make:
        conditions.append("canonical_make = ?")
        params.append(make)
    if model:
        conditions.append("canonical_model = ?")
        params.append(model)
    query = f"""
        SELECT DISTINCT CAST(canonical_year AS INTEGER) AS model_year
        FROM vehicle_identity
        WHERE {' AND '.join(conditions)}
        ORDER BY model_year DESC
    """
    years = pd.to_numeric(read_sql(query, tuple(params))["model_year"], errors="coerce")
    return years.dropna().astype(int).tolist()


@st.cache_data(show_spinner=False)
def get_trim_options(make: str | None, model: str | None, year: int | None) -> list[str]:
    if not make or not model or not year:
        return []
    query = """
        SELECT DISTINCT canonical_trim AS trim_proxy
        FROM vehicle_identity
        WHERE canonical_make = ?
          AND canonical_model = ?
          AND CAST(canonical_year AS INTEGER) = ?
          AND canonical_trim IS NOT NULL
          AND canonical_trim <> 'UNKNOWN_TRIM'
        ORDER BY canonical_trim
    """
    df = read_sql(query, (make, model, int(year)))
    return df["trim_proxy"].dropna().astype(str).tolist()


def filter_params(make: str | None, model: str | None, year: int | None) -> tuple[str, list[Any]]:
    conditions = ["l.price IS NOT NULL", "l.price > 0"]
    params: list[Any] = []
    if make:
        conditions.append("l.canonical_make = ?")
        params.append(make)
    if model:
        conditions.append("l.canonical_model = ?")
        params.append(model)
    if year:
        conditions.append("CAST(l.canonical_year AS INTEGER) = ?")
        params.append(int(year))
    return " AND ".join(conditions), params


def add_trim_proxy(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    if "canonical_trim" not in df.columns:
        df["trim_proxy"] = "UNKNOWN_TRIM"
        return df
    df["trim_proxy"] = df["canonical_trim"].map(normalize_trim_proxy)
    return df


@st.cache_data(show_spinner=False)
def load_filtered_listings(
    make: str | None,
    model: str | None,
    year: int | None,
    limit: int = 5000,
) -> pd.DataFrame:
    where_sql, params = filter_params(make, model, year)
    listing_cols = set(table_columns("listings"))
    nhtsa_cols = set(table_columns("nhtsa_enrichment"))
    listing_trim_selects = [
        select_or_null(listing_cols, "l", column)
        for column in [
            "canonical_title",
            "canonical_year",
            "canonical_make",
            "canonical_model",
            "canonical_trim",
            "canonical_trim_raw",
            "canonical_trim_source",
            "canonical_match_confidence",
            "canonical_match_status",
            "epa_vehicle_id",
            "epa_match_status",
            "normalization_version",
            "nhtsa_year_agrees",
            "nhtsa_make_agrees",
            "nhtsa_model_agrees",
            "nhtsa_trim_agrees",
            "title_trim",
            "trim_combined",
            "trim_source",
        ]
    ]
    nhtsa_trim_selects = [
        select_or_null(nhtsa_cols, "n", column)
        for column in ["nhtsa_Trim", "nhtsa_Trim2", "nhtsa_Trim_source", "nhtsa_trim_combined"]
    ]
    nhtsa_feature_selects = [
        select_or_null(nhtsa_cols, "n", column)
        for column in [
            "nhtsa_BasePrice",
            "nhtsa_BodyClass",
            "nhtsa_DriveType",
            "nhtsa_FuelTypePrimary",
            "nhtsa_ElectrificationLevel",
            "nhtsa_TransmissionStyle",
            "nhtsa_TransmissionSpeeds",
            "nhtsa_EngineHP",
            "nhtsa_EngineCylinders",
            "nhtsa_total_recalls",
            "nhtsa_total_complaints",
        ]
    ]
    query = f"""
        WITH ranked AS (
            SELECT
                l.vin,
                l.date,
                l.loaddate,
                l.title,
                l.location,
                l.locationCode,
                l.distance,
                l.priceRecentChange,
                l.price,
                l.mileage,
                l.sourceName,
                l.sellerType,
                l.listingType,
                l.vehicleTitle,
                l.vehicleTitleDesc,
                {", ".join(listing_trim_selects)},
                n.nhtsa_Make,
                n.nhtsa_Model,
                n.nhtsa_ModelYear,
                {", ".join(nhtsa_trim_selects)},
                {", ".join(nhtsa_feature_selects)},
                ROW_NUMBER() OVER (
                    PARTITION BY l.vin
                    ORDER BY datetime(l.loaddate) DESC, datetime(l.date) DESC, l.rowid DESC
                ) AS rn
            FROM listings AS l
            INNER JOIN nhtsa_enrichment AS n USING(vin)
            WHERE {where_sql}
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
        ORDER BY datetime(loaddate) DESC, price DESC
        LIMIT ?
    """
    df = read_sql(query, tuple(params + [int(limit)]))
    for col in ["price", "mileage", "nhtsa_BasePrice", "nhtsa_EngineHP"]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in ["canonical_year", "nhtsa_ModelYear", "nhtsa_total_recalls", "nhtsa_total_complaints"]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    if {"nhtsa_TransmissionStyle", "nhtsa_TransmissionSpeeds"}.issubset(df.columns):
        df["nhtsa_Transmission"] = [
            combine_transmission(style, speeds)
            for style, speeds in zip(df["nhtsa_TransmissionStyle"], df["nhtsa_TransmissionSpeeds"])
        ]
    return add_trim_proxy(df)


@st.cache_data(show_spinner=False)
def load_vehicle_model_row(vin: str) -> pd.DataFrame:
    return load_vehicle_model_rows((vin,))


@st.cache_data(show_spinner=False)
def load_vehicle_model_rows(vins: tuple[str, ...]) -> pd.DataFrame:
    if not vins:
        return pd.DataFrame()
    listing_cols = table_columns("listings")
    nhtsa_cols = [column for column in table_columns("nhtsa_enrichment") if column != "vin"]
    select_cols = [f"l.{column}" for column in listing_cols]
    select_cols.extend(f"n.{column}" for column in nhtsa_cols)
    placeholders = ", ".join("?" for _ in vins)
    query = f"""
        WITH ranked AS (
            SELECT
                {', '.join(select_cols)},
                ROW_NUMBER() OVER (
                    PARTITION BY l.vin
                    ORDER BY datetime(l.loaddate) DESC, datetime(l.date) DESC, l.rowid DESC
                ) AS rn
            FROM listings AS l
            INNER JOIN nhtsa_enrichment AS n USING(vin)
            WHERE l.vin IN ({placeholders})
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
    """
    return read_sql(query, vins)


@st.cache_data(show_spinner=False)
def load_price_history(vin: str) -> pd.DataFrame:
    query = """
        WITH combined_history AS (
            SELECT history_date, price, mileage, trend, 'price_history' AS price_source
            FROM price_history
            WHERE vin = ?
            UNION ALL
            SELECT history_date, price, mileage, NULL AS trend, 'listing_history' AS price_source
            FROM listing_history
            WHERE vin = ?
        )
        SELECT history_date, price, mileage, trend, price_source
        FROM combined_history
        ORDER BY datetime(history_date)
    """
    df = read_sql(query, (vin, vin))
    if df.empty:
        return df
    df["history_date"] = pd.to_datetime(df["history_date"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["mileage"] = pd.to_numeric(df["mileage"], errors="coerce")
    df = df.dropna(subset=["history_date", "price"]).sort_values("history_date")
    df = df.drop_duplicates(subset=["history_date", "price", "price_source"], keep="last")
    if df.empty:
        return df
    grouped = df.groupby("price_source", sort=False)["price"]
    first_by_source = grouped.transform("first").replace(0, np.nan)
    df["price_change_from_previous"] = grouped.diff()
    df["price_change_from_first"] = df["price"] - first_by_source
    df["pct_change_from_first"] = df["price"] / first_by_source - 1
    return df


@st.cache_data(show_spinner=False)
def load_cohort_historical_prices(
    make: str,
    model: str,
    year: int,
    trim_proxy: str | None,
) -> pd.DataFrame:
    query = """
        SELECT
            ph.vin,
            ph.history_date,
            ph.price,
            ph.mileage,
            vi.canonical_make,
            vi.canonical_model,
            vi.canonical_year,
            vi.canonical_trim
        FROM price_history AS ph
        INNER JOIN vehicle_identity AS vi ON ph.vin = vi.vin
        WHERE ph.price IS NOT NULL
          AND ph.price > 0
          AND vi.canonical_make = ?
          AND vi.canonical_model = ?
          AND CAST(vi.canonical_year AS INTEGER) = ?
    """
    df = read_sql(query, (make, model, int(year)))
    if df.empty:
        return df
    df["history_date"] = pd.to_datetime(df["history_date"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["mileage"] = pd.to_numeric(df["mileage"], errors="coerce")
    df = df.dropna(subset=["history_date", "price"])
    df = add_trim_proxy(df)

    if trim_proxy:
        trim_match = df[df["trim_proxy"].astype(str).eq(str(trim_proxy))].copy()
        if not trim_match.empty:
            df = trim_match

    historical = (
        df.groupby("history_date", as_index=False)
        .agg(
            historical_median_price=("price", "median"),
            historical_avg_price=("price", "mean"),
            historical_unique_vins=("vin", "nunique"),
            historical_volume=("vin", "count"),
        )
        .sort_values("history_date")
    )
    return historical


@st.cache_data(show_spinner=False)
def load_cohort_prices(make: str, model: str, year: int) -> pd.DataFrame:
    listing_cols = set(table_columns("listings"))
    nhtsa_cols = set(table_columns("nhtsa_enrichment"))
    listing_trim_selects = [
        select_or_null(listing_cols, "l", column)
        for column in ["canonical_title", "canonical_make", "canonical_model", "canonical_year", "canonical_trim"]
    ]
    nhtsa_trim_selects = [
        select_or_null(nhtsa_cols, "n", column)
        for column in ["nhtsa_Trim", "nhtsa_Trim2", "nhtsa_Trim_source", "nhtsa_trim_combined"]
    ]
    nhtsa_feature_selects = [
        select_or_null(nhtsa_cols, "n", column)
        for column in ["nhtsa_BasePrice", "nhtsa_total_recalls", "nhtsa_total_complaints"]
    ]
    query = """
        WITH ranked AS (
            SELECT
                l.vin,
                l.title,
                l.vehicleTitle,
                {listing_trim_cols},
                l.price,
                l.mileage,
                l.loaddate,
                n.nhtsa_Make,
                n.nhtsa_Model,
                n.nhtsa_ModelYear,
                {nhtsa_trim_cols},
                {nhtsa_feature_cols},
                ROW_NUMBER() OVER (
                    PARTITION BY l.vin
                    ORDER BY datetime(l.loaddate) DESC, datetime(l.date) DESC, l.rowid DESC
                ) AS rn
            FROM listings AS l
            INNER JOIN nhtsa_enrichment AS n USING(vin)
            WHERE l.canonical_make = ?
              AND l.canonical_model = ?
              AND CAST(l.canonical_year AS INTEGER) = ?
              AND l.price IS NOT NULL
              AND l.price > 0
        )
        SELECT *
        FROM ranked
        WHERE rn = 1
    """.format(
        listing_trim_cols=",\n                ".join(listing_trim_selects),
        nhtsa_trim_cols=",\n                ".join(nhtsa_trim_selects),
        nhtsa_feature_cols=",\n                ".join(nhtsa_feature_selects),
    )
    df = read_sql(query, (make, model, int(year)))
    if df.empty:
        return df
    for col in ["price", "mileage", "nhtsa_BasePrice", "nhtsa_total_recalls", "nhtsa_total_complaints"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return add_trim_proxy(df)


@st.cache_data(show_spinner=False)
def load_forecasts() -> pd.DataFrame:
    if not FORECAST_PATH.exists():
        return pd.DataFrame()
    df = pd.read_csv(FORECAST_PATH)
    if df.empty:
        return df
    df["forecast_date"] = pd.to_datetime(df["forecast_date"], errors="coerce")
    if "observed_month_start" in df.columns:
        df["observed_month_start"] = pd.to_datetime(df["observed_month_start"], errors="coerce")
    elif "observed_week_start" in df.columns:
        df["observed_week_start"] = pd.to_datetime(df["observed_week_start"], errors="coerce")
    if "forecast_month" not in df.columns and "forecast_horizon_days" in df.columns:
        df["forecast_month"] = (pd.to_numeric(df["forecast_horizon_days"], errors="coerce") / 30.4375).round().astype("Int64")
    df["model_year"] = pd.to_numeric(df["model_year"], errors="coerce").astype("Int64")
    if "trim_proxy" in df.columns:
        df["trim_proxy"] = df["trim_proxy"].map(normalize_trim_proxy)
    if "model_family" not in df.columns:
        method = df["forecast_method"] if "forecast_method" in df.columns else pd.Series("global_ml", index=df.index)
        df["model_family"] = method.astype(str).str.replace("_model", "", regex=False)
    if "cohort_id" not in df.columns:
        df["cohort_id"] = df[["make", "model", "model_year", "trim_proxy"]].astype("string").fillna("UNKNOWN").agg("|".join, axis=1)
    return df


@st.cache_data(show_spinner=False)
def load_backtesting_kpis() -> pd.DataFrame:
    if not BACKTEST_KPI_PATH.exists():
        return pd.DataFrame()
    df = pd.read_csv(BACKTEST_KPI_PATH)
    if df.empty:
        return df
    for column in [
        "forecast_month",
        "backtest_rows",
        "backtest_cohorts",
        "future_price_mae",
        "future_price_wape",
        "future_price_mae_skill_vs_naive",
        "depreciation_mae_pct_points",
        "future_price_bias",
    ]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    return df


@st.cache_data(show_spinner=False)
def load_backtesting_results() -> pd.DataFrame:
    if not BACKTEST_RESULTS_PATH.exists():
        return pd.DataFrame()
    df = pd.read_csv(BACKTEST_RESULTS_PATH)
    if df.empty:
        return df
    for column in ["origin_month_start", "forecast_date"]:
        if column in df.columns:
            df[column] = pd.to_datetime(df[column], errors="coerce")
    for column in [
        "forecast_month",
        "model_year",
        "observed_median_price",
        "actual_median_price",
        "predicted_median_price",
        "predicted_depreciation_pct",
        "actual_depreciation_pct",
        "absolute_error",
        "naive_absolute_error",
    ]:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce")
    if "trim_proxy" in df.columns:
        df["trim_proxy"] = df["trim_proxy"].map(normalize_trim_proxy)
    return df


@st.cache_resource(show_spinner=False)
def load_current_model(model_name: str, modified_ns: int) -> Any:
    _ = modified_ns
    return joblib.load(MODELS_DIR / f"{model_name}.joblib")


def current_model_predictions(vin: str, model_names: list[str]) -> tuple[pd.DataFrame, list[str]]:
    raw = load_vehicle_model_row(vin)
    if raw.empty:
        return pd.DataFrame(), [f"No current-price model row found for VIN {vin}."]

    engineered = engineer_current_price_features(raw)
    if engineered.empty:
        return pd.DataFrame(), [f"VIN {vin} was removed by model feature cleaning."]

    X, y, _ = make_feature_matrix(engineered)
    actual_price = float(y.iloc[0])
    rows: list[dict[str, Any]] = []
    errors: list[str] = []

    for model_name in model_names:
        artifact_path = MODELS_DIR / f"{model_name}.joblib"
        if not artifact_path.exists():
            errors.append(f"{model_name}: artifact not found.")
            continue
        try:
            model = load_current_model(model_name, artifact_path.stat().st_mtime_ns)
            prediction = float(np.ravel(model.predict(X))[0])
            rows.append(
                {
                    "model": model_name,
                    "predicted_price": prediction,
                    "actual_price": actual_price,
                    "difference_vs_actual": prediction - actual_price,
                    "absolute_difference": abs(prediction - actual_price),
                    "pct_difference": (prediction / actual_price - 1) if actual_price else np.nan,
                }
            )
        except Exception as exc:  # pragma: no cover - defensive dashboard path
            errors.append(f"{model_name}: {exc}")

    return pd.DataFrame(rows), errors


def filtered_current_model_metrics(
    filtered_df: pd.DataFrame,
    model_names: list[str],
    max_rows: int = 100,
) -> tuple[pd.DataFrame, list[str]]:
    if filtered_df.empty:
        return pd.DataFrame(), ["No filtered VINs are available for scoped model scoring."]

    vins = tuple(filtered_df["vin"].dropna().astype(str).drop_duplicates().head(max_rows).tolist())
    raw = load_vehicle_model_rows(vins)
    if raw.empty:
        return pd.DataFrame(), ["No model-ready rows were found for the filtered VIN set."]

    engineered = engineer_current_price_features(raw)
    if engineered.empty:
        return pd.DataFrame(), ["Filtered VIN rows were removed by model feature cleaning."]

    X, y, _ = make_feature_matrix(engineered)
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    y_values = y.to_numpy(dtype="float64")
    denominator = np.where(y_values == 0, np.nan, y_values)

    for model_name in model_names:
        artifact_path = MODELS_DIR / f"{model_name}.joblib"
        if not artifact_path.exists():
            errors.append(f"{model_name}: artifact not found.")
            continue
        try:
            model = load_current_model(model_name, artifact_path.stat().st_mtime_ns)
            predictions = np.ravel(model.predict(X)).astype("float64")
            errors_abs = np.abs(y_values - predictions)
            rows.append(
                {
                    "model": model_name,
                    "scored_rows": int(len(y_values)),
                    "mae": float(np.mean(errors_abs)),
                    "rmse": float(math.sqrt(np.mean((y_values - predictions) ** 2))),
                    "mape": float(np.nanmean(errors_abs / denominator)),
                    "mean_prediction": float(np.mean(predictions)),
                    "mean_actual": float(np.mean(y_values)),
                    "bias": float(np.mean(predictions - y_values)),
                }
            )
        except Exception as exc:  # pragma: no cover - defensive dashboard path
            errors.append(f"{model_name}: {exc}")

    return pd.DataFrame(rows).sort_values("mae", na_position="last"), errors


def current_metrics_table(report: dict[str, Any]) -> pd.DataFrame:
    recommended = report.get("recommended_model")
    fit_metadata = report.get("model_fit_metadata", {})
    rows = []
    for model_name, metrics in report.get("models", {}).items():
        value_segments = metrics.get("segment_metrics", {}).get("is_high_value", {})
        everyday = value_segments.get("everyday", {})
        high_value = value_segments.get("high_value", {})
        rows.append(
            {
                "model": model_name,
                "recommended": model_name == recommended,
                "mae": metrics.get("mae"),
                "rmse": metrics.get("rmse"),
                "rmsle": metrics.get("rmsle"),
                "mape": metrics.get("mape"),
                "r2": metrics.get("r2"),
                "everyday_rows": everyday.get("rows"),
                "everyday_mae": everyday.get("mae"),
                "high_value_rows": high_value.get("rows"),
                "high_value_mae": high_value.get("mae"),
                "final_fit_rows": fit_metadata.get(model_name, {}).get("final_fit_rows"),
                "fit_strategy": fit_metadata.get(model_name, {}).get("final_fit_strategy"),
            }
        )
    return pd.DataFrame(rows).sort_values("mae", na_position="last")


def cohort_metrics_table(report: dict[str, Any]) -> pd.DataFrame:
    rows = []
    for target, model_report in report.get("models", {}).items():
        metrics = model_report.get("metrics") or {}
        target_name = model_report.get("target") or target
        horizon_label = target_name.replace("target_depreciation_pct_", "")
        if horizon_label.endswith("m"):
            horizon_months = horizon_label.replace("m", "")
        elif horizon_label.endswith("d"):
            horizon_months = float(horizon_label.replace("d", "")) / 30.4375
        else:
            horizon_months = None
        rows.append(
            {
                "model_key": target,
                "target": target_name,
                "model_family": model_report.get("model_family", "global_ml"),
                "forecast_method": model_report.get("forecast_method"),
                "horizon_months": horizon_months,
                "future_price_mae": metrics.get("future_price_mae"),
                "future_price_wape": metrics.get("future_price_wape"),
                "future_price_mae_skill_vs_naive": metrics.get("future_price_mae_skill_vs_naive"),
                "depreciation_mae_pct_points": metrics.get("depreciation_mae_pct_points"),
                "depreciation_r2": metrics.get("depreciation_r2"),
                "backtest_rows": metrics.get("backtest_rows", model_report.get("test_rows")),
                "backtest_cohorts": metrics.get("backtest_cohorts", model_report.get("test_cohorts")),
                "train_rows": model_report.get("train_rows"),
                "test_rows": model_report.get("test_rows"),
                "artifact_model_type": model_report.get("artifact_model_type"),
                "skip_reason": model_report.get("skipped") or model_report.get("validation_warning"),
            }
        )
    df = pd.DataFrame(rows)
    if "horizon_months" in df:
        df["horizon_months"] = pd.to_numeric(df["horizon_months"], errors="coerce")
        df = df.sort_values(["horizon_months", "model_family"])
    return df


def percentile_rank(values: pd.Series, value: float | None) -> float | None:
    if value is None:
        return None
    numeric = pd.to_numeric(values, errors="coerce").dropna()
    if numeric.empty:
        return None
    return float((numeric <= value).mean())


def selected_vehicle_context(row: pd.Series) -> tuple[str | None, str | None, int | None, str | None]:
    make = row.get("canonical_make")
    model = row.get("canonical_model")
    year = safe_float(row.get("canonical_year"))
    trim = row.get("trim_proxy")
    return (
        str(make) if pd.notna(make) else None,
        str(model) if pd.notna(model) else None,
        int(year) if year is not None else None,
        str(trim) if pd.notna(trim) else None,
    )


def selected_vin_label(row: pd.Series) -> str:
    price = fmt_currency(row.get("price"))
    title = row.get("canonical_title") if pd.notna(row.get("canonical_title")) else row.get("title")
    return f"{row.get('vin')} | {price} | {title}"


def render_cohort_sentiment(make: str, model: str, year: int, trim: str | None) -> None:
    st.markdown("#### Cohort sentiment")
    index, comments, match_label = load_cohort_sentiment(make, model, year, trim)
    if comments.empty:
        st.info("No scored YouTube comments match this canonical cohort.")
        return
    st.caption(f"Sentiment match: {match_label}. Comments are ranked by existing comment weight, then publication date.")
    if index.empty:
        st.info("Scored comments are available, but no aggregate sentiment index has been generated yet.")
    else:
        aggregate = index.iloc[0]
        metric_cols = st.columns(4)
        metric_cols[0].metric("Enthusiasm", fmt_number(aggregate.get("General_Enthusiast_Score"), 1))
        metric_cols[1].metric("Reliability", fmt_number(aggregate.get("Reliability_Index"), 1))
        metric_cols[2].metric("Comments", fmt_number(aggregate.get("Sample_Size")))
        metric_cols[3].metric("Confidence", aggregate.get("Confidence_Level") or "n/a")
        aspect_cols = st.columns(3)
        for column, label, target in [
            ("value_sentiment", "Value", aspect_cols[0]),
            ("performance_sentiment", "Performance", aspect_cols[1]),
            ("comfort_sentiment", "Comfort", aspect_cols[2]),
        ]:
            values = pd.to_numeric(comments[column], errors="coerce").dropna()
            target.metric(label, fmt_number((values.mean() + 1) * 50 if not values.empty else np.nan, 1))
    st.dataframe(
        comments,
        hide_index=True,
        width="stretch",
        column_config={
            "text": st.column_config.TextColumn("Comment", width="large"),
            "like_count": st.column_config.NumberColumn("Likes", format="%d"),
            "comment_weight": st.column_config.NumberColumn("Comment weight", format="%.2f"),
            "reliability_sentiment": st.column_config.NumberColumn("Reliability", format="%.2f"),
            "value_sentiment": st.column_config.NumberColumn("Value", format="%.2f"),
            "performance_sentiment": st.column_config.NumberColumn("Performance", format="%.2f"),
            "comfort_sentiment": st.column_config.NumberColumn("Comfort", format="%.2f"),
        },
    )


def render_actuals_page(
    filtered_df: pd.DataFrame,
    selected_row: pd.Series | None,
    filter_state: dict[str, Any],
) -> None:
    st.subheader("Actuals")
    if filtered_df.empty:
        st.info("No cleaned VINs match the current filters.")
        return

    metric_cols = st.columns(5)
    metric_cols[0].metric("VINs", f"{filtered_df['vin'].nunique():,}")
    metric_cols[1].metric("Median Price", fmt_currency(filtered_df["price"].median()))
    metric_cols[2].metric("Median Mileage", fmt_number(filtered_df["mileage"].median()))
    metric_cols[3].metric("Avg Recalls", fmt_number(filtered_df["nhtsa_total_recalls"].mean(), 1))
    metric_cols[4].metric("Avg Complaints", fmt_number(filtered_df["nhtsa_total_complaints"].mean(), 1))

    display_cols = [
        "vin",
        "price",
        "mileage",
        "canonical_title",
        "canonical_trim",
        "canonical_trim_source",
        "canonical_match_confidence",
        "title",
        "vehicleTitle",
        "trim_proxy",
        "nhtsa_Trim",
        "nhtsa_Trim2",
        "nhtsa_BasePrice",
        "nhtsa_total_recalls",
        "nhtsa_total_complaints",
        "nhtsa_Make",
        "nhtsa_Model",
        "nhtsa_ModelYear",
        "nhtsa_Transmission",
        "sourceName",
        "sellerType",
    ]
    st.dataframe(
        filtered_df[[col for col in display_cols if col in filtered_df.columns]],
        hide_index=True,
        width="stretch",
        column_config={
            "price": st.column_config.NumberColumn("price", format="$%d"),
            "mileage": st.column_config.NumberColumn("mileage", format="%d"),
            "nhtsa_BasePrice": st.column_config.NumberColumn("nhtsa_BasePrice", format="$%d"),
        },
    )

    if selected_row is None:
        return

    make, model, year, trim = selected_vehicle_context(selected_row)
    selected_price = safe_float(selected_row.get("price"))
    selected_mileage = safe_float(selected_row.get("mileage"))

    if not make or not model or year is None:
        st.warning("Selected VIN is missing make, model, or model year enrichment.")
        return

    render_cohort_sentiment(make, model, year, trim)

    cohort_df = load_cohort_prices(make, model, year)
    if cohort_df.empty:
        st.warning("No cohort rows were available for the selected VIN.")
        return

    chart_cohort_df = cohort_df
    cohort_label = f"{make} {model} {year}"
    active_trim = filter_state.get("trim")
    if active_trim:
        trim_scoped = cohort_df[cohort_df["trim_proxy"].eq(active_trim)].copy()
        if not trim_scoped.empty:
            chart_cohort_df = trim_scoped
            cohort_label = f"{cohort_label} {active_trim}"

    price_percentile = percentile_rank(chart_cohort_df["price"], selected_price)
    mileage_percentile = percentile_rank(chart_cohort_df["mileage"], selected_mileage)
    base_price = safe_float(selected_row.get("nhtsa_BasePrice"))
    price_to_base = selected_price - base_price if selected_price is not None and base_price else None
    cohort_median = safe_float(chart_cohort_df["price"].median())
    price_to_cohort = selected_price - cohort_median if selected_price is not None and cohort_median else None

    st.markdown("#### Selected VIN Position")
    vin_cols = st.columns(5)
    vin_cols[0].metric("Selected Price", fmt_currency(selected_price), fmt_currency(price_to_cohort))
    vin_cols[1].metric("Price Percentile", fmt_percent(price_percentile))
    vin_cols[2].metric("Mileage Percentile", fmt_percent(mileage_percentile))
    vin_cols[3].metric("NHTSA Base Price", fmt_currency(base_price))
    vin_cols[4].metric("Price vs Base", fmt_currency(price_to_base))

    fig = px.histogram(
        chart_cohort_df.dropna(subset=["price"]),
        x="price",
        nbins=40,
        labels={"price": "Price", "count": "VINs"},
        title=f"{cohort_label} Price Distribution",
    )
    if selected_price is not None:
        fig.add_vline(
            x=selected_price,
            line_color="#dc2626",
            line_width=3,
            annotation_text="selected VIN",
            annotation_position="top right",
        )
    if cohort_median is not None:
        fig.add_vline(
            x=cohort_median,
            line_color="#0f766e",
            line_dash="dash",
            annotation_text="cohort median",
            annotation_position="top left",
        )
    fig.update_layout(margin=dict(l=10, r=10, t=55, b=10), bargap=0.04)
    st.plotly_chart(fig, width="stretch")

    scatter_df = chart_cohort_df.dropna(subset=["price", "mileage"]).copy()
    if scatter_df.shape[0] > 3000:
        scatter_df = scatter_df.sample(3000, random_state=42)
    if not scatter_df.empty:
        scatter_fig = px.scatter(
            scatter_df,
            x="mileage",
            y="price",
            color="nhtsa_total_recalls",
            hover_data=["vin", "trim_proxy", "nhtsa_total_complaints"],
            labels={
                "mileage": "Mileage",
                "price": "Price",
                "nhtsa_total_recalls": "Recalls",
            },
            title="Cohort Price and Mileage Position",
        )
        if selected_price is not None and selected_mileage is not None:
            scatter_fig.add_trace(
                go.Scatter(
                    x=[selected_mileage],
                    y=[selected_price],
                    mode="markers",
                    marker=dict(color="#dc2626", size=16, symbol="x"),
                    name="Selected VIN",
                )
            )
        scatter_fig.update_layout(margin=dict(l=10, r=10, t=55, b=10))
        st.plotly_chart(scatter_fig, width="stretch")

    history_df = load_price_history(str(selected_row.get("vin")))
    st.markdown("#### Price History")
    if history_df.empty:
        st.info("No price_history or listing_history rows are available for the selected VIN.")
    else:
        price_history_metric_df = history_df[history_df["price_source"].eq("price_history")]
        metric_history_df = price_history_metric_df if not price_history_metric_df.empty else history_df
        first_price = safe_float(metric_history_df["price"].iloc[0])
        last_price = safe_float(metric_history_df["price"].iloc[-1])
        change = last_price - first_price if first_price is not None and last_price is not None else None
        pct_change = (last_price / first_price - 1) if first_price else None
        history_cols = st.columns(4)
        history_cols[0].metric("History Points", f"{history_df.shape[0]:,}")
        history_cols[1].metric("First Price", fmt_currency(first_price))
        history_cols[2].metric("Latest Price", fmt_currency(last_price))
        history_cols[3].metric("History Change", fmt_currency(change), fmt_percent(pct_change))

        history_fig = px.line(
            history_df,
            x="history_date",
            y="price",
            color="price_source",
            markers=True,
            labels={
                "history_date": "History Date",
                "price": "Price",
                "price_source": "History Source",
            },
            title="VIN Price History",
        )
        history_fig.update_layout(margin=dict(l=10, r=10, t=55, b=10))
        st.plotly_chart(history_fig, width="stretch")
        history_table_cols = [
            "history_date",
            "price_source",
            "price",
            "price_change_from_previous",
            "price_change_from_first",
            "pct_change_from_first",
            "mileage",
            "trend",
        ]
        st.dataframe(
            history_df[[col for col in history_table_cols if col in history_df.columns]],
            hide_index=True,
            width="stretch",
            column_config={
                "price": st.column_config.NumberColumn("price", format="$%d"),
                "price_change_from_previous": st.column_config.NumberColumn(
                    "price_change_from_previous",
                    format="$%d",
                ),
                "price_change_from_first": st.column_config.NumberColumn(
                    "price_change_from_first",
                    format="$%d",
                ),
                "pct_change_from_first": st.column_config.NumberColumn(
                    "pct_change_from_first",
                    format="%.2f",
                ),
                "mileage": st.column_config.NumberColumn("mileage", format="%d"),
            },
        )


def render_models_page(selected_row: pd.Series | None, filtered_df: pd.DataFrame) -> None:
    st.subheader("Model Predictions")
    current_report = load_json(str(CURRENT_REPORT_PATH))
    cohort_report = load_json(str(COHORT_REPORT_PATH))
    if not normalization_versions_match(current_report, cohort_report):
        st.error(
            "Predictions are disabled because the model-report normalization version does not "
            "match the cleaned database. Retrain both model workflows against this canonical schema."
        )
        return

    current_metrics = current_metrics_table(current_report)
    if not current_metrics.empty:
        top_cols = st.columns(4)
        recommended = current_report.get("recommended_model")
        best_row = current_metrics[current_metrics["model"].eq(recommended)].head(1)
        best_row = best_row.iloc[0] if not best_row.empty else current_metrics.iloc[0]
        selected_is_high_value = safe_float(selected_row.get("price")) >= 150_000 if selected_row is not None else False
        segment_label = "High-value" if selected_is_high_value else "Everyday"
        segment_metric = "high_value_mae" if selected_is_high_value else "everyday_mae"
        segment_rows = "high_value_rows" if selected_is_high_value else "everyday_rows"
        top_cols[0].metric("Recommended Model", recommended or "n/a")
        top_cols[1].metric(f"{segment_label} MAE", fmt_currency(best_row.get(segment_metric)))
        top_cols[2].metric(f"{segment_label} Test Rows", fmt_number(best_row.get(segment_rows)))
        top_cols[3].metric("Global RMSE", fmt_currency(best_row.get("rmse")))
        st.caption("Headlines use the selected vehicle's value segment; global metrics remain context because the high-value tail can dominate RMSE.")

        chart_df = current_metrics.melt(
            id_vars=["model", "recommended"],
            value_vars=["mae", "rmse"],
            var_name="metric",
            value_name="value",
        )
        metric_fig = px.bar(
            chart_df,
            x="model",
            y="value",
            color="metric",
            barmode="group",
            labels={"value": "Dollars", "model": "Model"},
            title="Current-Price Error by Model",
        )
        metric_fig.update_layout(margin=dict(l=10, r=10, t=55, b=10), xaxis_tickangle=-20)
        st.plotly_chart(metric_fig, width="stretch")

        st.dataframe(
            current_metrics,
            hide_index=True,
            width="stretch",
            column_config={
                "mae": st.column_config.NumberColumn("mae", format="$%d"),
                "rmse": st.column_config.NumberColumn("rmse", format="$%d"),
                "mape": st.column_config.NumberColumn("mape", format="%.2f"),
                "r2": st.column_config.NumberColumn("r2", format="%.4f"),
                "everyday_mae": st.column_config.NumberColumn("everyday MAE", format="$%d"),
                "high_value_mae": st.column_config.NumberColumn("high-value MAE", format="$%d"),
                "final_fit_rows": st.column_config.NumberColumn("final_fit_rows", format="%d"),
            },
        )

    model_names = list(current_report.get("models", {}).keys())
    if model_names and not filtered_df.empty:
        with st.spinner("Scoring models on the active filtered VIN set..."):
            scoped_metrics, scoped_errors = filtered_current_model_metrics(filtered_df, model_names)
        if scoped_errors:
            with st.expander("Filtered scoring messages"):
                for error in scoped_errors:
                    st.write(error)
        if not scoped_metrics.empty:
            scoped_metrics["recommended"] = scoped_metrics["model"].eq(current_report.get("recommended_model"))
            st.markdown("#### Filtered VIN Scoring")
            scoped_cols = st.columns(4)
            scoped_best = scoped_metrics.iloc[0]
            scoped_cols[0].metric("Scored VINs", fmt_number(scoped_best.get("scored_rows")))
            scoped_cols[1].metric("Best Filtered MAE", fmt_currency(scoped_best.get("mae")))
            scoped_cols[2].metric("Best Filtered MAPE", fmt_percent(scoped_best.get("mape")))
            scoped_cols[3].metric("Best Filtered Bias", fmt_currency(scoped_best.get("bias")))

            scoped_fig = px.bar(
                scoped_metrics,
                x="model",
                y="mae",
                color="recommended",
                labels={"mae": "MAE on Active Filter", "model": "Model"},
                title="Filtered Current-Price Error by Model",
            )
            scoped_fig.update_layout(margin=dict(l=10, r=10, t=55, b=10), xaxis_tickangle=-20)
            st.plotly_chart(scoped_fig, width="stretch")
            st.dataframe(
                scoped_metrics,
                hide_index=True,
                width="stretch",
                column_config={
                    "mae": st.column_config.NumberColumn("mae", format="$%d"),
                    "rmse": st.column_config.NumberColumn("rmse", format="$%d"),
                    "mape": st.column_config.NumberColumn("mape", format="%.2f"),
                    "mean_prediction": st.column_config.NumberColumn("mean_prediction", format="$%d"),
                    "mean_actual": st.column_config.NumberColumn("mean_actual", format="$%d"),
                    "bias": st.column_config.NumberColumn("bias", format="$%d"),
                    "scored_rows": st.column_config.NumberColumn("scored_rows", format="%d"),
                },
            )

    if selected_row is None:
        st.info("No VIN is selected for model scoring.")
        return

    if model_names:
        with st.spinner("Loading current-price models and scoring selected VIN..."):
            predictions, errors = current_model_predictions(str(selected_row.get("vin")), model_names)
        if errors:
            with st.expander("Model scoring messages"):
                for error in errors:
                    st.write(error)
        if not predictions.empty:
            predictions["recommended"] = predictions["model"].eq(current_report.get("recommended_model"))
            pred_cols = st.columns(4)
            recommended_pred = predictions[predictions["recommended"]]
            if recommended_pred.empty:
                recommended_pred = predictions.sort_values("absolute_difference").head(1)
            pred_cols[0].metric("Actual Price", fmt_currency(predictions["actual_price"].iloc[0]))
            pred_cols[1].metric("Recommended Prediction", fmt_currency(recommended_pred["predicted_price"].iloc[0]))
            pred_cols[2].metric("Prediction Delta", fmt_currency(recommended_pred["difference_vs_actual"].iloc[0]))
            pred_cols[3].metric("Percent Delta", fmt_percent(recommended_pred["pct_difference"].iloc[0]))

            pred_fig = px.bar(
                predictions.sort_values("predicted_price"),
                x="model",
                y="predicted_price",
                color="recommended",
                labels={"predicted_price": "Predicted Price", "model": "Model"},
                title="VIN Current-Price Predictions",
            )
            pred_fig.add_hline(
                y=float(predictions["actual_price"].iloc[0]),
                line_dash="dash",
                line_color="#0f766e",
                annotation_text="actual",
            )
            pred_fig.update_layout(margin=dict(l=10, r=10, t=55, b=10), xaxis_tickangle=-20)
            st.plotly_chart(pred_fig, width="stretch")
            st.dataframe(
                predictions.sort_values("absolute_difference"),
                hide_index=True,
                width="stretch",
                column_config={
                    "predicted_price": st.column_config.NumberColumn("predicted_price", format="$%d"),
                    "actual_price": st.column_config.NumberColumn("actual_price", format="$%d"),
                    "difference_vs_actual": st.column_config.NumberColumn("difference_vs_actual", format="$%d"),
                    "absolute_difference": st.column_config.NumberColumn("absolute_difference", format="$%d"),
                    "pct_difference": st.column_config.NumberColumn("pct_difference", format="%.2f"),
                },
            )

    cohort_metrics = cohort_metrics_table(cohort_report)
    if not cohort_metrics.empty:
        st.markdown("#### Depreciation Forecast Models")
        cohort_cols = st.columns(4)
        validated_metrics = cohort_metrics.dropna(subset=["future_price_mae_skill_vs_naive"]).copy()
        best_row = (
            validated_metrics.sort_values(
                ["future_price_mae_skill_vs_naive", "future_price_wape"],
                ascending=[False, True],
            ).head(1)
            if not validated_metrics.empty
            else pd.DataFrame()
        )
        best_skill = best_row["future_price_mae_skill_vs_naive"].iloc[0] if not best_row.empty else np.nan
        cohort_cols[0].metric("Cohort Rows", fmt_number(cohort_report.get("cohort_month_rows") or cohort_report.get("cohort_week_rows")))
        cohort_cols[1].metric("Cohorts", fmt_number(cohort_report.get("cohorts")))
        cohort_cols[2].metric("Model Families", fmt_number(cohort_metrics["model_family"].nunique()))
        cohort_cols[3].metric("Best Skill vs Naive", fmt_percent(best_skill))

        if not validated_metrics.empty:
            horizon_fig = px.line(
                validated_metrics,
                x="horizon_months",
                y="future_price_mae",
                color="model_family",
                markers=True,
                labels={
                    "horizon_months": "Forecast Horizon Months",
                    "future_price_mae": "Future Price MAE",
                    "model_family": "Model Family",
                },
                title="Backtested Future-Price Error by Model Family",
            )
            horizon_fig.update_layout(margin=dict(l=10, r=10, t=55, b=10))
            st.plotly_chart(horizon_fig, width="stretch")

        st.dataframe(
            cohort_metrics[
                [
                    "model_family",
                    "forecast_method",
                    "target",
                    "horizon_months",
                    "future_price_mae",
                    "future_price_wape",
                    "future_price_mae_skill_vs_naive",
                    "depreciation_mae_pct_points",
                    "backtest_rows",
                    "backtest_cohorts",
                    "skip_reason",
                ]
            ],
            hide_index=True,
            width="stretch",
            column_config={
                "future_price_mae": st.column_config.NumberColumn("future_price_mae", format="$%d"),
                "future_price_wape": st.column_config.NumberColumn("future_price_wape", format="percent"),
                "future_price_mae_skill_vs_naive": st.column_config.NumberColumn(
                    "future_price_mae_skill_vs_naive",
                    format="percent",
                ),
                "depreciation_mae_pct_points": st.column_config.NumberColumn(
                    "depreciation_mae_pct_points",
                    format="%.2f",
                ),
            },
        )

    backtest_kpis = load_backtesting_kpis()
    if not backtest_kpis.empty:
        model_horizon_kpis = backtest_kpis[backtest_kpis["kpi_scope"].eq("model_horizon")].copy()
        if not model_horizon_kpis.empty:
            st.markdown("#### Time-Series Backtesting KPIs")
            kpi_cols = st.columns(4)
            best_kpi = model_horizon_kpis.sort_values(
                ["future_price_mae_skill_vs_naive", "future_price_wape"],
                ascending=[False, True],
            ).iloc[0]
            kpi_cols[0].metric("Backtest Rows", fmt_number(model_horizon_kpis["backtest_rows"].sum()))
            kpi_cols[1].metric("Backtested Cohorts", fmt_number(model_horizon_kpis["backtest_cohorts"].max()))
            kpi_cols[2].metric("Best Backtest MAE", fmt_currency(best_kpi.get("future_price_mae")))
            kpi_cols[3].metric("Best Backtest Skill", fmt_percent(best_kpi.get("future_price_mae_skill_vs_naive")))

            backtest_fig = px.bar(
                model_horizon_kpis.sort_values(["forecast_month", "model_family"]),
                x="model_family",
                y="future_price_mae",
                color="forecast_method",
                facet_col="forecast_month",
                labels={
                    "future_price_mae": "Future Price MAE",
                    "model_family": "Model Family",
                    "forecast_method": "Forecast Method",
                },
                title="Backtest MAE by Model and Horizon",
            )
            backtest_fig.update_layout(margin=dict(l=10, r=10, t=55, b=10))
            st.plotly_chart(backtest_fig, width="stretch")
            st.dataframe(
                model_horizon_kpis.sort_values(["forecast_month", "future_price_mae"]),
                hide_index=True,
                width="stretch",
                column_config={
                    "future_price_mae": st.column_config.NumberColumn("future_price_mae", format="$%d"),
                    "future_price_wape": st.column_config.NumberColumn("future_price_wape", format="percent"),
                    "future_price_mae_skill_vs_naive": st.column_config.NumberColumn(
                        "future_price_mae_skill_vs_naive",
                        format="percent",
                    ),
                    "future_price_bias": st.column_config.NumberColumn("future_price_bias", format="$%d"),
                    "depreciation_mae_pct_points": st.column_config.NumberColumn(
                        "depreciation_mae_pct_points",
                        format="%.2f",
                    ),
                },
            )

    render_forecast_for_selected_vehicle(selected_row)


def render_forecast_for_selected_vehicle(selected_row: pd.Series) -> None:
    make, model, year, trim = selected_vehicle_context(selected_row)
    if not make or not model or year is None:
        return
    forecasts = load_forecasts()
    if forecasts.empty:
        st.info("No cohort future forecast CSV is available.")
        return

    matched = forecasts[
        (forecasts["make"].astype(str) == make)
        & (forecasts["model"].astype(str) == model)
        & (pd.to_numeric(forecasts["model_year"], errors="coerce") == year)
    ].copy()
    if matched.empty:
        st.info("No cohort future forecasts match the selected VIN make, model, and year.")
        return

    exact = matched[matched["trim_proxy"].astype(str) == str(trim)].copy() if trim else pd.DataFrame()
    if exact.empty:
        forecast_df = (
            matched.sort_values(
                ["forecast_method", "forecast_month", "unique_vins", "volume"],
                ascending=[True, True, False, False],
            )
            .groupby(["forecast_method", "forecast_month"], as_index=False)
            .head(1)
            .copy()
        )
        match_label = "Fallback"
    else:
        forecast_df = exact.sort_values(["forecast_method", "forecast_month"]).copy()
        match_label = "Exact"

    line_df = forecast_df.sort_values("forecast_date")
    forecast_trim = str(line_df["trim_proxy"].dropna().iloc[0]) if "trim_proxy" in line_df and line_df["trim_proxy"].notna().any() else "UNKNOWN_TRIM"
    cohort_key = f"{make} {model} {year} {forecast_trim}"

    st.markdown("#### Selected VIN Cohort Forecast")
    forecast_cols = st.columns(4)
    forecast_cols[0].metric("Forecast Cohort", cohort_key)
    forecast_cols[1].metric("Trim Match", match_label)
    forecast_cols[2].metric("Observed Median", fmt_currency(forecast_df["observed_median_price"].iloc[0]))
    forecast_cols[3].metric("Forecast Methods", fmt_number(forecast_df["forecast_method"].nunique()))
    historical_df = load_cohort_historical_prices(make, model, year, forecast_trim)

    forecast_fig = go.Figure()
    if not historical_df.empty:
        forecast_fig.add_trace(
            go.Scatter(
                x=historical_df["history_date"],
                y=historical_df["historical_median_price"],
                mode="lines+markers",
                name="Historical cohort median",
                line=dict(color="#0f766e"),
                customdata=historical_df[
                    ["historical_unique_vins", "historical_volume", "historical_avg_price"]
                ],
                hovertemplate=(
                    "%{x|%Y-%m-%d}<br>"
                    "Median price: $%{y:,.0f}<br>"
                    "Average price: $%{customdata[2]:,.0f}<br>"
                    "VINs: %{customdata[0]:,.0f}<br>"
                    "Rows: %{customdata[1]:,.0f}<extra></extra>"
                ),
            )
        )
    for method, method_df in line_df.groupby("forecast_method", dropna=False):
        forecast_fig.add_trace(
            go.Scatter(
                x=method_df["forecast_date"],
                y=method_df["predicted_median_price"],
                mode="lines+markers",
                name=str(method),
                customdata=method_df[["forecast_month", "model_family"]],
                hovertemplate=(
                    "%{x|%Y-%m-%d}<br>"
                    "Predicted median price: $%{y:,.0f}<br>"
                    "Forecast month: %{customdata[0]}<br>"
                    "Model family: %{customdata[1]}<extra></extra>"
                ),
            )
        )
    observed_date = line_df.get("observed_month_start", line_df.get("observed_week_start")).iloc[0]
    observed_price = line_df["observed_median_price"].iloc[0]
    forecast_fig.add_trace(
        go.Scatter(
            x=[observed_date],
            y=[observed_price],
            mode="markers",
            marker=dict(color="#0f766e", size=13),
            name="Observed cohort median",
        )
    )
    forecast_fig.update_layout(
        title="Future Cohort Price by Month",
        xaxis_title="Date",
        yaxis_title="Median Price",
        margin=dict(l=10, r=10, t=55, b=10),
    )
    st.plotly_chart(forecast_fig, width="stretch")

    st.dataframe(
        forecast_df[
            [
                "model_family",
                "forecast_method",
                "trim_proxy",
                "observed_month_start" if "observed_month_start" in forecast_df.columns else "observed_week_start",
                "observed_median_price",
                "forecast_month",
                "forecast_date",
                "predicted_depreciation_pct",
                "predicted_median_price",
                "unique_vins",
                "volume",
            ]
        ].sort_values(["forecast_method", "forecast_month"]),
        hide_index=True,
        width="stretch",
        column_config={
            "observed_median_price": st.column_config.NumberColumn("observed_median_price", format="$%d"),
            "predicted_median_price": st.column_config.NumberColumn("predicted_median_price", format="$%d"),
            "predicted_depreciation_pct": st.column_config.NumberColumn("predicted_depreciation_pct", format="%.2f"),
        },
    )

    backtests = load_backtesting_results()
    if backtests.empty:
        return
    backtest_match = backtests[
        (backtests["make"].astype(str) == make)
        & (backtests["model"].astype(str) == model)
        & (pd.to_numeric(backtests["model_year"], errors="coerce") == year)
    ].copy()
    if trim:
        exact_backtest = backtest_match[backtest_match["trim_proxy"].astype(str) == str(trim)].copy()
        if not exact_backtest.empty:
            backtest_match = exact_backtest
    if backtest_match.empty:
        return

    st.markdown("#### Selected Cohort Backtests")
    if "backtest_scheme" in backtest_match.columns:
        st.caption("Expanding rolling-origin backtest: each prediction uses only data observed at or before its origin, with at least six prior cohort months.")
    st.dataframe(
        backtest_match[
            [
                "model_family",
                "forecast_method",
                *[column for column in ["backtest_scheme", "train_window_start", "train_window_end", "minimum_history_months"] if column in backtest_match.columns],
                "origin_month_start",
                "forecast_month",
                "forecast_date",
                "observed_median_price",
                "actual_median_price",
                "predicted_median_price",
                "absolute_error",
                "naive_absolute_error",
            ]
        ].sort_values(["forecast_month", "absolute_error"]),
        hide_index=True,
        width="stretch",
        column_config={
            "observed_median_price": st.column_config.NumberColumn("observed_median_price", format="$%d"),
            "actual_median_price": st.column_config.NumberColumn("actual_median_price", format="$%d"),
            "predicted_median_price": st.column_config.NumberColumn("predicted_median_price", format="$%d"),
            "absolute_error": st.column_config.NumberColumn("absolute_error", format="$%d"),
            "naive_absolute_error": st.column_config.NumberColumn("naive_absolute_error", format="$%d"),
        },
    )


def render_research_page() -> None:
    st.subheader("Research Context")
    current_report = load_json(str(CURRENT_REPORT_PATH))
    cohort_report = load_json(str(COHORT_REPORT_PATH))
    sources = []
    for report_name, report in [
        ("Current-price models", current_report),
        ("Cohort depreciation models", cohort_report),
    ]:
        for source in report.get("research_sources", []):
            sources.append(
                {
                    "area": report_name,
                    "title": source.get("title"),
                    "url": source.get("url"),
                    "reason": source.get("reason"),
                }
            )
    if sources:
        st.dataframe(pd.DataFrame(sources), hide_index=True, width="stretch")

    current_notes = current_report.get("notes") or []
    cohort_notes = [
        "Cohort grain is make, model, model year, and trim proxy.",
        "Forecast targets are future median-price depreciation percentages at monthly horizons.",
        "Global ML, SARIMAX, Prophet, and TimesFM forecasts are compared when their dependencies and cohort support are available.",
        "Backtesting KPIs compare future-price MAE and WAPE with a no-change naive baseline.",
    ]
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("#### Current-Price Model Controls")
        for note in current_notes:
            st.write(f"- {note}")
    with col2:
        st.markdown("#### Depreciation Model Controls")
        for note in cohort_notes:
            st.write(f"- {note}")


def sidebar_filters() -> tuple[pd.DataFrame, pd.Series | None, dict[str, Any]]:
    st.sidebar.title("Filters")
    make_options = ["All"] + get_makes()
    default_make_index = make_options.index("TOYOTA") if "TOYOTA" in make_options else 0
    make_value = st.sidebar.selectbox("Make", make_options, index=default_make_index)
    make = None if make_value == "All" else make_value

    model_options = ["All"] + get_models(make)
    model_value = st.sidebar.selectbox("Model", model_options)
    model = None if model_value == "All" else model_value

    year_options = ["All"] + get_years(make, model)
    year_value = st.sidebar.selectbox("Year", year_options)
    year = None if year_value == "All" else int(year_value)

    trim_options = ["All"] + get_trim_options(make, model, year)
    trim_disabled = len(trim_options) == 1
    trim_value = st.sidebar.selectbox("Trim", trim_options, disabled=trim_disabled)
    trim = None if trim_value == "All" else trim_value

    row_limit = st.sidebar.slider("VIN row limit", min_value=250, max_value=10000, value=5000, step=250)
    filtered_df = load_filtered_listings(make, model, year, row_limit)
    if trim:
        filtered_df = filtered_df[filtered_df["trim_proxy"].eq(trim)].copy()

    if filtered_df.empty:
        st.sidebar.info("No VINs match the selected filters.")
        return filtered_df, None, {"make": make, "model": model, "year": year, "trim": trim}

    labels = {
        row.vin: f"{row.vin} | {fmt_currency(row.price)} | {row.title or row.vehicleTitle}"
        for row in filtered_df.itertuples(index=False)
    }
    selected_vin = st.sidebar.selectbox(
        "VIN",
        filtered_df["vin"].astype(str).tolist(),
        format_func=lambda vin: labels.get(vin, vin),
    )
    selected = filtered_df[filtered_df["vin"].astype(str).eq(str(selected_vin))].iloc[0]
    return filtered_df, selected, {"make": make, "model": model, "year": year, "trim": trim}


def main() -> None:
    st.title("Automotive Market Dashboard")
    st.caption("Cleaned listing actuals, NHTSA enrichment, current-price models, and cohort depreciation forecasts.")

    missing_paths = [str(path.relative_to(BASE_DIR)) for path in [DB_PATH, CURRENT_REPORT_PATH] if not path.exists()]
    if missing_paths:
        st.error(f"Missing required project outputs: {', '.join(missing_paths)}")
        return

    if not has_canonical_schema():
        st.warning(
            "The cleaned database does not contain the canonical vehicle identity schema. "
            "Run DataPipeline/DataCleaning.py before using filters or predictions."
        )
        return

    quality = normalization_quality_metrics()
    quality_cols = st.columns(5)
    quality_cols[0].metric("Canonical vehicles", f"{quality.get('vehicles', 0):,}")
    quality_cols[1].metric("Trim coverage", fmt_percent(quality.get("trim_coverage")))
    quality_cols[2].metric("EPA match rate", fmt_percent(quality.get("epa_match_rate")))
    quality_cols[3].metric("Unresolved titles", f"{quality.get('unresolved', 0):,}")
    quality_cols[4].metric(
        "NHTSA identity disagreements",
        f"{quality.get('nhtsa_identity_disagreements', 0):,}",
    )

    filtered_df, selected_row, filter_state = sidebar_filters()

    if selected_row is not None:
        st.markdown(f"**Selected VIN:** `{selected_row.get('vin')}`")
        st.caption(selected_vin_label(selected_row))

    actuals_tab, models_tab, research_tab = st.tabs(["Actuals", "Model Predictions", "Research Context"])
    with actuals_tab:
        render_actuals_page(filtered_df, selected_row, filter_state)
    with models_tab:
        render_models_page(selected_row, filtered_df)
    with research_tab:
        render_research_page()


if __name__ == "__main__":
    main()
