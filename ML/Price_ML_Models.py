"""
Train current vehicle price models with leakage-safe validation.

By default the current-price task uses every eligible VIN, while SQLite selects
the latest listing snapshot before Python materializes the joined model frame.
Pass a positive ``--sample-size`` for bounded development and smoke runs.
"""

from __future__ import annotations

import argparse
import gc
import json
import sqlite3
import warnings
from datetime import datetime
from math import prod
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
import scipy.sparse as sp
from category_encoders import TargetEncoder
from lightgbm import LGBMClassifier, LGBMRegressor
from sklearn.base import BaseEstimator, RegressorMixin, clone
from sklearn.compose import ColumnTransformer, TransformedTargetRegressor
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
from sklearn.linear_model import ElasticNet, Ridge
from sklearn.metrics import (
    mean_absolute_error,
    mean_absolute_percentage_error,
    mean_squared_error,
    r2_score,
)
from sklearn.model_selection import GroupKFold, GroupShuffleSplit, RandomizedSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer, OneHotEncoder, StandardScaler

warnings.filterwarnings("ignore")

BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "CAR_DATA_OUTPUT" / "CAR_DATA_CLEANED.db"
ABSA_DB_PATH = BASE_DIR / "CAR_DATA_OUTPUT" / "CAR_YOUTUBE_COMMENTS.db"
OUTPUT_DIR = BASE_DIR / "MODELS_OUTPUT"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

HIGH_VALUE_THRESHOLD = 150_000
RANDOM_STATE = 42
DEFAULT_SAMPLE_SIZE = 0
DEFAULT_SAMPLE_STRATEGY = "recent"
DEFAULT_SQL_CHUNK_SIZE = 100_000
DEFAULT_TUNING_SAMPLE_SIZE = 200_000
MAX_PARALLEL_TUNING_ROWS = 500_000
DEFAULT_FEATURE_PROFILE_ROWS = 200_000
RANDOM_FOREST_FULL_FIT_MAX_ROWS = 300_000
RANDOM_FOREST_MAX_CATEGORIES_PER_FEATURE = 25
RANDOM_FOREST_MAX_LEAF_NODES = 32_768
MAX_MODEL_WEIGHT_ROWS = 40
READABLE_MODEL_WEIGHT_REPORT_ROWS = 30

DROP_FEATURE_COLUMNS = {
    "price",
    "price_band",
    "vin",
    "date",
    "loaddate",
    "Vehicle_Entity",
    "title",
    "canonical_title",
}

PRICE_LEAKAGE_FEATURE_COLUMNS = {
    "nhtsa_BasePrice",
    "nhtsa_BasePrice_source",
}

TARGET_ENCODE_TOKENS = (
    "make",
    "model",
    "trim",
    "manufacturer",
    "segment",
    "title",
    "location",
)

TRIM_FEATURE_CANDIDATES = ["canonical_trim"]

TRIM_METADATA_COLUMNS = [
    "canonical_trim",
    "title_trim",
    "trim_combined",
    "trim_source",
    "nhtsa_Trim",
    "nhtsa_Trim2",
    "nhtsa_Trim_source",
    "nhtsa_trim_combined",
]

IDENTITY_DIAGNOSTIC_COLUMNS = {
    "title_trim",
    "trim_combined",
    "trim_source",
    "nhtsa_Trim",
    "nhtsa_Trim2",
    "nhtsa_Trim_source",
    "nhtsa_trim_combined",
    "nhtsa_Make",
    "nhtsa_Model",
    "nhtsa_ModelYear",
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
}


def assert_canonical_identity_contract(feature_df: pd.DataFrame) -> None:
    """Fail fast if a legacy trim can enter the price-model feature matrix."""
    forbidden = {
        "nhtsa_Trim", "nhtsa_Trim2", "nhtsa_trim_combined", "trim_combined",
        "title_trim", "trim_source", "canonical_trim_raw", "canonical_trim_source",
        "canonical_match_confidence", "canonical_match_status", "epa_vehicle_id",
        "epa_match_status", "nhtsa_trim_agrees",
    }
    leaked = sorted(forbidden.intersection(feature_df.columns))
    if leaked:
        raise AssertionError("Non-canonical trim metadata entered the feature matrix: " + ", ".join(leaked))
    if "canonical_trim" not in feature_df.columns:
        raise AssertionError("canonical_trim must be present in the price-model feature matrix")

RESEARCH_REFERENCES = [
    {
        "title": "How much is my car worth? A methodology for predicting used cars prices using Random Forest",
        "url": "https://arxiv.org/abs/1711.06970",
        "reason": "Supports tree-based supervised vehicle pricing with careful feature selection.",
    },
    {
        "title": "ProbSAINT: Probabilistic Tabular Regression for Used Car Pricing",
        "url": "https://arxiv.org/abs/2403.03812",
        "reason": "Frames used-car pricing as tabular regression where uncertainty and dynamic market context matter.",
    },
    {
        "title": "Manheim Used Vehicle Value Index Summary Methodology",
        "url": "https://site.manheim.com/wp-content/uploads/sites/2/2024/02/Used-Vehicle-Summary-Methodology.pdf",
        "reason": "Motivates mileage, mix, outlier, and seasonality controls for used-vehicle price modeling.",
    },
]


def normalize_trim_feature(value: Any, fallback: str = "UNKNOWN_TRIM") -> str:
    if value is None or pd.isna(value):
        return fallback
    text = str(value).strip().upper().replace(" ", "_")
    return text if text and text not in {"NA", "N_A", "NONE", "NULL", "UNKNOWN"} else fallback


def normalize_trim_series(
    series: pd.Series,
    fallback: str = "UNKNOWN_TRIM",
) -> pd.Series:
    """Normalize canonical trim with vectorized Arrow string operations."""
    text = (
        series.astype("string[pyarrow]")
        .str.strip()
        .str.upper()
        .str.replace(" ", "_", regex=False)
    )
    invalid = text.isna() | text.eq("") | text.isin(
        ["NA", "N_A", "NONE", "NULL", "UNKNOWN"]
    )
    return text.mask(invalid, fallback).astype("string[pyarrow]")


def identity_normalization_profile(df: pd.DataFrame) -> dict[str, Any]:
    """Summarize the canonical identity contract recorded with model outputs."""
    total = max(int(df.shape[0]), 1)

    def distribution(column: str) -> dict[str, int]:
        if column not in df.columns:
            return {}
        return {
            str(key): int(value)
            for key, value in df[column].fillna("NULL").astype("string[pyarrow]").value_counts().items()
        }

    canonical_trim = df.get(
        "canonical_trim",
        pd.Series(index=df.index, dtype="string[pyarrow]"),
    ).astype("string[pyarrow]")
    unresolved = canonical_trim.fillna("UNKNOWN_TRIM").eq("UNKNOWN_TRIM")
    epa_matched = df.get("epa_vehicle_id", pd.Series(index=df.index, dtype="object")).notna()
    disagreements = {}
    for column in ["nhtsa_year_agrees", "nhtsa_make_agrees", "nhtsa_model_agrees", "nhtsa_trim_agrees"]:
        if column in df.columns:
            observed = df[column].dropna()
            disagreements[column] = {
                "observed_rows": int(observed.shape[0]),
                "disagreement_rate": float((observed == 0).mean()) if not observed.empty else None,
            }
    return {
        "versions": distribution("normalization_version"),
        "trim_coverage_rate": float(1 - unresolved.sum() / total),
        "unresolved_rows": int(unresolved.sum()),
        "source_distribution": distribution("canonical_trim_source"),
        "confidence_distribution": distribution("canonical_match_confidence"),
        "epa_match_rate": float(epa_matched.sum() / total),
        "nhtsa_disagreement": disagreements,
        "identity_precedence": "NHTSA make/model anchors; title-derived trim; EPA validation/standardization",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Train vehicle price ML models.")
    parser.add_argument("--db-path", default=str(DB_PATH), help="Path to CAR_DATA_CLEANED.db.")
    parser.add_argument("--absa-db-path", default=str(ABSA_DB_PATH), help="Path to sentiment DB.")
    parser.add_argument("--output-dir", default=str(OUTPUT_DIR), help="Directory for model artifacts.")
    parser.add_argument(
        "--task",
        choices=["current", "all"],
        default="current",
        help="'all' runs current-price modeling and then delegates history tasks to Time_Series_Price.",
    )
    parser.add_argument(
        "--split-date",
        default=None,
        help="Validation date cutoff. Defaults to the 80th percentile listing load date.",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=DEFAULT_SAMPLE_SIZE,
        help=(
            "Listing snapshots to scope before latest-per-VIN selection. "
            "The default 0 uses every eligible VIN; pass a positive value for a bounded run."
        ),
    )
    parser.add_argument(
        "--sample-strategy",
        choices=["recent", "rowid_even", "none"],
        default=DEFAULT_SAMPLE_STRATEGY,
        help=(
            "How to bound listings when --sample-size is positive. 'recent' uses "
            "the loaddate index, 'rowid_even' spreads reads across the table, and "
            "'none' is equivalent to the first N rows."
        ),
    )
    parser.add_argument(
        "--keep-duplicate-vins",
        action="store_true",
        help="Keep repeated VIN rows. By default, training keeps the latest loaddate per VIN.",
    )
    parser.add_argument(
        "--target-months",
        default="1",
        help="Monthly depreciation target(s) passed through when --task all is used.",
    )
    parser.add_argument(
        "--forecast-months",
        type=int,
        default=60,
        help="Number of future monthly depreciation forecast points when --task all is used.",
    )
    return parser.parse_args()


def _table_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    return [row[1] for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()]


def _table_schema(conn: sqlite3.Connection, table: str) -> dict[str, str]:
    return {
        str(row[1]): str(row[2]).upper()
        for row in conn.execute(f"PRAGMA table_info('{table}')").fetchall()
    }


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    if "." in table:
        schema, name = table.split(".", 1)
        row = conn.execute(
            f"SELECT 1 FROM {schema}.sqlite_master WHERE type = 'table' AND name = ?",
            (name,),
        ).fetchone()
        return row is not None
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _scoped_listings_sql(sample_size: int | None, sample_strategy: str) -> str:
    """Return SQL for a bounded listings scope that remains friendly to SQLite."""
    if not sample_size or sample_size <= 0:
        return "SELECT * FROM listings"

    sample_size = int(sample_size)
    if sample_strategy == "recent":
        return f"""
            SELECT *
            FROM listings
            ORDER BY loaddate DESC
            LIMIT {sample_size}
        """
    if sample_strategy == "rowid_even":
        return f"""
            WITH RECURSIVE max_row AS (
                SELECT MAX(rowid) AS max_rowid FROM listings
            ),
            sample_ids(n, rid) AS (
                SELECT
                    0,
                    1
                UNION ALL
                SELECT
                    n + 1,
                    CAST(1 + (n + 1) * ((max_rowid - 1.0) / {sample_size}) AS INTEGER)
                FROM sample_ids, max_row
                WHERE n + 1 < {sample_size}
            )
            SELECT l.*
            FROM sample_ids AS s
            JOIN listings AS l
                ON l.rowid = s.rid
        """
    return f"SELECT * FROM listings LIMIT {sample_size}"


def _eligible_listings_sql(sample_size: int | None, sample_strategy: str) -> str:
    """Apply the same target-row validity checks before materialization."""
    scoped = _scoped_listings_sql(sample_size, sample_strategy)
    return f"""
        SELECT *
        FROM (
            {scoped}
        )
        WHERE price IS NOT NULL
          AND CAST(price AS REAL) > 0
          AND mileage IS NOT NULL
          AND CAST(mileage AS REAL) >= 0
    """


def _modeling_listings_sql(
    sample_size: int | None,
    sample_strategy: str,
    deduplicate_vins: bool,
) -> str:
    """Return eligible listing rows, reducing to the latest snapshot in SQLite."""
    eligible = _eligible_listings_sql(sample_size, sample_strategy)
    if not deduplicate_vins:
        return eligible

    # Full runs avoid materializing the entire listings table as a CTE. The
    # cleaned table's (vin, loaddate) primary key makes this group-and-join
    # deterministic and index-friendly.
    if not sample_size or sample_size <= 0:
        return """
            SELECT l.*
            FROM listings AS l
            INNER JOIN (
                SELECT vin, MAX(loaddate) AS latest_loaddate
                FROM listings
                WHERE price IS NOT NULL
                  AND CAST(price AS REAL) > 0
                  AND mileage IS NOT NULL
                  AND CAST(mileage AS REAL) >= 0
                GROUP BY vin
            ) AS latest
                ON l.vin = latest.vin
               AND l.loaddate = latest.latest_loaddate
        """

    return f"""
        WITH eligible_listings AS (
            {eligible}
        ),
        latest AS (
            SELECT vin, MAX(loaddate) AS latest_loaddate
            FROM eligible_listings
            GROUP BY vin
        )
        SELECT l.*
        FROM eligible_listings AS l
        INNER JOIN latest
            ON l.vin = latest.vin
           AND l.loaddate = latest.latest_loaddate
    """


def _downcast_sql_chunk(
    chunk: pd.DataFrame,
    declared_types: dict[str, str],
) -> pd.DataFrame:
    """Narrow SQLite's 64-bit numerics before chunks are combined."""
    for column in chunk.columns:
        declared_type = declared_types.get(column, "")
        if declared_type == "BOOLEAN":
            chunk[column] = chunk[column].astype("int8[pyarrow]")
        elif "INT" in declared_type:
            try:
                chunk[column] = chunk[column].astype("int32[pyarrow]")
            except (OverflowError, TypeError, ValueError):
                chunk[column] = chunk[column].astype("int64[pyarrow]")
        elif any(token in declared_type for token in ("REAL", "FLOA", "DOUB")):
            chunk[column] = chunk[column].astype("float32[pyarrow]")
    return chunk


def _read_modeling_query_in_chunks(
    query: str,
    conn: sqlite3.Connection,
    declared_types: dict[str, str],
    chunk_size: int,
) -> pd.DataFrame:
    """Read SQLite incrementally using nullable Arrow-backed pandas columns."""
    chunks: list[pd.DataFrame] = []
    reader = pd.read_sql_query(
        query,
        conn,
        chunksize=max(1, int(chunk_size)),
        dtype_backend="pyarrow",
    )
    for chunk in reader:
        chunks.append(_downcast_sql_chunk(chunk, declared_types))

    if not chunks:
        return pd.DataFrame()
    if len(chunks) == 1:
        return chunks[0].reset_index(drop=True)
    return pd.concat(chunks, ignore_index=True, copy=False)


def _scoped_listing_row_count(
    conn: sqlite3.Connection,
    sample_size: int | None,
    sample_strategy: str,
) -> int:
    if not sample_size or sample_size <= 0:
        return int(conn.execute("SELECT COUNT(*) FROM listings").fetchone()[0])
    scoped = _scoped_listings_sql(sample_size, sample_strategy)
    return int(conn.execute(f"SELECT COUNT(*) FROM ({scoped})").fetchone()[0])


def build_data_profile(
    conn: sqlite3.Connection,
    sample_size: int | None,
    sample_strategy: str = DEFAULT_SAMPLE_STRATEGY,
) -> dict[str, Any]:
    """Collect schema and bounded sample metadata without expensive full scans."""
    tables = {}
    for table in ["listings", "nhtsa_enrichment", "listing_history", "price_history"]:
        if not _table_exists(conn, table):
            continue
        tables[table] = {
            "columns": _table_columns(conn, table),
            "indexes": [row[1] for row in conn.execute(f"PRAGMA index_list('{table}')").fetchall()],
        }

    listing_sample_limit = sample_size if sample_size and sample_size > 0 else 10_000
    scoped_listings = _scoped_listings_sql(listing_sample_limit, sample_strategy)
    profile_query = f"""
        SELECT
            COUNT(*) AS sampled_rows,
            MIN(price) AS min_price,
            AVG(price) AS avg_price,
            MAX(price) AS max_price,
            MIN(mileage) AS min_mileage,
            AVG(mileage) AS avg_mileage,
            MAX(mileage) AS max_mileage,
            MIN(loaddate) AS min_loaddate,
            MAX(loaddate) AS max_loaddate
        FROM (
            {scoped_listings}
        )
    """
    sample_stats = pd.read_sql_query(profile_query, conn).iloc[0].to_dict()
    return {
        "database_tables": tables,
        "listing_sample_limit": int(listing_sample_limit),
        "listing_sample_strategy": sample_strategy,
        "listing_sample_stats": sample_stats,
    }


def load_modeling_frame(
    db_path: Path | str = DB_PATH,
    absa_db_path: Path | str | None = ABSA_DB_PATH,
    sample_size: int | None = DEFAULT_SAMPLE_SIZE,
    sample_strategy: str = DEFAULT_SAMPLE_STRATEGY,
    deduplicate_vins: bool = True,
    chunk_size: int = DEFAULT_SQL_CHUNK_SIZE,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Load the current-price frame through a bounded-memory SQLite reader."""
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    conn = sqlite3.connect(str(db_path))
    try:
        profile = build_data_profile(conn, sample_size, sample_strategy)
        scoped_snapshot_rows = _scoped_listing_row_count(
            conn,
            sample_size,
            sample_strategy,
        )
        listings_schema = _table_schema(conn, "listings")
        nhtsa_schema = _table_schema(conn, "nhtsa_enrichment")
        listings_cols = list(listings_schema)
        nhtsa_cols = [
            c
            for c in nhtsa_schema
            if c != "vin" and c not in PRICE_LEAKAGE_FEATURE_COLUMNS
        ]
        declared_types = {
            **listings_schema,
            **{column: nhtsa_schema[column] for column in nhtsa_cols},
        }

        listing_select = [f"l.{c}" for c in listings_cols]
        nhtsa_select = [f"n.{c}" for c in nhtsa_cols]
        query_cols = listing_select + nhtsa_select

        absa_join = ""
        if absa_db_path and Path(absa_db_path).exists():
            try:
                conn.execute("ATTACH DATABASE ? AS absa", (str(absa_db_path),))
                if _table_exists(conn, "absa.Vehicle_Sentiment_Index"):
                    entity_expr = (
                        "CAST(l.canonical_year AS TEXT) || ' ' || "
                        "l.canonical_make || ' ' || l.canonical_model"
                    )
                    query_cols.extend(
                        [
                            f"{entity_expr} AS Vehicle_Entity",
                            "vsi.Reliability_Index",
                            "vsi.General_Enthusiast_Score",
                            "vsi.Sentiment_Volatility_StdDev",
                            "vsi.Sentiment_Trend_Slope",
                            "vsi.Confidence_Level",
                        ]
                    )
                    declared_types.update(
                        {
                            "Vehicle_Entity": "TEXT",
                            "Reliability_Index": "REAL",
                            "General_Enthusiast_Score": "REAL",
                            "Sentiment_Volatility_StdDev": "REAL",
                            "Sentiment_Trend_Slope": "REAL",
                            "Confidence_Level": "REAL",
                        }
                    )
                    absa_join = f"""
                        LEFT JOIN absa.Vehicle_Sentiment_Index AS vsi
                            ON {entity_expr} = vsi.Vehicle_Entity
                    """
            except sqlite3.Error:
                absa_join = ""

        scoped_listings = _modeling_listings_sql(
            sample_size,
            sample_strategy,
            deduplicate_vins,
        )

        query = f"""
            WITH scoped_listings AS (
                {scoped_listings}
            )
            SELECT {', '.join(query_cols)}
            FROM scoped_listings AS l
            INNER JOIN nhtsa_enrichment AS n USING(vin)
            {absa_join}
        """
        df = _read_modeling_query_in_chunks(
            query,
            conn,
            declared_types,
            chunk_size,
        )
        if deduplicate_vins:
            df.attrs["deduplication"] = {
                "deduplicated_vins": True,
                "rows_before": scoped_snapshot_rows,
                "rows_after": int(df.shape[0]),
                "distinct_vins_before": int(df.shape[0]),
                "rows_removed": int(max(0, scoped_snapshot_rows - df.shape[0])),
            }
    finally:
        conn.close()

    if df.empty:
        raise ValueError("No modeling rows were returned from the database query.")
    return df, profile


def engineer_current_price_features(
    df: pd.DataFrame,
    copy_frame: bool = True,
) -> pd.DataFrame:
    """Create derived features from cleaned listing and NHTSA columns."""
    if copy_frame:
        df = df.copy()
    required_identity = {"canonical_make", "canonical_model", "canonical_year", "canonical_trim"}
    missing_identity = sorted(required_identity - set(df.columns))
    if missing_identity:
        raise ValueError(
            "Cleaned database lacks canonical identity columns: " + ", ".join(missing_identity)
        )
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df["mileage"] = pd.to_numeric(df["mileage"], errors="coerce")
    valid_rows = (
        df["price"].notna()
        & (df["price"] > 0)
        & df["mileage"].notna()
        & (df["mileage"] >= 0)
    )
    if not bool(valid_rows.all()):
        df = df.loc[valid_rows].copy()

    for col in ["date", "loaddate"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    model_year_col = "canonical_year"
    if model_year_col in df.columns:
        model_year = pd.to_numeric(df[model_year_col], errors="coerce")
        df["vehicle_age"] = (datetime.now().year - model_year).clip(lower=0)
        df["vehicle_age_squared"] = df["vehicle_age"] ** 2
        df["miles_per_year"] = df["mileage"] / df["vehicle_age"].clip(lower=1)
        df["model_year_bucket"] = (
            (model_year // 5 * 5)
            .astype("int32[pyarrow]")
            .astype("string[pyarrow]")
        )
    else:
        df["vehicle_age"] = np.nan
        df["vehicle_age_squared"] = np.nan
        df["miles_per_year"] = np.nan
        df["model_year_bucket"] = "UNKNOWN"

    df["log_mileage"] = np.log1p(df["mileage"])
    df["mileage_age_interaction"] = df["log_mileage"] * df["vehicle_age"].fillna(0)
    df["mileage_bucket"] = pd.cut(
        df["mileage"],
        bins=[-1, 100, 5_000, 25_000, 60_000, 100_000, 150_000, np.inf],
        labels=[
            "delivery",
            "under_5k",
            "5k_25k",
            "25k_60k",
            "60k_100k",
            "100k_150k",
            "150k_plus",
        ],
    ).astype("string[pyarrow]")
    if "loaddate" in df.columns and df["loaddate"].notna().any():
        max_load_date = df["loaddate"].max()
        df["listing_recency_days"] = (max_load_date - df["loaddate"]).dt.days
        df["listing_month"] = df["loaddate"].dt.month.astype("Int64")
        df["listing_week"] = df["loaddate"].dt.isocalendar().week.astype("Int64")
    else:
        df["listing_recency_days"] = np.nan
        df["listing_month"] = np.nan
        df["listing_week"] = np.nan

    if "locationCode" in df.columns:
        location_text = df["locationCode"].astype("string[pyarrow]").str.zfill(5)
        df["location_region"] = location_text.str.slice(0, 2).fillna("UNKNOWN")

    for col in ["pendingSale", "priceRecentChange"]:
        if col in df.columns:
            df[col] = (
                df[col]
                .astype("string[pyarrow]")
                .str.lower()
                .isin(["1", "true", "yes"])
                .astype("int8[pyarrow]")
            )

    for col in ["vehicleTitle", "vehicleTitleDesc", "title"]:
        if col in df.columns:
            text = df[col].astype("string[pyarrow]").fillna("")
            df[f"{col}_length"] = text.str.len()
            df[f"{col}_word_count"] = text.str.split().str.len()

    trim_candidates = []
    for col in TRIM_FEATURE_CANDIDATES:
        if col in df.columns:
            normalized = normalize_trim_series(df[col])
            df[col] = normalized
            trim_candidates.append(normalized)

    df["trim_proxy"] = "UNKNOWN_TRIM"
    for candidate in trim_candidates:
        usable = candidate.notna() & candidate.ne("UNKNOWN_TRIM") & candidate.ne("")
        df.loc[df["trim_proxy"].eq("UNKNOWN_TRIM") & usable, "trim_proxy"] = candidate[usable]
    df["trim_proxy"] = df["trim_proxy"].astype("string[pyarrow]")

    if "vehicleTitle" in df.columns:
        title_parts = [
            df[col].astype("string[pyarrow]").fillna("")
            for col in ["vehicleTitle", "vehicleTitleDesc", "title", "trim_proxy"]
            if col in df.columns
        ]
        title = (
            title_parts[0]
            if title_parts
            else pd.Series("", index=df.index, dtype="string[pyarrow]")
        )
        for part in title_parts[1:]:
            title = title + " " + part
        df["title_mentions_certified"] = title.str.contains(
            "certified|cpo",
            case=False,
            na=False,
        ).astype("int8[pyarrow]")
        df["title_mentions_awd_4wd"] = title.str.contains(
            "awd|4wd|4x4|all wheel",
            case=False,
            na=False,
        ).astype("int8[pyarrow]")
        df["title_mentions_luxury_trim"] = title.str.contains(
            "premium|platinum|limited|reserve|s|amg|m sport|rs|performance|gt350|gt500",
            case=False,
            na=False,
        ).astype("int8[pyarrow]")

    if "sourceName" in df.columns:
        source = df["sourceName"].astype("string[pyarrow]").fillna("UNKNOWN")
        df["source_is_marketplace"] = source.str.contains(
            "Cars.com|TrueCar|CarGurus|AutoTempest",
            case=False,
            na=False,
        ).astype("int8[pyarrow]")

    make = df["canonical_make"].astype("string[pyarrow]").fillna("UNKNOWN")
    model = df["canonical_model"].astype("string[pyarrow]").fillna("UNKNOWN")
    year = (
        pd.to_numeric(df["canonical_year"], errors="coerce")
        .astype("int32[pyarrow]")
        .astype("string[pyarrow]")
        .fillna("UNKNOWN")
    )
    body = (
        df.get("nhtsa_BodyClass", pd.Series("UNKNOWN", index=df.index))
        .astype("string[pyarrow]")
        .fillna("UNKNOWN")
    )
    fuel = (
        df.get("nhtsa_FuelTypePrimary", pd.Series("UNKNOWN", index=df.index))
        .astype("string[pyarrow]")
        .fillna("UNKNOWN")
    )

    df["make_model_year"] = make + "_" + model + "_" + year
    df["make_model_year_trim"] = df["make_model_year"] + "_" + df["trim_proxy"].fillna("UNKNOWN_TRIM")
    df["body_fuel_segment"] = body + "_" + fuel
    df["is_ev_or_hybrid"] = (
        fuel.str.contains("electric|hybrid", case=False, na=False)
        | df.get("nhtsa_ElectrificationLevel", pd.Series("", index=df.index))
        .astype("string[pyarrow]")
        .str.contains("electric|hybrid|bev|phev", case=False, na=False)
    ).astype("int8[pyarrow]")

    df["price_band"] = pd.cut(
        df["price"],
        bins=[0, 25_000, 50_000, 100_000, HIGH_VALUE_THRESHOLD, np.inf],
        labels=["under_25k", "25k_50k", "50k_100k", "100k_150k", "150k_plus"],
    ).astype("string[pyarrow]")

    for column in [
        "vehicle_age",
        "vehicle_age_squared",
        "listing_recency_days",
        "listing_month",
        "listing_week",
        "vehicleTitle_length",
        "vehicleTitle_word_count",
        "vehicleTitleDesc_length",
        "vehicleTitleDesc_word_count",
        "title_length",
        "title_word_count",
    ]:
        if column in df.columns:
            df[column] = df[column].astype("int32[pyarrow]")
    for column in [
        "miles_per_year",
        "log_mileage",
        "mileage_age_interaction",
    ]:
        if column in df.columns:
            df[column] = df[column].astype("float32[pyarrow]")

    return df.reset_index(drop=True)


def keep_latest_listing_per_vin(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Keep one current-price row per VIN, preferring the latest load date."""
    if "vin" not in df.columns:
        return df, {"deduplicated_vins": False, "reason": "vin column missing"}

    before_rows = int(df.shape[0])
    before_vins = int(df["vin"].nunique(dropna=True))
    sort_cols = [c for c in ["vin", "loaddate", "date"] if c in df.columns]

    if sort_cols:
        df = df.sort_values(sort_cols)
    df = df.drop_duplicates(subset=["vin"], keep="last").reset_index(drop=True)

    metadata = {
        "deduplicated_vins": True,
        "rows_before": before_rows,
        "rows_after": int(df.shape[0]),
        "distinct_vins_before": before_vins,
        "rows_removed": int(before_rows - df.shape[0]),
    }
    return df, metadata


def split_train_test(
    df: pd.DataFrame,
    split_date: str | None = None,
    test_size: float = 0.2,
    copy_frame: bool = True,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Prefer a time cutoff, then remove overlapping VINs from the training set."""
    if copy_frame:
        df = df.copy()
    if "loaddate" in df.columns:
        df["loaddate"] = pd.to_datetime(df["loaddate"], errors="coerce")

    if "loaddate" in df.columns and df["loaddate"].notna().nunique() >= 2:
        cutoff = pd.Timestamp(split_date) if split_date else df["loaddate"].quantile(1 - test_size)
        raw_train = df[df["loaddate"] < cutoff].copy()
        test_df = df[df["loaddate"] >= cutoff].copy()
        test_vins = set(test_df["vin"].dropna())
        train_df = raw_train[~raw_train["vin"].isin(test_vins)].copy()
        strategy = "time_cutoff_plus_vin_exclusion"

        if train_df.shape[0] >= 100 and test_df.shape[0] >= 50:
            metadata = {
                "split_strategy": strategy,
                "split_date": str(cutoff.date()),
                "train_rows_removed_for_vin_overlap": int(raw_train.shape[0] - train_df.shape[0]),
                "vin_overlap": int(len(set(train_df["vin"]).intersection(test_vins))),
            }
            return train_df.reset_index(drop=True), test_df.reset_index(drop=True), metadata

    splitter = GroupShuffleSplit(n_splits=1, test_size=test_size, random_state=RANDOM_STATE)
    groups = df["vin"].fillna("UNKNOWN")
    train_idx, test_idx = next(splitter.split(df, df["price"], groups=groups))
    train_df = df.iloc[train_idx].copy()
    test_df = df.iloc[test_idx].copy()
    metadata = {
        "split_strategy": "group_shuffle_by_vin",
        "split_date": None,
        "train_rows_removed_for_vin_overlap": 0,
        "vin_overlap": int(len(set(train_df["vin"]).intersection(set(test_df["vin"])))),
    }
    return train_df.reset_index(drop=True), test_df.reset_index(drop=True), metadata


def make_feature_matrix(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    y = df["price"].astype("float32")
    drop_columns = DROP_FEATURE_COLUMNS | PRICE_LEAKAGE_FEATURE_COLUMNS | IDENTITY_DIAGNOSTIC_COLUMNS
    feature_df = df.drop(columns=[c for c in drop_columns if c in df.columns], errors="ignore")
    assert_canonical_identity_contract(feature_df)
    return feature_df, y, df[["vin", "price", "price_band", "canonical_make", "canonical_year"]].copy()


def to_float32(X: Any) -> Any:
    """Cast transformed numeric blocks to float32 to reduce full-fit memory."""
    return X.astype(np.float32)


def normalize_categorical_missing_values(X: Any) -> Any:
    """Replace pandas nullable sentinels before scikit-learn object conversion."""
    if isinstance(X, pd.DataFrame):
        normalized = X.copy(deep=False)
        for column in normalized.columns:
            values = normalized[column]
            if isinstance(values.dtype, pd.CategoricalDtype) and values.isna().any():
                values = values.astype("string[pyarrow]")
            normalized[column] = values.fillna("UNKNOWN")
        return normalized

    values = np.asarray(X, dtype=object)
    missing = pd.isna(values)
    if missing.any():
        values = values.copy()
        values[missing] = "UNKNOWN"
    return values


def build_preprocessors(X_train: pd.DataFrame) -> tuple[ColumnTransformer, Pipeline, dict[str, Any]]:
    numeric_features = X_train.select_dtypes(include=[np.number, "bool"]).columns.tolist()
    categorical_features = X_train.select_dtypes(exclude=[np.number, "bool"]).columns.tolist()

    low_cardinality_threshold = 50
    forced_target_cols = [
        c for c in categorical_features if any(token in c.lower() for token in TARGET_ENCODE_TOKENS)
    ]
    low_card_cols = [
        c
        for c in categorical_features
        if c not in forced_target_cols and X_train[c].nunique(dropna=True) <= low_cardinality_threshold
    ]
    high_card_cols = [
        c
        for c in categorical_features
        if c in forced_target_cols or X_train[c].nunique(dropna=True) > low_cardinality_threshold
    ]

    float32_cast = FunctionTransformer(to_float32, feature_names_out="one-to-one")
    categorical_missing = FunctionTransformer(
        normalize_categorical_missing_values,
        feature_names_out="one-to-one",
    )
    numeric_pipeline = Pipeline(
        [
            ("imputer", SimpleImputer(strategy="median")),
            ("float32", float32_cast),
        ]
    )
    categorical_low = Pipeline(
        [
            ("normalize_missing", categorical_missing),
            ("imputer", SimpleImputer(strategy="constant", fill_value="UNKNOWN")),
            (
                "onehot",
                OneHotEncoder(
                    handle_unknown="infrequent_if_exist",
                    min_frequency=10,
                    max_categories=RANDOM_FOREST_MAX_CATEGORIES_PER_FEATURE,
                    sparse_output=True,
                    dtype=np.float32,
                ),
            ),
        ]
    )
    categorical_high = Pipeline(
        [
            ("normalize_missing", categorical_missing),
            ("imputer", SimpleImputer(strategy="constant", fill_value="UNKNOWN")),
            ("target", TargetEncoder(min_samples_leaf=20, smoothing=10)),
            ("float32", float32_cast),
        ]
    )

    tree_preprocessor = ColumnTransformer(
        [
            ("num", numeric_pipeline, numeric_features),
            ("cat_low", categorical_low, low_card_cols),
            ("cat_high", categorical_high, high_card_cols),
        ],
        remainder="drop",
    )
    linear_preprocessor = Pipeline(
        [
            ("features", tree_preprocessor),
            ("scaler", StandardScaler(with_mean=False)),
        ]
    )
    metadata = {
        "numeric_features": numeric_features,
        "low_cardinality_categorical_features": low_card_cols,
        "high_cardinality_categorical_features": high_card_cols,
    }
    return tree_preprocessor, linear_preprocessor, metadata


class HighValueRoutedRegressor(BaseEstimator, RegressorMixin):
    """
    Route predictions through separate regressors for everyday and high-value cars.

    The classifier is trained only on training labels, then inference uses the
    predicted high-value probability. This tests whether a sparse luxury/exotic
    tail benefits from segmentation without leaking the actual price band.
    """

    def __init__(
        self,
        regressor: BaseEstimator | None = None,
        threshold: int = HIGH_VALUE_THRESHOLD,
        probability_cutoff: float = 0.35,
        min_high_value_rows: int = 50,
        random_state: int = RANDOM_STATE,
    ):
        self.regressor = regressor
        self.threshold = threshold
        self.probability_cutoff = probability_cutoff
        self.min_high_value_rows = min_high_value_rows
        self.random_state = random_state

    def _default_regressor(self) -> TransformedTargetRegressor:
        return TransformedTargetRegressor(
            regressor=LGBMRegressor(
                objective="regression",
                n_estimators=700,
                learning_rate=0.05,
                num_leaves=63,
                min_child_samples=50,
                subsample=0.85,
                colsample_bytree=0.85,
                reg_lambda=1.0,
                random_state=self.random_state,
                n_jobs=-1,
                verbose=-1,
            ),
            func=np.log1p,
            inverse_func=np.expm1,
        )

    @staticmethod
    def _row_subset(X: Any, mask: np.ndarray) -> Any:
        if hasattr(X, "iloc"):
            return X.iloc[mask]
        return X[mask]

    def fit(self, X: Any, y: pd.Series) -> "HighValueRoutedRegressor":
        y_array = np.asarray(y, dtype="float64")
        high_mask = y_array > self.threshold
        high_count = int(high_mask.sum())
        low_count = int((~high_mask).sum())
        base_regressor = self.regressor if self.regressor is not None else self._default_regressor()

        self.classifier_ = LGBMClassifier(
            objective="binary",
            n_estimators=300,
            learning_rate=0.05,
            num_leaves=31,
            min_child_samples=25,
            class_weight="balanced",
            random_state=self.random_state,
            n_jobs=-1,
            verbose=-1,
        )
        self.low_regressor_ = clone(base_regressor)
        self.high_regressor_ = clone(self.low_regressor_)
        self.global_regressor_ = clone(self.low_regressor_)

        self.global_regressor_.fit(X, y_array)
        self.has_low_value_expert_ = low_count > 0
        self.has_high_value_expert_ = (
            high_count >= self.min_high_value_rows
            and low_count > 0
        )

        if self.has_low_value_expert_:
            self.low_regressor_.fit(self._row_subset(X, ~high_mask), y_array[~high_mask])

        if self.has_high_value_expert_:
            self.classifier_.fit(X, high_mask.astype("int8"))
            self.high_regressor_.fit(self._row_subset(X, high_mask), y_array[high_mask])

        return self

    def predict(self, X: Any) -> np.ndarray:
        global_preds = self.global_regressor_.predict(X)

        if not self.has_low_value_expert_ or not self.has_high_value_expert_:
            return np.clip(global_preds, a_min=0, a_max=None)

        low_preds = self.low_regressor_.predict(X)
        high_probability = self.classifier_.predict_proba(X)[:, 1]
        high_preds = self.high_regressor_.predict(X)
        routed_preds = np.where(
            high_probability >= self.probability_cutoff,
            high_preds,
            low_preds,
        )

        # Blend lightly with the global model to reduce hard-router volatility.
        blended = 0.85 * routed_preds + 0.15 * global_preds
        return np.clip(blended, a_min=0, a_max=None)


def model_candidates(tree_preprocessor: ColumnTransformer, linear_preprocessor: Pipeline) -> dict[str, tuple[Pipeline, dict[str, list[Any]]]]:
    def routed_pipeline(preprocessor: Any, regressor: BaseEstimator) -> Pipeline:
        return Pipeline(
            [
                ("preprocessor", clone(preprocessor)),
                ("model", HighValueRoutedRegressor(regressor=regressor)),
            ]
        )

    router_params = {
        "model__probability_cutoff": [0.25, 0.35, 0.5],
        "model__min_high_value_rows": [25, 50, 100],
    }
    ridge = TransformedTargetRegressor(
        regressor=Ridge(random_state=RANDOM_STATE),
        func=np.log1p,
        inverse_func=np.expm1,
    )
    elastic = TransformedTargetRegressor(
        regressor=ElasticNet(max_iter=5_000, random_state=RANDOM_STATE),
        func=np.log1p,
        inverse_func=np.expm1,
    )
    lightgbm = TransformedTargetRegressor(
        regressor=LGBMRegressor(
            objective="regression",
            n_estimators=700,
            learning_rate=0.05,
            num_leaves=63,
            min_child_samples=50,
            subsample=0.85,
            colsample_bytree=0.85,
            reg_lambda=1.0,
            random_state=RANDOM_STATE,
            n_jobs=-1,
            verbose=-1,
        ),
        func=np.log1p,
        inverse_func=np.expm1,
    )
    random_forest = TransformedTargetRegressor(
        regressor=RandomForestRegressor(
            n_estimators=250,
            min_samples_leaf=5,
            max_depth=24,
            max_leaf_nodes=RANDOM_FOREST_MAX_LEAF_NODES,
            max_samples=0.5,
            max_features="sqrt",
            n_jobs=2,
            random_state=RANDOM_STATE,
        ),
        func=np.log1p,
        inverse_func=np.expm1,
    )

    return {
        "Ridge": (
            routed_pipeline(linear_preprocessor, ridge),
            {
                **router_params,
                "model__regressor__regressor__alpha": [0.3, 1.0, 3.0, 10.0, 30.0],
            },
        ),
        "ElasticNet": (
            routed_pipeline(linear_preprocessor, elastic),
            {
                **router_params,
                "model__regressor__regressor__alpha": [0.0005, 0.001, 0.005, 0.01],
                "model__regressor__regressor__l1_ratio": [0.1, 0.3, 0.5, 0.7],
            },
        ),
        "LightGBM": (
            routed_pipeline(tree_preprocessor, lightgbm),
            {
                **router_params,
                "model__regressor__regressor__num_leaves": [31, 63, 127],
                "model__regressor__regressor__learning_rate": [0.03, 0.05, 0.08],
                "model__regressor__regressor__min_child_samples": [25, 50, 100],
                "model__regressor__regressor__reg_lambda": [0.5, 1.0, 2.0],
            },
        ),
        "RandomForest": (
            routed_pipeline(tree_preprocessor, random_forest),
            {
                **router_params,
                "model__regressor__regressor__n_estimators": [150, 250],
                "model__regressor__regressor__min_samples_leaf": [10, 25, 50],
                "model__regressor__regressor__max_leaf_nodes": [16_384, RANDOM_FOREST_MAX_LEAF_NODES],
                "model__regressor__regressor__max_features": ["sqrt", 0.5],
            },
        ),
    }


def _rmsle(y_true: pd.Series, preds: np.ndarray) -> float:
    clipped = np.clip(preds, a_min=0, a_max=None)
    return float(np.sqrt(mean_squared_error(np.log1p(y_true), np.log1p(clipped))))


def evaluate_predictions(y_true: pd.Series, preds: np.ndarray) -> dict[str, float]:
    rmse = np.sqrt(mean_squared_error(y_true, preds))
    return {
        "mae": float(mean_absolute_error(y_true, preds)),
        "rmse": float(rmse),
        "rmsle": _rmsle(y_true, preds),
        "mape": float(mean_absolute_percentage_error(y_true, preds)),
        "r2": float(r2_score(y_true, preds)),
    }


def segment_metrics(
    y_true: pd.Series,
    preds: np.ndarray,
    segment_frame: pd.DataFrame,
) -> dict[str, dict[str, dict[str, float]]]:
    scored = segment_frame.copy()
    scored["actual"] = y_true.to_numpy()
    scored["prediction"] = preds
    scored["abs_error"] = (scored["actual"] - scored["prediction"]).abs()
    scored["is_high_value"] = np.where(scored["actual"] >= HIGH_VALUE_THRESHOLD, "high_value", "everyday")

    output: dict[str, dict[str, dict[str, float]]] = {}
    for column in ["price_band", "canonical_make", "canonical_year", "is_high_value"]:
        if column not in scored.columns:
            continue
        groups = {}
        for value, group in scored.groupby(column, dropna=False):
            groups[str(value)] = {
                "rows": int(group.shape[0]),
                "mae": float(group["abs_error"].mean()),
            }
        output[column] = groups
    return output


def tuning_search_n_jobs(row_count: int) -> int:
    """Avoid process pickling multi-million-row frames during CV search."""
    return -1 if row_count <= MAX_PARALLEL_TUNING_ROWS else 1


def stratified_tuning_sample_positions(
    segment_frame: pd.DataFrame,
    max_rows: int = DEFAULT_TUNING_SAMPLE_SIZE,
) -> np.ndarray:
    """Return row positions for a deterministic price/make/year tuning sample."""
    segment_frame = segment_frame.reset_index(drop=True)
    row_count = int(segment_frame.shape[0])
    if max_rows <= 0 or row_count <= max_rows:
        return np.arange(row_count)

    available_cols = [c for c in ["price_band", "canonical_make", "canonical_year"] if c in segment_frame.columns]
    if not available_cols:
        return np.sort(
            np.random.default_rng(RANDOM_STATE).choice(row_count, size=max_rows, replace=False)
        )

    strata = (
        segment_frame[available_cols]
        .astype("string[pyarrow]")
        .fillna("UNKNOWN")
        .agg("|".join, axis=1)
    )
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


def transformed_matrix_memory_mb(matrix: Any) -> float:
    if sp.issparse(matrix):
        bytes_used = matrix.data.nbytes + matrix.indices.nbytes + matrix.indptr.nbytes
    else:
        bytes_used = matrix.nbytes
    return float(bytes_used / 1024**2)


def profile_feature_space(
    X_train: pd.DataFrame,
    y_train: pd.Series,
    train_segments: pd.DataFrame,
    tree_preprocessor: ColumnTransformer,
    max_rows: int = DEFAULT_FEATURE_PROFILE_ROWS,
) -> dict[str, Any]:
    """Profile the transformed feature matrix on a bounded training sample."""
    positions = stratified_tuning_sample_positions(train_segments, max_rows)
    X_profile = X_train.iloc[positions]
    y_profile = y_train.iloc[positions]
    transformed = clone(tree_preprocessor).fit_transform(X_profile, y_profile)
    sample_memory_mb = transformed_matrix_memory_mb(transformed)
    projected_memory_mb = sample_memory_mb * (X_train.shape[0] / max(1, X_profile.shape[0]))
    categorical_cardinality = (
        X_train.select_dtypes(exclude=[np.number, "bool"])
        .nunique(dropna=True)
        .sort_values(ascending=False)
    )
    return {
        "profile_rows": int(X_profile.shape[0]),
        "training_rows": int(X_train.shape[0]),
        "raw_pandas_training_frame_mb": float(X_train.memory_usage(deep=True).sum() / 1024**2),
        "transformed_type": type(transformed).__name__,
        "transformed_dtype": str(transformed.dtype),
        "transformed_shape_on_profile": [int(transformed.shape[0]), int(transformed.shape[1])],
        "transformed_sample_memory_mb": sample_memory_mb,
        "projected_full_training_matrix_memory_mb": projected_memory_mb,
        "is_sparse": bool(sp.issparse(transformed)),
        "nonzero_entries": int(transformed.nnz) if sp.issparse(transformed) else None,
        "top_categorical_cardinality": [
            {"feature": str(feature), "unique_values": int(count)}
            for feature, count in categorical_cardinality.head(25).items()
        ],
    }


def final_fit_positions_for_model(
    name: str,
    train_segments: pd.DataFrame,
    row_count: int,
) -> np.ndarray:
    if name != "RandomForest" or row_count <= RANDOM_FOREST_FULL_FIT_MAX_ROWS:
        return np.arange(row_count)
    return stratified_tuning_sample_positions(train_segments, RANDOM_FOREST_FULL_FIT_MAX_ROWS)


def get_preprocessor_feature_names(preprocessor: Any, expected_count: int | None = None) -> list[str]:
    readable_names = get_column_transformer_feature_names(preprocessor)
    if readable_names and (expected_count is None or len(readable_names) == expected_count):
        return readable_names

    try:
        names = list(preprocessor.get_feature_names_out())
    except Exception:
        names = []
    if expected_count is not None and len(names) != expected_count:
        names = [f"feature_{idx}" for idx in range(expected_count)]
    return [str(name) for name in names]


def get_column_transformer_feature_names(preprocessor: Any) -> list[str]:
    """Return report-friendly feature names, including target-encoded source fields."""
    if isinstance(preprocessor, Pipeline):
        if "features" in preprocessor.named_steps:
            return get_column_transformer_feature_names(preprocessor.named_steps["features"])
        if "preprocessor" in preprocessor.named_steps:
            return get_column_transformer_feature_names(preprocessor.named_steps["preprocessor"])

    if not hasattr(preprocessor, "transformers_"):
        return []

    feature_names: list[str] = []
    for block_name, transformer, columns in preprocessor.transformers_:
        if block_name == "remainder" and transformer == "drop":
            continue
        if columns is None:
            continue
        if isinstance(columns, slice):
            continue
        if isinstance(columns, (str, bytes)):
            source_columns = [str(columns)]
        else:
            source_columns = [str(column) for column in list(columns)]
        if not source_columns:
            continue

        if block_name == "cat_high":
            feature_names.extend([f"target_encoded__{column}" for column in source_columns])
            continue

        try:
            names = list(transformer.get_feature_names_out(source_columns))
        except Exception:
            names = [f"{block_name}__{column}" for column in source_columns]
        feature_names.extend([str(name) for name in names])

    return feature_names


def fitted_regressor_weights(estimator: Any) -> tuple[np.ndarray | None, str]:
    """Return weights from a fitted regressor, unwrapping target transformers."""
    if hasattr(estimator, "regressor_"):
        estimator = estimator.regressor_
    if hasattr(estimator, "coef_"):
        return np.ravel(estimator.coef_).astype("float64"), "coefficient"
    if hasattr(estimator, "feature_importances_"):
        return np.ravel(estimator.feature_importances_).astype("float64"), "feature_importance"
    return None, "feature_importance"


def extract_model_feature_weights(model: Pipeline, top_n: int = MAX_MODEL_WEIGHT_ROWS) -> list[dict[str, Any]]:
    """Return coefficient or feature-importance rows for fitted sklearn pipelines."""
    if "preprocessor" not in model.named_steps or "model" not in model.named_steps:
        return []

    model_step = model.named_steps["model"]
    estimator = getattr(model_step, "regressor_", model_step)
    weights, weight_type = fitted_regressor_weights(estimator)

    if weights is None and hasattr(estimator, "global_regressor_"):
        weights, nested_weight_type = fitted_regressor_weights(estimator.global_regressor_)
        weight_type = f"global_{nested_weight_type}"

    if weights is None:
        return []

    feature_names = get_preprocessor_feature_names(model.named_steps["preprocessor"], len(weights))
    rows = [
        {
            "feature": feature,
            "weight": float(weight),
            "abs_weight": float(abs(weight)),
            "weight_type": weight_type,
        }
        for feature, weight in zip(feature_names, weights)
    ]
    return sorted(rows, key=lambda row: row["abs_weight"], reverse=True)[:top_n]


def tune_and_fit(
    name: str,
    pipeline: Pipeline,
    param_grid: dict[str, list[Any]],
    X_train: pd.DataFrame,
    y_train: pd.Series,
    groups: pd.Series,
    train_segments: pd.DataFrame,
    tuning_sample_size: int = DEFAULT_TUNING_SAMPLE_SIZE,
) -> tuple[Pipeline, dict[str, Any]]:
    fit_metadata: dict[str, Any] = {
        "tuned": False,
        "max_tuning_rows": int(tuning_sample_size),
        "final_fit_rows": int(X_train.shape[0]),
        "final_fit_strategy": "full_training_split",
    }

    def fit_final(candidate: Pipeline, params: dict[str, Any] | None = None) -> Pipeline:
        final_positions = final_fit_positions_for_model(name, train_segments, X_train.shape[0])
        X_fit = X_train.iloc[final_positions]
        y_fit = y_train.iloc[final_positions]
        fit_metadata["final_fit_rows"] = int(X_fit.shape[0])
        fit_metadata["final_fit_strategy"] = (
            "representative_bounded_fit" if X_fit.shape[0] < X_train.shape[0] else "full_training_split"
        )
        if params:
            fit_metadata["best_params"] = params
        if X_fit.shape[0] < X_train.shape[0]:
            fit_metadata["full_training_rows"] = int(X_train.shape[0])
            fit_metadata["reason"] = (
                f"{name} is sample-bounded to avoid multi-million-row tree-node memory growth."
            )
            print(
                f"Fitting {name} on {X_fit.shape[0]:,} representative rows "
                f"instead of {X_train.shape[0]:,} full rows to bound memory."
            )
        candidate.fit(X_fit, y_fit)
        return candidate

    if not param_grid or X_train.shape[0] < 1_000 or groups.nunique() < 3:
        return fit_final(pipeline), fit_metadata

    tuning_positions = stratified_tuning_sample_positions(train_segments, tuning_sample_size)
    X_tune = X_train.iloc[tuning_positions]
    y_tune = y_train.iloc[tuning_positions]
    groups_tune = groups.iloc[tuning_positions]
    if groups_tune.nunique() < 3:
        return fit_final(pipeline), fit_metadata

    total_param_combinations = prod(len(values) for values in param_grid.values())
    cv = GroupKFold(n_splits=min(3, int(groups_tune.nunique())))
    tuning_n_jobs = tuning_search_n_jobs(X_tune.shape[0])
    search = RandomizedSearchCV(
        pipeline,
        param_distributions=param_grid,
        n_iter=min(8, total_param_combinations),
        cv=cv,
        scoring="neg_root_mean_squared_error",
        n_jobs=tuning_n_jobs,
        random_state=RANDOM_STATE,
        verbose=1,
    )
    print(f"Tuning {name} on {X_tune.shape[0]:,} representative rows...")
    if tuning_n_jobs == 1:
        print(
            "Large training frame detected; running CV search in one process "
            "to avoid Windows joblib pickling memory pressure."
        )
    search.fit(X_tune, y_tune, groups=groups_tune)
    print(f"Best parameters for {name}: {search.best_params_}")
    fit_metadata.update(
        {
            "tuned": True,
            "tuning_rows": int(X_tune.shape[0]),
            "cv_folds": int(cv.n_splits),
            "candidate_count": int(min(8, total_param_combinations)),
            "best_params": search.best_params_,
        }
    )
    if X_tune.shape[0] < X_train.shape[0]:
        print(f"Refitting {name} with tuned parameters after tuning...")
        tuned_model = clone(pipeline).set_params(**search.best_params_)
        return fit_final(tuned_model, search.best_params_), fit_metadata
    fit_metadata["final_fit_rows"] = int(X_tune.shape[0])
    return search.best_estimator_, fit_metadata


def train_current_price_models(
    db_path: Path | str = DB_PATH,
    absa_db_path: Path | str | None = ABSA_DB_PATH,
    output_dir: Path | str = OUTPUT_DIR,
    sample_size: int | None = DEFAULT_SAMPLE_SIZE,
    sample_strategy: str = DEFAULT_SAMPLE_STRATEGY,
    deduplicate_vins: bool = True,
    split_date: str | None = None,
) -> dict[str, Any]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    raw_df, data_profile = load_modeling_frame(
        db_path,
        absa_db_path,
        sample_size,
        sample_strategy,
        deduplicate_vins=deduplicate_vins,
    )
    sql_dedup_metadata = raw_df.attrs.get("deduplication")
    raw_rows = int(raw_df.shape[0])
    identity_profile = identity_normalization_profile(raw_df)
    model_df = engineer_current_price_features(raw_df, copy_frame=False)
    del raw_df
    gc.collect()

    dedup_metadata = {"deduplicated_vins": False}
    if deduplicate_vins:
        if sql_dedup_metadata:
            dedup_metadata = dict(sql_dedup_metadata)
            dedup_metadata["rows_after"] = int(model_df.shape[0])
            dedup_metadata["rows_removed"] = int(
                max(0, dedup_metadata["rows_before"] - model_df.shape[0])
            )
        else:
            model_df, dedup_metadata = keep_latest_listing_per_vin(model_df)
    model_rows = int(model_df.shape[0])
    canonical_trim_input = "canonical_trim" if "canonical_trim" in model_df.columns else None
    comparison_columns_available = [
        col for col in TRIM_METADATA_COLUMNS if col != "canonical_trim" and col in model_df.columns
    ]
    engineered_trim_columns = [
        col for col in ["trim_proxy", "make_model_year_trim"] if col in model_df.columns
    ]
    train_df, test_df, split_metadata = split_train_test(
        model_df,
        split_date,
        copy_frame=False,
    )
    del model_df
    gc.collect()

    X_train, y_train, train_segments = make_feature_matrix(train_df)
    X_test, y_test, test_segments = make_feature_matrix(test_df)
    tree_preprocessor, linear_preprocessor, feature_metadata = build_preprocessors(X_train)
    feature_space_profile = profile_feature_space(
        X_train,
        y_train,
        train_segments,
        tree_preprocessor,
    )

    metrics: dict[str, Any] = {}
    model_fit_metadata: dict[str, Any] = {}
    model_feature_weights: dict[str, list[dict[str, Any]]] = {}
    best_name = ""
    best_model: Pipeline | None = None
    best_mae = float("inf")

    for name, (pipeline, param_grid) in model_candidates(tree_preprocessor, linear_preprocessor).items():
        model, fit_metadata = tune_and_fit(
            name,
            pipeline,
            param_grid,
            X_train,
            y_train,
            train_df["vin"].fillna("UNKNOWN"),
            train_segments,
        )
        model_fit_metadata[name] = fit_metadata
        predictions = model.predict(X_test)
        model_metrics = evaluate_predictions(y_test, predictions)
        model_metrics["segment_metrics"] = segment_metrics(y_test, predictions, test_segments)
        metrics[name] = model_metrics
        model_feature_weights[name] = extract_model_feature_weights(model)
        joblib.dump(model, output_dir / f"{name}.joblib")
        gc.collect()

        if model_metrics["mae"] < best_mae:
            best_mae = model_metrics["mae"]
            best_name = name
            best_model = model

    if best_model is None:
        raise RuntimeError("No model was trained.")

    joblib.dump(best_model, output_dir / "Current_Price_Best_Model.joblib")

    report = {
        "task": "current_price",
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "database_path": str(Path(db_path)),
        "sample_size": int(sample_size or 0),
        "sample_strategy": sample_strategy,
        "row_counts": {
            "raw_rows": raw_rows,
            "model_rows": model_rows,
            "train_rows": int(train_df.shape[0]),
            "test_rows": int(test_df.shape[0]),
            "train_vins": int(train_df["vin"].nunique()),
            "test_vins": int(test_df["vin"].nunique()),
        },
        "deduplication": dedup_metadata,
        "date_ranges": {
            "train_loaddate_min": str(train_df["loaddate"].min()) if "loaddate" in train_df else None,
            "train_loaddate_max": str(train_df["loaddate"].max()) if "loaddate" in train_df else None,
            "test_loaddate_min": str(test_df["loaddate"].min()) if "loaddate" in test_df else None,
            "test_loaddate_max": str(test_df["loaddate"].max()) if "loaddate" in test_df else None,
        },
        "split": split_metadata,
        "features": feature_metadata,
        "trim_features": {
            "canonical_trim_input": canonical_trim_input,
            "comparison_columns_available": comparison_columns_available,
            "engineered_trim_columns": engineered_trim_columns,
        },
        "identity_normalization": identity_profile,
        "tuning": {
            "max_tuning_rows": DEFAULT_TUNING_SAMPLE_SIZE,
            "sampling_strategy": "deterministic stratified sample by diagnostic price band, canonical make, and canonical model year",
            "final_fit": "best hyperparameters are refit before evaluation; memory-heavy models can use a documented representative fit cap",
        },
        "model_fit_metadata": model_fit_metadata,
        "feature_space_profile": feature_space_profile,
        "model_feature_weights": model_feature_weights,
        "leakage_controls": {
            "dropped_answer_derived_columns": ["price", "price_band"],
            "dropped_price_leakage_columns": sorted(PRICE_LEAKAGE_FEATURE_COLUMNS),
            "price_band_usage": "diagnostic segmentation and high-value classifier labels only; never included in model inputs",
            "high_value_routing": (
                "Every candidate model first trains a leakage-safe classifier on training labels "
                f"for price > ${HIGH_VALUE_THRESHOLD:,}, then routes predictions to separately "
                "fit everyday or high-value regressors."
            ),
            "vin_overlap": split_metadata["vin_overlap"],
        },
        "models": metrics,
        "recommended_model": best_name,
        "notes": [
            "Training defaults to all eligible VINs; SQLite selects the latest listing row before materialization to avoid duplicate VIN overweighting.",
            "SQLite results are read in bounded chunks with Arrow-backed strings and nullable 32-bit numeric columns.",
            "Price bands are created only after observing price and are kept out of the feature matrix to avoid target leakage.",
            "NHTSA base-price fields are excluded because the cleaned base price can be filled from observed price history.",
            "NHTSA make/model anchor canonical identity; title-derived canonical trim is the only trim input.",
            "Hyperparameters are tuned on a representative bounded sample, then refit on the full training split.",
            "RandomForest is sample-bounded and leaf-bounded on large full-database runs because scikit-learn stores every fitted tree node in memory.",
            "All current-price candidates use the same high-value classifier router before model-specific regressors.",
            "CatBoost remains a strong future candidate for categorical-heavy modeling but is not required for this dependency set.",
        ],
        "research_sources": RESEARCH_REFERENCES,
        "data_profile": data_profile,
    }

    write_reports(output_dir, report)
    return report


def write_reports(output_dir: Path, report: dict[str, Any]) -> None:
    json_path = output_dir / "model_report.json"
    json_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    weight_rows = [
        {"model": model_name, "rank": rank, **row}
        for model_name, rows in report.get("model_feature_weights", {}).items()
        for rank, row in enumerate(rows, start=1)
    ]
    if weight_rows:
        pd.DataFrame(weight_rows).to_csv(output_dir / "current_price_model_feature_weights.csv", index=False)

    best_name = report["recommended_model"]
    best_metrics = report["models"][best_name]
    md = [
        "# Vehicle Price Model Report",
        "",
        f"Generated: {report['generated_at']}",
        f"Recommended model: **{best_name}**",
        "",
        "## Data",
        f"- Model rows: {report['row_counts']['model_rows']:,}",
        f"- Train/test rows: {report['row_counts']['train_rows']:,} / {report['row_counts']['test_rows']:,}",
        f"- Split strategy: {report['split']['split_strategy']}",
        f"- VIN overlap: {report['split']['vin_overlap']}",
        f"- Feature matrix profile: {report['feature_space_profile']['transformed_shape_on_profile'][1]:,} transformed columns on "
        f"{report['feature_space_profile']['profile_rows']:,} profiled rows; projected full training matrix "
        f"{report['feature_space_profile']['projected_full_training_matrix_memory_mb'] / 1024:,.2f} GB",
        f"- Canonical trim input: {report.get('trim_features', {}).get('canonical_trim_input') or 'missing'}",
        "",
        "## Best Model Metrics",
        f"- MAE: ${best_metrics['mae']:,.2f}",
        f"- RMSE: ${best_metrics['rmse']:,.2f}",
        f"- RMSLE: {best_metrics['rmsle']:.4f}",
        f"- MAPE: {best_metrics['mape']:.4f}",
        f"- R2: {best_metrics['r2']:.4f}",
        "",
        "## Model Comparison",
    ]
    for name, metrics in report["models"].items():
        md.append(
            f"- {name}: MAE ${metrics['mae']:,.2f}, RMSE ${metrics['rmse']:,.2f}, R2 {metrics['r2']:.4f}"
        )
    md.extend(["", "## Fit Safeguards"])
    for name, metadata in report.get("model_fit_metadata", {}).items():
        md.append(
            f"- {name}: {metadata.get('final_fit_strategy', 'unknown')} on "
            f"{metadata.get('final_fit_rows', 0):,} rows"
        )
    weights = report.get("model_feature_weights", {}).get(best_name, [])
    if weights:
        md.extend(["", "## Top Feature Weights For Best Model"])
        for row in weights[:READABLE_MODEL_WEIGHT_REPORT_ROWS]:
            md.append(
                f"- {row['feature']}: {row['weight']:.4g} ({row['weight_type']})"
            )
    readable_weights = {
        name: rows
        for name, rows in report.get("model_feature_weights", {}).items()
        if name in {"Ridge", "ElasticNet"} and rows
    }
    if readable_weights:
        md.extend(["", "## Top 30 Linear Model Weights"])
        for name, rows in readable_weights.items():
            md.extend(["", f"### {name}"])
            for row in rows[:READABLE_MODEL_WEIGHT_REPORT_ROWS]:
                md.append(
                    f"- {row['feature']}: {row['weight']:.4g} ({row['weight_type']})"
                )
    md.extend(
        [
            "",
            "## Notes",
            "- The current-price benchmark now uses leakage-safe VIN handling.",
            "- Answer-derived `price_band` is excluded from model features and used only for diagnostics and high-value router labels during training.",
            "- Every candidate model uses a leakage-safe classifier router before separately fit everyday/high-value regressors.",
            "- `nhtsa_BasePrice` and `nhtsa_BasePrice_source` are excluded because base price can be filled from observed price history.",
            "- Hyperparameters are tuned on a representative bounded sample, then refit on the full training split.",
            "- Full eligible-VIN training is the default; pass a positive `--sample-size` for bounded development runs.",
            "",
            "## Research Rationale",
            "- Used-car pricing literature supports supervised tabular/tree models when vehicle attributes, mileage, and market context are controlled.",
            "- Manheim's used-vehicle index methodology reinforces mileage, mix, outlier, and seasonal controls rather than raw price comparisons.",
        ]
    )
    (output_dir / "model_report.md").write_text("\n".join(md) + "\n", encoding="utf-8")


def load_and_split_data():
    """Backward-compatible helper used by the existing notebook."""
    raw_df, _ = load_modeling_frame(
        DB_PATH,
        ABSA_DB_PATH,
        DEFAULT_SAMPLE_SIZE,
        deduplicate_vins=True,
    )
    model_df = engineer_current_price_features(raw_df)
    train_df, test_df, _ = split_train_test(model_df)
    X_train, y_train, _ = make_feature_matrix(train_df)
    X_test, y_test, _ = make_feature_matrix(test_df)
    return X_train, y_train, X_test, y_test


def main() -> None:
    args = parse_args()
    report = train_current_price_models(
        db_path=args.db_path,
        absa_db_path=args.absa_db_path,
        output_dir=args.output_dir,
        sample_size=args.sample_size,
        sample_strategy=args.sample_strategy,
        deduplicate_vins=not args.keep_duplicate_vins,
        split_date=args.split_date,
    )
    print(f"Recommended current-price model: {report['recommended_model']}")
    print(f"Saved report to {Path(args.output_dir) / 'model_report.json'}")

    if args.task == "all":
        from Time_Series_Price import run_cohort_forecast

        target_months = [int(x.strip()) for x in args.target_months.split(",") if x.strip()]
        run_cohort_forecast(
            db_path=Path(args.db_path),
            output_dir=Path(args.output_dir),
            sample_size=args.sample_size,
            target_months=target_months,
            forecast_months=args.forecast_months,
            split_date=args.split_date,
        )


if __name__ == "__main__":
    main()
