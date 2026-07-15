import argparse
import logging
import os
import re
import sqlite3
from datetime import date
from pathlib import Path
from typing import Iterable

import pandas as pd
import polars as pl

try:
    from database import CarDatabase
    from VehicleNormalization import (
        NORMALIZATION_VERSION,
        VehicleNormalizer,
        build_epa_metadata_frame,
        build_vehicle_identity,
        download_epa_catalog,
        empty_epa_catalog,
        load_epa_catalog,
    )
except ImportError:  # pragma: no cover - used when imported as a package in tests
    from DataPipeline.database import CarDatabase
    from DataPipeline.VehicleNormalization import (
        NORMALIZATION_VERSION,
        VehicleNormalizer,
        build_epa_metadata_frame,
        build_vehicle_identity,
        download_epa_catalog,
        empty_epa_catalog,
        load_epa_catalog,
    )

LISTINGS_KEEP_COLUMNS = [
    "vin", "date", "loaddate", "title", "location", "locationCode",
    "countryCode", "pendingSale", "distance", "priceRecentChange", "price",
    "mileage", "sourceName", "sellerType", "listingType", "vehicleTitle",
    "vehicleTitleDesc",
]

NHTSA_DROP_COLUMNS = {
    "nhtsa_latest_recall_date", "nhtsa_recall_components", "nhtsa_common_complaint_areas",
    "nhtsa_complaint_deaths", "nhtsa_complaint_crash_related", "nhtsa_complaint_fire_related",
    "nhtsa_complaint_injuries", "nhtsa_side_crash_rating", "nhtsa_rollover_rating",
    "nhtsa_front_crash_rating", "nhtsa_overall_rating", "nhtsa_safety_ratings_count",
    "nhtsa_FuelTankMaterial", "nhtsa_FuelTankType",
    "nhtsa_AdditionalErrorText", "nhtsa_DisplacementCC", "nhtsa_DisplacementCI",
    "nhtsa_ModelID", "nhtsa_ManufacturerId", "nhtsa_MakeID", 'nhtsa_SemiautomaticHeadlampBeamSwitching',
    'nhtsa_LowerBeamHeadlampLightSource', 'nhtsa_EntertainmentSystem', 'nhtsa_DestinationMarket',
    'nhtsa_BrakeSystemDesc'
}

PRICE_MIN = 1000
PRICE_MAX = 1_000_000
PRICE_CONTEXT_MIN_MEDIAN = 5_000
PRICE_CONTEXT_HIGH_RATIO = 3.5
PRICE_CONTEXT_LOW_RATIO = 0.25
PRICE_CONTEXT_IQR_MULTIPLIER = 3.0
PRICE_REPEATED_HIGH_RATIO = 2.0
PRICE_REPEATED_LOW_RATIO = 0.50
PRICE_REPEATED_DIGIT_LENGTH = 5
PRICE_CONTEXT_LEVELS = [
    (["canonical_make", "canonical_model", "canonical_year", "canonical_trim"], 6, "make_model_year_trim"),
    (["canonical_make", "canonical_model", "canonical_year"], 10, "make_model_year"),
    (["canonical_make", "canonical_model"], 20, "make_model"),
]

VIN_BLACKLIST = {
    "1GCUY6ED0LF228114",
    "1FTFW6L8XSFB63087",
}

CARDINALITY_THRESHOLD = 0.005  # 0.5%

KNOWN_TRIMS = [
    "TRD OFF ROAD", "TRD SPORT", "HIGH COUNTRY", "KING RANCH", "BIG HORN",
    "M SPORT", "AMG", "PLATINUM", "LIMITED", "PREMIUM", "TOURING", "DENALI",
    "LARIAT", "RAPTOR", "LARAMIE", "LONGHORN", "RUBICON", "OVERLAND",
    "ALTITUDE", "GT350", "GT500", "PRO 4X", "TRD PRO", "RTL E", "EX L",
    "330I", "340I", "430I", "440I", "530I", "540I", "M340I", "M440I",
    "M3", "M4", "M5", "RS3", "RS5", "RS7", "S3", "S4", "S5", "S6", "S7",
    "2LT", "1LT", "3LT", "2SS", "1SS", "Z71", "LTZ", "RST", "SLT", "SLE",
    "SEL", "XLE", "XSE", "SR5", "SPORT", "BASE", "RTL", "SXT", "SRT",
    "XLT", "WILLYS", "SAHARA", "REBEL", "SV", "SR", "SL", "SE", "LE",
    "EX", "LX", "SI", "LT", "LS", "XL", "GT", "R T",
]

TITLE_STOPWORDS = {
    "NEW", "USED", "CERTIFIED", "CPO", "SEDAN", "COUPE", "SUV", "TRUCK",
    "VAN", "WAGON", "HATCHBACK", "CONVERTIBLE", "AWD", "FWD", "RWD", "4WD",
    "4X4", "AUTO", "AUTOMATIC", "MANUAL", "GAS", "GASOLINE", "HYBRID",
    "ELECTRIC", "DIESEL", "TURBO", "LOCAL", "CARFAX", "CLEAN", "TITLE",
    "NAVIGATION", "SUNROOF", "LEATHER", "BACKUP", "CAMERA", "LOW", "MILES",
}


def normalize_trim_text(value: object, fallback: str = "") -> str:
    """Normalize trim-like text while preserving useful alphanumeric badges."""
    if value is None or pd.isna(value):
        return fallback
    text = re.sub(r"[^A-Za-z0-9]+", " ", str(value).upper())
    text = re.sub(r"\s+", " ", text).strip()
    if not text or text in {"N A", "NA", "NONE", "NULL", "UNKNOWN", "NOT APPLICABLE"}:
        return fallback
    return text


def normalize_title(value: object) -> str:
    return normalize_trim_text(value, "")


def extract_clean_trim(make: object, model: object, model_year: object, title: object) -> str:
    """Extract a trim-like token from noisy listing titles."""
    title_text = normalize_title(title)
    if not title_text:
        return ""

    padded = f" {title_text} "
    for trim in KNOWN_TRIMS:
        normalized_trim = normalize_trim_text(trim)
        if f" {normalized_trim} " in padded:
            return normalized_trim.replace(" ", "_")

    remove_tokens = set(normalize_title(make).split())
    remove_tokens.update(normalize_title(model).split())
    if model_year is not None and not pd.isna(model_year):
        try:
            remove_tokens.add(str(int(float(model_year))))
        except (TypeError, ValueError):
            remove_tokens.add(str(model_year).strip())

    candidates = [
        token
        for token in title_text.split()
        if token not in remove_tokens
        and token not in TITLE_STOPWORDS
        and not token.isdigit()
        and len(token) > 1
    ]
    if not candidates:
        return ""
    return "_".join(candidates[:2])[:40]


def combine_trim_values(*values: object) -> str:
    """Return a stable, de-duplicated label for comparison-only trim values."""
    parts = []
    seen = set()
    for value in values:
        cleaned = normalize_trim_text(value).replace(" ", "_")
        if cleaned and cleaned != "UNKNOWN_TRIM" and cleaned not in seen:
            parts.append(cleaned)
            seen.add(cleaned)
    return " ".join(parts) if parts else "UNKNOWN_TRIM"


def has_repeated_digit_price(value: object) -> bool:
    if value is None or pd.isna(value):
        return False
    try:
        digits = str(int(float(value)))
    except (TypeError, ValueError):
        return False
    if len(digits) < PRICE_REPEATED_DIGIT_LENGTH:
        return False
    return len(set(digits)) == 1 and digits[0] != "0"


def reduce_cardinality(df: pl.DataFrame, columns: list[str], threshold: float = CARDINALITY_THRESHOLD) -> pl.DataFrame:
    """Replace rare categories with 'Other' for specified columns."""
    for col in columns:
        if col in df.columns:
            value_counts = df.select(col).value_counts()
            total = df.shape[0]
            rare_values = value_counts.filter(
                pl.col("counts") / total < threshold
            ).select(col).to_series().to_list()
            
            if rare_values:
                df = df.with_columns(
                    pl.when(pl.col(col).is_in(rare_values))
                    .then(pl.lit("Other"))
                    .otherwise(pl.col(col))
                    .alias(col)
                )
    
    return df


class DataCleaningPipeline:
    """Builds a filtered SQLite clone for cleaned analysis output."""

    def __init__(
        self,
        source_db_path: Path,
        target_db_path: Path,
        *,
        epa_cache_dir: Path | None = None,
        refresh_epa: bool = False,
        offline: bool = False,
        require_epa: bool = False,
    ):
        self.source_db_path = Path(source_db_path)
        self.target_db_path = Path(target_db_path)
        self.output_dir = self.target_db_path.parent
        self.epa_cache_dir = Path(epa_cache_dir) if epa_cache_dir is not None else None
        self.refresh_epa = refresh_epa
        self.offline = offline
        self.require_epa = require_epa
        self._setup_logging()

    def _load_epa_reference(self) -> tuple[pl.DataFrame, pl.DataFrame]:
        if self.epa_cache_dir is None:
            if self.require_epa:
                raise ValueError("EPA cache directory is required for this cleaning run")
            catalog = empty_epa_catalog()
            return catalog, build_epa_metadata_frame({"cache_status": "not_configured"}, catalog)
        archive, metadata = download_epa_catalog(
            self.epa_cache_dir,
            refresh=self.refresh_epa,
            offline=self.offline,
        )
        catalog = load_epa_catalog(archive)
        return catalog, build_epa_metadata_frame(metadata, catalog)

    def _setup_logging(self) -> None:
        os.makedirs(self.output_dir, exist_ok=True)
        log_file = self.output_dir / f"cleaning_{date.today()}.log"
        logging.basicConfig(
            filename=str(log_file),
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
            force=True,
        )

    @staticmethod
    def _read_table(conn: sqlite3.Connection, table_name: str) -> pl.DataFrame:
        cursor = conn.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        columns = [d[0] for d in cursor.description]
        return pl.DataFrame(rows, schema=columns, orient="row", infer_schema_length=None)

    @staticmethod
    def _filter_non_empty(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                df = df.filter(
                    pl.col(col).is_not_null()
                    & (pl.col(col).cast(pl.String).str.strip_chars() != "")
                )
        return df

    @staticmethod
    def _normalize_date_columns(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                base = pl.col(col).cast(pl.String).str.strip_chars()
                df = df.with_columns(
                    pl.coalesce(
                        [
                            base.str.to_date("%Y-%m-%d", strict=False),
                            base.str.to_date("%m/%d/%Y", strict=False),
                            base.str.to_date("%Y/%m/%d", strict=False),
                            base.str.to_date("%b %d %Y", strict=False),
                            base.str.to_date("%B %d %Y", strict=False),
                            base.str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False).dt.date(),
                            base.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False).dt.date(),
                            base.str.to_datetime("%Y-%m-%dT%H:%M:%S", strict=False).dt.date(),
                            base.str.to_datetime("%Y-%m-%dT%H:%M:%S%.f", strict=False).dt.date(),
                            base.str.to_datetime("%Y-%m-%dT%H:%M:%SZ", strict=False).dt.date(),
                        ]
                    ).alias(col)
                )
        return df

    @staticmethod
    def _drop_null_dates(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                df = df.filter(pl.col(col).is_not_null())
        return df

    @staticmethod
    def _normalize_numeric_to_int_ceil(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.String)
                    .str.strip_chars()
                    .str.replace_all(r"[\$,]", "")
                    .cast(pl.Float64, strict=False)
                    .ceil()
                    .cast(pl.Int64, strict=False)
                    .alias(col)
                )
        return df

    @staticmethod
    def _filter_price_range(df: pl.DataFrame, price_col: str = "price") -> pl.DataFrame:
        if price_col not in df.columns:
            return df
        return df.filter(
            pl.col(price_col).is_not_null()
            & (pl.col(price_col) >= PRICE_MIN)
            & (pl.col(price_col) <= PRICE_MAX)
        )

    @staticmethod
    def _ensure_columns(df: pl.DataFrame, columns: Iterable[str], dtype: pl.DataType = pl.String) -> pl.DataFrame:
        missing = [col for col in columns if col not in df.columns]
        if not missing:
            return df
        return df.with_columns([pl.lit(None, dtype=dtype).alias(col) for col in missing])

    @staticmethod
    def _normalize_trim_columns(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .map_elements(normalize_trim_text, return_dtype=pl.String)
                    .alias(col)
                )
        return df

    @staticmethod
    def _add_listing_trim_features(df: pl.DataFrame) -> pl.DataFrame:
        """Populate legacy compatibility fields from canonical trim only."""
        required = {"canonical_title", "canonical_trim", "canonical_trim_source"}
        if not required.issubset(set(df.columns)):
            return df
        return df.with_columns(
            [
                pl.col("canonical_title").alias("title_normalized"),
                pl.col("canonical_trim").alias("title_trim"),
                pl.col("canonical_trim").alias("trim_combined"),
                pl.col("canonical_trim_source").alias("trim_source"),
            ]
        )

    @staticmethod
    def _contextual_price_stats(
        df: pl.DataFrame,
        price_col: str,
        context_levels: list[tuple[list[str], int, str]],
    ) -> pl.DataFrame:
        stats_df = df
        valid_levels = [
            (idx, cols, min_count, label)
            for idx, (cols, min_count, label) in enumerate(context_levels)
            if all(col in stats_df.columns for col in cols)
        ]

        for idx, cols, _, _ in valid_levels:
            stats = stats_df.group_by(cols).agg(
                [
                    pl.len().alias(f"_price_context_count_{idx}"),
                    pl.col(price_col).median().alias(f"_price_context_median_{idx}"),
                    pl.col(price_col).quantile(0.25).alias(f"_price_context_q1_{idx}"),
                    pl.col(price_col).quantile(0.75).alias(f"_price_context_q3_{idx}"),
                ]
            )
            stats_df = stats_df.join(stats, on=cols, how="left")

        global_stats = df.select(
            [
                pl.len().alias("_price_context_count_global"),
                pl.col(price_col).median().alias("_price_context_median_global"),
                pl.col(price_col).quantile(0.25).alias("_price_context_q1_global"),
                pl.col(price_col).quantile(0.75).alias("_price_context_q3_global"),
            ]
        ).row(0, named=True)

        stats_df = stats_df.with_columns(
            [
                pl.lit(global_stats["_price_context_count_global"]).alias("_price_context_count_global"),
                pl.lit(global_stats["_price_context_median_global"]).alias("_price_context_median_global"),
                pl.lit(global_stats["_price_context_q1_global"]).alias("_price_context_q1_global"),
                pl.lit(global_stats["_price_context_q3_global"]).alias("_price_context_q3_global"),
            ]
        )

        count_expr = pl.col("_price_context_count_global")
        median_expr = pl.col("_price_context_median_global")
        q1_expr = pl.col("_price_context_q1_global")
        q3_expr = pl.col("_price_context_q3_global")
        level_expr = pl.lit("global")

        for idx, _, min_count, label in reversed(valid_levels):
            enough_rows = pl.col(f"_price_context_count_{idx}") >= min_count
            count_expr = pl.when(enough_rows).then(pl.col(f"_price_context_count_{idx}")).otherwise(count_expr)
            median_expr = pl.when(enough_rows).then(pl.col(f"_price_context_median_{idx}")).otherwise(median_expr)
            q1_expr = pl.when(enough_rows).then(pl.col(f"_price_context_q1_{idx}")).otherwise(q1_expr)
            q3_expr = pl.when(enough_rows).then(pl.col(f"_price_context_q3_{idx}")).otherwise(q3_expr)
            level_expr = pl.when(enough_rows).then(pl.lit(label)).otherwise(level_expr)

        return stats_df.with_columns(
            [
                count_expr.alias("_price_context_count"),
                median_expr.alias("_price_context_median"),
                q1_expr.alias("_price_context_q1"),
                q3_expr.alias("_price_context_q3"),
                level_expr.alias("_price_context_level"),
            ]
        )

    @staticmethod
    def _filter_contextual_price_outliers(
        df: pl.DataFrame,
        price_col: str = "price",
        label: str = "rows",
    ) -> pl.DataFrame:
        if price_col not in df.columns or df.is_empty():
            return df

        helper_prefixes = ("_price_context_",)
        df = DataCleaningPipeline._contextual_price_stats(df, price_col, PRICE_CONTEXT_LEVELS)
        df = df.with_columns(
            pl.col(price_col)
            .map_elements(has_repeated_digit_price, return_dtype=pl.Boolean)
            .alias("_price_repeated_digit")
        )

        iqr = (pl.col("_price_context_q3") - pl.col("_price_context_q1")).clip(lower_bound=0)
        upper_fence = pl.max_horizontal(
            pl.col("_price_context_q3") + (PRICE_CONTEXT_IQR_MULTIPLIER * iqr),
            pl.col("_price_context_median") + 25_000,
        )
        lower_fence = pl.min_horizontal(
            (pl.col("_price_context_q1") - (PRICE_CONTEXT_IQR_MULTIPLIER * iqr)).clip(lower_bound=0),
            pl.col("_price_context_median") * 0.5,
        )
        repeated_upper_fence = pl.max_horizontal(
            pl.col("_price_context_q3") + (1.5 * iqr),
            pl.col("_price_context_median") + 15_000,
        )
        valid_context = (
            (pl.col("_price_context_count") >= 6)
            & (pl.col("_price_context_median") >= PRICE_CONTEXT_MIN_MEDIAN)
        )
        high_context_outlier = (
            valid_context
            & (pl.col(price_col) > pl.col("_price_context_median") * PRICE_CONTEXT_HIGH_RATIO)
            & (pl.col(price_col) > upper_fence)
        )
        low_context_outlier = (
            valid_context
            & (pl.col(price_col) < pl.col("_price_context_median") * PRICE_CONTEXT_LOW_RATIO)
            & (pl.col(price_col) < lower_fence)
        )
        repeated_digit_outlier = (
            valid_context
            & pl.col("_price_repeated_digit")
            & (
                (
                    (pl.col(price_col) > pl.col("_price_context_median") * PRICE_REPEATED_HIGH_RATIO)
                    & (pl.col(price_col) > repeated_upper_fence)
                )
                | (
                    (pl.col(price_col) < pl.col("_price_context_median") * PRICE_REPEATED_LOW_RATIO)
                    & (pl.col(price_col) < pl.col("_price_context_q1"))
                )
            )
        )

        df = df.with_columns(
            (high_context_outlier | low_context_outlier | repeated_digit_outlier).alias("_price_context_outlier")
        )
        removed = int(df.filter(pl.col("_price_context_outlier")).height)
        if removed:
            logging.info("Dropped %d contextual price outliers from %s", removed, label)

        drop_cols = [
            col
            for col in df.columns
            if col.startswith(helper_prefixes) or col in {"_price_repeated_digit", "_price_context_outlier"}
        ]
        return df.filter(~pl.col("_price_context_outlier")).drop(drop_cols)

    @staticmethod
    def _fill_nhtsa_base_price_from_history(
        nhtsa: pl.DataFrame,
        price_history: pl.DataFrame,
        listing_history: pl.DataFrame,
    ) -> pl.DataFrame:
        """Fill missing NHTSA base price from earliest cleaned history prices."""
        nhtsa = DataCleaningPipeline._ensure_columns(nhtsa, ["nhtsa_BasePrice"], dtype=pl.Int64)

        def earliest_price(df: pl.DataFrame, alias: str) -> pl.DataFrame:
            if df.is_empty() or not {"vin", "history_date", "price"}.issubset(set(df.columns)):
                return pl.DataFrame({"vin": [], alias: []}, schema={"vin": pl.String, alias: pl.Int64})
            return (
                df.filter(pl.col("price").is_not_null() & (pl.col("price") > 0))
                .sort(["vin", "history_date"])
                .group_by("vin", maintain_order=True)
                .agg(pl.col("price").first().alias(alias))
            )

        price_first = earliest_price(price_history, "_base_price_from_price_history")
        listing_first = earliest_price(listing_history, "_base_price_from_listing_history")
        nhtsa = nhtsa.join(price_first, on="vin", how="left").join(listing_first, on="vin", how="left")
        history_fill = pl.coalesce(
            [pl.col("_base_price_from_price_history"), pl.col("_base_price_from_listing_history")]
        )
        missing_base_price = pl.col("nhtsa_BasePrice").is_null() | (pl.col("nhtsa_BasePrice") <= 0)
        nhtsa = nhtsa.with_columns(
            [
                pl.when(missing_base_price)
                .then(history_fill)
                .otherwise(pl.col("nhtsa_BasePrice"))
                .alias("nhtsa_BasePrice"),
                pl.when(~missing_base_price)
                .then(pl.lit("nhtsa"))
                .when(pl.col("_base_price_from_price_history").is_not_null())
                .then(pl.lit("price_history"))
                .when(pl.col("_base_price_from_listing_history").is_not_null())
                .then(pl.lit("listing_history"))
                .otherwise(pl.lit("missing"))
                .alias("nhtsa_BasePrice_source"),
            ]
        )
        return nhtsa.drop(["_base_price_from_price_history", "_base_price_from_listing_history"])

    @staticmethod
    def _filter_non_null_numeric(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                df = df.filter(pl.col(col).is_not_null())
        return df

    @staticmethod
    def _cast_to_int(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.String).str.strip_chars().cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).alias(col)
                )
        return df

    @staticmethod
    def _cast_to_float(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.String).str.strip_chars().cast(pl.Float64, strict=False).alias(col)
                )
        return df

    @staticmethod
    def _insert_dataframe(
        conn: sqlite3.Connection,
        table_name: str,
        df: pl.DataFrame,
        conflict_mode: str = "",
    ) -> int:
        if df.is_empty():
            return 0

        columns = df.columns
        placeholders = ", ".join(["?" for _ in columns])
        quoted_columns = ", ".join([f'"{c}"' for c in columns])
        conflict_clause = f"OR {conflict_mode} " if conflict_mode else ""
        sql = f"INSERT {conflict_clause}INTO {table_name} ({quoted_columns}) VALUES ({placeholders})"

        rows = [tuple(r) for r in df.iter_rows()]
        conn.executemany(sql, rows)
        return len(rows)

    @staticmethod
    def _create_tables_from_polars(conn: sqlite3.Connection, table_configs: dict[str, tuple[pl.DataFrame, list[str]]]) -> None:
        for table_name, (df, pks) in table_configs.items():
            cols = []
            for name, dtype in df.schema.items():
                if dtype.is_integer():
                    sql_type = "INTEGER"
                elif dtype.is_float():
                    sql_type = "REAL"
                elif dtype == pl.Boolean:
                    sql_type = "BOOLEAN"
                elif dtype == pl.Date:
                    sql_type = "DATE"
                elif isinstance(dtype, pl.Datetime):
                    sql_type = "TIMESTAMP"
                else:
                    sql_type = "TEXT"
                cols.append(f'"{name}" {sql_type}')

            if pks:
                pk_str = ", ".join(f'"{k}"' for k in pks)
                cols.append(f"PRIMARY KEY ({pk_str})")

            columns_sql = ",\n    ".join(cols)
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} (\n    {columns_sql}\n)")

    @staticmethod
    def _create_indexes(conn: sqlite3.Connection) -> None:
        """Create read-optimized indexes used by modeling and time-series tasks."""
        index_statements = [
            "CREATE INDEX IF NOT EXISTS idx_listings_vin ON listings (vin)",
            "CREATE INDEX IF NOT EXISTS idx_listings_loaddate ON listings (loaddate)",
            "CREATE INDEX IF NOT EXISTS idx_listings_price ON listings (price)",
            "CREATE INDEX IF NOT EXISTS idx_listings_canonical_identity ON listings (canonical_make, canonical_model, canonical_year, canonical_trim)",
            "CREATE INDEX IF NOT EXISTS idx_listings_normalization_audit ON listings (canonical_match_confidence, canonical_trim_source, canonical_match_status)",
            "CREATE INDEX IF NOT EXISTS idx_listings_epa_vehicle_id ON listings (epa_vehicle_id)",
            "CREATE INDEX IF NOT EXISTS idx_nhtsa_make_model_year ON nhtsa_enrichment (nhtsa_Make, nhtsa_Model, nhtsa_ModelYear)",
            "CREATE INDEX IF NOT EXISTS idx_vehicle_identity_canonical ON vehicle_identity (canonical_make, canonical_model, canonical_year, canonical_trim)",
            "CREATE INDEX IF NOT EXISTS idx_vehicle_identity_confidence ON vehicle_identity (canonical_match_confidence, conflict_count)",
            "CREATE INDEX IF NOT EXISTS idx_epa_catalog_normalized_identity ON epa_vehicle_catalog (year, normalized_make, normalized_model)",
            "CREATE INDEX IF NOT EXISTS idx_epa_catalog_normalized_base ON epa_vehicle_catalog (year, normalized_make, normalized_base_model)",
            "CREATE INDEX IF NOT EXISTS idx_listing_history_date ON listing_history (history_date)",
            "CREATE INDEX IF NOT EXISTS idx_listing_history_vin_date ON listing_history (vin, history_date)",
            "CREATE INDEX IF NOT EXISTS idx_price_history_date ON price_history (history_date)",
            "CREATE INDEX IF NOT EXISTS idx_price_history_vin_date ON price_history (vin, history_date)",
        ]
        for statement in index_statements:
            conn.execute(statement)

    def run(self) -> None:
        if not self.source_db_path.exists():
            raise FileNotFoundError(f"Source database not found: {self.source_db_path}")

        logging.info("Starting cleaned database build")
        logging.info("Source DB: %s", self.source_db_path)
        logging.info("Target DB: %s", self.target_db_path)

        epa_catalog, epa_metadata = self._load_epa_reference()
        logging.info("EPA catalog rows: %d", epa_catalog.height)

        source_conn = sqlite3.connect(str(self.source_db_path))
        try:
            listings = self._read_table(source_conn, "listings")
            listing_history = self._read_table(source_conn, "listing_history")
            price_history = self._read_table(source_conn, "price_history")
            nhtsa = self._read_table(source_conn, "nhtsa_enrichment")
        finally:
            source_conn.close()

        listings = listings.select([c for c in LISTINGS_KEEP_COLUMNS if c in listings.columns])
        listings = self._filter_non_empty(listings, ["vin", "loaddate"])
        listings = listings.filter(~pl.col("vin").is_in(VIN_BLACKLIST))
        listings = self._normalize_date_columns(listings, ["date", "loaddate"])
        listings = self._drop_null_dates(listings, ["date", "loaddate"])
        listings = self._normalize_numeric_to_int_ceil(listings, ["price", "mileage"])
        listings = self._filter_non_null_numeric(listings, ["price", "mileage"])
        listings = self._filter_price_range(listings, "price")

        listings = self._cast_to_int(listings, ["locationCode"])
        listings = self._cast_to_float(listings, ["distance"])

        nhtsa = self._ensure_columns(nhtsa, ["nhtsa_Trim", "nhtsa_Trim2"])
        nhtsa = self._filter_non_empty(nhtsa, ["vin", "nhtsa_Make", "nhtsa_Model", "nhtsa_ModelYear"]).with_columns(
            [
                pl.col("nhtsa_Make").cast(pl.String).str.strip_chars().str.to_uppercase().alias("nhtsa_Make"),
                pl.col("nhtsa_Model").cast(pl.String).str.strip_chars().str.to_uppercase().alias("nhtsa_Model"),
                pl.col("nhtsa_ModelYear").cast(pl.String).str.strip_chars().cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).alias("nhtsa_ModelYear"),
            ]
        )
        nhtsa = self._cast_to_int(nhtsa, [
            "nhtsa_Axles", "nhtsa_BedLengthIN", "nhtsa_BasePrice", "nhtsa_ChargerPowerKW", 
            "nhtsa_CurbWeightLB", "nhtsa_Doors", "nhtsa_EngineCycles", "nhtsa_EngineCylinders", 
            "nhtsa_EngineHP", "nhtsa_EngineHP_to", "nhtsa_EngineKW", "nhtsa_SAEAutomationLevel", 
            "nhtsa_SeatRows", "nhtsa_Seats", "nhtsa_TopSpeedMPH", "nhtsa_TransmissionSpeeds", 
            "nhtsa_WheelSizeFront", "nhtsa_WheelSizeRear", "nhtsa_Windows", "nhtsa_WheelBaseLong", 
            "nhtsa_WheelBaseShort"
        ])
        
        nhtsa = self._cast_to_float(nhtsa, [
            "nhtsa_DisplacementL", "nhtsa_TrackWidth"
        ])

        nhtsa = self._normalize_trim_columns(nhtsa, ["nhtsa_Trim", "nhtsa_Trim2"])

        listing_context_columns = [
            "vin", "nhtsa_Make", "nhtsa_Model", "nhtsa_ModelYear", "nhtsa_Trim", "nhtsa_Trim2"
        ]
        scoped_listings = listings.join(nhtsa.select(listing_context_columns), on="vin", how="inner")
        scoped_listings = VehicleNormalizer(epa_catalog).normalize_listings(scoped_listings)
        scoped_listings = self._add_listing_trim_features(scoped_listings)
        scoped_listings = self._filter_contextual_price_outliers(scoped_listings, "price", "listings")
        scoped_vins = scoped_listings.select("vin").unique()
        vehicle_identity = build_vehicle_identity(scoped_listings)

        nhtsa = nhtsa.with_columns(
            [
                pl.when(
                    (pl.col("nhtsa_Trim").is_not_null() & (pl.col("nhtsa_Trim") != ""))
                    | (pl.col("nhtsa_Trim2").is_not_null() & (pl.col("nhtsa_Trim2") != ""))
                )
                .then(pl.lit("nhtsa"))
                .otherwise(pl.lit("unknown"))
                .alias("nhtsa_Trim_source"),
                pl.struct(["nhtsa_Trim", "nhtsa_Trim2"])
                .map_elements(
                    lambda row: combine_trim_values(row["nhtsa_Trim"], row["nhtsa_Trim2"]),
                    return_dtype=pl.String,
                )
                .alias("nhtsa_trim_combined"),
            ]
        )

        vin_price_context = scoped_listings.select(
            ["vin", "canonical_make", "canonical_model", "canonical_year", "canonical_trim"]
        ).unique(subset=["vin"], keep="last")

        listing_helper_cols = ["nhtsa_Make", "nhtsa_Model", "nhtsa_ModelYear", "nhtsa_Trim", "nhtsa_Trim2"]
        scoped_listings = scoped_listings.drop([c for c in listing_helper_cols if c in scoped_listings.columns])

        cardinality_cols = [c for c in ['nhtsa_Make', 'nhtsa_Model'] if c in scoped_listings.columns]
        if cardinality_cols:
            scoped_listings = reduce_cardinality(scoped_listings, cardinality_cols, CARDINALITY_THRESHOLD)
        
        listing_history = listing_history.select([c for c in listing_history.columns if c != "id"])
        price_history = price_history.select([c for c in price_history.columns if c != "id"])

        listing_history = self._normalize_date_columns(listing_history, ["history_date"])
        listing_history = self._drop_null_dates(listing_history, ["history_date"])
        listing_history = self._normalize_numeric_to_int_ceil(listing_history, ["price", "mileage"])
        listing_history = self._filter_price_range(listing_history, "price")
        listing_history = listing_history.join(vin_price_context, on="vin", how="inner")
        listing_history = self._filter_contextual_price_outliers(listing_history, "price", "listing_history")
        listing_history = listing_history.drop(
            [
                c
                for c in ["canonical_make", "canonical_model", "canonical_year", "canonical_trim"]
                if c in listing_history.columns
            ]
        )

        price_history = self._normalize_date_columns(price_history, ["history_date"])
        price_history = self._drop_null_dates(price_history, ["history_date"])
        price_history = self._normalize_numeric_to_int_ceil(price_history, ["price", "mileage"])
        price_history = self._filter_price_range(price_history, "price")
        price_history = price_history.join(vin_price_context, on="vin", how="inner")
        price_history = self._filter_contextual_price_outliers(price_history, "price", "price_history")
        price_history = price_history.drop(
            [
                c
                for c in ["canonical_make", "canonical_model", "canonical_year", "canonical_trim"]
                if c in price_history.columns
            ]
        )

        # Keep all history rows, but restrict them to VINs in the make-filtered scope.
        listing_history = listing_history.join(scoped_vins, on="vin", how="inner")
        price_history = price_history.join(scoped_vins, on="vin", how="inner")

        nhtsa = self._fill_nhtsa_base_price_from_history(nhtsa, price_history, listing_history)
        keep_nhtsa_columns = [c for c in nhtsa.columns if c not in NHTSA_DROP_COLUMNS]
        scoped_nhtsa = nhtsa.join(scoped_vins, on="vin", how="inner").select(keep_nhtsa_columns)

        target_conn = sqlite3.connect(str(self.target_db_path))
        try:
            self._create_tables_from_polars(target_conn, {
                "listings": (scoped_listings, ["vin", "loaddate"]),
                "nhtsa_enrichment": (scoped_nhtsa, ["vin"]),
                "listing_history": (listing_history, ["vin", "history_date", "price", "mileage"]),
                "price_history": (price_history, ["vin", "history_date", "price"]),
                "vehicle_identity": (vehicle_identity, ["vin"]),
                "epa_vehicle_catalog": (epa_catalog, ["id"]),
                "epa_catalog_metadata": (epa_metadata, []),
            })

            inserted_listings = self._insert_dataframe(target_conn, "listings", scoped_listings, conflict_mode="REPLACE")
            inserted_nhtsa = self._insert_dataframe(target_conn, "nhtsa_enrichment", scoped_nhtsa, conflict_mode="REPLACE")
            inserted_listing_hist = self._insert_dataframe(target_conn, "listing_history", listing_history, conflict_mode="IGNORE")
            inserted_price_hist = self._insert_dataframe(target_conn, "price_history", price_history, conflict_mode="IGNORE")
            inserted_vehicle_identity = self._insert_dataframe(
                target_conn, "vehicle_identity", vehicle_identity, conflict_mode="REPLACE"
            )
            inserted_epa = self._insert_dataframe(
                target_conn, "epa_vehicle_catalog", epa_catalog, conflict_mode="REPLACE"
            )
            self._insert_dataframe(target_conn, "epa_catalog_metadata", epa_metadata)
            self._create_indexes(target_conn)
            target_conn.commit()
        finally:
            target_conn.close()

        logging.info("Finished cleaned database build")
        logging.info("Inserted listings: %d", inserted_listings)
        logging.info("Inserted nhtsa_enrichment: %d", inserted_nhtsa)
        logging.info("Inserted listing_history: %d", inserted_listing_hist)
        logging.info("Inserted price_history: %d", inserted_price_hist)
        logging.info("Inserted vehicle_identity: %d", inserted_vehicle_identity)
        logging.info("Inserted EPA vehicles: %d", inserted_epa)

        print("Cleaned database build complete")
        print(f"Target DB: {self.target_db_path}")
        print(f"listings rows: {inserted_listings}")
        print(f"nhtsa_enrichment rows: {inserted_nhtsa}")
        print(f"listing_history rows: {inserted_listing_hist}")
        print(f"price_history rows: {inserted_price_hist}")
        print(f"vehicle_identity rows: {inserted_vehicle_identity}")
        print(f"EPA catalog rows: {inserted_epa}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the canonical cleaned vehicle database")
    repo_root = Path(__file__).resolve().parent.parent
    parser.add_argument("--source-db", type=Path, default=repo_root / "CAR_DATA_OUTPUT" / "CAR_DATA.db")
    parser.add_argument("--target-db", type=Path, default=repo_root / "CAR_DATA_OUTPUT" / "CAR_DATA_CLEANED.db")
    parser.add_argument(
        "--epa-cache-dir",
        type=Path,
        default=repo_root / "CAR_DATA_OUTPUT" / "reference" / "epa_fuel_economy",
    )
    parser.add_argument("--offline", action="store_true", help="Require the validated local EPA cache")
    parser.add_argument("--no-epa-refresh", action="store_true", help="Use the validated EPA cache without checking upstream")
    args = parser.parse_args()

    print("Running data cleaning pipeline...")

    cleaner = DataCleaningPipeline(
        source_db_path=args.source_db,
        target_db_path=args.target_db,
        epa_cache_dir=args.epa_cache_dir,
        refresh_epa=not args.no_epa_refresh,
        offline=args.offline,
        require_epa=True,
    )
    cleaner.run()


if __name__ == "__main__":
    main()

