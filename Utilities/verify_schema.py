"""Verify raw or cleaned SQLite schemas without modifying the database."""

from __future__ import annotations

import argparse
import sqlite3
from contextlib import closing
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = BASE_DIR / "CAR_DATA_OUTPUT" / "CAR_DATA_CLEANED.db"

CANONICAL_LISTING_COLUMNS = {
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
}
CANONICAL_TABLES = {"vehicle_identity", "epa_vehicle_catalog", "epa_catalog_metadata"}
SENTIMENT_SCORED_COLUMNS = {
    "sentiment_make",
    "make_attribution_source",
    "make_attribution_version",
    "overall_sentiment",
    "overall_confidence",
    "sentiment_status",
    "model_revision",
}
SENTIMENT_TABLES = {"make_sentiment_index", "make_sentiment_monthly"}
NHTSA_TABLES = {
    "nhtsa_schema_meta",
    "nhtsa_ingestion_runs",
    "nhtsa_source_catalog",
    "nhtsa_vpic_decodes",
    "nhtsa_vpic_values",
    "nhtsa_vin_identity_resolution",
    "nhtsa_vehicle_queries",
    "nhtsa_safety_variants",
    "nhtsa_safety_details",
    "nhtsa_safety_rating_values",
    "nhtsa_recalls",
    "nhtsa_complaints",
    "nhtsa_complaint_products",
    "nhtsa_bulk_datasets",
    "nhtsa_bulk_rows",
    "nhtsa_api_extra_fields",
    "nhtsa_bulk_fields",
}
NHTSA_FORBIDDEN_RAW_COLUMNS = {
    "response_json", "result_json", "record_json", "raw_json",
    "variant_json", "detail_json", "value_json", "request_payload_json",
    "listing_context_json", "metadata_json", "details_json",
}


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info('{table}')")}


def verify_schema(db_path: Path) -> bool:
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return False
    with closing(sqlite3.connect(db_path)) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        listing_columns = table_columns(conn, "listings") if "listings" in tables else set()
        if "youtube_comments_scored" in tables:
            scored_columns = table_columns(conn, "youtube_comments_scored")
            missing_sentiment_columns = sorted(SENTIMENT_SCORED_COLUMNS - scored_columns)
            missing_sentiment_tables = sorted(SENTIMENT_TABLES - tables)
            print(f"Database: {db_path}")
            for table in sorted(tables):
                count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                print(f"  {table}: {count:,} rows")
            if missing_sentiment_columns:
                print("Missing make-level scored columns: " + ", ".join(missing_sentiment_columns))
            if missing_sentiment_tables:
                print("Missing make-level sentiment tables: " + ", ".join(missing_sentiment_tables))
            if not missing_sentiment_columns and not missing_sentiment_tables:
                print("Make-level sentiment schema verification passed")
                return True
            return False
        missing_columns = sorted(CANONICAL_LISTING_COLUMNS - listing_columns)
        missing_tables = sorted(CANONICAL_TABLES - tables)
        print(f"Database: {db_path}")
        for table in sorted(tables):
            count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            print(f"  {table}: {count:,} rows")
        if missing_columns:
            print("Missing canonical listing columns: " + ", ".join(missing_columns))
        if missing_tables:
            print("Missing canonical tables: " + ", ".join(missing_tables))
        if not missing_columns and not missing_tables:
            versions = conn.execute(
                "SELECT normalization_version, COUNT(*) FROM vehicle_identity GROUP BY normalization_version"
            ).fetchall()
            print(f"Canonical normalization versions: {versions}")
            print("Schema verification passed")
            return True
    return False


def verify_nhtsa_schema(db_path: Path) -> bool:
    """Verify the separate raw/normalized NHTSA database without modifying it."""
    if not db_path.exists():
        print(f"NHTSA database not found (not initialized yet): {db_path}")
        return True
    try:
        with closing(sqlite3.connect(db_path)) as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
                )
            }
            missing = sorted(NHTSA_TABLES - tables)
            version_row = conn.execute(
                "SELECT value FROM nhtsa_schema_meta WHERE key = 'schema_version'"
            ).fetchone() if "nhtsa_schema_meta" in tables else None
            foreign_key_errors = conn.execute("PRAGMA foreign_key_check").fetchall()
            print(f"NHTSA database: {db_path}")
            for table in sorted(tables):
                count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
                print(f"  {table}: {count:,} rows")
            if missing:
                print("Missing NHTSA tables: " + ", ".join(missing))
            raw_columns = {
                column
                for table in tables
                for column in table_columns(conn, table)
                if column in NHTSA_FORBIDDEN_RAW_COLUMNS
            }
            if version_row is None or version_row[0] != "2":
                print(f"Unexpected NHTSA schema version: {version_row[0] if version_row else 'missing'}")
            if raw_columns:
                print("Unexpected raw JSON columns: " + ", ".join(sorted(raw_columns)))
            if foreign_key_errors:
                print(f"NHTSA foreign-key errors: {len(foreign_key_errors)}")
            passed = (
                not missing and version_row is not None and version_row[0] == "2"
                and not foreign_key_errors and not raw_columns
            )
            print("NHTSA schema verification passed" if passed else "NHTSA schema verification failed")
            return passed
    except Exception as exc:
        print(f"NHTSA schema verification error: {exc}")
        return False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    parser.add_argument(
        "--nhtsa-db-path",
        type=Path,
        default=BASE_DIR / "CAR_DATA_OUTPUT" / "CAR_DATA_NHTSA.db",
    )
    args = parser.parse_args()
    primary_ok = verify_schema(args.db_path)
    nhtsa_ok = verify_nhtsa_schema(args.nhtsa_db_path)
    raise SystemExit(0 if primary_ok and nhtsa_ok else 1)


if __name__ == "__main__":
    main()
