"""Verify raw or cleaned SQLite schemas without modifying the database."""

from __future__ import annotations

import argparse
import sqlite3
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


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info('{table}')")}


def verify_schema(db_path: Path) -> bool:
    if not db_path.exists():
        print(f"Database not found: {db_path}")
        return False
    with sqlite3.connect(db_path) as conn:
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        listing_columns = table_columns(conn, "listings") if "listings" in tables else set()
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


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db-path", type=Path, default=DEFAULT_DB_PATH)
    args = parser.parse_args()
    raise SystemExit(0 if verify_schema(args.db_path) else 1)


if __name__ == "__main__":
    main()
