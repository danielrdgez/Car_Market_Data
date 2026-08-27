import csv
import hashlib
import json
import logging
import os
import sqlite3
import threading
import uuid
import zipfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Optional, List, Set

import pandas as pd  # Import pandas for to_sql method


NHTSA_SCHEMA_VERSION = 2


def backup_sqlite_database(
    source_path: str | os.PathLike[str],
    destination_path: str | os.PathLike[str],
) -> None:
    """Create a recoverable SQLite backup without modifying the source DB.

    The destination must not already exist so a prior backup cannot be
    overwritten accidentally. SQLite's backup API is used instead of copying
    files directly, which is safe for a live database and its WAL state.
    """
    source = Path(source_path)
    destination = Path(destination_path)
    if not source.exists():
        raise FileNotFoundError(f"SQLite source database not found: {source}")
    if source.resolve() == destination.resolve():
        raise ValueError("SQLite backup destination must differ from the source")
    if destination.exists():
        raise FileExistsError(f"SQLite backup already exists: {destination}")

    destination.parent.mkdir(parents=True, exist_ok=True)
    source_conn = sqlite3.connect(str(source), timeout=60)
    destination_conn = sqlite3.connect(str(destination), timeout=60)
    try:
        source_conn.backup(destination_conn)
        destination_conn.commit()
    finally:
        destination_conn.close()
        source_conn.close()


class NHTSADataStore:
    """Normalized NHTSA storage without full API-response or raw-row blobs.

    Every source field is retained either in a typed table column or a normalized
    field/value table. Transport wrappers and duplicate JSON payloads are not
    persisted.
    """

    def __init__(self, db_path: str | os.PathLike[str]):
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, timeout=60, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        self._init_db()

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=True, sort_keys=True, default=str)

    @classmethod
    def _hash(cls, value: Any) -> str:
        return hashlib.sha256(cls._json(value).encode("utf-8")).hexdigest()

    @classmethod
    def _db_text(cls, value: Any) -> Optional[str]:
        """Convert a source value into compact, queryable SQLite text."""
        if value is None:
            return None
        if isinstance(value, dict):
            return "; ".join(f"{key}={cls._db_text(item)}" for key, item in value.items())
        if isinstance(value, (list, tuple)):
            return "; ".join(filter(None, (cls._db_text(item) for item in value)))
        return str(value)

    @classmethod
    def _flatten_fields(cls, value: Any, prefix: str = "") -> list[tuple[str, Optional[str]]]:
        """Flatten nested source values so no opaque JSON is required."""
        if isinstance(value, dict):
            flattened: list[tuple[str, Optional[str]]] = []
            for key, item in value.items():
                name = f"{prefix}.{key}" if prefix else str(key)
                flattened.extend(cls._flatten_fields(item, name))
            return flattened
        if isinstance(value, (list, tuple)):
            flattened = []
            for index, item in enumerate(value):
                name = f"{prefix}[{index}]"
                flattened.extend(cls._flatten_fields(item, name))
            return flattened or [(prefix, None)]
        return [(prefix, None if value is None else str(value))]

    @staticmethod
    def _db_integer(value: Any) -> Optional[int]:
        try:
            return int(float(value)) if value not in (None, "") else None
        except (TypeError, ValueError):
            return None

    def _init_db(self) -> None:
        with self._lock, self.conn:
            self.conn.execute("PRAGMA foreign_keys = ON")
            self.conn.execute("PRAGMA journal_mode = WAL")
            has_meta = self.conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'nhtsa_schema_meta'"
            ).fetchone()
            if has_meta:
                version_row = self.conn.execute(
                    "SELECT value FROM nhtsa_schema_meta WHERE key = 'schema_version'"
                ).fetchone()
                if version_row and str(version_row[0]) != str(NHTSA_SCHEMA_VERSION):
                    raise RuntimeError(
                        "CAR_DATA_NHTSA.db uses schema version "
                        f"{version_row[0]}; recreate it for normalized-only schema version "
                        f"{NHTSA_SCHEMA_VERSION}."
                    )
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS nhtsa_schema_meta (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS nhtsa_ingestion_runs (
                    run_id TEXT PRIMARY KEY,
                    source TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    status TEXT NOT NULL,
                    requested_count INTEGER DEFAULT 0,
                    successful_count INTEGER DEFAULT 0,
                    failed_count INTEGER DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS nhtsa_source_catalog (
                    source_name TEXT PRIMARY KEY,
                    source_type TEXT NOT NULL,
                    endpoint_or_url TEXT,
                    source_version TEXT,
                    last_seen_at TEXT,
                    checksum TEXT
                );

                CREATE TABLE IF NOT EXISTS nhtsa_vpic_decodes (
                    decode_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    vin TEXT NOT NULL,
                    model_year_hint INTEGER,
                    endpoint TEXT NOT NULL,
                    response_status TEXT NOT NULL,
                    http_status INTEGER,
                    error_code TEXT,
                    error_text TEXT,
                    message TEXT,
                    response_hash TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    UNIQUE(vin, model_year_hint, response_hash),
                    FOREIGN KEY(run_id) REFERENCES nhtsa_ingestion_runs(run_id)
                );

                CREATE INDEX IF NOT EXISTS idx_nhtsa_vpic_latest
                    ON nhtsa_vpic_decodes(vin, model_year_hint, fetched_at);

                CREATE TABLE IF NOT EXISTS nhtsa_vpic_values (
                    decode_id INTEGER PRIMARY KEY,
                    FOREIGN KEY(decode_id) REFERENCES nhtsa_vpic_decodes(decode_id)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS nhtsa_vin_identity_resolution (
                    identity_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    decode_id INTEGER,
                    vin TEXT NOT NULL,
                    nhtsa_make TEXT,
                    nhtsa_model TEXT,
                    nhtsa_model_year INTEGER,
                    listing_make TEXT,
                    listing_model TEXT,
                    listing_model_year INTEGER,
                    resolved_make TEXT,
                    resolved_model TEXT,
                    resolved_model_year INTEGER,
                    make_source TEXT NOT NULL,
                    model_source TEXT NOT NULL,
                    model_year_source TEXT NOT NULL,
                    confidence TEXT NOT NULL,
                    conflict_flag INTEGER NOT NULL DEFAULT 0,
                    resolved_at TEXT NOT NULL,
                    FOREIGN KEY(run_id) REFERENCES nhtsa_ingestion_runs(run_id),
                    FOREIGN KEY(decode_id) REFERENCES nhtsa_vpic_decodes(decode_id)
                );

                CREATE INDEX IF NOT EXISTS idx_nhtsa_identity_vin
                    ON nhtsa_vin_identity_resolution(vin, resolved_at);

                CREATE UNIQUE INDEX IF NOT EXISTS idx_nhtsa_identity_distinct
                    ON nhtsa_vin_identity_resolution(
                        vin, decode_id, resolved_make, resolved_model,
                        resolved_model_year, confidence, conflict_flag
                    );

                CREATE TABLE IF NOT EXISTS nhtsa_vehicle_queries (
                    query_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    run_id TEXT,
                    query_type TEXT NOT NULL,
                    query_key TEXT NOT NULL,
                    make TEXT,
                    model TEXT,
                    model_year INTEGER,
                    vehicle_id INTEGER,
                    response_status TEXT NOT NULL,
                    http_status INTEGER,
                    error_text TEXT,
                    record_count INTEGER NOT NULL DEFAULT 0,
                    response_hash TEXT NOT NULL,
                    fetched_at TEXT NOT NULL,
                    UNIQUE(query_type, query_key, response_hash),
                    FOREIGN KEY(run_id) REFERENCES nhtsa_ingestion_runs(run_id)
                );

                CREATE INDEX IF NOT EXISTS idx_nhtsa_vehicle_query_key
                    ON nhtsa_vehicle_queries(query_type, query_key, fetched_at);

                CREATE TABLE IF NOT EXISTS nhtsa_safety_variants (
                    query_id INTEGER NOT NULL,
                    vehicle_id INTEGER NOT NULL,
                    vehicle_description TEXT,
                    PRIMARY KEY(query_id, vehicle_id),
                    FOREIGN KEY(query_id) REFERENCES nhtsa_vehicle_queries(query_id)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS nhtsa_safety_details (
                    query_id INTEGER NOT NULL,
                    vehicle_id INTEGER NOT NULL,
                    PRIMARY KEY(query_id, vehicle_id),
                    FOREIGN KEY(query_id, vehicle_id)
                        REFERENCES nhtsa_safety_variants(query_id, vehicle_id)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS nhtsa_safety_rating_values (
                    query_id INTEGER NOT NULL,
                    vehicle_id INTEGER NOT NULL,
                    field_name TEXT NOT NULL,
                    field_value TEXT,
                    PRIMARY KEY(query_id, vehicle_id, field_name),
                    FOREIGN KEY(query_id, vehicle_id)
                        REFERENCES nhtsa_safety_variants(query_id, vehicle_id)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS nhtsa_recalls (
                    query_id INTEGER NOT NULL,
                    record_key TEXT NOT NULL,
                    manufacturer TEXT,
                    nhtsa_campaign_number TEXT,
                    nhtsa_action_number TEXT,
                    report_received_date TEXT,
                    component TEXT,
                    model_year INTEGER,
                    make TEXT,
                    model TEXT,
                    park_it TEXT,
                    park_outside TEXT,
                    over_the_air_update TEXT,
                    summary TEXT,
                    consequence TEXT,
                    remedy TEXT,
                    notes TEXT,
                    PRIMARY KEY(query_id, record_key),
                    FOREIGN KEY(query_id) REFERENCES nhtsa_vehicle_queries(query_id)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS nhtsa_complaints (
                    query_id INTEGER NOT NULL,
                    record_key TEXT NOT NULL,
                    odi_number TEXT,
                    manufacturer TEXT,
                    crash TEXT,
                    fire TEXT,
                    number_of_injuries INTEGER,
                    number_of_deaths INTEGER,
                    date_of_incident TEXT,
                    date_complaint_filed TEXT,
                    vin TEXT,
                    components TEXT,
                    summary TEXT,
                    PRIMARY KEY(query_id, record_key),
                    FOREIGN KEY(query_id) REFERENCES nhtsa_vehicle_queries(query_id)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS nhtsa_complaint_products (
                    query_id INTEGER NOT NULL,
                    record_key TEXT NOT NULL,
                    product_index INTEGER NOT NULL,
                    product_type TEXT,
                    product_year INTEGER,
                    product_make TEXT,
                    product_model TEXT,
                    manufacturer TEXT,
                    PRIMARY KEY(query_id, record_key, product_index),
                    FOREIGN KEY(query_id, record_key)
                        REFERENCES nhtsa_complaints(query_id, record_key)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS nhtsa_bulk_datasets (
                    dataset_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    dataset_name TEXT NOT NULL,
                    source_url TEXT,
                    source_file TEXT NOT NULL,
                    source_version TEXT,
                    checksum TEXT NOT NULL,
                    loaded_at TEXT NOT NULL,
                    row_count INTEGER NOT NULL DEFAULT 0,
                    UNIQUE(dataset_name, checksum)
                );

                CREATE TABLE IF NOT EXISTS nhtsa_bulk_rows (
                    dataset_id INTEGER NOT NULL,
                    source_row_number INTEGER NOT NULL,
                    row_hash TEXT NOT NULL,
                    PRIMARY KEY(dataset_id, source_row_number),
                    FOREIGN KEY(dataset_id) REFERENCES nhtsa_bulk_datasets(dataset_id)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS nhtsa_api_extra_fields (
                    query_id INTEGER NOT NULL,
                    record_type TEXT NOT NULL,
                    record_key TEXT NOT NULL,
                    field_name TEXT NOT NULL,
                    field_value TEXT,
                    PRIMARY KEY(query_id, record_type, record_key, field_name),
                    FOREIGN KEY(query_id) REFERENCES nhtsa_vehicle_queries(query_id)
                        ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS nhtsa_bulk_fields (
                    dataset_id INTEGER NOT NULL,
                    source_row_number INTEGER NOT NULL,
                    field_name TEXT NOT NULL,
                    field_value TEXT,
                    PRIMARY KEY(dataset_id, source_row_number, field_name),
                    FOREIGN KEY(dataset_id, source_row_number)
                        REFERENCES nhtsa_bulk_rows(dataset_id, source_row_number)
                        ON DELETE CASCADE
                );

                INSERT INTO nhtsa_schema_meta(key, value)
                VALUES ('schema_version', '2')
                ON CONFLICT(key) DO UPDATE SET value = excluded.value;
                """
            )

    def start_run(self, source: str = "nhtsa", mode: str = "incremental") -> str:
        run_id = uuid.uuid4().hex
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO nhtsa_ingestion_runs
                    (run_id, source, mode, started_at, status)
                VALUES (?, ?, ?, ?, 'running')
                """,
                (run_id, source, mode, self._now()),
            )
        return run_id

    def finish_run(self, run_id: str, status: str, **counts: int) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """
                UPDATE nhtsa_ingestion_runs
                SET completed_at = ?, status = ?, requested_count = ?,
                    successful_count = ?, failed_count = ?
                WHERE run_id = ?
                """,
                (
                    self._now(),
                    status,
                    int(counts.get("requested_count", 0)),
                    int(counts.get("successful_count", 0)),
                    int(counts.get("failed_count", 0)),
                    run_id,
                ),
            )

    def register_source(self, source_name: str, source_type: str, endpoint_or_url: str = "", **metadata: Any) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO nhtsa_source_catalog
                    (source_name, source_type, endpoint_or_url, source_version,
                     last_seen_at, checksum)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_name) DO UPDATE SET
                    source_type = excluded.source_type,
                    endpoint_or_url = excluded.endpoint_or_url,
                    source_version = excluded.source_version,
                    last_seen_at = excluded.last_seen_at,
                    checksum = excluded.checksum
                """,
                (
                    source_name,
                    source_type,
                    endpoint_or_url,
                    metadata.get("source_version"),
                    self._now(),
                    metadata.get("checksum"),
                ),
            )

    def store_vpic_result(
        self,
        run_id: str,
        vin: str,
        result: dict[str, Any],
        *,
        model_year_hint: Optional[int] = None,
        endpoint: str = "DecodeVINValuesBatch",
        response: Optional[dict[str, Any]] = None,
        request_payload: Optional[dict[str, Any]] = None,
        http_status: Optional[int] = 200,
        response_status: str = "success",
    ) -> int:
        response = response if response is not None else {"Results": [result]}
        response_hash = self._hash(result)
        vin = str(vin).strip().upper()
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO nhtsa_vpic_decodes
                    (run_id, vin, model_year_hint, endpoint, response_status,
                     http_status, error_code, error_text, message,
                     response_hash, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    vin,
                    model_year_hint,
                    endpoint,
                    response_status,
                    http_status,
                    result.get("ErrorCode"),
                    result.get("ErrorText"),
                    response.get("Message") or response.get("message"),
                    response_hash,
                    self._now(),
                ),
            )
            row = self.conn.execute(
                """
                SELECT decode_id FROM nhtsa_vpic_decodes
                WHERE vin = ? AND model_year_hint IS ? AND response_hash = ?
                """,
                (vin, model_year_hint, response_hash),
            ).fetchone()
            decode_id = int(row[0])
            flattened = dict(self._flatten_fields(result))
            existing_columns = {
                str(column[1])
                for column in self.conn.execute("PRAGMA table_info('nhtsa_vpic_values')")
            }
            quote_name = lambda name: '"' + str(name).replace('"', '""') + '"'
            for field_name in flattened:
                if field_name not in existing_columns:
                    self.conn.execute(
                        f"ALTER TABLE nhtsa_vpic_values ADD COLUMN {quote_name(field_name)} TEXT"
                    )
                    existing_columns.add(field_name)
            columns = ["decode_id", *flattened]
            values = [decode_id, *flattened.values()]
            updates = ", ".join(
                f"{quote_name(name)} = excluded.{quote_name(name)}" for name in flattened
            )
            self.conn.execute(
                f"""
                INSERT INTO nhtsa_vpic_values ({', '.join(quote_name(name) for name in columns)})
                VALUES ({', '.join('?' for _ in columns)})
                ON CONFLICT(decode_id) DO UPDATE SET {updates}
                """,
                values,
            )
            return decode_id

    def store_vpic_failure(
        self,
        run_id: str,
        vin: str,
        *,
        model_year_hint: Optional[int],
        status: str,
        error_text: str,
        http_status: Optional[int] = None,
        request_payload: Optional[dict[str, Any]] = None,
    ) -> int:
        return self.store_vpic_result(
            run_id,
            vin,
            {"VIN": str(vin).strip().upper(), "ErrorText": error_text},
            model_year_hint=model_year_hint,
            endpoint="DecodeVINValuesBatch",
            response={"Results": [], "Message": error_text},
            request_payload=request_payload,
            http_status=http_status,
            response_status=status,
        )

    def get_latest_vpic_result(
        self,
        vin: str,
        model_year_hint: Optional[int],
        *,
        max_age_days: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        record = self.get_latest_vpic_record(
            vin,
            model_year_hint,
            max_age_days=max_age_days,
        )
        return record.get("result") if record else None

    def get_latest_vpic_record(
        self,
        vin: str,
        model_year_hint: Optional[int],
        *,
        max_age_days: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        query = """
            SELECT decode_id, response_status, fetched_at
            FROM nhtsa_vpic_decodes
            WHERE vin = ? AND model_year_hint IS ?
              AND response_status = 'success'
            ORDER BY fetched_at DESC, decode_id DESC
            LIMIT 1
        """
        with self._lock:
            row = self.conn.execute(query, (str(vin).strip().upper(), model_year_hint)).fetchone()
        if row is None:
            return None
        if max_age_days is not None:
            try:
                fetched = datetime.fromisoformat(str(row[2]).replace("Z", "+00:00"))
                if datetime.now(timezone.utc) - fetched > timedelta(days=max_age_days):
                    return None
            except ValueError:
                return None
        with self._lock:
            value_row = self.conn.execute(
                "SELECT * FROM nhtsa_vpic_values WHERE decode_id = ?", (int(row[0]),)
            ).fetchone()
        result = {
            str(name): value_row[name]
            for name in value_row.keys()
            if name != "decode_id" and "[" not in str(name) and "." not in str(name)
        } if value_row is not None else {}
        return {
            "decode_id": int(row[0]),
            "result": result,
            "response_status": row[1],
            "fetched_at": row[2],
        }

    def get_latest_vehicle_query(
        self,
        query_type: str,
        query_key: str,
        *,
        max_age_days: Optional[int] = None,
    ) -> Optional[dict[str, Any]]:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT query_id, response_status, fetched_at
                FROM nhtsa_vehicle_queries
                WHERE query_type = ? AND query_key = ?
                  AND response_status IN ('success', 'empty', 'partial')
                ORDER BY fetched_at DESC, query_id DESC
                LIMIT 1
                """,
                (query_type, query_key),
            ).fetchone()
        if row is None:
            return None
        if max_age_days is not None:
            try:
                fetched = datetime.fromisoformat(str(row[2]).replace("Z", "+00:00"))
                if datetime.now(timezone.utc) - fetched > timedelta(days=max_age_days):
                    return None
            except ValueError:
                return None
        return {
            "query_id": int(row[0]),
            "response_status": row[1],
            "fetched_at": row[2],
        }

    def get_safety_details(self, query_id: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT vehicle_id, field_name, field_value
                FROM nhtsa_safety_rating_values
                WHERE query_id = ?
                ORDER BY vehicle_id
                """,
                (query_id,),
            ).fetchall()
        by_vehicle: dict[int, dict[str, Any]] = {}
        for row in rows:
            by_vehicle.setdefault(int(row[0]), {"VehicleId": int(row[0])})[str(row[1])] = row[2]
        return list(by_vehicle.values())

    def get_safety_variant_ids(self, query_id: int) -> list[int]:
        with self._lock:
            rows = self.conn.execute(
                "SELECT vehicle_id FROM nhtsa_safety_variants WHERE query_id = ? ORDER BY vehicle_id",
                (query_id,),
            ).fetchall()
        return [int(row[0]) for row in rows]

    def store_identity_resolution(self, values: dict[str, Any]) -> None:
        columns = [
            "run_id", "decode_id", "vin", "nhtsa_make", "nhtsa_model",
            "nhtsa_model_year", "listing_make", "listing_model", "listing_model_year",
            "resolved_make", "resolved_model", "resolved_model_year", "make_source",
            "model_source", "model_year_source", "confidence", "conflict_flag",
            "resolved_at",
        ]
        payload = [values.get(column) for column in columns]
        with self._lock, self.conn:
            self.conn.execute(
                f"INSERT OR IGNORE INTO nhtsa_vin_identity_resolution ({', '.join(columns)}) "
                f"VALUES ({', '.join('?' for _ in columns)})",
                payload,
            )

    def store_vehicle_query(
        self,
        run_id: str,
        query_type: str,
        query_key: str,
        response: dict[str, Any],
        *,
        make: str = "",
        model: str = "",
        model_year: Optional[int] = None,
        vehicle_id: Optional[int] = None,
        response_status: str = "success",
        http_status: Optional[int] = 200,
        error_text: Optional[str] = None,
    ) -> int:
        records = self._results(response)
        response_hash = self._hash(records)
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO nhtsa_vehicle_queries
                    (run_id, query_type, query_key, make, model, model_year,
                     vehicle_id, response_status, http_status, error_text,
                     record_count, response_hash, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id, query_type, query_key, make, model, model_year,
                    vehicle_id, response_status, http_status, error_text,
                    len(records), response_hash, self._now(),
                ),
            )
            row = self.conn.execute(
                """
                SELECT query_id FROM nhtsa_vehicle_queries
                WHERE query_type = ? AND query_key = ? AND response_hash = ?
                """,
                (query_type, query_key, response_hash),
            ).fetchone()
            return int(row[0])

    def _store_extra_fields(
        self,
        query_id: int,
        record_type: str,
        record_key: str,
        record: dict[str, Any],
        known_fields: set[str],
    ) -> None:
        rows = []
        for field_name, field_value in self._flatten_fields(record):
            root_name = field_name.split(".", 1)[0].split("[", 1)[0]
            if root_name not in known_fields:
                rows.append((query_id, record_type, record_key, field_name, field_value))
        if rows:
            self.conn.executemany(
                """
                INSERT OR REPLACE INTO nhtsa_api_extra_fields
                    (query_id, record_type, record_key, field_name, field_value)
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )

    @staticmethod
    def _results(response: Optional[dict[str, Any]]) -> list[dict[str, Any]]:
        if not response:
            return []
        results = response.get("Results", response.get("results", []))
        return results if isinstance(results, list) else []

    def store_safety_variants(self, query_id: int, variants: Iterable[dict[str, Any]]) -> None:
        with self._lock, self.conn:
            for variant in variants:
                vehicle_id = variant.get("VehicleId", variant.get("vehicleId"))
                if vehicle_id in (None, ""):
                    continue
                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO nhtsa_safety_variants
                        (query_id, vehicle_id, vehicle_description)
                    VALUES (?, ?, ?)
                    """,
                    (
                        query_id,
                        int(vehicle_id),
                        variant.get("VehicleDescription") or variant.get("VehicleDescriptionName"),
                    ),
                )
                self._store_extra_fields(
                    query_id, "safety_variant", str(vehicle_id), variant,
                    {"VehicleId", "vehicleId", "VehicleDescription", "VehicleDescriptionName"},
                )

    def store_safety_detail(self, query_id: int, vehicle_id: int, detail: dict[str, Any]) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO nhtsa_safety_details
                    (query_id, vehicle_id)
                VALUES (?, ?)
                """,
                (query_id, int(vehicle_id)),
            )
            for field_name, field_value in self._flatten_fields(detail):
                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO nhtsa_safety_rating_values
                        (query_id, vehicle_id, field_name, field_value)
                    VALUES (?, ?, ?, ?)
                    """,
                    (query_id, int(vehicle_id), str(field_name), field_value),
                )

    def store_recalls(self, query_id: int, records: Iterable[dict[str, Any]]) -> None:
        with self._lock, self.conn:
            for record in records:
                # Always fingerprint the complete source row. Campaign/action
                # numbers are retained as fields but are not unique enough to
                # be used as the row key when a response contains variants.
                record_key = self._hash(record)
                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO nhtsa_recalls
                        (query_id, record_key, manufacturer, nhtsa_campaign_number,
                         nhtsa_action_number, report_received_date, component,
                         model_year, make, model, park_it, park_outside,
                         over_the_air_update, summary, consequence, remedy,
                         notes)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        query_id, record_key, self._db_text(record.get("Manufacturer")),
                        self._db_text(record.get("NHTSACampaignNumber")),
                        self._db_text(record.get("NHTSAActionNumber")),
                        self._db_text(record.get("ReportReceivedDate")),
                        self._db_text(record.get("Component")),
                        self._db_integer(record.get("ModelYear")),
                        self._db_text(record.get("Make")), self._db_text(record.get("Model")),
                        self._db_text(record.get("parkIt")), self._db_text(record.get("parkOutSide")),
                        self._db_text(record.get("overTheAirUpdate")), self._db_text(record.get("Summary")),
                        self._db_text(record.get("Consequence")), self._db_text(record.get("Remedy")),
                        self._db_text(record.get("Notes")),
                    ),
                )
                self._store_extra_fields(
                    query_id, "recall", record_key, record,
                    {"Manufacturer", "NHTSACampaignNumber", "NHTSAActionNumber",
                     "ReportReceivedDate", "Component", "ModelYear", "Make", "Model",
                     "parkIt", "parkOutSide", "overTheAirUpdate", "Summary",
                     "Consequence", "Remedy", "Notes"},
                )

    def store_complaints(self, query_id: int, records: Iterable[dict[str, Any]]) -> None:
        with self._lock, self.conn:
            for record in records:
                # ODI numbers identify a case, not necessarily a unique source
                # row in every export; retain each distinct response row.
                record_key = self._hash(record)
                self.conn.execute(
                    """
                    INSERT OR REPLACE INTO nhtsa_complaints
                        (query_id, record_key, odi_number, manufacturer, crash,
                         fire, number_of_injuries, number_of_deaths,
                         date_of_incident, date_complaint_filed, vin, components,
                         summary)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        query_id, record_key, self._db_text(record.get("odiNumber")),
                        self._db_text(record.get("manufacturer")),
                        self._db_text(record.get("crash")), self._db_text(record.get("fire")),
                        self._db_integer(record.get("numberOfInjuries")),
                        self._db_integer(record.get("numberOfDeaths")),
                        self._db_text(record.get("dateOfIncident")),
                        self._db_text(record.get("dateComplaintFiled")),
                        self._db_text(record.get("vin")), self._db_text(record.get("components")),
                        self._db_text(record.get("summary")),
                    ),
                )
                products = record.get("products") if isinstance(record.get("products"), list) else []
                for product_index, product in enumerate(products):
                    if not isinstance(product, dict):
                        continue
                    self.conn.execute(
                        """
                        INSERT OR REPLACE INTO nhtsa_complaint_products
                            (query_id, record_key, product_index, product_type,
                             product_year, product_make, product_model, manufacturer)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (query_id, record_key, product_index, self._db_text(product.get("type")),
                         self._db_integer(product.get("productYear")),
                         self._db_text(product.get("productMake")),
                         self._db_text(product.get("productModel")),
                         self._db_text(product.get("manufacturer"))),
                    )
                self._store_extra_fields(
                    query_id, "complaint", record_key, record,
                    {"odiNumber", "manufacturer", "crash", "fire", "numberOfInjuries",
                     "numberOfDeaths", "dateOfIncident", "dateComplaintFiled", "vin",
                     "components", "summary", "products"},
                )

    def get_recall_records(self, query_id: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT manufacturer, nhtsa_campaign_number, nhtsa_action_number,
                       report_received_date, component, model_year, make, model,
                       park_it, park_outside, over_the_air_update, summary,
                       consequence, remedy, notes
                FROM nhtsa_recalls WHERE query_id = ?
                """,
                (query_id,),
            ).fetchall()
        names = ["Manufacturer", "NHTSACampaignNumber", "NHTSAActionNumber",
                 "ReportReceivedDate", "Component", "ModelYear", "Make", "Model",
                 "parkIt", "parkOutSide", "overTheAirUpdate", "Summary",
                 "Consequence", "Remedy", "Notes"]
        return [dict(zip(names, row)) for row in rows]

    def get_complaint_records(self, query_id: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT odi_number, manufacturer, crash, fire, number_of_injuries,
                       number_of_deaths, date_of_incident, date_complaint_filed,
                       vin, components, summary
                FROM nhtsa_complaints WHERE query_id = ?
                """,
                (query_id,),
            ).fetchall()
        names = ["odiNumber", "manufacturer", "crash", "fire", "numberOfInjuries",
                 "numberOfDeaths", "dateOfIncident", "dateComplaintFiled", "vin",
                 "components", "summary"]
        return [dict(zip(names, row)) for row in rows]

    def ingest_bulk_file(
        self,
        path: str | os.PathLike[str],
        dataset_name: str,
        *,
        source_url: str = "",
        source_version: str = "",
    ) -> int:
        """Store source rows as normalized fields without retaining raw blobs."""
        source_path = Path(path)
        digest = hashlib.sha256()
        with source_path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        checksum = digest.hexdigest()

        def iter_member(name: str, handle: Any):
            suffix = Path(name).suffix.lower()
            if suffix == ".csv":
                text = (line.decode("utf-8-sig", errors="replace") for line in handle)
                yield from (dict(row) for row in csv.DictReader(text))
            elif suffix == ".json":
                payload = json.loads(handle.read().decode("utf-8-sig", errors="replace"))
                if isinstance(payload, list):
                    yield from (item if isinstance(item, dict) else {"value": item} for item in payload)
                elif isinstance(payload, dict):
                    yield payload
            else:
                for line in handle:
                    yield {"raw_line": line.decode("utf-8", errors="replace").rstrip("\r\n")}

        def iter_rows():
            if source_path.suffix.lower() == ".zip":
                with zipfile.ZipFile(source_path) as archive:
                    for member in archive.infolist():
                        if not member.is_dir():
                            with archive.open(member) as handle:
                                yield from iter_member(member.filename, handle)
            else:
                with source_path.open("rb") as handle:
                    yield from iter_member(source_path.name, handle)

        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO nhtsa_bulk_datasets
                    (dataset_name, source_url, source_file, source_version,
                     checksum, loaded_at, row_count)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (dataset_name, source_url, str(source_path), source_version,
                 checksum, self._now(), 0),
            )
            row = self.conn.execute(
                "SELECT dataset_id FROM nhtsa_bulk_datasets WHERE dataset_name = ? AND checksum = ?",
                (dataset_name, checksum),
            ).fetchone()
            dataset_id = int(row[0])
        row_count = 0
        row_batch: list[tuple[int, int, str]] = []
        field_batch: list[tuple[int, int, str, Optional[str]]] = []
        for record in iter_rows():
            row_count += 1
            row_batch.append((dataset_id, row_count, self._hash(record)))
            field_batch.extend(
                (dataset_id, row_count, field_name, field_value)
                for field_name, field_value in self._flatten_fields(record)
            )
            if len(row_batch) >= 2_000:
                with self._lock, self.conn:
                    self.conn.executemany(
                        """
                        INSERT OR IGNORE INTO nhtsa_bulk_rows
                            (dataset_id, source_row_number, row_hash)
                        VALUES (?, ?, ?)
                        """,
                        row_batch,
                    )
                    self.conn.executemany(
                        """
                        INSERT OR REPLACE INTO nhtsa_bulk_fields
                            (dataset_id, source_row_number, field_name, field_value)
                        VALUES (?, ?, ?, ?)
                        """,
                        field_batch,
                    )
                row_batch.clear()
                field_batch.clear()
        if row_batch:
            with self._lock, self.conn:
                self.conn.executemany(
                    """
                    INSERT OR IGNORE INTO nhtsa_bulk_rows
                        (dataset_id, source_row_number, row_hash)
                    VALUES (?, ?, ?)
                    """,
                    row_batch,
                )
                self.conn.executemany(
                    """
                    INSERT OR REPLACE INTO nhtsa_bulk_fields
                        (dataset_id, source_row_number, field_name, field_value)
                    VALUES (?, ?, ?, ?)
                    """,
                    field_batch,
                )
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE nhtsa_bulk_datasets SET row_count = ? WHERE dataset_id = ?",
                (row_count, dataset_id),
            )
        return row_count

    def close(self) -> None:
        with self._lock:
            if self.conn is not None:
                self.conn.close()
                self.conn = None


class CarDatabase:
    def __init__(self, db_path, thread_safe=False):
        self.db_path = db_path
        self.conn = None
        self._thread_safe = thread_safe
        self._lock = threading.Lock() if thread_safe else None
        self._local = threading.local() if thread_safe else None
        self._init_db()

    def _get_connection(self):
        if self._thread_safe:
            conn = getattr(self._local, 'conn', None)
            if conn is None:
                conn = sqlite3.connect(self.db_path, timeout=30)
                self._local.conn = conn
            return conn
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path, timeout=30)
        return self.conn

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # 1. Main Listings table (Snapshots over time)
            # Using (vin, loaddate) as composite primary key as requested.
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS listings
                           (
                               vin
                               TEXT,
                               loaddate
                               DATE,
                               year
                               INTEGER,
                               title
                               TEXT,
                               details
                               TEXT,
                               price
                               INTEGER,
                               mileage
                               INTEGER,
                               date
                               DATE,
                               location
                               TEXT,
                               locationCode
                               TEXT,
                               countryCode
                               TEXT,
                               pendingSale
                               BOOLEAN,
                               currentBid
                               REAL,
                               bids
                               INTEGER,
                               distance
                               REAL,
                               priceRecentChange
                               BOOLEAN,
                               sellerType
                               TEXT,
                               vehicleTitle
                               TEXT,
                               listingType
                               TEXT,
                               vehicleTitleDesc
                               TEXT,
                               sourceName
                               TEXT,
                               img
                               TEXT,
                               PRIMARY
                               KEY
                           (
                               vin,
                               loaddate
                           )
                               )
                           ''')

            # 2. Price History table (Normalized)
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS price_history
                           (
                               id
                               INTEGER
                               PRIMARY
                               KEY
                               AUTOINCREMENT,
                               vin
                               TEXT,
                               history_date
                               DATE,
                               mileage
                               INTEGER,
                               price
                               INTEGER,
                               trend
                               TEXT,
                               UNIQUE
                           (
                               vin,
                               history_date,
                               price
                           ),
                               FOREIGN KEY
                           (
                               vin
                           ) REFERENCES listings
                           (
                               vin
                           )
                               )
                           ''')

            # 3. Listing History table (Normalized)
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS listing_history
                           (
                               id
                               INTEGER
                               PRIMARY
                               KEY
                               AUTOINCREMENT,
                               vin
                               TEXT,
                               history_date
                               DATE,
                               mileage
                               REAL,
                               price
                               INTEGER,
                               UNIQUE
                           (
                               vin,
                               history_date,
                               price,
                               mileage
                           ),
                               FOREIGN KEY
                           (
                               vin
                           ) REFERENCES listings
                           (
                               vin
                           )
                               )
                           ''')

            # 4. NHTSA Enrichment table
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS nhtsa_enrichment (
                                                                           vin TEXT PRIMARY KEY,
                                                                           nhtsa_ABS TEXT,
                                                                           nhtsa_ActiveSafetySysNote TEXT,
                                                                           nhtsa_AdaptiveCruiseControl TEXT,
                                                                           nhtsa_AdaptiveDrivingBeam TEXT,
                                                                           nhtsa_AdaptiveHeadlights TEXT,
                                                                           nhtsa_AdditionalErrorText TEXT,
                                                                           nhtsa_AirBagLocCurtain TEXT,
                                                                           nhtsa_AirBagLocFront TEXT,
                                                                           nhtsa_AirBagLocKnee TEXT,
                                                                           nhtsa_AirBagLocSeatCushion TEXT,
                                                                           nhtsa_AirBagLocSide TEXT,
                                                                           nhtsa_AutoReverseSystem TEXT,
                                                                           nhtsa_AutomaticPedestrianAlertingSound TEXT,
                                                                           nhtsa_AxleConfiguration TEXT,
                                                                           nhtsa_Axles TEXT,
                                                                           nhtsa_BasePrice TEXT,
                                                                           nhtsa_BedLengthIN TEXT,
                                                                           nhtsa_BedType TEXT,
                                                                           nhtsa_BlindSpotIntervention TEXT,
                                                                           nhtsa_BlindSpotMon TEXT,
                                                                           nhtsa_BodyCabType TEXT,
                                                                           nhtsa_BodyClass TEXT,
                                                                           nhtsa_BrakeSystemDesc TEXT,
                                                                           nhtsa_BrakeSystemType TEXT,
                                                                           nhtsa_ChargerLevel TEXT,
                                                                           nhtsa_ChargerPowerKW TEXT,
                                                                           nhtsa_CombinedBrakingSystem TEXT,
                                                                           nhtsa_CoolingType TEXT,
                                                                           nhtsa_CurbWeightLB TEXT,
                                                                           nhtsa_DaytimeRunningLight TEXT,
                                                                           nhtsa_DestinationMarket TEXT,
                                                                           nhtsa_DisplacementCC TEXT,
                                                                           nhtsa_DisplacementCI TEXT,
                                                                           nhtsa_DisplacementL TEXT,
                                                                           nhtsa_Doors TEXT,
                                                                           nhtsa_DriveType TEXT,
                                                                           nhtsa_DriverAssist TEXT,
                                                                           nhtsa_DynamicBrakeSupport TEXT,
                                                                           nhtsa_EDR TEXT,
                                                                           nhtsa_ESC TEXT,
                                                                           nhtsa_EVDriveUnit TEXT,
                                                                           nhtsa_ElectrificationLevel TEXT,
                                                                           nhtsa_EngineConfiguration TEXT,
                                                                           nhtsa_EngineCycles TEXT,
                                                                           nhtsa_EngineCylinders TEXT,
                                                                           nhtsa_EngineHP TEXT,
                                                                           nhtsa_EngineHP_to TEXT,
                                                                           nhtsa_EngineKW TEXT,
                                                                           nhtsa_EngineManufacturer TEXT,
                                                                           nhtsa_EngineModel TEXT,
                                                                           nhtsa_EntertainmentSystem TEXT,
                                                                           nhtsa_ForwardCollisionWarning TEXT,
                                                                           nhtsa_FuelInjectionType TEXT,
                                                                           nhtsa_FuelTankMaterial TEXT,
                                                                           nhtsa_FuelTankType TEXT,
                                                                           nhtsa_FuelTypePrimary TEXT,
                                                                           nhtsa_FuelTypeSecondary TEXT,
                                                                           nhtsa_KeylessIgnition TEXT,
                                                                           nhtsa_LaneCenteringAssistance TEXT,
                                                                           nhtsa_LaneDepartureWarning TEXT,
                                                                           nhtsa_LaneKeepSystem TEXT,
                                                                           nhtsa_LowerBeamHeadlampLightSource TEXT,
                                                                           nhtsa_Make TEXT,
                                                                           nhtsa_MakeID TEXT,
                                                                           nhtsa_Manufacturer TEXT,
                                                                           nhtsa_ManufacturerId TEXT,
                                                                           nhtsa_Model TEXT,
                                                                           nhtsa_ModelID TEXT,
                                                                           nhtsa_ModelYear TEXT,
                                                                           nhtsa_OtherEngineInfo TEXT,
                                                                           nhtsa_ParkAssist TEXT,
                                                                           nhtsa_PedestrianAutomaticEmergencyBraking TEXT,
                                                                           nhtsa_RearAutomaticEmergencyBraking TEXT,
                                                                           nhtsa_RearCrossTrafficAlert TEXT,
                                                                           nhtsa_RearVisibilitySystem TEXT,
                                                                           nhtsa_SAEAutomationLevel TEXT,
                                                                           nhtsa_SAEAutomationLevel_to TEXT,
                                                                           nhtsa_SeatRows TEXT,
                                                                           nhtsa_Seats TEXT,
                                                                           nhtsa_SemiautomaticHeadlampBeamSwitching TEXT,
                                                                           nhtsa_TPMS TEXT,
                                                                           nhtsa_TopSpeedMPH TEXT,
                                                                           nhtsa_TrackWidth TEXT,
                                                                           nhtsa_TractionControl TEXT,
                                                                           nhtsa_TransmissionSpeeds TEXT,
                                                                           nhtsa_TransmissionStyle TEXT,
                                                                           nhtsa_Trim TEXT,
                                                                           nhtsa_Trim2 TEXT,
                                                                           nhtsa_WheelSizeFront TEXT,
                                                                           nhtsa_WheelSizeRear TEXT,
                                                                           nhtsa_Windows TEXT,
                                                                           nhtsa_VehicleType TEXT,
                                                                           nhtsa_WheelBaseLong TEXT,
                                                                           nhtsa_WheelBaseShort TEXT,
                                                                           nhtsa_WheelBaseType TEXT,
                                                                           nhtsa_safety_ratings_count INTEGER,
                                                                           nhtsa_overall_rating TEXT,
                                                                           nhtsa_front_crash_rating TEXT,
                                                                           nhtsa_rollover_rating TEXT,
                                                                           nhtsa_side_crash_rating TEXT,
                                                                           nhtsa_total_recalls INTEGER,
                                                                           nhtsa_recall_components TEXT,
                                                                           nhtsa_latest_recall_date DATE,
                                                                           nhtsa_total_complaints INTEGER,
                                                                           nhtsa_complaint_injuries INTEGER,
                                                                           nhtsa_complaint_deaths INTEGER,
                                                                           nhtsa_complaint_crash_related INTEGER,
                                                                           nhtsa_complaint_fire_related INTEGER,
                                                                           nhtsa_common_complaint_areas TEXT,
                                                                           FOREIGN KEY (vin) REFERENCES listings (vin)
                               )
                           ''')

            conn.commit()

        # Additive metadata keeps the legacy wide table usable while exposing
        # refresh state and identity provenance to the new NHTSA pipeline.
        self._ensure_nhtsa_columns()

    def _ensure_nhtsa_columns(self) -> None:
        additions = {
            "nhtsa_decode_status": "TEXT",
            "nhtsa_decode_error": "TEXT",
            "nhtsa_decode_fetched_at": "TEXT",
            "nhtsa_identity_source": "TEXT",
            "nhtsa_identity_confidence": "TEXT",
            "nhtsa_identity_conflict": "INTEGER",
            "nhtsa_source_run_id": "TEXT",
            "nhtsa_last_updated_at": "TEXT",
            "nhtsa_safety_status": "TEXT",
            "nhtsa_safety_vehicle_ids": "TEXT",
            "nhtsa_recalls_status": "TEXT",
            "nhtsa_complaints_status": "TEXT",
        }
        with self._get_connection() as conn:
            existing = {
                row[1]
                for row in conn.execute("PRAGMA table_info(nhtsa_enrichment)").fetchall()
            }
            for column, data_type in additions.items():
                if column not in existing:
                    conn.execute(f'ALTER TABLE nhtsa_enrichment ADD COLUMN "{column}" {data_type}')
            conn.commit()

    def insert_rows(self, rows, vin_cache=None):
        if not rows:
            return 0

        lock = self._lock if self._thread_safe else None
        if lock:
            lock.acquire()
        try:
            return self._insert_rows_impl(rows, vin_cache=vin_cache)
        finally:
            if lock:
                lock.release()

    def _insert_rows_impl(self, rows, vin_cache=None):
        inserted_count = 0
        today = date.today().isoformat()

        with self._get_connection() as conn:
            cursor = conn.cursor()

            for row in rows:
                vin = row.get('vin')
                loaddate = row.get('loaddate')
                if not vin or not loaddate:
                    continue

                # Always process normalized history tables first. UNIQUE constraints
                # + INSERT OR IGNORE prevent duplicates automatically.
                self._insert_price_history(cursor, vin, row.get('priceHistory'))
                self._insert_listing_history(cursor, vin, row.get('listingHistory'))

                # Skip listing snapshot if we have the VIN cached and its price/mileage hasn't changed.
                if vin_cache is not None and hasattr(vin_cache, "should_insert"):
                    if not vin_cache.should_insert(vin, row.get('price'), row.get('mileage')):
                        continue
                elif vin_cache is not None and hasattr(vin_cache, "contains") and vin_cache.contains(vin, today):
                    continue

                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO listings (
                            vin, loaddate, year, title, details, price, mileage, date,
                            location, locationCode, countryCode, pendingSale,
                            currentBid, bids, distance, priceRecentChange,
                            sellerType, vehicleTitle, listingType, vehicleTitleDesc,
                            sourceName, img
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        vin, loaddate, row.get('year'), row.get('title'), row.get('details'),
                        row.get('price'), row.get('mileage'), row.get('date'),
                        row.get('location'), row.get('locationCode'), row.get('countryCode'),
                        row.get('pendingSale'), row.get('currentBid'), row.get('bids'),
                        row.get('distance'), row.get('priceRecentChange'),
                        row.get('sellerType'), row.get('vehicleTitle'), row.get('listingType'),
                        row.get('vehicleTitleDesc'), row.get('sourceName'), row.get('img')
                    ))
                    if cursor.rowcount > 0:
                        inserted_count += 1
                except sqlite3.Error as e:
                    logging.error(f"Failed to insert listing for VIN {vin}, loaddate {loaddate}: {e}")

            conn.commit()

        return inserted_count

    def _insert_price_history(self, cursor, vin: str, history: Optional[List]):
        if not history or not isinstance(history, list):
            return
        for h in history:
            try:
                cursor.execute('''
                               INSERT
                               OR IGNORE INTO price_history (vin, history_date, mileage, price, trend)
                    VALUES (?, ?, ?, ?, ?)
                               ''', (vin, h.get('date'), h.get('mileage'), h.get('price'), h.get('trend')))
            except sqlite3.Error as e:
                logging.warning(f"Failed to insert priceHistory for VIN {vin}: {e}")

    def _insert_listing_history(self, cursor, vin: str, history: Optional[List]):
        if not history or not isinstance(history, list):
            return
        for h in history:
            try:
                cursor.execute('''
                               INSERT
                               OR IGNORE INTO listing_history (vin, history_date, mileage, price)
                    VALUES (?, ?, ?, ?)
                               ''', (vin, h.get('date'), h.get('mileage'), h.get('price')))
            except sqlite3.Error as e:
                logging.warning(f"Failed to insert listingHistory for VIN {vin}: {e}")

    def get_seen_vins(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Get the latest price and mileage for each VIN
            cursor.execute('SELECT vin, price, mileage, MAX(loaddate) FROM listings GROUP BY vin')
            return {row[0]: {'price': row[1], 'mileage': row[2]} for row in cursor.fetchall()}

    def get_vins_for_enrichment(self, include_listing_context: bool = False):
        """Return every distinct VIN and its latest listing fallback context.

        The historical implementation returned only VINs absent from the wide
        enrichment table.  That made old records permanently stale.  Callers can
        still receive the legacy list[str] shape, while the NHTSA pipeline asks
        for the context dictionaries needed for field-level fallback.
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                """
                SELECT l.*
                FROM listings AS l
                INNER JOIN (
                    SELECT vin, MAX(loaddate) AS latest_loaddate
                    FROM listings
                    WHERE vin IS NOT NULL AND TRIM(vin) <> ''
                    GROUP BY vin
                ) AS latest
                    ON latest.vin = l.vin AND latest.latest_loaddate = l.loaddate
                ORDER BY l.vin
                """
            )
            column_names = [description[0] for description in cursor.description]
            rows = [dict(zip(column_names, row)) for row in cursor.fetchall()]
            if include_listing_context:
                return [
                    {
                        **row,
                        "vin": row.get("vin"),
                        "listing_model_year": row.get("modelYear")
                        or row.get("model_year")
                        or row.get("year"),
                        "listing_make": row.get("make") or row.get("Make"),
                        "listing_model": row.get("model") or row.get("Model"),
                        "title": row.get("title"),
                        "vehicle_title": row.get("vehicleTitle"),
                        "details": row.get("details"),
                    }
                    for row in rows
                ]
            return [row.get("vin") for row in rows]

    def insert_nhtsa_enrichment(self, vin, enrichment_data):
        """Insert a single record into nhtsa_enrichment"""
        self.insert_nhtsa_enrichment_batch({vin: enrichment_data})

    def insert_nhtsa_enrichment_batch(self, enrichment_dict):
        """Upsert mapped compatibility records without allowing unknown columns."""
        if not enrichment_dict:
            return

        with self._get_connection() as conn:
            cursor = conn.cursor()
            table_columns = {
                row[1]
                for row in cursor.execute("PRAGMA table_info(nhtsa_enrichment)").fetchall()
            }

            for vin, enrichment_data in enrichment_dict.items():
                if not enrichment_data:
                    continue

                data = {
                    key: value
                    for key, value in enrichment_data.items()
                    if key in table_columns
                }
                data['vin'] = vin
                if not data:
                    continue

                quote = lambda name: '"' + str(name).replace('"', '""') + '"'
                columns = ', '.join(quote(key) for key in data)
                placeholders = ', '.join(['?' for _ in data])
                values = list(data.values())
                updates = ', '.join(
                    f'{quote(column)} = excluded.{quote(column)}'
                    for column in data
                    if column != 'vin'
                )

                try:
                    if updates:
                        cursor.execute(
                            f'''
                            INSERT INTO nhtsa_enrichment ({columns})
                            VALUES ({placeholders})
                            ON CONFLICT(vin) DO UPDATE SET {updates}
                            ''',
                            values,
                        )
                except sqlite3.Error as e:
                    logging.error(f"Failed to insert NHTSA enrichment for VIN {vin}: {e}")

            conn.commit()

    def close(self):
        """Close the database connection(s)"""
        if self._thread_safe and self._local:
            conn = getattr(self._local, 'conn', None)
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    try:
                        logging.error(f"Error closing thread-local database connection: {e}")
                    except (OSError, ValueError):
                        pass
                self._local.conn = None
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
            except Exception as e:
                try:
                    logging.error(f"Error closing database connection: {e}")
                except (OSError, ValueError):
                    pass
            else:
                # A different test/process may have left a closed log-file
                # handler on the root logger; closing the DB must not fail
                # because diagnostic logging cannot reopen that stale path.
                try:
                    logging.info("Database connection closed")
                except (OSError, ValueError):
                    pass
            self.conn = None


class YouTubeCommentsDatabase:
    FETCH_STATUS_PENDING = "pending"
    FETCH_STATUS_COMPLETE = "complete"
    FETCH_STATUS_ZERO_COMMENTS = "zero_comments"
    FETCH_STATUS_COMMENTS_DISABLED = "comments_disabled"
    FETCH_STATUS_QUOTA_EXHAUSTED = "quota_exhausted"
    FETCH_STATUS_API_ERROR = "api_error"
    RETRYABLE_STATUSES = {FETCH_STATUS_PENDING, FETCH_STATUS_QUOTA_EXHAUSTED, FETCH_STATUS_API_ERROR}

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self._init_db()

    def _get_connection(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path, timeout=30)
        return self.conn

    def _ensure_columns(self, table_name: str, required_columns: dict[str, str]) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            existing_columns = {
                row[1]
                for row in cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
            }
            missing_columns = [
                (column_name, column_type)
                for column_name, column_type in required_columns.items()
                if column_name not in existing_columns
            ]
            for column_name, column_type in missing_columns:
                cursor.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
                )
            if missing_columns:
                conn.commit()

    def _ensure_unique_index(self, table_name: str, column_name: str, index_name: str) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            existing_indexes = {
                row[1]
                for row in cursor.execute(f"PRAGMA index_list({table_name})").fetchall()
            }
            if index_name in existing_indexes:
                return

            duplicate_row = cursor.execute(
                f'''
                SELECT {column_name}, COUNT(*)
                FROM {table_name}
                GROUP BY {column_name}
                HAVING COUNT(*) > 1 OR {column_name} IS NULL
                LIMIT 1
                '''
            ).fetchone()
            if duplicate_row is not None:
                raise sqlite3.IntegrityError(
                    f"Cannot create unique index {index_name} on {table_name}({column_name}) "
                    "because duplicate or NULL values already exist."
                )

            cursor.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name}({column_name})"
            )
            conn.commit()

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS youtube_comments_sentiment
                           (
                               video_id
                               TEXT,
                               playlist_id
                               TEXT, -- Added playlist_id column
                               video_title
                               TEXT,
                               source
                               TEXT,
                               text
                               TEXT,
                               extracted_at
                               TEXT,
                               comment_id
                               TEXT
                               PRIMARY
                               KEY,
                               author
                               TEXT,
                               like_count
                               INTEGER,
                               reply_count
                               INTEGER,
                               published_at
                               TEXT,
                               updated_at
                               TEXT
                           )
                           ''')
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS youtube_playlist_fetch_state
                (
                    playlist_id TEXT PRIMARY KEY,
                    last_discovered_at TEXT,
                    last_status TEXT,
                    last_error TEXT
                )
                '''
            )
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS youtube_video_fetch_state
                (
                    video_id TEXT PRIMARY KEY,
                    playlist_id TEXT,
                    video_title TEXT,
                    discovered_at TEXT,
                    last_attempted_at TEXT,
                    last_succeeded_at TEXT,
                    last_status TEXT,
                    last_error TEXT,
                    comments_seen_count INTEGER DEFAULT 0,
                    next_eligible_at TEXT
                )
                '''
            )
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS youtube_comments_scored
                (
                    video_id TEXT,
                    playlist_id TEXT,
                    video_title TEXT,
                    source TEXT,
                    text TEXT,
                    extracted_at TEXT,
                    comment_id TEXT PRIMARY KEY,
                    author TEXT,
                    like_count REAL,
                    reply_count INTEGER,
                    published_at TEXT,
                    updated_at TEXT,
                    Vehicle_Entity TEXT,
                    original_text TEXT,
                    reliability_sentiment REAL,
                    reliability_mentioned INTEGER,
                    reliability_confidence REAL,
                    value_sentiment REAL,
                    value_mentioned INTEGER,
                    value_confidence REAL,
                    performance_sentiment REAL,
                    performance_mentioned INTEGER,
                    performance_confidence REAL,
                    comfort_sentiment REAL,
                    comfort_mentioned INTEGER,
                    comfort_confidence REAL,
                    consensus_weight REAL,
                    word_count INTEGER,
                    depth_weight REAL,
                    comment_weight REAL,
                    Weighted_Reliability_Score REAL,
                    Weighted_Value_Score REAL,
                    Weighted_Performance_Score REAL,
                    Weighted_Comfort_Score REAL,
                    sentiment_make TEXT,
                    make_attribution_source TEXT,
                    make_attribution_version TEXT,
                    overall_sentiment REAL,
                    overall_confidence REAL,
                    sentiment_status TEXT,
                    processed_at TEXT,
                    model_name TEXT,
                    model_revision TEXT,
                    aspect_version TEXT
                )
                '''
            )
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS make_sentiment_index
                (
                    sentiment_make TEXT PRIMARY KEY,
                    sentiment_overall_score REAL,
                    sentiment_reliability_score REAL,
                    sentiment_value_score REAL,
                    sentiment_performance_score REAL,
                    sentiment_comfort_score REAL,
                    sentiment_comment_count INTEGER,
                    sentiment_video_count INTEGER,
                    sentiment_aspect_coverage REAL,
                    sentiment_latest_comment_at TEXT,
                    sentiment_model_versions TEXT,
                    updated_at TEXT
                )
                '''
            )
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS make_sentiment_monthly
                (
                    sentiment_make TEXT,
                    sentiment_month TEXT,
                    sentiment_overall_score REAL,
                    sentiment_reliability_score REAL,
                    sentiment_value_score REAL,
                    sentiment_performance_score REAL,
                    sentiment_comfort_score REAL,
                    sentiment_comment_count INTEGER,
                    sentiment_video_count INTEGER,
                    sentiment_aspect_coverage REAL,
                    sentiment_latest_comment_at TEXT,
                    PRIMARY KEY (sentiment_make, sentiment_month)
                )
                '''
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_youtube_video_fetch_state_status ON youtube_video_fetch_state(last_status)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_youtube_video_fetch_state_next_eligible ON youtube_video_fetch_state(next_eligible_at)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_youtube_video_fetch_state_playlist ON youtube_video_fetch_state(playlist_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_youtube_comments_sentiment_video_id ON youtube_comments_sentiment(video_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_make_sentiment_monthly_lookup "
                "ON make_sentiment_monthly(sentiment_make, sentiment_month)"
            )
            conn.commit()
        self._ensure_columns(
            "youtube_comments_sentiment",
            {
                "playlist_id": "TEXT",
            },
        )
        self._ensure_columns(
            "youtube_comments_scored",
            {
                "sentiment_make": "TEXT",
                "make_attribution_source": "TEXT",
                "make_attribution_version": "TEXT",
                "overall_sentiment": "REAL",
                "overall_confidence": "REAL",
                "sentiment_status": "TEXT",
                "processed_at": "TEXT",
                "model_name": "TEXT",
                "model_revision": "TEXT",
                "aspect_version": "TEXT",
            },
        )
        with self._get_connection() as conn:
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_youtube_comments_scored_make "
                "ON youtube_comments_scored(sentiment_make)"
            )
            conn.commit()
        self._ensure_unique_index(
            "youtube_comments_scored",
            "comment_id",
            "idx_youtube_comments_scored_comment_id_unique",
        )

    @staticmethod
    def _utcnow_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    @staticmethod
    def _coerce_int(value: object, default: int = 0) -> int:
        try:
            return int(value) if value is not None else default
        except (TypeError, ValueError):
            return default

    @classmethod
    def _next_eligible_timestamp(
        cls,
        status: str,
        refresh_days: int = 30,
        backoff_hours: int = 6,
    ) -> str:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        if status in {cls.FETCH_STATUS_COMPLETE, cls.FETCH_STATUS_ZERO_COMMENTS, cls.FETCH_STATUS_COMMENTS_DISABLED}:
            return (now + timedelta(days=max(refresh_days, 0))).isoformat()
        if status in {cls.FETCH_STATUS_QUOTA_EXHAUSTED, cls.FETCH_STATUS_API_ERROR}:
            return (now + timedelta(hours=max(backoff_hours, 0))).isoformat()
        return now.isoformat()

    def insert_sentiment_data(self, df: pd.DataFrame, table_name: str = 'youtube_comments_sentiment'):
        """
        Inserts a pandas DataFrame into the specified SQL table,
        only adding new comments and avoiding duplicates based on comment_id.
        """
        if df.empty:
            logging.info(f"No data to insert into {table_name}.")
            return 0

        with self._get_connection() as conn:
            # Format date columns to MM-DD-YYYY for TEXT type
            for col in ['extracted_at', 'published_at', 'updated_at']:
                if col in df.columns and not df[col].empty:
                    df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.strftime('%m-%d-%Y')

            try:
                # Fetch existing comment_ids
                existing_comment_ids = pd.read_sql(f"SELECT comment_id FROM {table_name}", conn)['comment_id'].tolist()

                # Filter out comments that already exist in the database
                new_comments_df = df[~df['comment_id'].isin(existing_comment_ids)]

                # DROP DUPLICATES IN INCOMING BATCH
                new_comments_df = new_comments_df.drop_duplicates(subset=['comment_id'])

                if new_comments_df.empty:
                    logging.info(f"No new comments to insert into {table_name}.")
                    return 0

                # Insert only new comments
                new_comments_df.to_sql(table_name, conn, if_exists='append', index=False)
                logging.info(f"Successfully inserted {len(new_comments_df)} new rows into {table_name}.")
                return len(new_comments_df)
            except pd.io.sql.DatabaseError as e:
                # This error can occur if the table does not exist yet,
                # which is handled by _init_db, but might happen if table was dropped externally.
                # In this case, just append all data.
                logging.warning(f"Table {table_name} might not exist or other DB error. Attempting full insert: {e}")

                # CLEAN FULL DATAFRAME BEFORE FALLBACK INSERT
                clean_df = df.drop_duplicates(subset=['comment_id'])
                clean_df.to_sql(table_name, conn, if_exists='append', index=False)

                logging.info(f"Successfully inserted {len(clean_df)} rows into {table_name} (full insert).")
                return len(clean_df)
            except Exception as e:
                logging.error(f"Failed to insert sentiment data into {table_name}: {e}")
                return 0

    def ensure_video_fetch_state(
        self,
        video_id: str,
        playlist_id: Optional[str] = None,
        video_title: Optional[str] = None,
        discovered_at: Optional[str] = None,
    ) -> None:
        discovered_at = discovered_at or self._utcnow_iso()
        with self._get_connection() as conn:
            conn.execute(
                '''
                INSERT INTO youtube_video_fetch_state (
                    video_id,
                    playlist_id,
                    video_title,
                    discovered_at,
                    last_status,
                    comments_seen_count,
                    next_eligible_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    playlist_id = COALESCE(excluded.playlist_id, youtube_video_fetch_state.playlist_id),
                    video_title = COALESCE(excluded.video_title, youtube_video_fetch_state.video_title),
                    discovered_at = COALESCE(youtube_video_fetch_state.discovered_at, excluded.discovered_at),
                    next_eligible_at = COALESCE(youtube_video_fetch_state.next_eligible_at, excluded.next_eligible_at)
                ''',
                (
                    video_id,
                    playlist_id,
                    video_title,
                    discovered_at,
                    self.FETCH_STATUS_PENDING,
                    0,
                    discovered_at,
                ),
            )
            conn.commit()

    def upsert_playlist_discovery(
        self,
        playlist_id: str,
        videos: List[dict],
        status: str = FETCH_STATUS_COMPLETE,
        error: Optional[str] = None,
    ) -> None:
        discovered_at = self._utcnow_iso()
        with self._get_connection() as conn:
            conn.execute(
                '''
                INSERT INTO youtube_playlist_fetch_state (
                    playlist_id,
                    last_discovered_at,
                    last_status,
                    last_error
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(playlist_id) DO UPDATE SET
                    last_discovered_at = excluded.last_discovered_at,
                    last_status = excluded.last_status,
                    last_error = excluded.last_error
                ''',
                (playlist_id, discovered_at, status, error),
            )
            for video in videos:
                conn.execute(
                    '''
                    INSERT INTO youtube_video_fetch_state (
                        video_id,
                        playlist_id,
                        video_title,
                        discovered_at,
                        last_status,
                        comments_seen_count,
                        next_eligible_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(video_id) DO UPDATE SET
                        playlist_id = COALESCE(excluded.playlist_id, youtube_video_fetch_state.playlist_id),
                        video_title = COALESCE(excluded.video_title, youtube_video_fetch_state.video_title),
                        discovered_at = COALESCE(youtube_video_fetch_state.discovered_at, excluded.discovered_at)
                    ''',
                    (
                        video.get("video_id"),
                        playlist_id,
                        video.get("title"),
                        discovered_at,
                        self.FETCH_STATUS_PENDING,
                        0,
                        discovered_at,
                    ),
                )
            conn.commit()

    def mark_playlist_discovery_error(self, playlist_id: str, status: str, error: str) -> None:
        with self._get_connection() as conn:
            conn.execute(
                '''
                INSERT INTO youtube_playlist_fetch_state (
                    playlist_id,
                    last_discovered_at,
                    last_status,
                    last_error
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(playlist_id) DO UPDATE SET
                    last_discovered_at = excluded.last_discovered_at,
                    last_status = excluded.last_status,
                    last_error = excluded.last_error
                ''',
                (playlist_id, self._utcnow_iso(), status, error),
            )
            conn.commit()

    def update_video_fetch_outcome(
        self,
        video_id: str,
        status: str,
        comments_seen_count: Optional[int] = None,
        error: Optional[str] = None,
        refresh_days: int = 30,
        backoff_hours: int = 6,
        playlist_id: Optional[str] = None,
        video_title: Optional[str] = None,
    ) -> None:
        attempted_at = self._utcnow_iso()
        succeeded_at = attempted_at if status in {
            self.FETCH_STATUS_COMPLETE,
            self.FETCH_STATUS_ZERO_COMMENTS,
            self.FETCH_STATUS_COMMENTS_DISABLED,
        } else None
        next_eligible_at = self._next_eligible_timestamp(
            status=status,
            refresh_days=refresh_days,
            backoff_hours=backoff_hours,
        )
        with self._get_connection() as conn:
            conn.execute(
                '''
                INSERT INTO youtube_video_fetch_state (
                    video_id,
                    playlist_id,
                    video_title,
                    discovered_at,
                    last_attempted_at,
                    last_succeeded_at,
                    last_status,
                    last_error,
                    comments_seen_count,
                    next_eligible_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    playlist_id = COALESCE(excluded.playlist_id, youtube_video_fetch_state.playlist_id),
                    video_title = COALESCE(excluded.video_title, youtube_video_fetch_state.video_title),
                    last_attempted_at = excluded.last_attempted_at,
                    last_succeeded_at = COALESCE(excluded.last_succeeded_at, youtube_video_fetch_state.last_succeeded_at),
                    last_status = excluded.last_status,
                    last_error = excluded.last_error,
                    comments_seen_count = COALESCE(excluded.comments_seen_count, youtube_video_fetch_state.comments_seen_count),
                    next_eligible_at = excluded.next_eligible_at
                ''',
                (
                    video_id,
                    playlist_id,
                    video_title,
                    attempted_at,
                    attempted_at,
                    succeeded_at,
                    status,
                    error,
                    comments_seen_count,
                    next_eligible_at,
                ),
            )
            conn.commit()

    def get_video_fetch_state(self, video_id: str) -> Optional[dict]:
        with self._get_connection() as conn:
            row = conn.execute(
                '''
                SELECT video_id, playlist_id, video_title, discovered_at, last_attempted_at,
                       last_succeeded_at, last_status, last_error, comments_seen_count, next_eligible_at
                FROM youtube_video_fetch_state
                WHERE video_id = ?
                ''',
                (video_id,),
            ).fetchone()
        if row is None:
            return None
        columns = [
            "video_id",
            "playlist_id",
            "video_title",
            "discovered_at",
            "last_attempted_at",
            "last_succeeded_at",
            "last_status",
            "last_error",
            "comments_seen_count",
            "next_eligible_at",
        ]
        return dict(zip(columns, row))

    def get_candidate_videos(
        self,
        refresh_days: int = 30,
        force_recheck: bool = False,
        limit: Optional[int] = None,
        playlist_ids: Optional[List[str]] = None,
        video_ids: Optional[List[str]] = None,
        now_iso: Optional[str] = None,
    ) -> List[dict]:
        now_iso = now_iso or self._utcnow_iso()
        filters = []
        params: List[object] = [
            self.FETCH_STATUS_PENDING,
            now_iso,
            self.FETCH_STATUS_PENDING,
            now_iso,
            now_iso,
            1 if force_recheck else 0,
        ]

        if playlist_ids:
            placeholders = ", ".join("?" for _ in playlist_ids)
            filters.append(f"playlist_id IN ({placeholders})")
            params.extend(playlist_ids)
        if video_ids:
            placeholders = ", ".join("?" for _ in video_ids)
            filters.append(f"video_id IN ({placeholders})")
            params.extend(video_ids)

        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        query = f'''
            SELECT
                video_id,
                playlist_id,
                video_title,
                discovered_at,
                last_attempted_at,
                last_succeeded_at,
                last_status,
                last_error,
                comments_seen_count,
                next_eligible_at,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM youtube_comments_sentiment AS comments
                        WHERE comments.video_id = youtube_video_fetch_state.video_id
                        LIMIT 1
                    ) THEN 1
                    ELSE 0
                END AS has_existing_comments,
                CASE
                    WHEN (last_status IS NULL OR last_status = ?)
                         AND NOT EXISTS (
                             SELECT 1
                             FROM youtube_comments_sentiment AS comments
                             WHERE comments.video_id = youtube_video_fetch_state.video_id
                             LIMIT 1
                         ) THEN 1
                    WHEN last_status IN ('quota_exhausted', 'api_error')
                         AND (next_eligible_at IS NULL OR next_eligible_at <= ?)
                         AND NOT EXISTS (
                             SELECT 1
                             FROM youtube_comments_sentiment AS comments
                             WHERE comments.video_id = youtube_video_fetch_state.video_id
                             LIMIT 1
                         ) THEN 2
                    WHEN last_status IS NULL OR last_status = ? THEN 3
                    WHEN last_status IN ('quota_exhausted', 'api_error')
                         AND (next_eligible_at IS NULL OR next_eligible_at <= ?) THEN 3
                    WHEN last_status IN ('complete', 'zero_comments', 'comments_disabled')
                         AND (next_eligible_at IS NULL OR next_eligible_at <= ?) THEN 5
                    WHEN ? = 1 THEN 6
                    ELSE 99
                END AS priority_bucket
            FROM youtube_video_fetch_state
            {where_sql}
            ORDER BY priority_bucket ASC,
                     has_existing_comments ASC,
                     COALESCE(next_eligible_at, discovered_at, '') ASC,
                     COALESCE(last_attempted_at, discovered_at, '') ASC,
                     video_id ASC
        '''

        with self._get_connection() as conn:
            rows = conn.execute(query, params).fetchall()

        columns = [
            "video_id",
            "playlist_id",
            "video_title",
            "discovered_at",
            "last_attempted_at",
            "last_succeeded_at",
            "last_status",
            "last_error",
            "comments_seen_count",
            "next_eligible_at",
            "has_existing_comments",
            "priority_bucket",
        ]
        candidates = [dict(zip(columns, row)) for row in rows if row[-1] < 99]
        if not force_recheck:
            candidates = [row for row in candidates if row["priority_bucket"] < 6]
        if limit is not None:
            return candidates[:limit]
        return candidates

    def load_comments_for_absa(self, force_reprocess: bool = False, limit: Optional[int] = None) -> pd.DataFrame:
        query = '''
            SELECT raw.*
            FROM youtube_comments_sentiment AS raw
        '''
        if not force_reprocess:
            query += '''
                LEFT JOIN youtube_comments_scored AS scored
                    ON raw.comment_id = scored.comment_id
                WHERE scored.comment_id IS NULL
            '''
        query += ' ORDER BY raw.published_at ASC, raw.comment_id ASC'
        if limit is not None:
            query += f' LIMIT {int(limit)}'
        with self._get_connection() as conn:
            return pd.read_sql_query(query, conn)

    def upsert_scored_comments(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        rows = df.drop_duplicates(subset=["comment_id"]).to_dict(orient="records")
        if not rows:
            return 0
        columns = list(rows[0].keys())
        placeholders = ", ".join("?" for _ in columns)
        update_assignments = ", ".join(
            f"{column}=excluded.{column}" for column in columns if column != "comment_id"
        )
        values = [tuple(row.get(column) for column in columns) for row in rows]
        with self._get_connection() as conn:
            conn.executemany(
                f'''
                INSERT INTO youtube_comments_scored ({", ".join(columns)})
                VALUES ({placeholders})
                ON CONFLICT(comment_id) DO UPDATE SET
                    {update_assignments}
                ''',
                values,
            )
            conn.commit()
        return len(rows)

    def summarize_playlist_completion(self, playlist_id: str) -> dict:
        with self._get_connection() as conn:
            row = conn.execute(
                '''
                SELECT
                    COUNT(*) AS total_videos,
                    SUM(CASE WHEN last_status IS NULL OR last_status = 'pending' THEN 1 ELSE 0 END) AS pending_videos,
                    SUM(CASE WHEN last_status IN ('complete', 'zero_comments', 'comments_disabled') THEN 1 ELSE 0 END) AS completed_videos,
                    SUM(CASE WHEN last_status IN ('quota_exhausted', 'api_error') THEN 1 ELSE 0 END) AS retryable_videos
                FROM youtube_video_fetch_state
                WHERE playlist_id = ?
                ''',
                (playlist_id,),
            ).fetchone()
        total, pending, completed, retryable = row or (0, 0, 0, 0)
        return {
            "playlist_id": playlist_id,
            "total_videos": self._coerce_int(total),
            "pending_videos": self._coerce_int(pending),
            "completed_videos": self._coerce_int(completed),
            "retryable_videos": self._coerce_int(retryable),
        }

    def get_processed_video_ids(self, table_name: str = 'youtube_comments_sentiment') -> Set[str]:
        """Retrieves a set of all video_ids already present in the sentiment table."""
        with self._get_connection() as conn:
            try:
                return set(pd.read_sql(f"SELECT DISTINCT video_id FROM {table_name} WHERE video_id IS NOT NULL", conn)[
                               'video_id'].tolist())
            except pd.io.sql.DatabaseError:
                # Table might not exist yet, return empty set
                return set()

    def get_processed_playlist_ids(self, table_name: str = 'youtube_comments_sentiment') -> Set[str]:
        """Retrieves a set of all playlist_ids already present in the sentiment table."""
        with self._get_connection() as conn:
            try:
                return set(
                    pd.read_sql(f"SELECT DISTINCT playlist_id FROM {table_name} WHERE playlist_id IS NOT NULL", conn)[
                        'playlist_id'].tolist())
            except pd.io.sql.DatabaseError:
                # Table might not exist yet, return empty set
                return set()

    def close(self):
        """Close the database connection"""
        if self.conn:
            try:
                self.conn.close()
                logging.info("YouTube Comments Database connection closed")
            except Exception as e:
                logging.error(f"Error closing YouTube Comments Database connection: {e}")
            self.conn = None
