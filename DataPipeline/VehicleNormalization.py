"""NHTSA-anchored make/model and title-derived trim normalization."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sqlite3
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import polars as pl
import requests


EPA_VEHICLES_URL = "https://www.fueleconomy.gov/feg/epadata/vehicles.csv.zip"
EPA_ARCHIVE_NAME = "vehicles.csv.zip"
EPA_METADATA_NAME = "metadata.json"
EPA_REQUIRED_COLUMNS = {"id", "year", "make", "model"}
NORMALIZATION_VERSION = "title_epa_v1"

UNKNOWN_VALUES = {
    "",
    "N A",
    "NA",
    "NONE",
    "NULL",
    "UNKNOWN",
    "NOT APPLICABLE",
    "OTHER",
}

MAKE_ALIASES = {
    "MERCEDES BENZ": "MERCEDES-BENZ",
    "MERCEDES": "MERCEDES-BENZ",
    "LANDROVER": "LAND ROVER",
    "LAND ROVER": "LAND ROVER",
    "ALFA ROMEO": "ALFA ROMEO",
    "ROLLS ROYCE": "ROLLS-ROYCE",
    "ASTON MARTIN": "ASTON MARTIN",
    "VOLKSWAGEN": "VOLKSWAGEN",
    "VW": "VOLKSWAGEN",
    "CHEVY": "CHEVROLET",
}

# Ordered by specificity after normalization. Values are the stable modeling labels.
TRIM_ALIASES = {
    "SHELBY GT350R": "GT350R",
    "SHELBY GT350": "GT350",
    "SHELBY GT500": "GT500",
    "GT350R": "GT350R",
    "GT350": "GT350",
    "GT500": "GT500",
    "M3 COMPETITION XDRIVE": "M3_COMPETITION_XDRIVE",
    "COMPETITION XDRIVE": "M3_COMPETITION_XDRIVE",
    "M3 COMPETITION": "M3_COMPETITION",
    "TRD OFF ROAD PREMIUM": "TRD_OFF_ROAD_PREMIUM",
    "TRD OFF ROAD": "TRD_OFF_ROAD",
    "TRD SPORT": "TRD_SPORT",
    "TRD PRO": "TRD_PRO",
    "HIGH COUNTRY": "HIGH_COUNTRY",
    "KING RANCH": "KING_RANCH",
    "BIG HORN": "BIG_HORN",
    "M SPORT": "M_SPORT",
    "PRO 4X": "PRO_4X",
    "RTL E": "RTL_E",
    "EX L": "EX_L",
    "MACH 1": "MACH_1",
    "DARK HORSE": "DARK_HORSE",
}

# Make/model/year-scoped rules are evaluated before global aliases. Changes to
# this table require a NORMALIZATION_VERSION bump.
IDENTITY_TRIM_ALIASES = {
    ("FORD", "MUSTANG"): [
        (2015, 2020, "SHELBY GT350R", "GT350R"),
        (2015, 2020, "SHELBY GT350", "GT350"),
        (2020, 2022, "SHELBY GT500", "GT500"),
    ],
    ("TOYOTA", "TACOMA"): [
        (2005, 2030, "TRD OFF ROAD PREMIUM", "TRD_OFF_ROAD_PREMIUM"),
        (2005, 2030, "TRD OFF ROAD", "TRD_OFF_ROAD"),
        (2005, 2030, "TRD SPORT", "TRD_SPORT"),
        (2015, 2030, "TRD PRO", "TRD_PRO"),
    ],
    ("BMW", "M3"): [
        (2021, 2030, "COMPETITION XDRIVE", "M3_COMPETITION_XDRIVE"),
        (2021, 2030, "COMPETITION", "M3_COMPETITION"),
    ],
}

MARKETING_PHRASES = {
    "FOR SALE",
    "LOW MILES",
    "ONE OWNER",
    "CLEAN CARFAX",
    "CLEAN TITLE",
    "NO ACCIDENTS",
    "SEE VIDEO",
    "FULLY LOADED",
    "WELL MAINTAINED",
    "GREAT CONDITION",
}

NON_TRIM_TOKENS = {
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
    "HATCHBACK",
    "CONVERTIBLE",
    "CABRIOLET",
    "ROADSTER",
    "AWD",
    "FWD",
    "RWD",
    "4WD",
    "4X4",
    "AUTO",
    "AUTOMATIC",
    "MANUAL",
    "GAS",
    "GASOLINE",
    "DIESEL",
    "HYBRID",
    "ELECTRIC",
    "TURBO",
    "LOCAL",
    "CARFAX",
    "CLEAN",
    "TITLE",
    "NAVIGATION",
    "SUNROOF",
    "LEATHER",
    "BACKUP",
    "CAMERA",
    "LOADED",
    "MILES",
    "MILE",
    "PACKAGE",
    "PKG",
}


def normalize_vehicle_text(value: Any) -> str:
    """Normalize text for boundary-aware catalog and title matching."""
    if value is None:
        return ""
    text = re.sub(r"[^A-Za-z0-9]+", " ", str(value).upper())
    text = re.sub(r"\s+", " ", text).strip()
    return "" if text in UNKNOWN_VALUES else text


def canonical_token(value: Any) -> str:
    text = normalize_vehicle_text(value)
    return text.replace(" ", "_") if text else ""


def _contains_phrase(text: str, phrase: str) -> bool:
    return bool(re.search(rf"(?:^| ){re.escape(phrase)}(?: |$)", text))


def _remove_phrase(text: str, phrase: str) -> str:
    updated = re.sub(rf"(?:^| ){re.escape(phrase)}(?= |$)", " ", text, count=1)
    return re.sub(r"\s+", " ", updated).strip()


def _clean_trim_remainder(value: str) -> str:
    text = normalize_vehicle_text(value)
    for phrase in sorted(MARKETING_PHRASES, key=len, reverse=True):
        text = _remove_phrase(text, normalize_vehicle_text(phrase))
    tokens = [
        token
        for token in text.split()
        if token not in NON_TRIM_TOKENS and not re.fullmatch(r"\d{5,}", token)
    ]
    return " ".join(tokens).strip()


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    temporary_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    os.replace(temporary_path, path)


def _validate_epa_archive(path: Path) -> None:
    if not path.exists() or path.stat().st_size == 0:
        raise ValueError(f"EPA archive is missing or empty: {path}")
    with zipfile.ZipFile(path) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith("vehicles.csv")]
        if not csv_names:
            raise ValueError("EPA archive does not contain vehicles.csv")


def download_epa_catalog(
    cache_dir: Path,
    *,
    refresh: bool = True,
    offline: bool = False,
    timeout: int = 120,
) -> tuple[Path, dict[str, Any]]:
    """Refresh the official EPA archive and fall back to a validated cache."""
    cache_dir = Path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    archive_path = cache_dir / EPA_ARCHIVE_NAME
    metadata_path = cache_dir / EPA_METADATA_NAME
    metadata: dict[str, Any] = {}
    if metadata_path.exists():
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            metadata = {}

    if offline or not refresh:
        _validate_epa_archive(archive_path)
        metadata["cache_status"] = "offline" if offline else "cached"
        return archive_path, metadata

    headers = {}
    if metadata.get("etag"):
        headers["If-None-Match"] = str(metadata["etag"])
    if metadata.get("last_modified"):
        headers["If-Modified-Since"] = str(metadata["last_modified"])

    try:
        response = requests.get(EPA_VEHICLES_URL, headers=headers, stream=True, timeout=timeout)
        if response.status_code == 304:
            _validate_epa_archive(archive_path)
            metadata["cache_status"] = "not_modified"
            metadata["checked_at_utc"] = datetime.now(timezone.utc).isoformat()
            _write_json_atomic(metadata_path, metadata)
            return archive_path, metadata
        response.raise_for_status()
        temporary_path = archive_path.with_suffix(archive_path.suffix + ".tmp")
        with temporary_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    handle.write(chunk)
        _validate_epa_archive(temporary_path)
        os.replace(temporary_path, archive_path)
        metadata = {
            "source_url": EPA_VEHICLES_URL,
            "downloaded_at_utc": datetime.now(timezone.utc).isoformat(),
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
            "etag": response.headers.get("ETag"),
            "last_modified": response.headers.get("Last-Modified"),
            "sha256": _sha256(archive_path),
            "archive_bytes": archive_path.stat().st_size,
            "cache_status": "downloaded",
        }
        _write_json_atomic(metadata_path, metadata)
        return archive_path, metadata
    except (requests.RequestException, OSError, ValueError, zipfile.BadZipFile) as exc:
        if archive_path.exists():
            _validate_epa_archive(archive_path)
            metadata["cache_status"] = "stale_fallback"
            metadata["refresh_error"] = str(exc)
            return archive_path, metadata
        raise RuntimeError("EPA refresh failed and no valid cached archive exists") from exc


def load_epa_catalog(archive_path: Path) -> pl.DataFrame:
    """Load the complete EPA vehicle CSV and add normalized lookup columns."""
    _validate_epa_archive(Path(archive_path))
    with zipfile.ZipFile(archive_path) as archive:
        csv_name = next(name for name in archive.namelist() if name.lower().endswith("vehicles.csv"))
        with archive.open(csv_name) as handle:
            frame = pl.read_csv(
                handle,
                infer_schema_length=10_000,
                ignore_errors=True,
                null_values=["", "NA", "N/A"],
            )
    missing = sorted(EPA_REQUIRED_COLUMNS - set(frame.columns))
    if missing:
        raise ValueError(f"EPA vehicle CSV is missing required columns: {', '.join(missing)}")
    if "baseModel" not in frame.columns:
        frame = frame.with_columns(pl.col("model").alias("baseModel"))
    frame = frame.with_columns(
        [
            pl.col("year").cast(pl.Int64, strict=False),
            pl.col("id").cast(pl.Int64, strict=False),
            pl.col("make").map_elements(normalize_vehicle_text, return_dtype=pl.String).alias("normalized_make"),
            pl.col("model").map_elements(normalize_vehicle_text, return_dtype=pl.String).alias("normalized_model"),
            pl.col("baseModel")
            .map_elements(normalize_vehicle_text, return_dtype=pl.String)
            .alias("normalized_base_model"),
        ]
    )
    return frame


def build_epa_metadata_frame(metadata: dict[str, Any], catalog: pl.DataFrame) -> pl.DataFrame:
    years = catalog.get_column("year").drop_nulls() if "year" in catalog.columns else pl.Series([], dtype=pl.Int64)
    return pl.DataFrame(
        {
            "source_url": [metadata.get("source_url", EPA_VEHICLES_URL)],
            "downloaded_at_utc": [metadata.get("downloaded_at_utc")],
            "checked_at_utc": [metadata.get("checked_at_utc")],
            "etag": [metadata.get("etag")],
            "last_modified": [metadata.get("last_modified")],
            "sha256": [metadata.get("sha256")],
            "cache_status": [metadata.get("cache_status")],
            "row_count": [catalog.height],
            "min_model_year": [int(years.min()) if len(years) else None],
            "max_model_year": [int(years.max()) if len(years) else None],
            "normalization_version": [NORMALIZATION_VERSION],
        }
    )


def import_epa_catalog_sqlite(
    db_path: Path,
    catalog: pl.DataFrame,
    metadata: pl.DataFrame,
) -> None:
    """Atomically replace only the EPA reference tables in an existing SQLite database."""
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"SQLite database not found: {db_path}")

    def sql_type(dtype: pl.DataType) -> str:
        if dtype.is_integer():
            return "INTEGER"
        if dtype.is_float():
            return "REAL"
        if dtype == pl.Boolean:
            return "BOOLEAN"
        return "TEXT"

    def create_and_insert(conn: sqlite3.Connection, table: str, frame: pl.DataFrame) -> None:
        columns_sql = ", ".join(
            f'"{column}" {sql_type(dtype)}' for column, dtype in frame.schema.items()
        )
        conn.execute(f'DROP TABLE IF EXISTS "{table}"')
        conn.execute(f'CREATE TABLE "{table}" ({columns_sql})')
        if frame.is_empty():
            return
        quoted_columns = ", ".join(f'"{column}"' for column in frame.columns)
        placeholders = ", ".join("?" for _ in frame.columns)
        statement = f'INSERT INTO "{table}" ({quoted_columns}) VALUES ({placeholders})'
        for batch in frame.iter_slices(2_000):
            conn.executemany(statement, batch.iter_rows())

    conn = sqlite3.connect(str(db_path))
    try:
        conn.execute("BEGIN IMMEDIATE")
        create_and_insert(conn, "epa_vehicle_catalog_new", catalog)
        create_and_insert(conn, "epa_catalog_metadata_new", metadata)
        conn.execute("DROP TABLE IF EXISTS epa_vehicle_catalog")
        conn.execute("DROP TABLE IF EXISTS epa_catalog_metadata")
        conn.execute("ALTER TABLE epa_vehicle_catalog_new RENAME TO epa_vehicle_catalog")
        conn.execute("ALTER TABLE epa_catalog_metadata_new RENAME TO epa_catalog_metadata")
        conn.execute("CREATE UNIQUE INDEX idx_epa_catalog_id ON epa_vehicle_catalog (id)")
        conn.execute(
            "CREATE INDEX idx_epa_catalog_normalized_identity "
            "ON epa_vehicle_catalog (year, normalized_make, normalized_model)"
        )
        conn.execute(
            "CREATE INDEX idx_epa_catalog_normalized_base "
            "ON epa_vehicle_catalog (year, normalized_make, normalized_base_model)"
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


@dataclass(frozen=True)
class ParsedIdentity:
    canonical_title: str
    canonical_year: int | None
    canonical_make: str
    canonical_model: str
    canonical_trim: str
    canonical_trim_raw: str
    canonical_trim_source: str
    canonical_match_confidence: str
    canonical_match_status: str
    epa_vehicle_id: int | None
    epa_match_status: str
    normalization_version: str
    nhtsa_year_agrees: bool | None
    nhtsa_make_agrees: bool | None
    nhtsa_model_agrees: bool | None
    nhtsa_trim_agrees: bool | None


class VehicleNormalizer:
    """Parse authoritative identity fields from listing titles."""

    def __init__(self, epa_catalog: pl.DataFrame | None = None):
        self.epa_catalog = epa_catalog if epa_catalog is not None else pl.DataFrame()
        self.make_aliases: dict[str, str] = dict(MAKE_ALIASES)
        self.models_by_year_make: dict[tuple[int, str], set[str]] = {}
        self.epa_ids: dict[tuple[int, str, str], list[int]] = {}
        self.base_model_ids: dict[tuple[int, str, str], list[int]] = {}
        self.epa_trim_candidates: dict[tuple[int, str, str], set[str]] = {}
        self._build_indexes()

    def _build_indexes(self) -> None:
        required = {"year", "id", "normalized_make", "normalized_model", "normalized_base_model"}
        if not required.issubset(self.epa_catalog.columns):
            return
        for row in self.epa_catalog.select(sorted(required)).iter_rows(named=True):
            year = row.get("year")
            make = row.get("normalized_make") or ""
            model = row.get("normalized_model") or ""
            base_model = row.get("normalized_base_model") or model
            vehicle_id = row.get("id")
            if year is None or not make:
                continue
            year = int(year)
            canonical_make = MAKE_ALIASES.get(make, make)
            self.make_aliases.setdefault(make, canonical_make)
            candidates = self.models_by_year_make.setdefault((year, canonical_make), set())
            if base_model:
                candidates.add(base_model)
            if model:
                candidates.add(model)
            if vehicle_id is not None and model:
                self.epa_ids.setdefault((year, canonical_make, model), []).append(int(vehicle_id))
            if vehicle_id is not None and base_model:
                self.base_model_ids.setdefault((year, canonical_make, base_model), []).append(int(vehicle_id))
            if base_model and model.startswith(f"{base_model} "):
                suffix = _clean_trim_remainder(model[len(base_model) :])
                if suffix:
                    self.epa_trim_candidates.setdefault((year, canonical_make, base_model), set()).add(suffix)

    @staticmethod
    def _parse_year(title_text: str) -> int | None:
        match = re.search(r"\b(19[8-9]\d|20[0-3]\d)\b", title_text)
        return int(match.group(1)) if match else None

    def _parse_make(self, title_text: str, nhtsa_make: Any) -> tuple[str, str]:
        context_make = normalize_vehicle_text(nhtsa_make)
        if context_make:
            context_make = MAKE_ALIASES.get(context_make, context_make)
            self.make_aliases.setdefault(context_make, context_make)
            matching_aliases = [
                alias
                for alias, canonical in self.make_aliases.items()
                if canonical == context_make and alias and _contains_phrase(title_text, alias)
            ]
            matched_alias = max(matching_aliases, key=len) if matching_aliases else ""
            return context_make, matched_alias
        matches = [
            (alias, canonical)
            for alias, canonical in self.make_aliases.items()
            if alias and _contains_phrase(title_text, alias)
        ]
        if matches:
            alias, canonical = max(matches, key=lambda item: (len(item[0].split()), len(item[0])))
            return canonical, alias
        return context_make, ""

    def _parse_model(
        self,
        title_text: str,
        year: int | None,
        make: str,
        nhtsa_model: Any,
    ) -> tuple[str, str]:
        context_model = normalize_vehicle_text(nhtsa_model)
        if context_model:
            return context_model, context_model if _contains_phrase(title_text, context_model) else ""
        candidates: set[str] = set()
        if year is not None and make:
            candidates.update(self.models_by_year_make.get((year, make), set()))
        matches = [candidate for candidate in candidates if candidate and _contains_phrase(title_text, candidate)]
        if matches:
            matched = max(matches, key=lambda value: (len(value.split()), len(value)))
            return matched, matched
        return context_model, ""

    def _canonicalize_trim(
        self,
        trim_raw: str,
        canonical_model: str,
        year: int | None,
        canonical_make: str,
    ) -> tuple[str, str]:
        scoped_aliases = IDENTITY_TRIM_ALIASES.get((canonical_make, canonical_model), [])
        for min_year, max_year, alias, canonical in sorted(scoped_aliases, key=lambda rule: len(rule[2]), reverse=True):
            if year is not None and min_year <= year <= max_year and _contains_phrase(trim_raw, alias):
                return canonical, "title_alias"
        for alias, canonical in sorted(TRIM_ALIASES.items(), key=lambda item: len(item[0]), reverse=True):
            if _contains_phrase(trim_raw, alias):
                return canonical, "title_alias"
        if year is not None:
            epa_candidates = self.epa_trim_candidates.get((year, canonical_make, canonical_model), set())
            matches = [candidate for candidate in epa_candidates if _contains_phrase(trim_raw, candidate)]
            if matches:
                return canonical_token(max(matches, key=lambda value: (len(value.split()), len(value)))), "title_epa"
        token = canonical_token(trim_raw)
        if canonical_model in {"M3", "M4", "M5"} and token.startswith("COMPETITION"):
            token = f"{canonical_model}_{token}"
        return (token, "title_remainder") if token else ("UNKNOWN_TRIM", "unresolved")

    def _epa_match(self, year: int | None, make: str, model: str) -> tuple[int | None, str]:
        if year is None or not make or not model:
            return None, "none"
        exact = self.epa_ids.get((year, make, model), [])
        if exact:
            return min(exact), "exact" if len(exact) == 1 else "ambiguous_best"
        base = self.base_model_ids.get((year, make, model), [])
        if base:
            return min(base), "base_model" if len(base) == 1 else "ambiguous_best"
        prefixed_models = [
            candidate
            for candidate in self.models_by_year_make.get((year, make), set())
            if candidate.startswith(f"{model} ")
        ]
        prefixed_ids = [
            vehicle_id
            for candidate in prefixed_models
            for vehicle_id in self.epa_ids.get((year, make, candidate), [])
        ]
        if prefixed_ids:
            return min(prefixed_ids), "prefix_model" if len(prefixed_ids) == 1 else "ambiguous_best"
        return None, "none"

    def parse_title(
        self,
        title: Any,
        *,
        nhtsa_make: Any = None,
        nhtsa_model: Any = None,
        nhtsa_year: Any = None,
        nhtsa_trim: Any = None,
        nhtsa_trim2: Any = None,
    ) -> ParsedIdentity:
        title_text = normalize_vehicle_text(title)
        year = self._parse_year(title_text)
        make, matched_make = self._parse_make(title_text, nhtsa_make)
        model, matched_model = self._parse_model(title_text, year, make, nhtsa_model)

        remainder = title_text
        if year is not None:
            remainder = _remove_phrase(remainder, str(year))
        if matched_make:
            remainder = _remove_phrase(remainder, matched_make)
        if matched_model:
            remainder = _remove_phrase(remainder, matched_model)
        trim_raw = _clean_trim_remainder(remainder)
        canonical_trim, trim_source = self._canonicalize_trim(trim_raw, model, year, make)
        epa_vehicle_id, epa_status = self._epa_match(year, make, model)

        parsed_from_title = bool(year and matched_make and matched_model)
        if canonical_trim == "UNKNOWN_TRIM":
            confidence = "unresolved"
            status = "unresolved"
        elif parsed_from_title and epa_status in {"exact", "base_model"}:
            confidence = "high"
            status = "confirmed"
        elif parsed_from_title or trim_source == "title_alias":
            confidence = "medium"
            status = "best_candidate"
        else:
            confidence = "low"
            status = "best_candidate"

        nhtsa_year_value: int | None
        try:
            nhtsa_year_value = int(float(nhtsa_year)) if nhtsa_year is not None else None
        except (TypeError, ValueError):
            nhtsa_year_value = None
        nhtsa_make_text = MAKE_ALIASES.get(normalize_vehicle_text(nhtsa_make), normalize_vehicle_text(nhtsa_make))
        nhtsa_model_text = normalize_vehicle_text(nhtsa_model)
        nhtsa_trims = {
            canonical_token(value)
            for value in (nhtsa_trim, nhtsa_trim2)
            if canonical_token(value)
        }

        canonical_parts = [str(year) if year else "UNKNOWN_YEAR", make or "UNKNOWN_MAKE", model or "UNKNOWN_MODEL"]
        if canonical_trim != "UNKNOWN_TRIM":
            trim_display = canonical_trim.replace("_", " ")
            if model and trim_display.startswith(f"{model} "):
                trim_display = trim_display[len(model) + 1 :]
            canonical_parts.append(trim_display)
        canonical_title = " ".join(canonical_parts)
        return ParsedIdentity(
            canonical_title=canonical_title,
            canonical_year=year,
            canonical_make=make or "UNKNOWN_MAKE",
            canonical_model=model or "UNKNOWN_MODEL",
            canonical_trim=canonical_trim,
            canonical_trim_raw=trim_raw,
            canonical_trim_source=trim_source,
            canonical_match_confidence=confidence,
            canonical_match_status=status,
            epa_vehicle_id=epa_vehicle_id,
            epa_match_status=epa_status,
            normalization_version=NORMALIZATION_VERSION,
            nhtsa_year_agrees=(year == nhtsa_year_value) if year and nhtsa_year_value else None,
            nhtsa_make_agrees=bool(matched_make) if nhtsa_make_text else None,
            nhtsa_model_agrees=bool(matched_model) if nhtsa_model_text else None,
            nhtsa_trim_agrees=(canonical_trim in nhtsa_trims) if nhtsa_trims else None,
        )

    def normalize_listings(self, frame: pl.DataFrame) -> pl.DataFrame:
        """Normalize distinct title/context combinations, then join them to all rows."""
        if "title" not in frame.columns:
            raise ValueError("listings frame must contain a title column")
        output_schema = {
            "canonical_title": pl.String,
            "canonical_year": pl.Int64,
            "canonical_make": pl.String,
            "canonical_model": pl.String,
            "canonical_trim": pl.String,
            "canonical_trim_raw": pl.String,
            "canonical_trim_source": pl.String,
            "canonical_match_confidence": pl.String,
            "canonical_match_status": pl.String,
            "epa_vehicle_id": pl.Int64,
            "epa_match_status": pl.String,
            "normalization_version": pl.String,
            "nhtsa_year_agrees": pl.Boolean,
            "nhtsa_make_agrees": pl.Boolean,
            "nhtsa_model_agrees": pl.Boolean,
            "nhtsa_trim_agrees": pl.Boolean,
        }
        if frame.is_empty():
            return frame.with_columns(
                [pl.lit(None, dtype=dtype).alias(column) for column, dtype in output_schema.items()]
            )
        context_columns = [
            column
            for column in [
                "title",
                "nhtsa_Make",
                "nhtsa_Model",
                "nhtsa_ModelYear",
            ]
            if column in frame.columns
        ]
        keyed = frame.with_columns(
            pl.concat_str(
                [pl.col(column).cast(pl.String).fill_null("") for column in context_columns],
                separator="\u241f",
            ).alias("_normalization_key")
        )
        unique_rows = keyed.select(["_normalization_key", *context_columns]).unique("_normalization_key")
        parsed_rows: list[dict[str, Any]] = []
        for row in unique_rows.iter_rows(named=True):
            parsed = self.parse_title(
                row.get("title"),
                nhtsa_make=row.get("nhtsa_Make"),
                nhtsa_model=row.get("nhtsa_Model"),
                nhtsa_year=row.get("nhtsa_ModelYear"),
            )
            parsed_rows.append({"_normalization_key": row["_normalization_key"], **parsed.__dict__})
        normalized = pl.DataFrame(parsed_rows, infer_schema_length=None)
        result = keyed.join(normalized, on="_normalization_key", how="left").drop("_normalization_key")
        if {"nhtsa_Trim", "nhtsa_Trim2"}.intersection(result.columns):
            result = result.with_columns(
                pl.struct(
                    [column for column in ["canonical_trim", "nhtsa_Trim", "nhtsa_Trim2"] if column in result.columns]
                )
                .map_elements(
                    lambda row: (
                        row["canonical_trim"]
                        in {
                            canonical_token(row.get("nhtsa_Trim")),
                            canonical_token(row.get("nhtsa_Trim2")),
                        }
                    )
                    if canonical_token(row.get("nhtsa_Trim")) or canonical_token(row.get("nhtsa_Trim2"))
                    else None,
                    return_dtype=pl.Boolean,
                )
                .alias("nhtsa_trim_agrees")
            )
        return result


def build_vehicle_identity(listings: pl.DataFrame) -> pl.DataFrame:
    """Select one deterministic, title-derived consensus identity per VIN."""
    required = {"vin", "canonical_trim", "canonical_match_confidence"}
    if not required.issubset(listings.columns):
        raise ValueError("canonical listing fields are required to build vehicle_identity")
    rank = (
        pl.when(pl.col("canonical_match_confidence") == "high")
        .then(pl.lit(3))
        .when(pl.col("canonical_match_confidence") == "medium")
        .then(pl.lit(2))
        .when(pl.col("canonical_match_confidence") == "low")
        .then(pl.lit(1))
        .otherwise(pl.lit(0))
    )
    identity_columns = ["canonical_year", "canonical_make", "canonical_model", "canonical_trim"]
    repeated_support = listings.group_by(["vin", *identity_columns]).agg(
        pl.len().alias("_identity_support_count")
    )
    ranked = listings.join(repeated_support, on=["vin", *identity_columns], how="left")
    sort_columns = ["vin", "_confidence_rank", "_identity_support_count"]
    descending = [False, True, True]
    for column in ["loaddate", "date"]:
        if column in listings.columns:
            sort_columns.append(column)
            descending.append(True)
    ranked = ranked.with_columns(rank.alias("_confidence_rank")).sort(sort_columns, descending=descending)
    selected_columns = [
        column
        for column in [
            "vin",
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
        ]
        if column in ranked.columns
    ]
    consensus = ranked.unique("vin", keep="first").select(selected_columns)
    winning_support = ranked.unique("vin", keep="first").select(
        ["vin", pl.col("_identity_support_count").alias("winning_identity_support_count")]
    )
    support = listings.group_by("vin").agg(
        [
            pl.len().alias("supporting_listing_count"),
            pl.col("canonical_trim").n_unique().alias("distinct_trim_count"),
        ]
    ).with_columns((pl.col("distinct_trim_count") - 1).clip(lower_bound=0).alias("conflict_count"))
    return consensus.join(winning_support, on="vin", how="left").join(support, on="vin", how="left")


def empty_epa_catalog() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "id": pl.Int64,
            "year": pl.Int64,
            "make": pl.String,
            "model": pl.String,
            "baseModel": pl.String,
            "normalized_make": pl.String,
            "normalized_model": pl.String,
            "normalized_base_model": pl.String,
        }
    )


def normalize_title_records(
    records: Iterable[dict[str, Any]],
    epa_catalog: pl.DataFrame | None = None,
) -> list[ParsedIdentity]:
    """Small public helper used by tests and bounded audits."""
    normalizer = VehicleNormalizer(epa_catalog)
    normalized: list[ParsedIdentity] = []
    for record in records:
        values = dict(record)
        normalized.append(normalizer.parse_title(values.pop("title"), **values))
    return normalized


def main() -> None:
    repo_root = Path(__file__).resolve().parent.parent
    parser = argparse.ArgumentParser(description="Refresh and validate the official EPA vehicle catalog")
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=repo_root / "CAR_DATA_OUTPUT" / "reference" / "epa_fuel_economy",
    )
    parser.add_argument("--offline", action="store_true")
    parser.add_argument("--no-refresh", action="store_true")
    parser.add_argument(
        "--import-db",
        type=Path,
        help="Atomically replace EPA reference tables in an existing SQLite database",
    )
    args = parser.parse_args()
    archive, metadata = download_epa_catalog(
        args.cache_dir,
        refresh=not args.no_refresh,
        offline=args.offline,
    )
    catalog = load_epa_catalog(archive)
    metadata_frame = build_epa_metadata_frame(metadata, catalog)
    if args.import_db:
        import_epa_catalog_sqlite(args.import_db, catalog, metadata_frame)
    summary = metadata_frame.row(0, named=True)
    print(json.dumps(summary, indent=2, default=str))


if __name__ == "__main__":
    main()
