"""Incremental, complete NHTSA enrichment for VIN and vehicle-level data.

The pipeline keeps the existing ``nhtsa_enrichment`` compatibility snapshot in
CAR_DATA.db while storing complete source-field coverage in normalized records
inside CAR_DATA_NHTSA.db. Full response and per-record JSON payloads are not
persisted. All network calls are rate-limited, retryable, and recorded with their
response status so an interrupted refresh can be resumed safely.
"""

from __future__ import annotations

import argparse
import logging
import os
import random
import re
import threading
import time
from collections import Counter
from datetime import date, datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import quote

import pandas as pd
import requests

try:
    from .database import CarDatabase, NHTSADataStore, backup_sqlite_database
except ImportError:
    from database import CarDatabase, NHTSADataStore, backup_sqlite_database


class NHTSARequestError(RuntimeError):
    """A request failed after the configured retry policy was exhausted."""

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


class NHTSAEnricher:
    """Enrich listing VINs with complete, auditable NHTSA responses."""

    BASE_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/"
    DECODE_ENDPOINT = "DecodeVinValuesExtended/"
    BATCH_DECODE_ENDPOINT = "DecodeVINValuesBatch/"
    RATINGS_BASE_URL = "https://api.nhtsa.gov/SafetyRatings/"
    RECALLS_BASE_URL = "https://api.nhtsa.gov/recalls/"
    COMPLAINTS_BASE_URL = "https://api.nhtsa.gov/complaints/"
    MAX_BATCH_SIZE = 50
    RETRYABLE_STATUS_CODES = {408, 425, 429, 500, 502, 503, 504}

    # This is only the stable compatibility projection. The raw NHTSA store is
    # dynamic and retains every field returned by the API.
    COMPATIBILITY_FIELDS = (
        "ABS", "ActiveSafetySysNote", "AdaptiveCruiseControl", "AdaptiveDrivingBeam",
        "AdaptiveHeadlights", "AdditionalErrorText", "AirBagLocCurtain", "AirBagLocFront",
        "AirBagLocKnee", "AirBagLocSeatCushion", "AirBagLocSide", "AutoReverseSystem",
        "AutomaticPedestrianAlertingSound", "AxleConfiguration", "Axles", "BasePrice",
        "BedLengthIN", "BedType", "BlindSpotIntervention", "BlindSpotMon", "BodyCabType",
        "BodyClass", "BrakeSystemDesc", "BrakeSystemType", "ChargerLevel", "ChargerPowerKW",
        "CombinedBrakingSystem", "CoolingType", "CurbWeightLB", "DaytimeRunningLight",
        "DestinationMarket", "DisplacementCC", "DisplacementCI", "DisplacementL", "Doors",
        "DriveType", "DriverAssist", "DynamicBrakeSupport", "EDR", "ESC", "EVDriveUnit",
        "ElectrificationLevel", "EngineConfiguration", "EngineCycles", "EngineCylinders",
        "EngineHP", "EngineHP_to", "EngineKW", "EngineManufacturer", "EngineModel",
        "EntertainmentSystem", "ForwardCollisionWarning", "FuelInjectionType",
        "FuelTankMaterial", "FuelTankType", "FuelTypePrimary", "FuelTypeSecondary",
        "KeylessIgnition", "LaneCenteringAssistance", "LaneDepartureWarning", "LaneKeepSystem",
        "LowerBeamHeadlampLightSource", "Make", "MakeID", "Manufacturer", "ManufacturerId",
        "Model", "ModelID", "ModelYear", "OtherEngineInfo", "ParkAssist",
        "PedestrianAutomaticEmergencyBraking", "RearAutomaticEmergencyBraking",
        "RearCrossTrafficAlert", "RearVisibilitySystem", "SAEAutomationLevel",
        "SAEAutomationLevel_to", "SeatRows", "Seats", "SemiautomaticHeadlampBeamSwitching",
        "TPMS", "TopSpeedMPH", "TrackWidth", "TractionControl", "TransmissionSpeeds",
        "TransmissionStyle", "Trim", "Trim2", "WheelSizeFront", "WheelSizeRear", "Windows",
        "VehicleType", "WheelBaseLong", "WheelBaseShort", "WheelBaseType",
    )

    COMMON_MAKES = (
        "ALFA ROMEO", "ASTON MARTIN", "AUDI", "BMW", "BUICK", "CADILLAC", "CHEVROLET",
        "CHRYSLER", "DODGE", "FIAT", "FORD", "GENESIS", "GMC", "HONDA", "HYUNDAI",
        "INFINITI", "JAGUAR", "JEEP", "KIA", "LAND ROVER", "LEXUS", "LINCOLN",
        "MASERATI", "MAZDA", "MCLAREN", "MERCEDES BENZ", "MERCEDES-BENZ", "MINI",
        "MITSUBISHI", "NISSAN", "PORSCHE", "RAM", "RIVIAN", "ROLLS ROYCE", "SAAB",
        "SATURN", "SCION", "SMART", "SUBARU", "TESLA", "TOYOTA", "VOLKSWAGEN", "VOLVO",
    )

    def __init__(
        self,
        rate_limit_delay: float = 1.0,
        output_dir: Optional[str] = None,
        db_path: Optional[str] = None,
        nhtsa_db_path: Optional[str] = None,
        max_workers: int = 1,
        refresh_days: int = 30,
        request_timeout: tuple[float, float] = (10.0, 60.0),
        backup_path: Optional[str] = None,
    ):
        self.rate_limit_delay = max(0.0, float(rate_limit_delay))
        self.max_workers = max(1, int(max_workers))
        self.refresh_days = max(0, int(refresh_days))
        self.request_timeout = request_timeout
        self.output_dir = output_dir or os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "CAR_DATA_OUTPUT"
        )
        os.makedirs(self.output_dir, exist_ok=True)
        self.db_path = db_path or os.path.join(self.output_dir, "CAR_DATA.db")
        self.nhtsa_db_path = nhtsa_db_path or os.path.join(self.output_dir, "CAR_DATA_NHTSA.db")

        if backup_path and Path(self.db_path).exists():
            backup_sqlite_database(self.db_path, backup_path)
        self.db = CarDatabase(self.db_path)
        self.nhtsa = NHTSADataStore(self.nhtsa_db_path)
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": "CarPriceDataPipeline/1.0 (research data ingestion)",
        })
        self.cache_safety: dict[str, dict[str, Any]] = {}
        self.cache_lock = threading.RLock()
        self.rate_lock = threading.Lock()
        self.next_request_at = 0.0
        self.refresh_all_active = False
        self.logger = logging.getLogger(f"NHTSAEnricher.{id(self)}")
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False
        self.log_handler: Optional[logging.Handler] = None
        self.setup_logging()

        self.nhtsa.register_source(
            "vpic_decode_values_extended", "api",
            f"{self.BASE_URL}{self.DECODE_ENDPOINT}",
            documentation="https://vpic.nhtsa.dot.gov/api/Home/Index",
        )
        self.nhtsa.register_source(
            "vpic_decode_values_batch", "api",
            f"{self.BASE_URL}{self.BATCH_DECODE_ENDPOINT}",
            documentation="https://vpic.nhtsa.dot.gov/api/Home/Index/LanguageExamples",
        )
        self.nhtsa.register_source("safety_ratings", "api", self.RATINGS_BASE_URL)
        self.nhtsa.register_source("recalls", "api", self.RECALLS_BASE_URL)
        self.nhtsa.register_source("complaints", "api", self.COMPLAINTS_BASE_URL)

    def setup_logging(self) -> None:
        log_file = os.path.join(self.output_dir, f"nhtsa_enrichment_{date.today()}.log")
        handler = logging.FileHandler(log_file)
        handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        self.logger.addHandler(handler)
        self.log_handler = handler

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _normalise_vin(vin: Any) -> str:
        if vin is None or pd.isna(vin):
            return ""
        return str(vin).strip().upper()

    @classmethod
    def _vin_status(cls, vin: Any) -> str:
        value = cls._normalise_vin(vin)
        if not value or "INVALID" in value:
            return "invalid"
        if len(value) > 17 or not re.fullmatch(r"[A-HJ-NPR-Z0-9*]+", value):
            return "invalid"
        if len(value) == 17 and "*" not in value:
            return "valid"
        if 3 <= len(value) <= 17:
            return "partial"
        return "invalid"

    def _is_valid_vin(self, vin: Any) -> bool:
        return self._vin_status(vin) != "invalid"

    @staticmethod
    def _model_year(value: Any) -> Optional[int]:
        try:
            year = int(float(value))
        except (TypeError, ValueError):
            return None
        return year if 1886 <= year <= 2100 else None

    @staticmethod
    def _usable(value: Any) -> bool:
        if value is None:
            return False
        return str(value).strip().upper() not in {"", "N/A", "NA", "NONE", "NULL", "UNKNOWN"}

    @staticmethod
    def _text(value: Any) -> str:
        return str(value).strip().upper() if value is not None else ""

    def _apply_rate_limit(self) -> None:
        """Schedule the next request without holding the lock during sleep."""
        with self.rate_lock:
            now = time.monotonic()
            wait_for = max(0.0, self.next_request_at - now)
            self.next_request_at = max(now, self.next_request_at) + self.rate_limit_delay
        if wait_for:
            time.sleep(wait_for)

    def _request_json(
        self,
        method: str,
        url: str,
        *,
        params: Optional[dict[str, Any]] = None,
        data: Optional[dict[str, Any]] = None,
        attempts: int = 4,
    ) -> tuple[dict[str, Any], int]:
        last_error: Optional[Exception] = None
        for attempt in range(1, attempts + 1):
            self._apply_rate_limit()
            try:
                response = self.session.request(
                    method,
                    url,
                    params=params,
                    data=data,
                    timeout=self.request_timeout,
                )
                status = response.status_code
                if status in self.RETRYABLE_STATUS_CODES and attempt < attempts:
                    retry_after = response.headers.get("Retry-After")
                    try:
                        delay = float(retry_after) if retry_after else 0.0
                    except ValueError:
                        try:
                            retry_at = parsedate_to_datetime(str(retry_after))
                            if retry_at.tzinfo is None:
                                retry_at = retry_at.replace(tzinfo=timezone.utc)
                            delay = max(0.0, retry_at.timestamp() - time.time())
                        except (TypeError, ValueError, OverflowError):
                            delay = 0.0
                    delay = max(delay, min(60.0, (2 ** (attempt - 1)) + random.random()))
                    time.sleep(delay)
                    continue
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict):
                    raise NHTSARequestError("NHTSA returned a non-object JSON response", status)
                return payload, status
            except NHTSARequestError:
                raise
            except requests.RequestException as exc:
                last_error = exc
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status not in self.RETRYABLE_STATUS_CODES or attempt >= attempts:
                    raise NHTSARequestError(str(exc), status) from exc
            except (TypeError, ValueError) as exc:
                raise NHTSARequestError(f"Invalid JSON response from NHTSA: {exc}") from exc
        raise NHTSARequestError(str(last_error or "NHTSA request failed"))

    @staticmethod
    def _results(response: Optional[dict[str, Any]]) -> list[dict[str, Any]]:
        if not response:
            return []
        values = response.get("Results", response.get("results", []))
        return values if isinstance(values, list) else []

    def decode_vin(self, vin: str, model_year: Optional[Any] = None) -> Optional[Dict]:
        vin = self._normalise_vin(vin)
        if not self._is_valid_vin(vin):
            self.logger.warning("Invalid VIN format: %s", vin)
            return None
        params: dict[str, Any] = {"format": "json"}
        year = self._model_year(model_year)
        if year is not None:
            params["modelyear"] = year
        try:
            payload, _ = self._request_json(
                "GET",
                f"{self.BASE_URL}{self.DECODE_ENDPOINT}{quote(vin, safe='*')}",
                params=params,
            )
            return payload
        except NHTSARequestError as exc:
            self.logger.error("Error decoding VIN %s: %s", vin, exc)
            return None

    def decode_vins_batch(
        self,
        vins: List[str],
        model_years: Optional[dict[str, Any]] = None,
    ) -> Optional[Dict]:
        valid: list[str] = []
        payload_items: list[str] = []
        model_years = model_years or {}
        for value in vins[: self.MAX_BATCH_SIZE]:
            vin = self._normalise_vin(value)
            if not self._is_valid_vin(vin):
                continue
            valid.append(vin)
            year = self._model_year(model_years.get(vin))
            payload_items.append(f"{vin},{year}" if year is not None else vin)
        if not valid:
            return None
        payload = {"data": ";".join(payload_items), "format": "json"}
        try:
            response, _ = self._request_json(
                "POST",
                f"{self.BASE_URL}{self.BATCH_DECODE_ENDPOINT}",
                data=payload,
            )
            return response
        except NHTSARequestError as exc:
            self.logger.error("Error decoding VIN batch of %d: %s", len(valid), exc)
            return None

    def extract_specs_from_results(self, results: List[Dict] | Dict) -> Dict:
        """Project stable compatibility fields from one decoded result."""
        result = results[0] if isinstance(results, list) and results else results
        result = result if isinstance(result, dict) else {}
        return {f"nhtsa_{field}": result.get(field) for field in self.COMPATIBILITY_FIELDS}

    def _query_path(self, base_url: str, model_year: int, make: str, model: str) -> str:
        return (
            f"{base_url}modelyear/{quote(str(model_year), safe='')}/"
            f"make/{quote(make, safe='')}/model/{quote(model, safe='')}"
        )

    def get_safety_ratings(self, model_year: str, make: str, model: str) -> Optional[Dict]:
        try:
            response, _ = self._request_json(
                "GET",
                self._query_path(self.RATINGS_BASE_URL, int(model_year), make, model),
                params={"format": "json"},
            )
            return response
        except (ValueError, NHTSARequestError) as exc:
            self.logger.error("Error querying safety variants for %s %s %s: %s", model_year, make, model, exc)
            return None

    def get_recalls(self, make: str, model: str, model_year: str) -> Optional[Dict]:
        try:
            response, _ = self._request_json(
                "GET",
                f"{self.RECALLS_BASE_URL}recallsByVehicle",
                params={"make": make, "model": model, "modelYear": int(model_year), "format": "json"},
            )
            return response
        except (ValueError, NHTSARequestError) as exc:
            self.logger.error("Error querying recalls for %s %s %s: %s", model_year, make, model, exc)
            return None

    def get_complaints(self, make: str, model: str, model_year: str) -> Optional[Dict]:
        try:
            response, _ = self._request_json(
                "GET",
                f"{self.COMPLAINTS_BASE_URL}complaintsByVehicle",
                params={"make": make, "model": model, "modelYear": int(model_year), "format": "json"},
            )
            return response
        except (ValueError, NHTSARequestError) as exc:
            self.logger.error("Error querying complaints for %s %s %s: %s", model_year, make, model, exc)
            return None

    @staticmethod
    def _join_top(values: Iterable[Any], limit: int = 3) -> str:
        counts = Counter(str(value).strip() for value in values if str(value).strip())
        ordered = sorted(counts.items(), key=lambda item: (-item[1], item[0]))
        return "; ".join(value for value, _ in ordered[:limit])

    @staticmethod
    def _date_key(value: Any) -> tuple[int, str]:
        text = str(value or "").strip()
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
            try:
                return (1, datetime.strptime(text, fmt).date().isoformat())
            except ValueError:
                continue
        return (0, text)

    @staticmethod
    def _number(value: Any) -> int:
        try:
            return int(float(value or 0))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _boolean(value: Any) -> bool:
        return str(value).strip().lower() in {"1", "true", "yes", "y", "t"}

    def extract_ratings_data(self, results: List[Dict]) -> Dict:
        details = [row for row in results if isinstance(row, dict)]
        return {
            "nhtsa_safety_ratings_count": len(details),
            "nhtsa_overall_rating": "; ".join(sorted({
                str(row.get("OverallRating", "")).strip()
                for row in details if self._usable(row.get("OverallRating"))
            })),
            "nhtsa_front_crash_rating": "; ".join(sorted({
                str(row.get("OverallFrontCrashRating", row.get("FrontCrashRating", ""))).strip()
                for row in details
                if self._usable(row.get("OverallFrontCrashRating", row.get("FrontCrashRating")))
            })),
            "nhtsa_rollover_rating": "; ".join(sorted({
                str(row.get("RolloverRating", "")).strip()
                for row in details if self._usable(row.get("RolloverRating"))
            })),
            "nhtsa_side_crash_rating": "; ".join(sorted({
                str(row.get("OverallSideCrashRating", row.get("SideCrashRating", ""))).strip()
                for row in details
                if self._usable(row.get("OverallSideCrashRating", row.get("SideCrashRating")))
            })),
        }

    def extract_recalls_data(self, results: List[Dict]) -> Dict:
        records = [row for row in results if isinstance(row, dict)]
        dates = [row.get("ReportReceivedDate") for row in records if self._usable(row.get("ReportReceivedDate"))]
        latest_date = max(dates, key=self._date_key) if dates else None
        return {
            "nhtsa_total_recalls": len(records),
            "nhtsa_recall_components": self._join_top(row.get("Component") for row in records),
            "nhtsa_latest_recall_date": latest_date,
        }

    def extract_complaints_data(self, results: List[Dict]) -> Dict:
        records = [row for row in results if isinstance(row, dict)]
        return {
            "nhtsa_total_complaints": len(records),
            "nhtsa_complaint_injuries": sum(self._number(row.get("numberOfInjuries")) for row in records),
            "nhtsa_complaint_deaths": sum(self._number(row.get("numberOfDeaths")) for row in records),
            "nhtsa_complaint_crash_related": sum(self._boolean(row.get("crash")) for row in records),
            "nhtsa_complaint_fire_related": sum(self._boolean(row.get("fire")) for row in records),
            "nhtsa_common_complaint_areas": self._join_top(row.get("components") for row in records),
        }

    def _listing_identity(self, context: dict[str, Any]) -> dict[str, Any]:
        title = self._text(context.get("title") or context.get("vehicle_title"))
        year = self._model_year(
            context.get("listing_model_year")
            or context.get("modelYear")
            or context.get("model_year")
        )
        if year is None:
            match = re.search(r"\b(19[8-9]\d|20[0-3]\d)\b", title)
            year = int(match.group(1)) if match else None

        make = self._text(context.get("listing_make") or context.get("make"))
        if not make:
            make_matches = [make_name for make_name in self.COMMON_MAKES if make_name in title]
            make = max(make_matches, key=len) if make_matches else ""

        model = self._text(context.get("listing_model") or context.get("model"))
        if not model and make:
            remainder = title.replace(make, " ")
            if year is not None:
                remainder = remainder.replace(str(year), " ")
            tokens = [token for token in re.split(r"\s+", remainder) if token]
            model = tokens[0] if tokens else ""

        return {"make": make, "model": model, "model_year": year}

    def _resolve_identity(self, result: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
        listing = self._listing_identity(context)
        nhtsa = {
            "make": self._text(result.get("Make")),
            "model": self._text(result.get("Model")),
            "model_year": self._model_year(result.get("ModelYear")),
        }
        resolved: dict[str, Any] = {}
        sources: dict[str, str] = {}
        conflicts: list[str] = []
        for field in ("make", "model", "model_year"):
            nhtsa_value = nhtsa[field]
            listing_value = listing[field]
            if self._usable(nhtsa_value):
                resolved[field] = nhtsa_value
                sources[field] = "nhtsa_decode"
            elif self._usable(listing_value):
                resolved[field] = listing_value
                sources[field] = "listing"
            else:
                resolved[field] = None
                sources[field] = "unknown"
            if self._usable(nhtsa_value) and self._usable(listing_value):
                if self._text(nhtsa_value) != self._text(listing_value):
                    conflicts.append(field)
        populated = sum(self._usable(resolved[field]) for field in ("make", "model", "model_year"))
        nhtsa_populated = sum(self._usable(nhtsa[field]) for field in ("make", "model", "model_year"))
        confidence = "high" if nhtsa_populated == 3 else "medium" if populated >= 2 else "low" if populated else "unknown"
        return {
            "nhtsa_make": nhtsa["make"] or None,
            "nhtsa_model": nhtsa["model"] or None,
            "nhtsa_model_year": nhtsa["model_year"],
            "listing_make": listing["make"] or None,
            "listing_model": listing["model"] or None,
            "listing_model_year": listing["model_year"],
            "resolved_make": resolved["make"],
            "resolved_model": resolved["model"],
            "resolved_model_year": resolved["model_year"],
            "make_source": sources["make"],
            "model_source": sources["model"],
            "model_year_source": sources["model_year"],
            "confidence": confidence,
            "conflict_flag": int(bool(conflicts)),
            "conflict_fields": conflicts,
        }

    @staticmethod
    def _mmy_key(identity: dict[str, Any]) -> Optional[str]:
        if not all(identity.get(field) for field in ("resolved_make", "resolved_model", "resolved_model_year")):
            return None
        return "|".join([
            str(identity["resolved_model_year"]),
            str(identity["resolved_make"]).strip().upper(),
            str(identity["resolved_model"]).strip().upper(),
        ])

    def _cached_mmy_bundle(self, key: str) -> Optional[dict[str, Any]]:
        safety = self.nhtsa.get_latest_vehicle_query("safety_variants", key, max_age_days=self.refresh_days)
        recalls = self.nhtsa.get_latest_vehicle_query("recalls", key, max_age_days=self.refresh_days)
        complaints = self.nhtsa.get_latest_vehicle_query("complaints", key, max_age_days=self.refresh_days)
        if not safety or not recalls or not complaints:
            return None
        variant_ids = self.nhtsa.get_safety_variant_ids(safety["query_id"])
        details = self.nhtsa.get_safety_details(safety["query_id"])
        summary = self.extract_ratings_data(details)
        summary["nhtsa_safety_vehicle_ids"] = ";".join(str(vehicle_id) for vehicle_id in variant_ids)
        summary["nhtsa_safety_status"] = "success" if not variant_ids or details else "partial"
        summary.update(self.extract_recalls_data(self.nhtsa.get_recall_records(recalls["query_id"])))
        summary["nhtsa_recalls_status"] = recalls["response_status"]
        summary.update(self.extract_complaints_data(self.nhtsa.get_complaint_records(complaints["query_id"])))
        summary["nhtsa_complaints_status"] = complaints["response_status"]
        return summary

    def _store_query_failure(
        self,
        run_id: str,
        query_type: str,
        key: str,
        identity: dict[str, Any],
        error: NHTSARequestError,
        vehicle_id: Optional[int] = None,
    ) -> int:
        return self.nhtsa.store_vehicle_query(
            run_id, query_type, key, {"error": str(error)},
            make=str(identity.get("resolved_make") or ""),
            model=str(identity.get("resolved_model") or ""),
            model_year=identity.get("resolved_model_year"),
            vehicle_id=vehicle_id,
            response_status="request_failed", http_status=error.status_code,
            error_text=str(error),
        )

    def _query_mmy(self, run_id: str, key: str, identity: dict[str, Any]) -> dict[str, Any]:
        with self.cache_lock:
            cached = self.cache_safety.get(key)
            if cached is not None:
                return cached
        cached = None if self.refresh_all_active else self._cached_mmy_bundle(key)
        if cached is not None:
            with self.cache_lock:
                self.cache_safety[key] = cached
            return cached

        year = int(identity["resolved_model_year"])
        make = str(identity["resolved_make"])
        model = str(identity["resolved_model"])
        summary: dict[str, Any] = {}

        try:
            safety_response, safety_status = self._request_json(
                "GET",
                self._query_path(self.RATINGS_BASE_URL, year, make, model),
                params={"format": "json"},
            )
            safety_query_id = self.nhtsa.store_vehicle_query(
                run_id, "safety_variants", key, safety_response,
                make=make, model=model, model_year=year, http_status=safety_status,
                response_status="success" if self._results(safety_response) else "empty",
            )
            variants = self._results(safety_response)
            self.nhtsa.store_safety_variants(safety_query_id, variants)
            details: list[dict[str, Any]] = []
            detail_failures = 0
            for variant in sorted(variants, key=lambda item: int(item.get("VehicleId", 0) or 0)):
                vehicle_id = variant.get("VehicleId")
                if vehicle_id in (None, ""):
                    continue
                try:
                    detail_response, detail_http_status = self._request_json(
                        "GET",
                        f"{self.RATINGS_BASE_URL}VehicleId/{quote(str(vehicle_id), safe='')}",
                        params={"format": "json"},
                    )
                    detail_query_key = f"{key}|{int(vehicle_id)}"
                    detail_query_id = self.nhtsa.store_vehicle_query(
                        run_id, "safety_detail", detail_query_key, detail_response,
                        make=make, model=model, model_year=year,
                        vehicle_id=int(vehicle_id), http_status=detail_http_status,
                        response_status="success" if self._results(detail_response) else "empty",
                    )
                    detail_results = self._results(detail_response)
                    detail = detail_results[0] if detail_results else detail_response
                    if isinstance(detail, dict):
                        details.append(detail)
                        self.nhtsa.store_safety_detail(safety_query_id, int(vehicle_id), detail)
                except NHTSARequestError as exc:
                    detail_failures += 1
                    self._store_query_failure(
                        run_id, "safety_detail", f"{key}|{int(vehicle_id)}", identity, exc,
                        vehicle_id=int(vehicle_id),
                    )
                    self.logger.error("Safety detail failed for VehicleId %s: %s", vehicle_id, exc)
            summary.update(self.extract_ratings_data(details))
            summary["nhtsa_safety_vehicle_ids"] = ";".join(
                str(row.get("VehicleId")) for row in variants if row.get("VehicleId") not in (None, "")
            )
            summary["nhtsa_safety_status"] = "partial" if detail_failures else "success"
        except NHTSARequestError as exc:
            self._store_query_failure(run_id, "safety_variants", key, identity, exc)
            summary.update({
                "nhtsa_safety_ratings_count": 0,
                "nhtsa_safety_status": "request_failed",
            })

        for query_type, extractor, status_name in (
            ("recalls", self.extract_recalls_data, "nhtsa_recalls_status"),
            ("complaints", self.extract_complaints_data, "nhtsa_complaints_status"),
        ):
            base_url = self.RECALLS_BASE_URL if query_type == "recalls" else self.COMPLAINTS_BASE_URL
            try:
                response, status = self._request_json(
                    "GET",
                    f"{base_url}{query_type}ByVehicle",
                    params={"make": make, "model": model, "modelYear": year, "format": "json"},
                )
                records = self._results(response)
                query_id = self.nhtsa.store_vehicle_query(
                    run_id, query_type, key, response, make=make, model=model,
                    model_year=year, http_status=status,
                    response_status="success" if records else "empty",
                )
                if query_type == "recalls":
                    self.nhtsa.store_recalls(query_id, records)
                else:
                    self.nhtsa.store_complaints(query_id, records)
                summary.update(extractor(records))
                summary[status_name] = "success" if records else "empty"
            except NHTSARequestError as exc:
                self._store_query_failure(run_id, query_type, key, identity, exc)
                summary[status_name] = "request_failed"

        with self.cache_lock:
            self.cache_safety[key] = summary
        return summary

    def _compatibility_for_failure(self, status: str, error: str = "") -> dict[str, Any]:
        return {
            "nhtsa_decode_status": status,
            "nhtsa_decode_error": error or None,
            "nhtsa_last_updated_at": self._now(),
        }

    def _process_result(
        self,
        run_id: str,
        context: dict[str, Any],
        result: Optional[dict[str, Any]],
        *,
        decode_id: Optional[int] = None,
        decode_status: str = "success",
        decode_error: str = "",
    ) -> tuple[dict[str, Any], bool]:
        vin = self._normalise_vin(context.get("vin"))
        if result is None:
            specs = self._compatibility_for_failure(decode_status, decode_error)
            self.db.insert_nhtsa_enrichment_batch({vin: specs})
            return specs, False

        identity = self._resolve_identity(result, context)
        specs = self.extract_specs_from_results(result)
        specs.update({
            "nhtsa_decode_status": decode_status,
            "nhtsa_decode_error": result.get("ErrorText") or None,
            "nhtsa_decode_fetched_at": self._now(),
            "nhtsa_identity_source": ";".join([
                f"make:{identity['make_source']}",
                f"model:{identity['model_source']}",
                f"year:{identity['model_year_source']}",
            ]),
            "nhtsa_identity_confidence": identity["confidence"],
            "nhtsa_identity_conflict": identity["conflict_flag"],
            "nhtsa_source_run_id": run_id,
            "nhtsa_last_updated_at": self._now(),
        })
        key = self._mmy_key(identity)
        if key:
            specs.update(self._query_mmy(run_id, key, identity))
        else:
            specs.update({
                "nhtsa_safety_status": "missing_identity",
                "nhtsa_recalls_status": "missing_identity",
                "nhtsa_complaints_status": "missing_identity",
            })

        self.nhtsa.store_identity_resolution({
            "run_id": run_id,
            "decode_id": decode_id,
            "vin": vin,
            **identity,
            "resolved_at": self._now(),
        })
        self.db.insert_nhtsa_enrichment_batch({vin: specs})
        return specs, True

    def enrich_database(
        self,
        *,
        refresh_all: bool = False,
        resume: bool = True,
        max_vins: Optional[int] = None,
    ) -> int:
        contexts = self.db.get_vins_for_enrichment(include_listing_context=True)
        if max_vins is not None:
            contexts = contexts[: max(0, int(max_vins))]
        run_mode = "refresh_all" if refresh_all else "incremental"
        run_id = self.nhtsa.start_run(source="nhtsa_vehicle_enrichment", mode=run_mode)
        requested = len(contexts)
        successful = 0
        failed = 0
        self.logger.info("Starting NHTSA run %s for %d VINs", run_id, requested)
        print(f"Querying NHTSA for {requested} VINs; run={run_id}")

        try:
            pending: list[dict[str, Any]] = []
            for context in contexts:
                vin = self._normalise_vin(context.get("vin"))
                status = self._vin_status(vin)
                if status == "invalid":
                    self.nhtsa.store_vpic_failure(
                        run_id, vin, model_year_hint=self._model_year(context.get("listing_model_year")),
                        status="invalid_vin", error_text="VIN failed format validation",
                    )
                    self._process_result(run_id, context, None, decode_status="invalid_vin", decode_error="VIN failed format validation")
                    failed += 1
                    continue

                hint = self._model_year(context.get("listing_model_year"))
                cached = None if (refresh_all or not resume) else self.nhtsa.get_latest_vpic_record(
                    vin, hint, max_age_days=self.refresh_days
                )
                if cached is not None:
                    _, ok = self._process_result(
                        run_id, context, cached["result"], decode_id=cached["decode_id"]
                    )
                    successful += int(ok)
                else:
                    pending.append(context)

            for offset in range(0, len(pending), self.MAX_BATCH_SIZE):
                batch = pending[offset : offset + self.MAX_BATCH_SIZE]
                model_years = {
                    self._normalise_vin(item.get("vin")): self._model_year(item.get("listing_model_year"))
                    for item in batch
                }
                request_payload = {
                    "data": ";".join(
                        f"{self._normalise_vin(item.get('vin'))},{model_years[self._normalise_vin(item.get('vin'))]}"
                        if model_years[self._normalise_vin(item.get("vin"))] is not None
                        else self._normalise_vin(item.get("vin"))
                        for item in batch
                    ),
                    "format": "json",
                }
                try:
                    response, _ = self._request_json(
                        "POST",
                        f"{self.BASE_URL}{self.BATCH_DECODE_ENDPOINT}",
                        data=request_payload,
                    )
                    by_vin: dict[str, dict[str, Any]] = {}
                    for result in self._results(response):
                        result_vin = self._normalise_vin(result.get("VIN"))
                        if result_vin:
                            by_vin[result_vin] = result
                            decode_id = self.nhtsa.store_vpic_result(
                                run_id, result_vin, result,
                                model_year_hint=model_years.get(result_vin),
                                request_payload=request_payload,
                                response=response,
                            )
                            result["_decode_id"] = decode_id

                    for context in batch:
                        vin = self._normalise_vin(context.get("vin"))
                        result = by_vin.get(vin)
                        if result is None:
                            error = "VIN was absent from the successful batch response"
                            self.nhtsa.store_vpic_failure(
                                run_id, vin, model_year_hint=model_years.get(vin),
                                status="missing_result", error_text=error,
                                request_payload=request_payload,
                            )
                            self._process_result(run_id, context, None, decode_status="missing_result", decode_error=error)
                            failed += 1
                            continue
                        _, ok = self._process_result(
                            run_id, context, result, decode_id=result.get("_decode_id")
                        )
                        successful += int(ok)
                        failed += int(not ok)
                except NHTSARequestError as exc:
                    for context in batch:
                        vin = self._normalise_vin(context.get("vin"))
                        self.nhtsa.store_vpic_failure(
                            run_id, vin, model_year_hint=model_years.get(vin),
                            status="request_failed", error_text=str(exc),
                            http_status=exc.status_code, request_payload=request_payload,
                        )
                        self._process_result(run_id, context, None, decode_status="request_failed", decode_error=str(exc))
                        failed += 1
                print(f"[{min(offset + len(batch), len(pending))}/{len(pending)}] VIN batches processed")

            self.nhtsa.finish_run(
                run_id,
                "completed" if failed == 0 else "completed_with_errors",
                requested_count=requested,
                successful_count=successful,
                failed_count=failed,
            )
            return successful
        except Exception:
            self.nhtsa.finish_run(
                run_id, "failed", requested_count=requested,
                successful_count=successful, failed_count=failed,
            )
            raise

    def enrich_data_from_csv(self, input_csv: str, output_csv: Optional[str] = None) -> pd.DataFrame:
        """Legacy CSV interface backed by the corrected API helpers."""
        df = pd.read_csv(input_csv)
        if "vin" not in df.columns:
            raise ValueError("Input CSV must contain a vin column")
        rows: dict[str, dict[str, Any]] = {}
        run_id = self.nhtsa.start_run(source="legacy_csv_enrichment", mode="csv")
        try:
            for value in df["vin"].dropna().unique().tolist():
                vin = self._normalise_vin(value)
                if not self._is_valid_vin(vin):
                    continue
                response = self.decode_vin(vin)
                api_results = self._results(response)
                result = api_results[0] if api_results else None
                rows[vin] = self.extract_specs_from_results(result or {})
                if result:
                    identity = self._resolve_identity(result, {"vin": vin})
                    key = self._mmy_key(identity)
                    if key:
                        rows[vin].update(self._query_mmy(run_id, key, identity))
            self.nhtsa.finish_run(
                run_id, "completed", requested_count=len(rows),
                successful_count=len(rows), failed_count=0,
            )
        except Exception:
            self.nhtsa.finish_run(run_id, "failed", requested_count=len(rows), successful_count=0, failed_count=1)
            raise
        enriched = df.merge(
            pd.DataFrame.from_dict(rows, orient="index").rename_axis("vin").reset_index(),
            on="vin", how="left",
        )
        if output_csv is None:
            output_csv = os.path.join(self.output_dir, f"ENRICHED_CAR_DATA_{date.today()}.csv")
        enriched.to_csv(output_csv, index=False)
        return enriched

    def get_latest_car_data_file(self) -> str:
        files = [
            name for name in os.listdir(self.output_dir)
            if name.startswith("CAR_DATA_") and name.endswith(".csv")
        ]
        if not files:
            raise FileNotFoundError("No CAR_DATA CSV files found")
        return max(files, key=lambda name: os.path.getctime(os.path.join(self.output_dir, name)))

    def close(self) -> None:
        self.nhtsa.close()
        self.db.close()
        self.session.close()
        if self.log_handler is not None:
            self.logger.removeHandler(self.log_handler)
            self.log_handler.close()
            self.log_handler = None

    def run(self, *, refresh_all: bool = False, resume: bool = True, max_vins: Optional[int] = None) -> int:
        self.refresh_all_active = refresh_all
        try:
            return self.enrich_database(refresh_all=refresh_all, resume=resume, max_vins=max_vins)
        finally:
            self.refresh_all_active = False


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    repo_root = Path(__file__).resolve().parent.parent
    parser.add_argument("--db-path", type=Path, default=repo_root / "CAR_DATA_OUTPUT" / "CAR_DATA.db")
    parser.add_argument("--nhtsa-db-path", type=Path, default=repo_root / "CAR_DATA_OUTPUT" / "CAR_DATA_NHTSA.db")
    parser.add_argument("--output-dir", type=Path, default=repo_root / "CAR_DATA_OUTPUT")
    parser.add_argument("--refresh-all", action="store_true", help="Refresh every VIN and every MMY query")
    parser.add_argument(
        "--backfill-legacy",
        action="store_true",
        help="Refresh every historical VIN with the new process; alias for --refresh-all",
    )
    parser.add_argument("--resume", action="store_true", default=True, help="Resume from cached successful responses")
    parser.add_argument("--no-resume", action="store_false", dest="resume")
    parser.add_argument("--max-vins", type=int, default=None, help="Bound a development run to this many VINs")
    parser.add_argument("--refresh-days", type=int, default=30)
    parser.add_argument("--rate-limit-delay", type=float, default=1.0)
    parser.add_argument("--max-workers", type=int, default=1, help="Bounded concurrency setting; default is serial")
    parser.add_argument(
        "--backup-path",
        type=Path,
        default=None,
        help="Create a one-time SQLite backup before opening CAR_DATA.db; destination must not exist",
    )
    parser.add_argument("--bulk-file", type=Path, action="append", default=[], help="Optional NHTSA CSV/JSON/TXT/ZIP file to archive")
    parser.add_argument("--bulk-dataset-name", default="local_bulk_file")
    args = parser.parse_args()

    enricher = NHTSAEnricher(
        rate_limit_delay=args.rate_limit_delay,
        output_dir=str(args.output_dir),
        db_path=str(args.db_path),
        nhtsa_db_path=str(args.nhtsa_db_path),
        max_workers=args.max_workers,
        refresh_days=args.refresh_days,
        backup_path=str(args.backup_path) if args.backup_path else None,
    )
    try:
        for bulk_file in args.bulk_file:
            rows = enricher.nhtsa.ingest_bulk_file(bulk_file, args.bulk_dataset_name)
            print(f"Archived {rows:,} rows from {bulk_file}")
        count = enricher.run(
            refresh_all=args.refresh_all or args.backfill_legacy,
            resume=args.resume,
            max_vins=args.max_vins,
        )
        print(f"NHTSA enrichment completed. Successful VINs: {count:,}")
    finally:
        enricher.close()


if __name__ == "__main__":
    main()
