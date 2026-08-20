"""Pure helpers for custom vehicle price and depreciation scenarios.

The Streamlit dashboard uses this module to assemble a complete model row from
the small set of vehicle attributes a user is likely to know.  No Streamlit
state, database connections, or model artifacts are handled here so the
inference rules remain directly testable.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd


SCENARIO_EXCLUDED_COLUMNS = {
    "price",
    "price_band",
    "nhtsa_BasePrice",
    "nhtsa_BasePrice_source",
    "vin",
    "date",
    "loaddate",
    "rowid",
}

TRANSMISSION_COLUMNS = ["nhtsa_TransmissionStyle", "nhtsa_TransmissionSpeeds"]
ENGINE_COLUMNS = [
    "nhtsa_EngineModel",
    "nhtsa_EngineConfiguration",
    "nhtsa_DisplacementL",
    "nhtsa_EngineHP",
    "nhtsa_EngineHP_to",
    "nhtsa_EngineKW",
    "nhtsa_EngineCylinders",
    "nhtsa_EngineManufacturer",
    "nhtsa_OtherEngineInfo",
]

DISPLAY_SCENARIO_FIELDS = [
    "canonical_make",
    "canonical_model",
    "canonical_year",
    "canonical_trim",
    "mileage",
    *TRANSMISSION_COLUMNS,
    *ENGINE_COLUMNS,
    "nhtsa_BodyClass",
    "nhtsa_DriveType",
    "nhtsa_FuelTypePrimary",
    "nhtsa_ElectrificationLevel",
    "nhtsa_total_recalls",
    "nhtsa_total_complaints",
    "sellerType",
    "sourceName",
]


def normalize_scenario_key(value: Any, fallback: str = "UNKNOWN") -> str:
    """Normalize identity/profile labels without changing stored source values."""
    if value is None or pd.isna(value):
        return fallback
    text = str(value).strip()
    if not text:
        return fallback
    return text.upper().replace(" ", "_")


def _stable_sort_key(value: Any) -> tuple[str, str]:
    return (type(value).__name__, str(value))


def deterministic_mode(series: pd.Series) -> tuple[Any, int]:
    """Return a deterministic non-null mode and its support count."""
    if series is None:
        return None, 0
    values = series.dropna()
    if values.empty:
        return None, 0
    if values.dtype == object or pd.api.types.is_string_dtype(values.dtype):
        values = values.astype(str).str.strip()
        values = values[values.ne("")]
    if values.empty:
        return None, 0
    counts = values.value_counts(dropna=True)
    top_count = int(counts.max())
    candidates = [value for value, count in counts.items() if int(count) == top_count]
    selected = sorted(candidates, key=_stable_sort_key)[0]
    return selected, top_count


def latest_per_vin(frame: pd.DataFrame) -> pd.DataFrame:
    """Deduplicate a raw scenario pool so snapshots do not overweight modes."""
    if frame.empty or "vin" not in frame.columns:
        return frame.copy()
    result = frame.copy()
    sort_columns = [column for column in ["vin", "loaddate", "date"] if column in result.columns]
    if sort_columns:
        result = result.sort_values(sort_columns, kind="mergesort")
    return result.drop_duplicates("vin", keep="last").reset_index(drop=True)


def select_scenario_pool(
    frame: pd.DataFrame,
    make: str,
    model: str,
    model_year: int,
    trim: str | None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Select an exact trim cohort, then a same-year make/model fallback."""
    if frame.empty:
        return frame.copy(), {
            "match_scope": "unavailable",
            "fallback_used": False,
            "support_rows": 0,
        }

    latest = latest_per_vin(frame)
    make_key = normalize_scenario_key(make)
    model_key = normalize_scenario_key(model)
    year_values = pd.to_numeric(latest.get("canonical_year"), errors="coerce")
    base_mask = (
        latest.get("canonical_make", pd.Series(index=latest.index)).map(normalize_scenario_key).eq(make_key)
        & latest.get("canonical_model", pd.Series(index=latest.index)).map(normalize_scenario_key).eq(model_key)
        & year_values.eq(int(model_year))
    )
    year_pool = latest.loc[base_mask].copy()

    selected_trim_key = normalize_scenario_key(trim, "UNKNOWN_TRIM") if trim else None
    if selected_trim_key and not year_pool.empty and "canonical_trim" in year_pool.columns:
        exact_mask = year_pool["canonical_trim"].map(normalize_scenario_key).eq(selected_trim_key)
        exact_pool = year_pool.loc[exact_mask].copy()
        if not exact_pool.empty:
            return exact_pool.reset_index(drop=True), {
                "match_scope": "exact make/model/year/trim",
                "fallback_used": False,
                "support_rows": int(exact_pool.shape[0]),
            }

    if not year_pool.empty:
        fallback_used = bool(selected_trim_key)
        return year_pool.reset_index(drop=True), {
            "match_scope": "make/model/year fallback" if fallback_used else "make/model/year cohort",
            "fallback_used": fallback_used,
            "support_rows": int(year_pool.shape[0]),
        }
    return year_pool, {
        "match_scope": "unavailable",
        "fallback_used": bool(selected_trim_key),
        "support_rows": 0,
    }


def infer_modal_profile(
    frame: pd.DataFrame,
    excluded_columns: set[str] | None = None,
) -> tuple[dict[str, Any], pd.DataFrame]:
    """Infer each available field independently from its deterministic mode."""
    excluded = SCENARIO_EXCLUDED_COLUMNS | set(excluded_columns or set())
    row: dict[str, Any] = {}
    records: list[dict[str, Any]] = []
    for column in frame.columns:
        if column in excluded:
            continue
        value, support = deterministic_mode(frame[column])
        row[column] = value
        records.append(
            {
                "field": column,
                "value": value,
                "source": "cohort mode" if support else "unavailable / model imputation",
                "support_count": int(support),
            }
        )
    return row, pd.DataFrame(records, columns=["field", "value", "source", "support_count"])


def format_transmission_profile(row: pd.Series | dict[str, Any]) -> str:
    style = row.get("nhtsa_TransmissionStyle") if hasattr(row, "get") else None
    speeds = pd.to_numeric(row.get("nhtsa_TransmissionSpeeds"), errors="coerce") if hasattr(row, "get") else np.nan
    style_text = str(style).strip() if pd.notna(style) and str(style).strip() else "Unknown transmission"
    speed_text = f"{int(speeds)} speed" if pd.notna(speeds) and speeds > 0 else ""
    return " / ".join(part for part in [style_text, speed_text] if part)


def format_engine_profile(row: pd.Series | dict[str, Any]) -> str:
    values = []
    for column in ["nhtsa_EngineModel", "nhtsa_EngineConfiguration"]:
        value = row.get(column) if hasattr(row, "get") else None
        if pd.notna(value) and str(value).strip():
            values.append(str(value).strip())
    displacement = pd.to_numeric(row.get("nhtsa_DisplacementL"), errors="coerce") if hasattr(row, "get") else np.nan
    horsepower = pd.to_numeric(row.get("nhtsa_EngineHP"), errors="coerce") if hasattr(row, "get") else np.nan
    cylinders = pd.to_numeric(row.get("nhtsa_EngineCylinders"), errors="coerce") if hasattr(row, "get") else np.nan
    if pd.notna(displacement):
        values.append(f"{float(displacement):g}L")
    if pd.notna(horsepower):
        values.append(f"{int(horsepower)} hp")
    if pd.notna(cylinders):
        values.append(f"{int(cylinders)} cyl")
    return " / ".join(values) if values else "Unknown engine"


def _profile_options(
    frame: pd.DataFrame,
    profile_kind: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    if frame.empty:
        return []
    labels = (
        frame.apply(format_transmission_profile, axis=1)
        if profile_kind == "transmission"
        else frame.apply(format_engine_profile, axis=1)
    )
    valid = labels.ne("Unknown transmission") if profile_kind == "transmission" else labels.ne("Unknown engine")
    if not bool(valid.any()):
        return []
    working = frame.loc[valid].copy()
    working["_profile_label"] = labels.loc[valid]
    counts = working["_profile_label"].value_counts()
    result: list[dict[str, Any]] = []
    columns = TRANSMISSION_COLUMNS if profile_kind == "transmission" else ENGINE_COLUMNS
    for label, count in counts.sort_index().sort_values(ascending=False).head(limit).items():
        group = working.loc[working["_profile_label"].eq(label)]
        representative = group.iloc[0]
        result.append(
            {
                "label": str(label),
                "support_count": int(count),
                "values": {column: representative.get(column) for column in columns},
            }
        )
    return result


def transmission_profile_options(frame: pd.DataFrame, limit: int = 20) -> list[dict[str, Any]]:
    return _profile_options(frame, "transmission", limit)


def engine_profile_options(frame: pd.DataFrame, limit: int = 20) -> list[dict[str, Any]]:
    return _profile_options(frame, "engine", limit)


def _set_resolution_value(
    resolution: pd.DataFrame,
    field: str,
    value: Any,
    source: str,
    support_count: int,
) -> pd.DataFrame:
    if resolution.empty or not resolution["field"].eq(field).any():
        addition = pd.DataFrame(
            [{"field": field, "value": value, "source": source, "support_count": int(support_count)}]
        )
        return pd.concat([resolution, addition], ignore_index=True)
    result = resolution.copy()
    mask = result["field"].eq(field)
    result.loc[mask, "value"] = value
    result.loc[mask, "source"] = source
    result.loc[mask, "support_count"] = int(support_count)
    return result


def build_resolved_vehicle_row(
    pool: pd.DataFrame,
    make: str,
    model: str,
    model_year: int,
    trim: str | None,
    mileage: float,
    transmission_profile: dict[str, Any] | None = None,
    engine_profile: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    """Build one complete raw listing/NHTSA row and its provenance table."""
    if pool.empty:
        raise ValueError("No same-year cohort rows are available for this vehicle.")

    row, resolution = infer_modal_profile(pool)
    selected_trim = trim if trim else row.get("canonical_trim") or "UNKNOWN_TRIM"
    explicit_values = {
        "canonical_make": make,
        "canonical_model": model,
        "canonical_year": int(model_year),
        "canonical_trim": selected_trim,
        "mileage": float(mileage),
    }
    for field, value in explicit_values.items():
        row[field] = value
        resolution = _set_resolution_value(resolution, field, value, "user input", 0)

    if transmission_profile:
        for field in TRANSMISSION_COLUMNS:
            if field in transmission_profile.get("values", {}):
                value = transmission_profile["values"].get(field)
                row[field] = value
                resolution = _set_resolution_value(
                    resolution,
                    field,
                    value,
                    "user override",
                    int(transmission_profile.get("support_count", 0)),
                )
    if engine_profile:
        for field in ENGINE_COLUMNS:
            if field in engine_profile.get("values", {}):
                value = engine_profile["values"].get(field)
                row[field] = value
                resolution = _set_resolution_value(
                    resolution,
                    field,
                    value,
                    "user override",
                    int(engine_profile.get("support_count", 0)),
                )

    metadata = {
        "pool_rows": int(pool.shape[0]),
        "match_scope": "exact cohort" if trim else "cohort mode",
        "selected_trim": selected_trim,
    }
    return pd.DataFrame([row]), resolution.sort_values("field").reset_index(drop=True), metadata


def apply_monthly_scenario_overrides(
    latest_row: pd.DataFrame,
    resolved_raw_row: pd.DataFrame,
    mileage: float,
    as_of: pd.Timestamp | None = None,
) -> pd.DataFrame:
    """Apply user identity, mileage, and technical overrides to a cohort row."""
    if latest_row.empty:
        return latest_row.copy()
    result = latest_row.copy().reset_index(drop=True)
    raw = resolved_raw_row.iloc[0]
    model_year = int(pd.to_numeric(raw.get("canonical_year"), errors="coerce"))
    trim = normalize_scenario_key(raw.get("canonical_trim"), "UNKNOWN_TRIM")
    result.loc[:, "make"] = str(raw.get("canonical_make"))
    result.loc[:, "model"] = str(raw.get("canonical_model"))
    result.loc[:, "model_year"] = model_year
    result.loc[:, "trim_proxy"] = trim

    mileage_value = float(mileage)
    for column in ["avg_mileage", "median_mileage", "rolling_avg_mileage_3m"]:
        if column in result.columns:
            result.loc[:, column] = mileage_value

    current = pd.Timestamp(as_of) if as_of is not None else pd.Timestamp(datetime.now())
    age_months = max((current.year - model_year) * 12 + current.month - 1, 0)
    if "avg_vehicle_age_months" in result.columns:
        result.loc[:, "avg_vehicle_age_months"] = float(age_months)
    if "avg_miles_per_year" in result.columns:
        result.loc[:, "avg_miles_per_year"] = mileage_value / max(age_months / 12, 0.25)

    raw_to_monthly = {
        "nhtsa_BodyClass": "body_class",
        "nhtsa_DriveType": "drive_type",
        "nhtsa_FuelTypePrimary": "fuel_type",
        "nhtsa_ElectrificationLevel": "electrification_level",
        "sellerType": "dominant_seller_type",
        "sourceName": "dominant_source_name",
        "nhtsa_EngineHP": "engine_hp",
        "nhtsa_EngineCylinders": "engine_cylinders",
        "nhtsa_total_recalls": "total_recalls",
        "nhtsa_total_complaints": "total_complaints",
    }
    for source, target in raw_to_monthly.items():
        if target not in result.columns or source not in raw:
            continue
        value = raw.get(source)
        if pd.notna(value):
            result.loc[:, target] = value
    return result


def apply_price_anchor(frame: pd.DataFrame, anchor_price: float) -> pd.DataFrame:
    """Set observed price-state inputs to a scenario's starting price."""
    result = frame.copy()
    anchor = float(max(anchor_price, 0))
    for column in [
        "median_price",
        "avg_price",
        "price_p25",
        "price_p75",
        "lag_median_price_1",
        "rolling_median_price_3m",
    ]:
        if column in result.columns:
            result.loc[:, column] = anchor
    if "cohort_first_median_price" in result.columns:
        first = pd.to_numeric(result["cohort_first_median_price"], errors="coerce")
        result.loc[first.isna() | first.le(0), "cohort_first_median_price"] = anchor
    if {"price_index_vs_cohort_first", "median_price", "cohort_first_median_price"}.issubset(result.columns):
        result["price_index_vs_cohort_first"] = (
            result["median_price"] / result["cohort_first_median_price"].replace(0, np.nan)
        )
    if {"cumulative_depreciation_pct", "price_index_vs_cohort_first"}.issubset(result.columns):
        result["cumulative_depreciation_pct"] = result["price_index_vs_cohort_first"] - 1
    return result


def select_stored_reference_rows(
    frame: pd.DataFrame,
    selected_trim: str | None,
) -> tuple[pd.DataFrame, str]:
    """Prefer an exact stored trim, otherwise choose the best-supported trim."""
    if frame.empty or "trim_proxy" not in frame.columns:
        return frame.copy(), "Unavailable"
    working = frame.copy()
    working["_trim_key"] = working["trim_proxy"].map(normalize_scenario_key)
    selected_key = normalize_scenario_key(selected_trim) if selected_trim else None
    if selected_key:
        exact = working.loc[working["_trim_key"].eq(selected_key)].copy()
        if not exact.empty:
            return exact.drop(columns=["_trim_key"]).reset_index(drop=True), "Exact stored cohort"

    unique_vins = working["unique_vins"] if "unique_vins" in working.columns else pd.Series(0, index=working.index)
    volume = working["volume"] if "volume" in working.columns else pd.Series(0, index=working.index)
    working["_unique_vins"] = pd.to_numeric(unique_vins, errors="coerce").fillna(0)
    working["_volume"] = pd.to_numeric(volume, errors="coerce").fillna(0)
    support = (
        working.groupby("_trim_key", as_index=False)
        .agg(
            forecast_methods=("forecast_method", "nunique"),
            unique_vins=("_unique_vins", "max"),
            volume=("_volume", "max"),
        )
        .sort_values(
            ["forecast_methods", "unique_vins", "volume", "_trim_key"],
            ascending=[False, False, False, True],
        )
    )
    if support.empty:
        return frame.iloc[0:0].copy(), "Unavailable"
    chosen = support.iloc[0]["_trim_key"]
    selected = working.loc[working["_trim_key"].eq(chosen)].drop(
        columns=["_trim_key", "_unique_vins", "_volume"]
    )
    return selected.reset_index(drop=True), "Coverage fallback"


def select_latest_cohort_feature_row(
    frame: pd.DataFrame,
    make: str,
    model: str,
    model_year: int,
    trim: str | None,
) -> tuple[pd.DataFrame, str]:
    """Select the latest monthly feature row for an exact or fallback cohort."""
    if frame.empty:
        return frame.copy(), "Unavailable"
    working = frame.copy()
    year_values = pd.to_numeric(working.get("model_year"), errors="coerce")
    base = (
        working.get("make", pd.Series(index=working.index)).map(normalize_scenario_key).eq(normalize_scenario_key(make))
        & working.get("model", pd.Series(index=working.index)).map(normalize_scenario_key).eq(normalize_scenario_key(model))
        & year_values.eq(int(model_year))
    )
    year_pool = working.loc[base].copy()
    if year_pool.empty:
        return year_pool, "Unavailable"
    selected_key = normalize_scenario_key(trim) if trim else None
    if selected_key and "trim_proxy" in year_pool.columns:
        exact = year_pool.loc[year_pool["trim_proxy"].map(normalize_scenario_key).eq(selected_key)].copy()
        if not exact.empty:
            year_pool = exact
            label = "Exact monthly cohort"
        else:
            label = "Make/model/year monthly fallback"
    else:
        label = "Modal monthly cohort"
    if "month_start" in year_pool.columns:
        year_pool = year_pool.sort_values("month_start").tail(1)
    return year_pool.reset_index(drop=True), label
