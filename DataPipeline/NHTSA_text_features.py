"""Incremental, source-specific NHTSA text experiments and collected-time joins.

Source databases are read-only. The derived sidecar contains no copied narratives.
Zero-shot scores are uncalibrated topic evidence, not sentiment or failure risk.
"""
from __future__ import annotations

import argparse
import csv
from contextlib import closing
import hashlib
import json
import re
import sqlite3
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SOURCE_DB = ROOT / "CAR_DATA_OUTPUT" / "CAR_DATA_NHTSA.db"
FEATURE_DB = ROOT / "CAR_DATA_OUTPUT" / "CAR_NHTSA_TEXT_FEATURES.db"
TAXONOMY = "nhtsa-topics-v1-token-chunks-256-overlap-32"
LABELS = {
    "complaint": {"loss_of_propulsion": "the vehicle lost propulsion or stalled",
                  "loss_of_control": "the driver lost steering or braking control",
                  "recurring_failure": "the problem recurred after repair",
                  "repair_delay": "repair or replacement parts were delayed"},
    "recall_hazard": {"loss_of_propulsion": "a defect could cause loss of propulsion",
                      "loss_of_control": "a defect could impair steering or braking",
                      "fire_hazard": "a defect could cause a fire"},
    "recall_remedy": {"software_remedy": "the remedy requires a software update",
                      "replacement_remedy": "the remedy requires replacement of a part"},
}
TEXT_FEATURES = [f"nhtsa_{role}_{label}_score" for role, labels in LABELS.items() for label in labels]
STRUCTURED_FEATURES = [f"nhtsa_{kind}_{name}" for kind in ("complaints", "recalls")
                       for name in ("report_count", "text_coverage", "known")]
SEVERITY_FEATURES = [f"nhtsa_complaints_{name}_{suffix}" for name in ("crash", "fire", "injury", "death") for suffix in ("report_share", "known_count")]
FEATURE_COLUMNS = STRUCTURED_FEATURES + SEVERITY_FEATURES + TEXT_FEATURES


def readonly(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)


def identity(value) -> str:
    if value is None or pd.isna(value):
        return ""
    return re.sub(r"[^A-Z0-9]+", " ", str(value).upper()).strip()


def text_hash(text: str) -> str:
    return hashlib.sha256(" ".join(text.split()).encode("utf-8")).hexdigest()


def initialize(conn: sqlite3.Connection, model: str, revision: str) -> None:
    conn.execute("CREATE TABLE IF NOT EXISTS configuration (model TEXT, revision TEXT, taxonomy TEXT)")
    config = conn.execute("SELECT model, revision, taxonomy FROM configuration").fetchone()
    expected = (model, revision, TAXONOMY)
    if config and config != expected:
        raise ValueError("Sidecar configuration differs; use a separate --output-db for this experiment")
    if not config:
        conn.execute("INSERT INTO configuration VALUES (?,?,?)", expected)
    conn.execute("""CREATE TABLE IF NOT EXISTS text_scores (
        text_hash TEXT, role TEXT, label TEXT, score REAL NOT NULL,
        scored_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY(text_hash, role, label))""")
    columns = ", ".join(f'"{c}" REAL' for c in FEATURE_COLUMNS)
    conn.execute(f"""CREATE TABLE IF NOT EXISTS query_features (
        query_id INTEGER PRIMARY KEY, source TEXT NOT NULL, make TEXT NOT NULL,
        model TEXT NOT NULL, model_year INTEGER NOT NULL, available_at TEXT NOT NULL,
        response_status TEXT NOT NULL, {columns})""")
    conn.execute("CREATE INDEX IF NOT EXISTS feature_lookup ON query_features(make,model,model_year,source,available_at)")
    conn.commit()


def documents(source: sqlite3.Connection, query: dict) -> list[dict]:
    kind = query["query_type"]
    table = "nhtsa_complaints" if kind == "complaints" else "nhtsa_recalls"
    rows = [dict(row) for row in source.execute(f"SELECT * FROM {table} WHERE query_id=? ORDER BY record_key", (query["query_id"],))]
    grouped: dict[str, dict[str, set[str]]] = {}
    severity = {}
    for row in rows:
        identifier = row.get("odi_number") if kind == "complaints" else row.get("nhtsa_campaign_number")
        # Retain records without an official event identifier; do not equate text reuse with event identity.
        key = str(identifier or ("record:" + str(row["record_key"])))
        flags = severity.setdefault(key, {})
        for name, field in [("crash", "crash"), ("fire", "fire"), ("injury", "number_of_injuries"), ("death", "number_of_deaths")]:
            value = row.get(field)
            if value is not None and str(value).strip():
                if name in {"crash", "fire"}:
                    parsed = {"Y": 1, "YES": 1, "TRUE": 1, "1": 1, "N": 0, "NO": 0, "FALSE": 0, "0": 0}.get(str(value).upper())
                else:
                    number = pd.to_numeric(value, errors="coerce")
                    parsed = int(number > 0) if pd.notna(number) and number >= 0 else None
                if parsed is not None:
                    flags[name] = max(flags.get(name, 0), parsed)
        roles = grouped.setdefault(key, {})
        fields = {"complaint": ("summary",)} if kind == "complaints" else {
            "recall_hazard": ("summary", "consequence"), "recall_remedy": ("remedy",)}
        for role, names in fields.items():
            text = " ".join(str(row.get(name) or "").strip() for name in names).strip()
            if text:
                roles.setdefault(role, set()).add(text)
    return [{"event_id": key, "severity": severity[key], "texts": {role: "\n".join(sorted(texts)) for role, texts in roles.items()}}
            for key, roles in grouped.items()]


def score_text(classifier, text: str, role: str) -> dict[str, float]:
    # Token chunks cover the full narrative; max pooling is evidence retrieval, not calibration.
    tokens = classifier.tokenizer.encode(text, add_special_tokens=False)
    chunks = [classifier.tokenizer.decode(tokens[i:i + 256], skip_special_tokens=True)
              for i in range(0, len(tokens), 224)]
    labels = LABELS[role]
    scores = {name: 0.0 for name in labels}
    for chunk in chunks:
        result = classifier(chunk, candidate_labels=list(labels.values()), multi_label=True,
                            hypothesis_template="This text states that {}.")
        values = dict(zip(result["labels"], result["scores"]))
        for name, label in labels.items():
            scores[name] = max(scores[name], float(values[label]))
    return scores


def create_classifier(model: str, revision: str, device: int):
    from transformers import pipeline
    return pipeline("zero-shot-classification", model=model, revision=revision, device=device)


def build_query_features(query: dict, docs: list[dict], output: sqlite3.Connection) -> dict:
    values = dict.fromkeys(FEATURE_COLUMNS)
    kind = query["query_type"]
    known = query["response_status"] in {"success", "empty"}
    values[f"nhtsa_{kind}_known"] = int(known)
    if known:
        values[f"nhtsa_{kind}_report_count"] = len(docs)
        if kind == "complaints":
            for name in ("crash", "fire", "injury", "death"):
                observed = [doc.get("severity", {}).get(name) for doc in docs]
                observed = [value for value in observed if value is not None]
                values[f"nhtsa_complaints_{name}_known_count"] = len(observed)
                values[f"nhtsa_complaints_{name}_report_share"] = float(np.mean(observed)) if observed else None
        completed = 0
        accum: dict[str, list[float]] = {}
        for doc in docs:
            complete = bool(doc["texts"])
            for role, text in doc["texts"].items():
                cached = dict(output.execute("SELECT label,score FROM text_scores WHERE text_hash=? AND role=?",
                                             (text_hash(text), role)))
                complete &= set(cached) == set(LABELS[role])
                for label, score in cached.items():
                    accum.setdefault(f"nhtsa_{role}_{label}_score", []).append(score)
            completed += int(complete)
        values[f"nhtsa_{kind}_text_coverage"] = completed / len(docs) if docs else None
        for name, scores in accum.items():
            values[name] = float(np.mean(scores)) if completed == len(docs) else None
    return values


def attach_features(frame: pd.DataFrame, date_column: str, path: Path = FEATURE_DB) -> pd.DataFrame:
    """Join exact normalized MMY at the start of the observation month.

    Strictly earlier collection timestamps only; no VIN or trim attribution.
    An unsuccessful latest query remains unknown instead of becoming zero.
    """
    frame = frame.copy(deep=False)
    for col in FEATURE_COLUMNS:
        frame[col] = np.full(len(frame), np.nan, dtype=np.float32)
    frame.attrs["nhtsa_features"] = {"available": False, "mode": "collected_before_observation_month"}
    required = {"canonical_make", "canonical_model", "canonical_year", date_column}
    if not required.issubset(frame.columns):
        frame.attrs["nhtsa_features"]["reason"] = "missing identity or observation date"
        return frame
    if not Path(path).exists() or frame.empty:
        return frame
    with closing(readonly(Path(path))) as conn:
        if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='query_features'").fetchone():
            raise ValueError("Invalid NHTSA feature sidecar: query_features table missing")
        # One temporary row per identity; source reads use the sidecar lookup index.
        keys = frame[["canonical_make", "canonical_model", "canonical_year"]].copy()
        keys.columns = ["make", "model", "model_year"]
        keys["make"] = keys["make"].map(identity)
        keys["model"] = keys["model"].map(identity)
        keys["model_year"] = pd.to_numeric(keys["model_year"], errors="coerce")
        conn.execute("CREATE TEMP TABLE requested (make TEXT,model TEXT,model_year INTEGER, PRIMARY KEY(make,model,model_year))")
        conn.executemany("INSERT OR IGNORE INTO requested VALUES (?,?,?)", keys.dropna().itertuples(index=False, name=None))
        records = pd.read_sql_query("SELECT f.* FROM requested r JOIN query_features f USING(make,model,model_year)", conn)
    if records.empty:
        return frame
    keys["cutoff"] = pd.to_datetime(frame[date_column], errors="coerce", utc=True).dt.tz_localize(None).dt.to_period("M").dt.start_time
    keys["position"] = np.arange(len(keys))
    records["available_at"] = pd.to_datetime(records["available_at"], utc=True).dt.tz_localize(None)
    valid = keys.dropna(subset=["cutoff", "model_year"]).sort_values("cutoff")
    for kind in ("complaints", "recalls"):
        selected = records[records["source"].eq(kind)].sort_values(["available_at", "query_id"])
        if selected.empty or valid.empty:
            continue
        selected["model_year"] = selected["model_year"].astype(float)
        left = valid.copy()
        left["model_year"] = left["model_year"].astype(float)
        joined = pd.merge_asof(left, selected, left_on="cutoff", right_on="available_at",
                               by=["make", "model", "model_year"], allow_exact_matches=False)
        cols = [c for c in FEATURE_COLUMNS if c.startswith("nhtsa_complaint" if kind == "complaints" else "nhtsa_recall")]
        for col in cols:
            frame.iloc[joined["position"].to_numpy(dtype=int), frame.columns.get_loc(col)] = pd.to_numeric(joined[col], errors="coerce").to_numpy(dtype=np.float32)
    frame.attrs["nhtsa_features"] = {"available": True, "mode": "collected_before_observation_month",
                                     "matched_rows": int(frame[["nhtsa_complaints_known", "nhtsa_recalls_known"]].notna().any(axis=1).sum())}
    return frame


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["pilot", "score", "review", "build"])
    parser.add_argument("--source-db", type=Path, default=SOURCE_DB)
    parser.add_argument("--output-db", type=Path, default=FEATURE_DB)
    parser.add_argument("--pilot-csv", type=Path, default=ROOT / "CAR_DATA_OUTPUT" / "nhtsa_text_pilot.csv")
    parser.add_argument("--pilot-only", action="store_true", help="Score only role/text hashes in the exported pilot CSV")
    parser.add_argument("--review-csv", type=Path, default=ROOT / "CAR_DATA_OUTPUT" / "nhtsa_text_pilot_scored.csv")
    parser.add_argument("--limit", type=int, default=0, help="Maximum new text documents to score/export; 0 means all")
    parser.add_argument("--model", default="facebook/bart-large-mnli")
    parser.add_argument("--revision", default="d7645e127eaf1aefc7862fd59a17a5aa8558b8ce")
    parser.add_argument("--device", type=int, default=-1, help="-1 CPU, 0 first CUDA GPU")
    args = parser.parse_args()
    if args.limit < 0 or not re.fullmatch(r"[0-9a-f]{40}", args.revision):
        parser.error("Use a nonnegative limit and an immutable 40-character model commit revision")
    if args.source_db.resolve() == args.output_db.resolve():
        parser.error("Derived output must be separate from the source database")
    if args.action == "review":
        if args.review_csv.exists():
            parser.error("Review CSV already exists; choose a new --review-csv")
        with args.pilot_csv.open(encoding="utf-8", newline="") as handle:
            rows = list(csv.DictReader(handle))
        if not rows:
            parser.error("Pilot CSV has no rows")
        labels = sorted({label for values in LABELS.values() for label in values})
        with closing(readonly(args.output_db)) as output:
            for row in rows:
                scores = dict(output.execute("SELECT label,score FROM text_scores WHERE text_hash=? AND role=?",
                                             (row["text_hash"], row["role"])))
                row.update({f"score_{label}": scores.get(label) for label in labels})
        args.review_csv.parent.mkdir(parents=True, exist_ok=True)
        with args.review_csv.open("x", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        print(f"Exported {len(rows)} pilot rows and cached topic scores to {args.review_csv}")
        return
    if args.action == "pilot" and not args.limit:
        args.limit = 300
    pilot_keys = None
    if args.pilot_only:
        if args.action != "score":
            parser.error("--pilot-only is only valid for score")
        with args.pilot_csv.open(encoding="utf-8", newline="") as handle:
            pilot_keys = {(row["role"], row["text_hash"]) for row in csv.DictReader(handle)}
    classifier = None
    if args.action == "pilot" and args.pilot_csv.exists():
        parser.error("Pilot CSV already exists; choose a new --pilot-csv to preserve annotations")
    args.output_db.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    pilot_rows = []
    pilot_seen = set()
    role_counts = dict.fromkeys(LABELS, 0)
    with closing(readonly(args.source_db)) as source, closing(sqlite3.connect(args.output_db)) as output:
        source.row_factory = sqlite3.Row
        initialize(output, args.model, args.revision)
        output.execute("CREATE TABLE IF NOT EXISTS source_identity (path TEXT NOT NULL)")
        source_path = str(args.source_db.resolve())
        prior_source = output.execute("SELECT path FROM source_identity").fetchone()
        if prior_source and prior_source[0] != source_path:
            raise ValueError("Use a separate sidecar for a different source database")
        if not prior_source:
            output.execute("INSERT INTO source_identity VALUES (?)", (source_path,))
        output.commit()
        queries = source.execute("""SELECT * FROM nhtsa_vehicle_queries
            WHERE query_type IN ('complaints','recalls') ORDER BY fetched_at,query_id""")
        for row in queries:
            query = dict(row)
            if not query.get("make") or not query.get("model") or not query.get("model_year"):
                continue
            available = pd.to_datetime(query["fetched_at"], errors="coerce", utc=True)
            if pd.isna(available):
                continue
            docs = documents(source, query) if query["response_status"] == "success" else []
            if args.action in {"score", "pilot"}:
                for doc in docs:
                    for role, text in doc["texts"].items():
                        if args.limit and count >= args.limit:
                            break
                        digest = text_hash(text)
                        if pilot_keys is not None and (role, digest) not in pilot_keys:
                            continue
                        if args.action == "pilot":
                            quota = max(1, args.limit // len(LABELS))
                            if role_counts[role] >= quota or (role, digest) in pilot_seen:
                                continue
                            pilot_seen.add((role, digest))
                            role_counts[role] += 1
                            pilot_rows.append({"query_id": query["query_id"], "event_id": doc["event_id"], "role": role,
                                               "make": query["make"], "model": query["model"], "model_year": query["model_year"],
                                               "text_hash": digest, "text": text, "human_labels": "", "split": "development"})
                        else:
                            cached = output.execute("SELECT COUNT(*) FROM text_scores WHERE text_hash=? AND role=?", (digest, role)).fetchone()[0]
                            if cached == len(LABELS[role]):
                                continue
                            if args.limit and count >= args.limit:
                                break
                            if classifier is None:
                                classifier = create_classifier(args.model, args.revision, args.device)
                            scores = score_text(classifier, text, role)
                            output.executemany("INSERT OR REPLACE INTO text_scores (text_hash,role,label,score) VALUES (?,?,?,?)", [(digest, role, label, score) for label, score in scores.items()])
                            output.commit()
                        count += 1
                if (args.limit and count >= args.limit) or (args.action == "pilot" and all(n >= max(1, args.limit // len(LABELS)) for n in role_counts.values())):
                    break
            else:
                values = build_query_features(query, docs, output)
                columns = ["query_id", "source", "make", "model", "model_year", "available_at", "response_status"] + FEATURE_COLUMNS
                data = [query["query_id"], query["query_type"], identity(query["make"]), identity(query["model"]), int(query["model_year"]), available.isoformat(), query["response_status"]] + [values[c] for c in FEATURE_COLUMNS]
                output.execute(f"INSERT OR REPLACE INTO query_features ({','.join(columns)}) VALUES ({','.join('?' for _ in columns)})", data)
                count += 1
                if count % 100 == 0:
                    output.commit()
        output.commit()
    if pilot_rows:
        args.pilot_csv.parent.mkdir(parents=True, exist_ok=True)
        with args.pilot_csv.open("x", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(pilot_rows[0]))
            writer.writeheader()
            writer.writerows(pilot_rows)
    print(json.dumps({"action": args.action, "processed": count, "output": str(args.output_db),
                      "warning": "Topic scores require manual validation; report counts are not failure rates."}))


if __name__ == "__main__":
    main()
