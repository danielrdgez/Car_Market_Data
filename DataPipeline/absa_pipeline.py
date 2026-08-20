import argparse
import logging
import os
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv

try:
    from database import CarDatabase, YouTubeCommentsDatabase
    from VehicleNormalization import MAKE_ALIASES, normalize_vehicle_text
except ImportError:  # pragma: no cover - used when imported as a package in tests
    from DataPipeline.database import CarDatabase, YouTubeCommentsDatabase
    from DataPipeline.VehicleNormalization import MAKE_ALIASES, normalize_vehicle_text

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)
load_dotenv(Path(__file__).resolve().parent.parent / ".env", override=False)

ASPECT_VERSION = "v3_make_grain_zero_shot"
MAKE_ATTRIBUTION_VERSION = "make_attribution_v1"
DEFAULT_MODEL_NAME = "facebook/bart-large-mnli"
ASPECTS = ["reliability", "value", "performance", "comfort"]
ASPECT_LABELS = {
    "reliability": {
        "positive": "positive owner sentiment about vehicle reliability, durability, or long-term dependability",
        "negative": "negative owner sentiment about vehicle reliability, durability, breakdowns, or costly repairs",
    },
    "value": {
        "positive": "positive sentiment about vehicle value, affordability, fair pricing, or cost of ownership",
        "negative": "negative sentiment about vehicle value, overpricing, poor resale value, or expensive ownership",
    },
    "performance": {
        "positive": "positive sentiment about vehicle performance, power, acceleration, handling, or driving dynamics",
        "negative": "negative sentiment about vehicle performance, weak power, poor handling, or disappointing driving dynamics",
    },
    "comfort": {
        "positive": "positive sentiment about vehicle comfort, interior quality, cabin space, ride quality, or features",
        "negative": "negative sentiment about vehicle comfort, interior quality, cramped space, harsh ride, or poor features",
    },
}
HYPOTHESIS_TEMPLATE = "This vehicle comment expresses {}."
SENTENCE_SPLIT_PATTERN = re.compile(r"(?<=[.!?])\s+|\n+")

MAKES_LIST = [
    "Toyota", "Lexus", "Honda", "Acura", "Ford", "Lincoln", "Chevrolet", "Chevy", "Cadillac", "GMC", "Buick",
    "Jeep", "Dodge", "Ram", "Chrysler", "Subaru", "Nissan", "Infiniti", "Mazda", "Mitsubishi", "Hyundai",
    "Kia", "Genesis", "Volkswagen", "VW", "Audi", "Porsche", "BMW", "Mercedes-Benz", "Mercedes", "Volvo",
    "Land Rover", "Jaguar", "Tesla", "Rivian", "Lucid", "Polestar", "Fiat", "Alfa Romeo", "Ferrari", "Lamborghini",
    "Aston Martin", "Bentley", "Rolls-Royce", "McLaren", "Maserati", "Lotus", "Mini", "Pontiac", "Saab",
    "Suzuki", "Isuzu", "Scion", "Hummer", "Saturn", "Oldsmobile", "Plymouth", "Mercury", "Geo",
    "Koenigsegg", "Pagani", "Bugatti", "Saleen", "Fisker", "VinFast"
]

MAKE_NORM_MAP = {
    "chevy": "Chevrolet",
    "vw": "Volkswagen",
    "mercedes": "Mercedes-Benz",
    "mercedes-benz": "Mercedes-Benz"
}


def build_make_alias_map(db_path: str | Path | None = None) -> dict[str, str]:
    """Build one normalized alias map shared by new scoring and migration."""
    aliases: dict[str, str] = {}
    for make in MAKES_LIST:
        normalized = normalize_vehicle_text(make)
        if normalized:
            aliases[normalized] = MAKE_ALIASES.get(normalized, normalized)
    for alias, canonical in MAKE_ALIASES.items():
        normalized_alias = normalize_vehicle_text(alias)
        normalized_canonical = normalize_vehicle_text(canonical)
        if normalized_alias and normalized_canonical:
            aliases[normalized_alias] = normalized_canonical
            aliases.setdefault(normalized_canonical, normalized_canonical)

    path = Path(db_path) if db_path else None
    if path and path.exists():
        conn = None
        try:
            conn = sqlite3.connect(path)
            table_exists = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='nhtsa_enrichment'"
            ).fetchone()
            if table_exists:
                for (make,) in conn.execute(
                    "SELECT DISTINCT nhtsa_Make FROM nhtsa_enrichment WHERE nhtsa_Make IS NOT NULL"
                ):
                    normalized = normalize_vehicle_text(make)
                    if normalized:
                        aliases[normalized] = MAKE_ALIASES.get(normalized, normalized)
        except sqlite3.Error as exc:
            logger.warning("Could not extend make aliases from %s: %s", path, exc)
        finally:
            if conn is not None:
                conn.close()
    return aliases


def find_make_mentions(text: str, alias_map: dict[str, str]) -> set[str]:
    normalized_text = normalize_vehicle_text(text)
    if not normalized_text:
        return set()
    matches = set()
    for alias, canonical in sorted(alias_map.items(), key=lambda item: len(item[0]), reverse=True):
        if re.search(rf"(?<![A-Z0-9]){re.escape(alias)}(?![A-Z0-9])", normalized_text):
            matches.add(canonical)
    return matches


def attribute_comment_make(
    comment_text: str,
    video_title: str,
    alias_map: dict[str, str],
) -> tuple[str | None, str]:
    comment_matches = find_make_mentions(comment_text, alias_map)
    if len(comment_matches) == 1:
        return next(iter(comment_matches)), "comment"
    if len(comment_matches) > 1:
        return None, "ambiguous_comment"

    title_matches = find_make_mentions(video_title, alias_map)
    if len(title_matches) == 1:
        return next(iter(title_matches)), "video_title"
    if len(title_matches) > 1:
        return None, "ambiguous_video_title"
    return None, "unknown"

YEAR_PATTERN = re.compile(r"\b(19[89]\d|20[0-2]\d)\b")

MODEL_STOP_WORDS = {
    "review", "reviews", "vs", "versus", "comparison", "walkaround", "interior", "exterior",
    "drive", "first", "test", "is", "the", "with", "and", "at", "for", "on", "by", "new",
    "cheap", "expensive", "best", "worst", "good", "bad", "reliability", "problems", "issues",
    "buying", "guide", "channel", "care", "nut", "pov", "pros", "cons", "features", "options",
    "specs", "spec", "price", "worth", "cost", "value", "verdict", "thoughts", "opinion",
    "opinions", "real", "truth", "honest", "why", "how", "what", "to", "buy", "avoid", "hate",
    "love", "broken", "fail", "failed", "garbage", "trash", "perfect", "amazing", "ultimate",
    "owner", "owners", "ownership", "mile", "miles", "k", "years", "year", "month", "months"
}

URL_PATTERN = re.compile(r"https?://\S+|www\.\S+")
HTML_PATTERN = re.compile(r"<.*?>")
SPAM_PATTERNS = [re.compile(p, re.IGNORECASE) for p in [
    r"\bsubscribe\b", r"\bmy channel\b", r"\bcheck out my\b", r"\bclick the link\b",
    r"\bclick here\b", r"\btelegram\b", r"\bwhatsapp\b", r"\bcontact me\b",
    r"\bpromo\b", r"\bdiscount\b", r"\bgiveaway\b"
]]
BOT_PATTERN = re.compile(r"(.)\1{5,}")
PHONE_PATTERN = re.compile(r"\+\d{1,3}[-.\s]?\d{3,4}[-.\s]?\d{4}")
CRYPTO_PATTERN1 = re.compile(r"\b0x[a-fA-F0-9]{40}\b")
CRYPTO_PATTERN2 = re.compile(r"\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b")
EXCESS_BANG = re.compile(r"!{2,}")
EXCESS_QUESTION = re.compile(r"\?{2,}")
EXCESS_DOTS = re.compile(r"\.{4,}")


def extract_vehicle(title: str) -> str:
    if not title or not isinstance(title, str):
        return None

    year_match = YEAR_PATTERN.search(title)
    if not year_match:
        return None
    year = year_match.group(1)

    found_make = None
    make_end = -1
    for make in MAKES_LIST:
        match = re.search(rf"\b{re.escape(make)}\b", title, re.IGNORECASE)
        if match:
            found_make = make
            make_end = match.end()
            break

    if not found_make:
        return None

    post_make_str = title[make_end:].strip()
    words = post_make_str.split()
    model_words = []
    for word in words:
        cleaned_word = word.strip(".,;:!?|()-â€“â€”\"'[]{}")
        cleaned_word_lower = cleaned_word.lower()

        if word.startswith(("|", "-", "â€“", "â€”", ":", "/", "\\")):
            break
        if not cleaned_word:
            continue
        if cleaned_word == year or (cleaned_word.isdigit() and len(cleaned_word) == 4):
            continue
        if cleaned_word_lower in MODEL_STOP_WORDS:
            break
        if cleaned_word.islower() and not any(c.isdigit() for c in cleaned_word) and "-" not in cleaned_word:
            break

        model_words.append(cleaned_word)
        if word.endswith((":", "|", "-", "â€“", "â€”", ".", "?", "!")):
            break

    model_words = model_words[:3]
    if not model_words:
        return None

    model = " ".join(model_words)
    normalized_make = MAKE_NORM_MAP.get(found_make.lower(), found_make.capitalize())
    return f"{year} {normalized_make} {model}"


def clean_comment_text(text: str) -> str:
    if not text or not isinstance(text, str):
        return None

    text = URL_PATTERN.sub("", text)
    text = HTML_PATTERN.sub("", text)
    text_lower = text.strip().lower()
    if text_lower in {"first", "first!", "first!!", "first comment", "subscribe", "sub"}:
        return None

    for pattern in SPAM_PATTERNS:
        if pattern.search(text):
            return None
    if BOT_PATTERN.search(text) or PHONE_PATTERN.search(text):
        return None
    if CRYPTO_PATTERN1.search(text) or CRYPTO_PATTERN2.search(text):
        return None

    text = EXCESS_BANG.sub("!", text)
    text = EXCESS_QUESTION.sub("?", text)
    text = EXCESS_DOTS.sub("...", text)
    text = text.strip()
    if len(text.split()) < 3:
        return None
    return text


def split_comment_into_chunks(text: str, max_chunk_words: int = 45) -> list[str]:
    if not text:
        return []
    sentences = [part.strip() for part in SENTENCE_SPLIT_PATTERN.split(text) if part.strip()]
    if not sentences:
        sentences = [text.strip()]

    chunks: list[str] = []
    current_words: list[str] = []
    for sentence in sentences:
        words = sentence.split()
        if not words:
            continue
        if current_words and len(current_words) + len(words) > max_chunk_words:
            chunks.append(" ".join(current_words))
            current_words = []
        if len(words) > max_chunk_words:
            for start in range(0, len(words), max_chunk_words):
                chunk_words = words[start:start + max_chunk_words]
                if chunk_words:
                    chunks.append(" ".join(chunk_words))
            continue
        current_words.extend(words)
    if current_words:
        chunks.append(" ".join(current_words))
    return chunks or [text.strip()]


def load_data(db_path: str, force_reprocess: bool = False, limit: int = None) -> pd.DataFrame:
    logger.info("Connecting to database at %s...", db_path)
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")

    db = YouTubeCommentsDatabase(db_path)
    try:
        df = db.load_comments_for_absa(force_reprocess=force_reprocess, limit=limit)
        logger.info("Loaded %s comments for ABSA scoring.", len(df))
        return df
    finally:
        db.close()


def load_all_scored_comments(db_path: str) -> pd.DataFrame:
    db = CarDatabase(db_path)
    try:
        conn = db._get_connection()
        return pd.read_sql_query("SELECT * FROM youtube_comments_scored", conn)
    finally:
        db.close()


def run_phase1_preprocessing(
    df: pd.DataFrame,
    alias_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    logger.info("Starting Phase 1: Data Ingestion & Preprocessing...")
    if df.empty:
        return df.copy()

    alias_map = alias_map or build_make_alias_map()
    df_clean = df.copy()
    df_clean["original_text"] = df_clean["text"]
    df_clean["text"] = df_clean["text"].apply(clean_comment_text)
    cleaned_drop_count = df_clean["text"].isna().sum()
    df_clean = df_clean.dropna(subset=["text"]).copy()
    attributions = [
        attribute_comment_make(text, title, alias_map)
        for text, title in zip(df_clean["text"], df_clean["video_title"].fillna(""))
    ]
    df_clean["sentiment_make"] = [value[0] for value in attributions]
    df_clean["make_attribution_source"] = [value[1] for value in attributions]
    df_clean["make_attribution_version"] = MAKE_ATTRIBUTION_VERSION
    df_clean["sentiment_status"] = np.where(
        df_clean["sentiment_make"].notna(),
        "ready",
        df_clean["make_attribution_source"],
    )
    identified_count = int(df_clean["sentiment_make"].notna().sum())
    logger.info(
        "Make attribution summary: %s identified, %s ambiguous/unknown.",
        identified_count,
        len(df_clean) - identified_count,
    )
    logger.info("Dropped %s rows during text cleaning. %s rows remain.", cleaned_drop_count, len(df_clean))
    return df_clean


def _aggregate_chunk_scores(chunk_scores: list[dict]) -> dict:
    aggregated: dict[str, dict[str, float]] = {
        aspect: {"sentiment": np.nan, "mentioned": 0, "confidence": 0.0}
        for aspect in ASPECTS
    }
    for aspect in ASPECTS:
        aspect_sentiments = []
        aspect_confidences = []
        for item in chunk_scores:
            score = item[aspect]
            if score["mentioned"]:
                aspect_sentiments.append(score["sentiment"])
                aspect_confidences.append(score["confidence"])
        if aspect_confidences:
            weights = np.array(aspect_confidences, dtype=float)
            sentiments = np.array(aspect_sentiments, dtype=float)
            aggregated[aspect]["mentioned"] = 1
            aggregated[aspect]["confidence"] = float(weights.max())
            aggregated[aspect]["sentiment"] = float(np.average(sentiments, weights=weights))
    return aggregated


def run_absa_on_comments(
    df: pd.DataFrame,
    model_name: str = DEFAULT_MODEL_NAME,
    model_revision: str | None = None,
    limit: int = None,
) -> pd.DataFrame:
    logger.info("Initializing HuggingFace zero-shot classification pipeline...")
    import torch
    from transformers import pipeline

    device = 0 if torch.cuda.is_available() else -1
    pipeline_kwargs = {"model": model_name, "device": device}
    if model_revision:
        pipeline_kwargs["revision"] = model_revision
    classifier = pipeline("zero-shot-classification", **pipeline_kwargs)
    logger.info("Loaded classifier on device: %s", "GPU" if device == 0 else "CPU")

    if limit:
        df = df.iloc[:limit].copy()
    else:
        df = df.copy()
    if df.empty:
        return df

    for aspect in ASPECTS:
        df[f"{aspect}_sentiment"] = np.nan
        df[f"{aspect}_mentioned"] = 0
        df[f"{aspect}_confidence"] = 0.0

    comment_chunks: list[str] = []
    comment_positions: list[int] = []
    for idx, text in enumerate(df["text"].tolist()):
        chunks = split_comment_into_chunks(text)
        for chunk in chunks:
            comment_chunks.append(chunk)
            comment_positions.append(idx)

    candidate_labels = [label for aspect in ASPECTS for label in ASPECT_LABELS[aspect].values()]
    batch_size = 16
    per_comment_scores: dict[int, list[dict]] = {}
    logger.info("Running zero-shot classification on %s comment chunks...", len(comment_chunks))

    for start in range(0, len(comment_chunks), batch_size):
        batch_chunks = comment_chunks[start:start + batch_size]
        batch_positions = comment_positions[start:start + batch_size]
        batch_results = classifier(
            batch_chunks,
            candidate_labels=candidate_labels,
            multi_label=True,
            hypothesis_template=HYPOTHESIS_TEMPLATE,
        )
        if isinstance(batch_results, dict):
            batch_results = [batch_results]

        for comment_idx, result in zip(batch_positions, batch_results):
            label_scores = dict(zip(result["labels"], result["scores"]))
            chunk_summary = {}
            for aspect in ASPECTS:
                pos_label = ASPECT_LABELS[aspect]["positive"]
                neg_label = ASPECT_LABELS[aspect]["negative"]
                s_pos = float(label_scores.get(pos_label, 0.0))
                s_neg = float(label_scores.get(neg_label, 0.0))
                max_score = max(s_pos, s_neg)
                mentioned = int(max_score >= 0.40)
                sentiment = (s_pos - s_neg) / (s_pos + s_neg + 1e-9) if mentioned else np.nan
                chunk_summary[aspect] = {
                    "mentioned": mentioned,
                    "confidence": max_score if mentioned else 0.0,
                    "sentiment": float(sentiment) if mentioned else np.nan,
                }
            per_comment_scores.setdefault(comment_idx, []).append(chunk_summary)

        processed = min(start + batch_size, len(comment_chunks))
        if processed % 64 == 0 or processed == len(comment_chunks):
            logger.info("Processed %s/%s comment chunks...", processed, len(comment_chunks))

    for idx in range(len(df)):
        aggregated = _aggregate_chunk_scores(per_comment_scores.get(idx, []))
        for aspect in ASPECTS:
            df.iloc[idx, df.columns.get_loc(f"{aspect}_mentioned")] = aggregated[aspect]["mentioned"]
            df.iloc[idx, df.columns.get_loc(f"{aspect}_confidence")] = aggregated[aspect]["confidence"]
            df.iloc[idx, df.columns.get_loc(f"{aspect}_sentiment")] = aggregated[aspect]["sentiment"]

    df["processed_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    df["model_name"] = model_name
    resolved_revision = getattr(getattr(classifier, "model", None), "config", None)
    df["model_revision"] = (
        getattr(resolved_revision, "_commit_hash", None) or model_revision or "unresolved"
    )
    df["aspect_version"] = ASPECT_VERSION
    df["sentiment_status"] = "scored"
    return df


def apply_weights(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting Phase 3: Weighted Scoring System...")
    df = df.copy()
    df["like_count"] = df["like_count"].fillna(0).astype(float)
    df["consensus_weight"] = 1.0 + np.log10(df["like_count"] + 1.0)
    df["word_count"] = df["text"].apply(lambda t: len(str(t).split()))
    df["depth_weight"] = np.where(df["word_count"] >= 20, 1.2, 1.0)
    df["comment_weight"] = df["consensus_weight"] * df["depth_weight"]
    for aspect in ASPECTS:
        df[f"Weighted_{aspect.capitalize()}_Score"] = df[f"{aspect}_sentiment"] * df["comment_weight"]
    sentiment_numerator = pd.Series(0.0, index=df.index, dtype="float64")
    confidence_total = pd.Series(0.0, index=df.index, dtype="float64")
    mentioned_count = pd.Series(0, index=df.index, dtype="int64")
    for aspect in ASPECTS:
        mentioned = pd.to_numeric(df[f"{aspect}_mentioned"], errors="coerce").fillna(0).eq(1)
        confidence = pd.to_numeric(df[f"{aspect}_confidence"], errors="coerce").fillna(0.0)
        sentiment = pd.to_numeric(df[f"{aspect}_sentiment"], errors="coerce")
        usable = mentioned & sentiment.notna() & confidence.gt(0)
        sentiment_numerator = sentiment_numerator.add(
            sentiment.where(usable, 0.0) * confidence.where(usable, 0.0),
            fill_value=0.0,
        )
        confidence_total = confidence_total.add(confidence.where(usable, 0.0), fill_value=0.0)
        mentioned_count = mentioned_count.add(usable.astype("int64"), fill_value=0)
    df["overall_sentiment"] = sentiment_numerator.div(confidence_total.where(confidence_total.gt(0)))
    df["overall_confidence"] = confidence_total.div(mentioned_count.where(mentioned_count.gt(0)))
    return df


def persist_scored_comments(df: pd.DataFrame, db_path: str) -> int:
    db = YouTubeCommentsDatabase(db_path)
    try:
        inserted = db.upsert_scored_comments(df)
        logger.info("Upserted %s scored comments into youtube_comments_scored.", inserted)
        return inserted
    finally:
        db.close()


def prepare_unattributed_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Persist clean but unattributable comments so incremental runs do not retry them."""
    result = df.copy()
    for aspect in ASPECTS:
        result[f"{aspect}_sentiment"] = np.nan
        result[f"{aspect}_mentioned"] = 0
        result[f"{aspect}_confidence"] = 0.0
    result = apply_weights(result)
    result["processed_at"] = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    result["model_name"] = None
    result["model_revision"] = None
    result["aspect_version"] = ASPECT_VERSION
    return result


def migrate_make_grain(
    db_path: str | Path,
    alias_map: dict[str, str] | None = None,
    chunk_size: int = 10_000,
) -> int:
    """Backfill make attribution and overall scores without rerunning inference."""
    db_path = Path(db_path)
    alias_map = alias_map or build_make_alias_map(db_path)
    db = YouTubeCommentsDatabase(str(db_path))
    updated = 0
    try:
        conn = db._get_connection()
        last_rowid = 0
        while True:
            batch = conn.execute(
                '''
                SELECT rowid, text, video_title,
                       reliability_sentiment, reliability_mentioned, reliability_confidence,
                       value_sentiment, value_mentioned, value_confidence,
                       performance_sentiment, performance_mentioned, performance_confidence,
                       comfort_sentiment, comfort_mentioned, comfort_confidence,
                       model_name, model_revision
                FROM youtube_comments_scored
                WHERE rowid > ?
                  AND (make_attribution_version IS NULL OR make_attribution_version <> ?)
                ORDER BY rowid
                LIMIT ?
                ''',
                (last_rowid, MAKE_ATTRIBUTION_VERSION, int(chunk_size)),
            ).fetchall()
            if not batch:
                break
            updates = []
            for row in batch:
                sentiment_make, source = attribute_comment_make(row[1], row[2], alias_map)
                numerator = 0.0
                confidence_total = 0.0
                confidence_values = []
                for offset in (3, 6, 9, 12):
                    sentiment, mentioned, confidence = row[offset:offset + 3]
                    if mentioned == 1 and sentiment is not None and confidence is not None and confidence > 0:
                        numerator += float(sentiment) * float(confidence)
                        confidence_total += float(confidence)
                        confidence_values.append(float(confidence))
                overall = numerator / confidence_total if confidence_total else None
                overall_confidence = (
                    sum(confidence_values) / len(confidence_values) if confidence_values else None
                )
                status = "scored" if sentiment_make else source
                updates.append(
                    (
                        sentiment_make,
                        source,
                        MAKE_ATTRIBUTION_VERSION,
                        overall,
                        overall_confidence,
                        status,
                        row[16] or ("legacy_unpinned" if row[15] else None),
                        row[0],
                    )
                )
            conn.executemany(
                '''
                UPDATE youtube_comments_scored
                SET sentiment_make = ?, make_attribution_source = ?, make_attribution_version = ?,
                    overall_sentiment = ?, overall_confidence = ?, sentiment_status = ?,
                    model_revision = ?
                WHERE rowid = ?
                ''',
                updates,
            )
            conn.commit()
            updated += len(updates)
            last_rowid = int(batch[-1][0])
            logger.info("Migrated make attribution for %s scored comments...", updated)
    finally:
        db.close()
    return updated


def _sqlite_comment_date(column: str = "published_at") -> str:
    return f'''
        CASE
            WHEN {column} LIKE '____-__-__%' THEN SUBSTR({column}, 1, 10)
            WHEN {column} LIKE '__-__-____%' THEN
                SUBSTR({column}, 7, 4) || '-' || SUBSTR({column}, 1, 2) || '-' || SUBSTR({column}, 4, 2)
            ELSE NULL
        END
    '''


def rebuild_make_sentiment_tables(db_path: str | Path) -> pd.DataFrame:
    """Rebuild current and cumulative monthly make aggregates in one transaction."""
    db = YouTubeCommentsDatabase(str(db_path))
    now_iso = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    date_expr = _sqlite_comment_date()
    try:
        conn = db._get_connection()
        conn.execute("BEGIN")
        conn.execute("DELETE FROM make_sentiment_index")
        conn.execute("DELETE FROM make_sentiment_monthly")
        conn.execute(
            f'''
            INSERT INTO make_sentiment_index (
                sentiment_make, sentiment_overall_score, sentiment_reliability_score,
                sentiment_value_score, sentiment_performance_score, sentiment_comfort_score,
                sentiment_comment_count, sentiment_video_count, sentiment_aspect_coverage,
                sentiment_latest_comment_at, sentiment_model_versions, updated_at
            )
            SELECT
                sentiment_make,
                SUM(CASE WHEN overall_sentiment IS NOT NULL THEN overall_sentiment * COALESCE(comment_weight, 1.0) ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN overall_sentiment IS NOT NULL THEN COALESCE(comment_weight, 1.0) ELSE 0 END), 0),
                SUM(CASE WHEN reliability_mentioned = 1 AND reliability_sentiment IS NOT NULL THEN reliability_sentiment * COALESCE(comment_weight, 1.0) ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN reliability_mentioned = 1 AND reliability_sentiment IS NOT NULL THEN COALESCE(comment_weight, 1.0) ELSE 0 END), 0),
                SUM(CASE WHEN value_mentioned = 1 AND value_sentiment IS NOT NULL THEN value_sentiment * COALESCE(comment_weight, 1.0) ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN value_mentioned = 1 AND value_sentiment IS NOT NULL THEN COALESCE(comment_weight, 1.0) ELSE 0 END), 0),
                SUM(CASE WHEN performance_mentioned = 1 AND performance_sentiment IS NOT NULL THEN performance_sentiment * COALESCE(comment_weight, 1.0) ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN performance_mentioned = 1 AND performance_sentiment IS NOT NULL THEN COALESCE(comment_weight, 1.0) ELSE 0 END), 0),
                SUM(CASE WHEN comfort_mentioned = 1 AND comfort_sentiment IS NOT NULL THEN comfort_sentiment * COALESCE(comment_weight, 1.0) ELSE 0 END)
                    / NULLIF(SUM(CASE WHEN comfort_mentioned = 1 AND comfort_sentiment IS NOT NULL THEN COALESCE(comment_weight, 1.0) ELSE 0 END), 0),
                COUNT(*),
                COUNT(DISTINCT video_id),
                SUM(COALESCE(reliability_mentioned, 0) + COALESCE(value_mentioned, 0)
                    + COALESCE(performance_mentioned, 0) + COALESCE(comfort_mentioned, 0)) / (4.0 * COUNT(*)),
                MAX({date_expr}),
                GROUP_CONCAT(DISTINCT COALESCE(model_name, 'unknown') || '@' || COALESCE(model_revision, 'unknown')),
                ?
            FROM youtube_comments_scored
            WHERE sentiment_make IS NOT NULL AND sentiment_status = 'scored'
            GROUP BY sentiment_make
            ''',
            (now_iso,),
        )
        conn.execute(
            f'''
            INSERT INTO make_sentiment_monthly (
                sentiment_make, sentiment_month, sentiment_overall_score,
                sentiment_reliability_score, sentiment_value_score,
                sentiment_performance_score, sentiment_comfort_score,
                sentiment_comment_count, sentiment_video_count,
                sentiment_aspect_coverage, sentiment_latest_comment_at
            )
            WITH base AS (
                SELECT *, {date_expr} AS comment_date,
                       SUBSTR({date_expr}, 1, 7) || '-01' AS sentiment_month,
                       COALESCE(comment_weight, 1.0) AS score_weight
                FROM youtube_comments_scored
                WHERE sentiment_make IS NOT NULL AND sentiment_status = 'scored'
                  AND {date_expr} IS NOT NULL
            ),
            monthly AS (
                SELECT sentiment_make, sentiment_month,
                       SUM(CASE WHEN overall_sentiment IS NOT NULL THEN overall_sentiment * score_weight ELSE 0 END) AS overall_num,
                       SUM(CASE WHEN overall_sentiment IS NOT NULL THEN score_weight ELSE 0 END) AS overall_den,
                       SUM(CASE WHEN reliability_mentioned = 1 AND reliability_sentiment IS NOT NULL THEN reliability_sentiment * score_weight ELSE 0 END) AS reliability_num,
                       SUM(CASE WHEN reliability_mentioned = 1 AND reliability_sentiment IS NOT NULL THEN score_weight ELSE 0 END) AS reliability_den,
                       SUM(CASE WHEN value_mentioned = 1 AND value_sentiment IS NOT NULL THEN value_sentiment * score_weight ELSE 0 END) AS value_num,
                       SUM(CASE WHEN value_mentioned = 1 AND value_sentiment IS NOT NULL THEN score_weight ELSE 0 END) AS value_den,
                       SUM(CASE WHEN performance_mentioned = 1 AND performance_sentiment IS NOT NULL THEN performance_sentiment * score_weight ELSE 0 END) AS performance_num,
                       SUM(CASE WHEN performance_mentioned = 1 AND performance_sentiment IS NOT NULL THEN score_weight ELSE 0 END) AS performance_den,
                       SUM(CASE WHEN comfort_mentioned = 1 AND comfort_sentiment IS NOT NULL THEN comfort_sentiment * score_weight ELSE 0 END) AS comfort_num,
                       SUM(CASE WHEN comfort_mentioned = 1 AND comfort_sentiment IS NOT NULL THEN score_weight ELSE 0 END) AS comfort_den,
                       COUNT(*) AS comment_count,
                       SUM(COALESCE(reliability_mentioned, 0) + COALESCE(value_mentioned, 0)
                           + COALESCE(performance_mentioned, 0) + COALESCE(comfort_mentioned, 0)) AS aspect_mentions,
                       MAX(comment_date) AS latest_comment_at
                FROM base
                GROUP BY sentiment_make, sentiment_month
            ),
            cumulative AS (
                SELECT *,
                       SUM(overall_num) OVER w AS cum_overall_num,
                       SUM(overall_den) OVER w AS cum_overall_den,
                       SUM(reliability_num) OVER w AS cum_reliability_num,
                       SUM(reliability_den) OVER w AS cum_reliability_den,
                       SUM(value_num) OVER w AS cum_value_num,
                       SUM(value_den) OVER w AS cum_value_den,
                       SUM(performance_num) OVER w AS cum_performance_num,
                       SUM(performance_den) OVER w AS cum_performance_den,
                       SUM(comfort_num) OVER w AS cum_comfort_num,
                       SUM(comfort_den) OVER w AS cum_comfort_den,
                       SUM(comment_count) OVER w AS cum_comment_count,
                       SUM(aspect_mentions) OVER w AS cum_aspect_mentions,
                       MAX(latest_comment_at) OVER w AS cum_latest_comment_at
                FROM monthly
                WINDOW w AS (PARTITION BY sentiment_make ORDER BY sentiment_month ROWS UNBOUNDED PRECEDING)
            )
            SELECT
                c.sentiment_make,
                c.sentiment_month,
                c.cum_overall_num / NULLIF(c.cum_overall_den, 0),
                c.cum_reliability_num / NULLIF(c.cum_reliability_den, 0),
                c.cum_value_num / NULLIF(c.cum_value_den, 0),
                c.cum_performance_num / NULLIF(c.cum_performance_den, 0),
                c.cum_comfort_num / NULLIF(c.cum_comfort_den, 0),
                c.cum_comment_count,
                (SELECT COUNT(DISTINCT b.video_id) FROM base AS b
                 WHERE b.sentiment_make = c.sentiment_make AND b.sentiment_month <= c.sentiment_month),
                c.cum_aspect_mentions / (4.0 * c.cum_comment_count),
                c.cum_latest_comment_at
            FROM cumulative AS c
            ''',
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_make_sentiment_monthly_lookup "
            "ON make_sentiment_monthly(sentiment_make, sentiment_month)"
        )
        conn.commit()
        result = pd.read_sql_query(
            "SELECT * FROM make_sentiment_index ORDER BY sentiment_overall_score DESC",
            conn,
        )
        logger.info("Rebuilt make sentiment tables for %s makes.", len(result))
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        db.close()


def run_phase4_aggregation(scored_df: pd.DataFrame, output_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Compatibility wrapper around the make-level SQL aggregation."""
    db_path = os.path.join(output_dir, "CAR_YOUTUBE_COMMENTS.db")
    return rebuild_make_sentiment_tables(db_path), scored_df


def run_tests():
    print("Running ABSA Pipeline Tests...")
    alias_map = build_make_alias_map()
    assert attribute_comment_make("Great ride", "Toyota Camry review", alias_map) == ("TOYOTA", "video_title")
    assert attribute_comment_make("Honda is better", "Toyota Camry review", alias_map) == ("HONDA", "comment")
    assert attribute_comment_make("Toyota beats Honda", "Toyota vs Honda", alias_map)[0] is None
    assert attribute_comment_make("Great ride", "Toyota vs Honda", alias_map)[1] == "ambiguous_video_title"

    test_comments = {
        "This car is amazing! I love the handling and design.": "This car is amazing! I love the handling and design.",
        "First!": None,
        "Subscribe to my channel for free crypto: http://scam.link": None,
        "Check out my whatsapp +1-555-0199 for investment tips!": None,
        "soooooooo goooooood": None,
        "Great car": None,
        "Good.": None,
    }
    for comment, expected in test_comments.items():
        res = clean_comment_text(comment)
        assert res == expected, f"Failed comment cleaning. Input: '{comment}', Expected: '{expected}', Got: '{res}'"

    chunks = split_comment_into_chunks(
        "Great power. But the ride is harsh and overpriced for what you get.",
        max_chunk_words=5,
    )
    assert len(chunks) >= 2, "Expected sentence chunking to split mixed-topic comments."
    print("All tests passed successfully!")


def main():
    parser = argparse.ArgumentParser(description="Automotive Aspect-Based Sentiment Analysis Pipeline")
    parser.add_argument("--db-path", type=str, default="", help="Path to CAR_YOUTUBE_COMMENTS.db")
    parser.add_argument("--inspect-phase1", action="store_true", help="Run only Phase 1 & inspect preprocessing results")
    parser.add_argument("--test", action="store_true", help="Run validation tests")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of comments to process")
    parser.add_argument("--run-all", action="store_true", help="Run all phases of the pipeline")
    parser.add_argument(
        "--migrate-make-grain",
        action="store_true",
        help="Backfill make attribution and rebuild make aggregates without model inference.",
    )
    parser.add_argument("--force-reprocess", action="store_true", help="Ignore prior comment-level scoring state.")
    parser.add_argument("--model-name", type=str, default=DEFAULT_MODEL_NAME, help="Hugging Face model for zero-shot ABSA.")
    parser.add_argument("--model-revision", type=str, default=None, help="Optional pinned Hugging Face model revision.")
    args = parser.parse_args()

    if args.test:
        run_tests()
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))
    workspace_dir = os.path.dirname(script_dir)
    db_path = args.db_path or os.path.join(workspace_dir, "CAR_DATA_OUTPUT", "CAR_YOUTUBE_COMMENTS.db")
    alias_map = build_make_alias_map(db_path)

    if args.migrate_make_grain:
        before_db = YouTubeCommentsDatabase(db_path)
        try:
            before_count = before_db._get_connection().execute(
                "SELECT COUNT(*) FROM youtube_comments_scored"
            ).fetchone()[0]
        finally:
            before_db.close()
        updated = migrate_make_grain(db_path, alias_map=alias_map)
        aggregate = rebuild_make_sentiment_tables(db_path)
        after_db = YouTubeCommentsDatabase(db_path)
        try:
            after_count = after_db._get_connection().execute(
                "SELECT COUNT(*) FROM youtube_comments_scored"
            ).fetchone()[0]
        finally:
            after_db.close()
        if before_count != after_count:
            raise RuntimeError(
                f"Migration changed scored row count from {before_count} to {after_count}."
            )
        print(
            f"Migrated {updated} scored comments without inference; "
            f"rebuilt aggregates for {len(aggregate)} makes."
        )
        return

    try:
        df_raw = load_data(db_path, force_reprocess=args.force_reprocess, limit=args.limit)
    except Exception as exc:
        logger.error("Failed to load data: %s", exc)
        return

    df_clean = run_phase1_preprocessing(df_raw, alias_map=alias_map)
    if args.inspect_phase1:
        print("\n=== Phase 1 Inspection: First 5 Rows of Cleaned Data ===")
        inspect_cols = [
            "video_title", "sentiment_make", "make_attribution_source",
            "author", "like_count", "text", "published_at",
        ]
        preview = df_clean[inspect_cols].head(5)
        print(preview.to_string(index=False))
        print("========================================================\n")
        return

    if args.run_all or (not args.inspect_phase1 and not args.test):
        attributable = df_clean[df_clean["sentiment_make"].notna()].copy()
        unattributed = df_clean[df_clean["sentiment_make"].isna()].copy()
        if not unattributed.empty:
            persist_scored_comments(prepare_unattributed_rows(unattributed), db_path)
        if not attributable.empty:
            df_absa = run_absa_on_comments(
                attributable,
                model_name=args.model_name,
                model_revision=args.model_revision,
                limit=None,
            )
            df_weighted = apply_weights(df_absa)
            persist_scored_comments(df_weighted, db_path)
        else:
            logger.info("No new comments required scoring in this run.")

        df_agg = rebuild_make_sentiment_tables(db_path)

        print("\n=== Aggregated Results Summary ===")
        print(df_agg.head(10).to_string(index=False))
        print("===================================\n")


if __name__ == "__main__":
    main()
