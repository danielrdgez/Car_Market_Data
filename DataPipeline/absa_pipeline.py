import argparse
import logging
import os
import re
from datetime import datetime, timezone

import numpy as np
import pandas as pd

try:
    from database import CarDatabase, YouTubeCommentsDatabase
except ImportError:  # pragma: no cover - used when imported as a package in tests
    from DataPipeline.database import CarDatabase, YouTubeCommentsDatabase

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

ASPECT_VERSION = "v2_zero_shot_sentence_chunks"
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
    "Land Rover", "Jaguar", "Tesla", "Rivian", "Lucid", "Polestar", "Fiat", "Alfa Romeo", "Ferrari", "Lamborghini"
]

MAKE_NORM_MAP = {
    "chevy": "Chevrolet",
    "vw": "Volkswagen",
    "mercedes": "Mercedes-Benz",
    "mercedes-benz": "Mercedes-Benz"
}

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


def run_phase1_preprocessing(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting Phase 1: Data Ingestion & Preprocessing...")
    if df.empty:
        return df.copy()

    df = df.copy()
    df["Vehicle_Entity"] = df["video_title"].apply(extract_vehicle)
    unidentified_count = df["Vehicle_Entity"].isna().sum()
    logger.info("Extraction summary: %s identified, %s unidentified.", len(df) - unidentified_count, unidentified_count)

    df_clean = df.dropna(subset=["Vehicle_Entity"]).copy()
    df_clean["original_text"] = df_clean["text"]
    df_clean["text"] = df_clean["text"].apply(clean_comment_text)
    cleaned_drop_count = df_clean["text"].isna().sum()
    df_clean = df_clean.dropna(subset=["text"]).copy()
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
    limit: int = None,
) -> pd.DataFrame:
    logger.info("Initializing HuggingFace zero-shot classification pipeline...")
    import torch
    from transformers import pipeline

    device = 0 if torch.cuda.is_available() else -1
    classifier = pipeline("zero-shot-classification", model=model_name, device=device)
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
    df["aspect_version"] = ASPECT_VERSION
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
    return df


def persist_scored_comments(df: pd.DataFrame, db_path: str) -> int:
    db = YouTubeCommentsDatabase(db_path)
    try:
        inserted = db.upsert_scored_comments(df)
        logger.info("Upserted %s scored comments into youtube_comments_scored.", inserted)
        return inserted
    finally:
        db.close()


def run_phase4_aggregation(scored_df: pd.DataFrame, output_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info("Starting Phase 4: Aggregation and Index Creation...")
    db_path = os.path.join(output_dir, "CAR_YOUTUBE_COMMENTS.db")
    if scored_df.empty:
        logger.warning("No scored comments available for aggregation.")
        df_agg = pd.DataFrame(columns=[
            "Vehicle_Entity",
            "Sample_Size",
            "Reliability_Index",
            "General_Enthusiast_Score",
            "Sentiment_Volatility_StdDev",
            "Sentiment_Trend_Slope",
            "Confidence_Level",
        ])
    else:
        grouped = scored_df.groupby("Vehicle_Entity")
        aggregated_results = []
        for entity, group in grouped:
            sample_size = len(group)

            reliability_group = group[group["reliability_mentioned"] == 1]
            if not reliability_group.empty:
                rel_weighted_sum = (reliability_group["reliability_sentiment"] * reliability_group["comment_weight"]).sum()
                rel_weighted_avg = rel_weighted_sum / (reliability_group["comment_weight"].sum() + 1e-9)
                reliability_index = (rel_weighted_avg + 1.0) * 50.0
            else:
                reliability_index = np.nan

            total_weighted_sentiment = 0.0
            total_weights = 0.0
            for aspect in ASPECTS:
                aspect_group = group[group[f"{aspect}_mentioned"] == 1]
                if not aspect_group.empty:
                    total_weighted_sentiment += (aspect_group[f"{aspect}_sentiment"] * aspect_group["comment_weight"]).sum()
                    total_weights += aspect_group["comment_weight"].sum()

            general_enthusiast_score = (
                (total_weighted_sentiment / total_weights + 1.0) * 50.0
                if total_weights > 0
                else np.nan
            )

            times = []
            sentiments = []
            for _, row in group.iterrows():
                date_val = pd.to_datetime(row["published_at"], errors="coerce")
                if pd.isna(date_val):
                    continue
                for aspect in ASPECTS:
                    sent = row[f"{aspect}_sentiment"]
                    if not pd.isna(sent):
                        times.append(date_val.timestamp() / (24 * 3600))
                        sentiments.append(sent)

            if len(sentiments) >= 5:
                times_np = np.array(times)
                sentiments_np = np.array(sentiments)
                sentiment_std_dev = float(np.std(sentiments_np))
                time_var = np.var(times_np)
                sentiment_trend_slope = 0.0 if time_var == 0 else float(np.cov(times_np, sentiments_np)[0, 1] / time_var)
            else:
                sentiment_std_dev = np.nan
                sentiment_trend_slope = np.nan

            confidence_level = "High Confidence" if sample_size >= 30 else "Low Confidence"
            aggregated_results.append({
                "Vehicle_Entity": entity,
                "Sample_Size": sample_size,
                "Reliability_Index": reliability_index,
                "General_Enthusiast_Score": general_enthusiast_score,
                "Sentiment_Volatility_StdDev": sentiment_std_dev,
                "Sentiment_Trend_Slope": sentiment_trend_slope,
                "Confidence_Level": confidence_level,
            })

        df_agg = pd.DataFrame(aggregated_results)
        if not df_agg.empty:
            df_agg = df_agg.sort_values(by="General_Enthusiast_Score", ascending=False).reset_index(drop=True)

    db = CarDatabase(db_path)
    try:
        conn = db._get_connection()
        df_agg.to_sql("vehicle_sentiment_index", conn, if_exists="replace", index=False)
        logger.info("Successfully rebuilt vehicle_sentiment_index from stored scored comments.")
    finally:
        db.close()
    return df_agg, scored_df


def run_tests():
    print("Running ABSA Pipeline Tests...")
    test_titles = {
        "2024 Toyota Camry Hybrid Review": "2024 Toyota Camry Hybrid",
        "2021 Ford F-150 Raptor vs Ram TRX": "2021 Ford F-150 Raptor",
        "Is the 2023 Honda Civic Type R worth $45k?": "2023 Honda Civic Type R",
        "2022 Hyundai Ioniq 5: An Amazing EV": "2022 Hyundai Ioniq 5",
        "Toyota Camry 2025 Review": "2025 Toyota Camry",
        "Just a random video without car title": None,
    }
    for title, expected in test_titles.items():
        res = extract_vehicle(title)
        assert res == expected, f"Failed title extraction. Input: '{title}', Expected: '{expected}', Got: '{res}'"

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

    chunks = split_comment_into_chunks("Great power. But the ride is harsh and overpriced for what you get.")
    assert len(chunks) >= 2, "Expected sentence chunking to split mixed-topic comments."
    print("All tests passed successfully!")


def main():
    parser = argparse.ArgumentParser(description="Automotive Aspect-Based Sentiment Analysis Pipeline")
    parser.add_argument("--db-path", type=str, default="", help="Path to CAR_YOUTUBE_COMMENTS.db")
    parser.add_argument("--inspect-phase1", action="store_true", help="Run only Phase 1 & inspect preprocessing results")
    parser.add_argument("--test", action="store_true", help="Run validation tests")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of comments to process")
    parser.add_argument("--run-all", action="store_true", help="Run all phases of the pipeline")
    parser.add_argument("--force-reprocess", action="store_true", help="Ignore prior comment-level scoring state.")
    parser.add_argument("--model-name", type=str, default=DEFAULT_MODEL_NAME, help="Hugging Face model for zero-shot ABSA.")
    args = parser.parse_args()

    if args.test:
        run_tests()
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))
    workspace_dir = os.path.dirname(script_dir)
    db_path = args.db_path or os.path.join(workspace_dir, "CAR_DATA_OUTPUT", "CAR_YOUTUBE_COMMENTS.db")
    output_dir = os.path.dirname(db_path)

    try:
        df_raw = load_data(db_path, force_reprocess=args.force_reprocess, limit=args.limit)
    except Exception as exc:
        logger.error("Failed to load data: %s", exc)
        return

    df_clean = run_phase1_preprocessing(df_raw)
    if args.inspect_phase1:
        print("\n=== Phase 1 Inspection: First 5 Rows of Cleaned Data ===")
        inspect_cols = ["video_title", "Vehicle_Entity", "author", "like_count", "text", "published_at"]
        preview = df_clean[inspect_cols].head(5)
        print(preview.to_string(index=False))
        print("========================================================\n")
        return

    if args.run_all or (not args.inspect_phase1 and not args.test):
        if not df_clean.empty:
            df_absa = run_absa_on_comments(df_clean, model_name=args.model_name, limit=None)
            df_weighted = apply_weights(df_absa)
            persist_scored_comments(df_weighted, db_path)
        else:
            logger.info("No new comments required scoring in this run.")

        all_scored_df = load_all_scored_comments(db_path)
        df_agg, _ = run_phase4_aggregation(all_scored_df, output_dir)

        print("\n=== Aggregated Results Summary ===")
        print(df_agg.head(10).to_string(index=False))
        print("===================================\n")


if __name__ == "__main__":
    main()
