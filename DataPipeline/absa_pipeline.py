import os
import re
import argparse
import logging
import pandas as pd
import numpy as np

# Database Import
from database import CarDatabase

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ==========================================
# CONSTANTS & PRE-COMPILED PATTERNS
# ==========================================

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

YEAR_PATTERN = re.compile(r'\b(19[89]\d|20[0-2]\d)\b')  # Matches 1980 - 2029

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

# Pre-compiled Regex Patterns for Performance Optimization
URL_PATTERN = re.compile(r'https?://\S+|www\.\S+')
HTML_PATTERN = re.compile(r'<.*?>')
SPAM_PATTERNS = [re.compile(p, re.IGNORECASE) for p in [
    r'\bsubscribe\b', r'\bmy channel\b', r'\bcheck out my\b', r'\bclick the link\b',
    r'\bclick here\b', r'\btelegram\b', r'\bwhatsapp\b', r'\bcontact me\b',
    r'\bpromo\b', r'\bdiscount\b', r'\bgiveaway\b'
]]
BOT_PATTERN = re.compile(r'(.)\1{5,}')
PHONE_PATTERN = re.compile(r'\+\d{1,3}[-.\s]?\d{3,4}[-.\s]?\d{4}')
CRYPTO_PATTERN1 = re.compile(r'\b0x[a-fA-F0-9]{40}\b')
CRYPTO_PATTERN2 = re.compile(r'\b[13][a-km-zA-HJ-NP-Z1-9]{25,34}\b')
EXCESS_BANG = re.compile(r'!{2,}')
EXCESS_QUESTION = re.compile(r'\?{2,}')
EXCESS_DOTS = re.compile(r'\.{4,}')


def extract_vehicle(title: str) -> str:
    """
    Extracts Year, Make, and Model from a video title.
    Returns f"{Year} {Make} {Model}" or None if extraction is not confident.
    """
    if not title or not isinstance(title, str):
        return None

    # 1. Extract Year
    year_match = YEAR_PATTERN.search(title)
    if not year_match:
        return None
    year = year_match.group(1)

    # 2. Extract Make
    found_make = None
    make_start = -1
    make_end = -1

    for make in MAKES_LIST:
        match = re.search(rf"\b{re.escape(make)}\b", title, re.IGNORECASE)
        if match:
            found_make = make
            make_start = match.start()
            make_end = match.end()
            break

    if not found_make:
        return None

    # 3. Extract Model from words after Make
    post_make_str = title[make_end:].strip()
    words = post_make_str.split()
    model_words = []

    for word in words:
        cleaned_word = word.strip(".,;:!?|()-–—\"'[]{}")
        cleaned_word_lower = cleaned_word.lower()

        if word.startswith(("|", "-", "–", "—", ":", "/", "\\")):
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

        if word.endswith((":", "|", "-", "–", "—", ".", "?", "!")):
            break

    model_words = model_words[:3]
    if not model_words:
        return None

    model = " ".join(model_words)
    normalized_make = MAKE_NORM_MAP.get(found_make.lower(), found_make.capitalize())

    return f"{year} {normalized_make} {model}"


def clean_comment_text(text: str) -> str:
    """
    Cleans comment text and filters out spam, short messages, and bot behaviors using pre-compiled regex.
    Returns cleaned text or None if the comment is filtered out.
    """
    if not text or not isinstance(text, str):
        return None

    text = URL_PATTERN.sub('', text)
    text = HTML_PATTERN.sub('', text)

    text_lower = text.strip().lower()
    if text_lower in {"first", "first!", "first!!", "first comment", "subscribe", "sub"}:
        return None

    for pattern in SPAM_PATTERNS:
        if pattern.search(text):
            return None

    if BOT_PATTERN.search(text):
        return None
    if PHONE_PATTERN.search(text):
        return None
    if CRYPTO_PATTERN1.search(text) or CRYPTO_PATTERN2.search(text):
        return None

    text = EXCESS_BANG.sub('!', text)
    text = EXCESS_QUESTION.sub('?', text)
    text = EXCESS_DOTS.sub('...', text)

    text = text.strip()

    words = text.split()
    if len(words) < 3:
        return None

    return text


def load_data(db_path: str) -> pd.DataFrame:
    """Loads comments from SQLite database using CarDatabase manager."""
    logger.info(f"Connecting to database at {db_path}...")
    if not os.path.exists(db_path):
        raise FileNotFoundError(f"Database file not found: {db_path}")

    db = CarDatabase(db_path)
    try:
        conn = db._get_connection()
        df = pd.read_sql_query("SELECT * FROM youtube_comments_sentiment", conn)
        logger.info(f"Successfully loaded {len(df)} comments from database.")
        return df
    finally:
        db.close()


def run_phase1_preprocessing(df: pd.DataFrame) -> pd.DataFrame:
    """Executes Phase 1: Ingestion & Preprocessing."""
    logger.info("Starting Phase 1: Data Ingestion & Preprocessing...")

    logger.info("Extracting vehicle entities from video titles...")
    df['Vehicle_Entity'] = df['video_title'].apply(extract_vehicle)

    unidentified_count = df['Vehicle_Entity'].isna().sum()
    logger.info(f"Extraction summary: {len(df) - unidentified_count} identified, {unidentified_count} unidentified.")

    df_clean = df.dropna(subset=['Vehicle_Entity']).copy()
    logger.info(
        f"Dropped {unidentified_count} rows with unidentified vehicle entities. {len(df_clean)} rows remaining.")

    logger.info("Cleaning text and filtering spam/bots/short comments...")

    # Simplified Pandas Dataframe logic for better performance
    df_clean['original_text'] = df_clean['text']
    df_clean['text'] = df_clean['text'].apply(clean_comment_text)

    cleaned_drop_count = df_clean['text'].isna().sum()
    df_clean = df_clean.dropna(subset=['text']).copy()

    logger.info(f"Dropped {cleaned_drop_count} rows due to text cleaning/filtering. {len(df_clean)} rows remaining.")

    return df_clean


def run_absa_on_comments(df: pd.DataFrame, model_name: str = "facebook/bart-large-mnli",
                         limit: int = None) -> pd.DataFrame:
    """Executes Phase 2: Aspect-Based Sentiment Analysis."""
    logger.info("Initializing HuggingFace zero-shot classification pipeline...")
    import torch
    from transformers import pipeline

    device = 0 if torch.cuda.is_available() else -1
    classifier = pipeline("zero-shot-classification", model=model_name, device=device)

    logger.info(f"Loaded classifier on device: {'GPU' if device == 0 else 'CPU'}")

    aspect_mapping = {
        "good reliability and durability": ("reliability", 1),
        "bad reliability and durability": ("reliability", -1),
        "good value and fair price": ("value", 1),
        "bad value and overpriced": ("value", -1),
        "good performance and handling": ("performance", 1),
        "poor performance and handling": ("performance", -1),
        "good comfort and interior": ("comfort", 1),
        "poor comfort and interior": ("comfort", -1),
    }
    candidate_labels = list(aspect_mapping.keys())

    if limit:
        df = df.iloc[:limit].copy()
    else:
        df = df.copy()

    texts = df['text'].tolist()

    for aspect in ["reliability", "value", "performance", "comfort"]:
        df[f"{aspect}_sentiment"] = np.nan
        df[f"{aspect}_mentioned"] = 0
        df[f"{aspect}_confidence"] = 0.0

    logger.info(f"Running zero-shot classification on {len(texts)} comments...")

    batch_size = 16
    results = []

    for i in range(0, len(texts), batch_size):
        batch_texts = texts[i:i + batch_size]
        batch_results = classifier(
            batch_texts,
            candidate_labels=candidate_labels,
            multi_label=True,
            hypothesis_template="This comment is about {}."
        )
        if isinstance(batch_results, dict):
            batch_results = [batch_results]
        results.extend(batch_results)

        if (i + batch_size) % 64 == 0 or (i + batch_size) >= len(texts):
            processed = min(i + batch_size, len(texts))
            logger.info(f"Processed {processed}/{len(texts)} comments...")

    for idx, res in enumerate(results):
        label_scores = dict(zip(res['labels'], res['scores']))

        aspects = ["reliability", "value", "performance", "comfort"]
        for aspect in aspects:
            pos_label = [l for l, (a, p) in aspect_mapping.items() if a == aspect and p == 1][0]
            neg_label = [l for l, (a, p) in aspect_mapping.items() if a == aspect and p == -1][0]

            s_pos = label_scores[pos_label]
            s_neg = label_scores[neg_label]

            mention_threshold = 0.35
            max_score = max(s_pos, s_neg)

            if max_score > mention_threshold:
                df.iloc[idx, df.columns.get_loc(f"{aspect}_mentioned")] = 1
                df.iloc[idx, df.columns.get_loc(f"{aspect}_confidence")] = float(max_score)
                sentiment = (s_pos - s_neg) / (s_pos + s_neg + 1e-9)
                df.iloc[idx, df.columns.get_loc(f"{aspect}_sentiment")] = float(sentiment)

    return df


def apply_weights(df: pd.DataFrame) -> pd.DataFrame:
    """Executes Phase 3: Weighted Scoring System."""
    logger.info("Starting Phase 3: Weighted Scoring System...")
    df = df.copy()

    df['like_count'] = df['like_count'].fillna(0).astype(float)
    df['consensus_weight'] = 1.0 + np.log10(df['like_count'] + 1.0)

    df['word_count'] = df['text'].apply(lambda t: len(str(t).split()))
    df['depth_weight'] = np.where(df['word_count'] >= 20, 1.2, 1.0)

    df['comment_weight'] = df['consensus_weight'] * df['depth_weight']

    aspects = ["reliability", "value", "performance", "comfort"]
    for aspect in aspects:
        df[f"Weighted_{aspect.capitalize()}_Score"] = df[f"{aspect}_sentiment"] * df['comment_weight']

    return df


def run_phase4_aggregation(df: pd.DataFrame, output_dir: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Executes Phase 4: Aggregation and Database Export."""
    logger.info("Starting Phase 4: Aggregation and Index Creation...")

    grouped = df.groupby('Vehicle_Entity')
    aggregated_results = []

    for entity, group in grouped:
        sample_size = len(group)

        reliability_group = group[group['reliability_mentioned'] == 1]
        if not reliability_group.empty:
            rel_weighted_sum = (reliability_group['reliability_sentiment'] * reliability_group['comment_weight']).sum()
            weight_sum = reliability_group['comment_weight'].sum()
            rel_weighted_avg = rel_weighted_sum / (weight_sum + 1e-9)
            reliability_index = (rel_weighted_avg + 1.0) * 50.0
        else:
            reliability_index = np.nan

        total_weighted_sentiment = 0.0
        total_weights = 0.0

        aspects = ["reliability", "value", "performance", "comfort"]
        for aspect in aspects:
            aspect_group = group[group[f"{aspect}_mentioned"] == 1]
            if not aspect_group.empty:
                total_weighted_sentiment += (aspect_group[f"{aspect}_sentiment"] * aspect_group['comment_weight']).sum()
                total_weights += aspect_group['comment_weight'].sum()

        if total_weights > 0:
            gen_weighted_avg = total_weighted_sentiment / total_weights
            general_enthusiast_score = (gen_weighted_avg + 1.0) * 50.0
        else:
            general_enthusiast_score = np.nan

        times = []
        sentiments = []
        for _, row in group.iterrows():
            date_val = pd.to_datetime(row['published_at'], errors='coerce')
            if pd.isna(date_val):
                continue

            for aspect in aspects:
                sent = row[f"{aspect}_sentiment"]
                if not pd.isna(sent):
                    times.append(date_val.timestamp() / (24 * 3600))
                    sentiments.append(sent)

        if len(sentiments) >= 5:
            times = np.array(times)
            sentiments = np.array(sentiments)
            sentiment_std_dev = float(np.std(sentiments))
            time_var = np.var(times)
            if time_var == 0:
                sentiment_trend_slope = 0.0
            else:
                sentiment_trend_slope = float(np.cov(times, sentiments)[0, 1] / time_var)
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
            "Confidence_Level": confidence_level
        })

    df_agg = pd.DataFrame(aggregated_results)

    if not df_agg.empty:
        df_agg = df_agg.sort_values(by="General_Enthusiast_Score", ascending=False).reset_index(drop=True)

    # === NEW DATABASE EXPORT LOGIC ===
    db_path = os.path.join(output_dir, "CAR_YOUTUBE_COMMENTS.db")
    logger.info(f"Exporting DataFrames to database: {db_path}...")

    db = CarDatabase(db_path)
    try:
        conn = db._get_connection()

        # Write tables to database, replacing previous runs
        df_agg.to_sql("vehicle_sentiment_index", conn, if_exists="replace", index=False)
        logger.info("Successfully wrote 'vehicle_sentiment_index' table to DB.")

        df.to_sql("youtube_comments_scored", conn, if_exists="replace", index=False)
        logger.info("Successfully wrote 'youtube_comments_scored' table to DB.")
    except Exception as e:
        logger.error(f"Failed to write to database: {e}")
    finally:
        db.close()

    return df_agg, df


def run_tests():
    """Runs a suite of tests on vehicle parsing and comment cleaning."""
    print("Running ABSA Pipeline Tests...")

    test_titles = {
        "2024 Toyota Camry Hybrid Review": "2024 Toyota Camry Hybrid",
        "2021 Ford F-150 Raptor vs Ram TRX": "2021 Ford F-150 Raptor",
        "Is the 2023 Honda Civic Type R worth $45k?": "2023 Honda Civic Type R",
        "2022 Hyundai Ioniq 5: An Amazing EV": "2022 Hyundai Ioniq 5",
        "Toyota Camry 2025 Review": "2025 Toyota Camry",
        "Just a random video without car title": None
    }

    for title, expected in test_titles.items():
        res = extract_vehicle(title)
        assert res == expected, f"Failed title extraction. Input: '{title}', Expected: '{expected}', Got: '{res}'"
        print(f"PASSED title extraction: '{title}' -> '{res}'")

    test_comments = {
        "This car is amazing! I love the handling and design.": "This car is amazing! I love the handling and design.",
        "First!": None,
        "Subscribe to my channel for free crypto: http://scam.link": None,
        "Check out my whatsapp +1-555-0199 for investment tips!": None,
        "soooooooo goooooood": None,
        "Great car": None,
        "Good.": None
    }

    for comment, expected in test_comments.items():
        res = clean_comment_text(comment)
        assert res == expected, f"Failed comment cleaning. Input: '{comment}', Expected: '{expected}', Got: '{res}'"
        print(f"PASSED comment cleaning: '{comment}' -> '{res}'")

    print("All tests passed successfully!")


def main():
    parser = argparse.ArgumentParser(description="Automotive Aspect-Based Sentiment Analysis Pipeline")
    parser.add_argument("--db-path", type=str, default="", help="Path to CAR_YOUTUBE_COMMENTS.db")
    parser.add_argument("--inspect-phase1", action="store_true",
                        help="Run only Phase 1 & inspect ingestion/preprocessing results")
    parser.add_argument("--test", action="store_true", help="Run validation tests")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of comments to process")
    parser.add_argument("--run-all", action="store_true", help="Run all 4 phases of the pipeline")

    args = parser.parse_args()

    if args.test:
        run_tests()
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))
    workspace_dir = os.path.dirname(script_dir)

    db_path = args.db_path
    if not db_path:
        db_path = os.path.join(workspace_dir, "CAR_DATA_OUTPUT", "CAR_YOUTUBE_COMMENTS.db")

    output_dir = os.path.dirname(db_path)

    try:
        df_raw = load_data(db_path)
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        return

    df_clean = run_phase1_preprocessing(df_raw)

    if args.inspect_phase1:
        print("\n=== Phase 1 Inspection: First 5 Rows of Cleaned Data ===")
        inspect_cols = ['video_title', 'Vehicle_Entity', 'author', 'like_count', 'text', 'published_at']
        preview = df_clean[inspect_cols].head(5)
        print(preview.to_string(index=False))
        print("========================================================\n")
        return

    if args.run_all or (not args.inspect_phase1 and not args.test):
        df_absa = run_absa_on_comments(df_clean, limit=args.limit)
        df_weighted = apply_weights(df_absa)
        df_agg, df_detailed = run_phase4_aggregation(df_weighted, output_dir)

        print("\n=== Aggregated Results Summary ===")
        print(df_agg.head(10).to_string(index=False))
        print("===================================\n")


if __name__ == "__main__":
    main()