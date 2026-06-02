import argparse
import logging
import os
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import pandas as pd
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from database import YouTubeCommentsDatabase # Import YouTubeCommentsDatabase

BASE_COLUMNS = [
    "video_id",
    "video_title",
    "source",
    "text",
    "extracted_at",
    "comment_id",
    "author",
    "like_count",
    "reply_count",
    "published_at",
    "updated_at",
]

# Playlists from channels: Auto Buyers Guide, The Car Care Nut, Doug Demuro, Out of Spec Reviews, Throttle House
DEFAULT_PLAYLIST_IDS: List[str] = ['PLdmWqCdsjCu4xf48gB1CvyGXHApkEj8ck', 'PLdmWqCdsjCu6XXTGcfwQE-v-8qZKOUbPv', 'PLdmWqCdsjCu4IAZtt7cFYCI-5wLOu-W5v',
                                   'PLdmWqCdsjCu6vdUJzxywmjNa9IMv9E4bP', 'PLdmWqCdsjCu4Ikxn-hvSiATgUkX5qo2iI', 'PLdmWqCdsjCu6sCNmmkITTmDBGJ1CRJ_ML',
                                   'PLdmWqCdsjCu7ZFuv4M5QZYPHwnK9Da-lG', 'PLdmWqCdsjCu7Y-UCY7jFP1f86lI-IFBES', 'PLdmWqCdsjCu7PO4TJSdIgdIrhAHWGwHvj',
                                   'PLeFzfl0Q8rQXWBaEZdyV1v5u1VIuESa-P', 'PLeFzfl0Q8rQX1NvPXH_Y9OgXqdYNT2TZ-', 'PLeFzfl0Q8rQWfHoQ9mKURykiclndxTn1_',
                                   'PLeFzfl0Q8rQVV48qGZhzQWpapc_F8elah', 'PLeFzfl0Q8rQUy9XXwsTDP44mSCssiNlui', 'PLeFzfl0Q8rQWTHQJQhG_p4yQrseZ9J0wd',
                                   'PLeFzfl0Q8rQW8DEc8TwYyc3LHqwATkHEl', 'PLeFzfl0Q8rQUIskEOInB1h4fUMuNa5P_Y', 'PLeFzfl0Q8rQWY5m2ivI9l6gk_PnkOYA_3',
                                   'PLeFzfl0Q8rQXo-oi6JoPtJNibDi39zj7I', 'PLeFzfl0Q8rQXkSiT50RyVAkC_JIHXw318', 'PLeFzfl0Q8rQUdQRUQJ6qBSn_kwvY3jCDz',
                                   'PLeFzfl0Q8rQVaJp_JfzPZm7oufiqv7yfS', 'PLeFzfl0Q8rQVkkMCL6YhpCO_zjSNhH0Er', 'PLeFzfl0Q8rQXIyh70R-2dKBuSlnFattem',
                                   'PLeFzfl0Q8rQWLwOtDhDNGtdZwgMPAZiQa', 'PLeFzfl0Q8rQXfwL5Nfy3EfNnmxhf6oX1P', 'PLRsapyZqDs1o2_cfJNIxQzb-X1-mpYbdA',
                                   'PLRsapyZqDs1ouavnWh8mpUBZsEjJYSCQI', 'PLRsapyZqDs1ptElTMW3o3PByTZLh4aHmO', 'PLRsapyZqDs1pmgg5rl2ox68CeUm72yG5-',
                                   'PLRsapyZqDs1pC9lvVceSwqUrqx1u8UJ8d', 'PLRsapyZqDs1qzeBRxL7cgjzAY6QEcAaZe', 'PLRsapyZqDs1r0rPWDqedN2NfnSi6BvnTz',
                                   'PLRsapyZqDs1rBVc5yES2M5cJY7sD2nrwN', 'PLRsapyZqDs1rCXGVr98GDWhf6xbHeCjDr', 'PLRsapyZqDs1qq65E6sWXSJELHGVqEeB_0',
                                   'PLRsapyZqDs1psCURs8OOvaxPk-e2WiJZ1', 'PLRsapyZqDs1od8oRXNJvrrINjYPLJ4XUL', 'PLRsapyZqDs1oWFpAszIPLBAggO3i7MynT',
                                   'PLRsapyZqDs1oRf6s2gk6qF0BN_kGKXSKZ', 'PLRsapyZqDs1paxezbET0LJ00M1IHesv0u', 'PLRsapyZqDs1oS4H-h-7PMeNF3bC1Df60q',
                                   'PLRsapyZqDs1oMzw7_bLfVPpHQgefNsxvC', 'PLRsapyZqDs1qjJB9zPXM5gFVCZ3NDUWLH', 'PLRsapyZqDs1oitmbWCluObhfUGQ3ZuvzC',
                                   'PLRsapyZqDs1ryftnOF61p1ez4K576556G', 'PLRsapyZqDs1r3sjTDWALSNHFuzejXk9vf', 'PLRsapyZqDs1rWoLJB2miI4tx_Q4_nAOXe',
                                   'PLVa4b_Vn4gbBWaieOY6Z_zd37XlbHvsG6', 'PLVa4b_Vn4gbDD5IrxxVnYU7GVr_xjp9Zx', 'PLVa4b_Vn4gbCcL-FHtFY9837w0Hw5mAiG',
                                   'PLuLtF2Rwd40U_7Tiia8CFUr62nXVrfQN-']
DEFAULT_VIDEO_IDS: List[str] = []

def _read_key_from_env_file(env_path: Path) -> Optional[str]:
    try:
        for raw_line in env_path.read_text(encoding="utf-8").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() in {"YOUTUBE_API_KEY", "GOOGLE_API_KEY"}:
                return value.strip().strip("\"'") or None
    except FileNotFoundError:
        return None
    return None


def load_api_key(api_key_file: Optional[str] = None) -> Optional[str]:
    key = os.getenv("YOUTUBE_API_KEY") or os.getenv("GOOGLE_API_KEY")
    if key:
        logging.info("API key loaded from environment variable.")
        return key.strip()

    env_path = Path(__file__).resolve().parent.parent / ".env"
    env_key = _read_key_from_env_file(env_path)
    if env_key:
        logging.info("API key loaded from .env file at: %s", env_path)
        return env_key

    if api_key_file:
        logging.info("Attempting to load API key from file: %s", api_key_file)
        try:
            return Path(api_key_file).read_text(encoding="utf-8").strip() or None
        except FileNotFoundError:
            logging.error("API key file not found: %s", api_key_file)
    return None


def fetch_comments(
    video_id: str,
    api_key: str, # api_key is now passed directly
    max_comments: int,
    order: str,
    video_title: Optional[str] = None,
) -> List[Dict]:
    youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
    comments: List[Dict] = []
    remaining = max_comments if max_comments > 0 else -1
    page_token = None

    while remaining != 0:
        batch_size = 100 if remaining < 0 else min(100, remaining)
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=batch_size,
            order=order,
            textFormat="plainText",
            pageToken=page_token,
        )
        response = request.execute()
        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            top_comment = snippet.get("topLevelComment", {}).get("snippet", {})
            comments.append(
                {
                    "video_id": video_id,
                    "video_title": video_title,
                    "source": "comment",
                    "text": top_comment.get("textOriginal")
                    or top_comment.get("textDisplay")
                    or "",
                    "comment_id": item.get("id"),
                    "author": top_comment.get("authorDisplayName"),
                    "like_count": top_comment.get("likeCount"),
                    "reply_count": snippet.get("totalReplyCount"),
                    "published_at": top_comment.get("publishedAt"),
                    "updated_at": top_comment.get("updatedAt"),
                }
            )
            if remaining > 0:
                remaining -= 1
                if remaining == 0:
                    break
        if remaining == 0:
            break
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return comments


def fetch_video_title(video_id: str, api_key: str) -> Optional[str]: # api_key is now passed directly
    youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
    request = youtube.videos().list(part="snippet", id=video_id, maxResults=1)
    response = request.execute()
    items = response.get("items", [])
    if not items:
        return None
    return items[0].get("snippet", {}).get("title")


def fetch_playlist_videos(
    playlist_id: str,
    api_key: str, # api_key is now passed directly
    max_videos: int,
) -> List[Dict]:
    youtube = build("youtube", "v3", developerKey=api_key, cache_discovery=False)
    videos: List[Dict] = []
    remaining = max_videos if max_videos > 0 else -1
    page_token = None

    while remaining != 0:
        batch_size = 50 if remaining < 0 else min(50, remaining)
        request = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=batch_size,
            pageToken=page_token,
        )
        response = request.execute()
        for item in response.get("items", []):
            snippet = item.get("snippet", {})
            resource = snippet.get("resourceId", {})
            video_id = resource.get("videoId")
            if not video_id:
                continue
            videos.append({"video_id": video_id, "title": snippet.get("title")})
            if remaining > 0:
                remaining -= 1
                if remaining == 0:
                    break
        if remaining == 0:
            break
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    return videos


def build_dataframe(rows: Iterable[Dict]) -> pd.DataFrame:
    df = pd.DataFrame(list(rows))
    if df.empty:
        return pd.DataFrame(columns=BASE_COLUMNS)
    for col in BASE_COLUMNS:
        if col not in df.columns:
            df[col] = None
    df = df[BASE_COLUMNS]
    return df


def run_pipeline(
    video_id: str,
    max_comments: int,
    order: str,
    db_path: Path,
    api_key: str, # api_key is now passed directly
) -> Path:
    extracted_at = datetime.now(timezone.utc).isoformat()
    rows: List[Dict] = []

    # Removed redundant load_api_key call
    # api_key = load_api_key(api_key_file)
    # if not api_key:
    #     raise ValueError(
    #         "Missing API key. Set YOUTUBE_API_KEY/GOOGLE_API_KEY or use --api-key-file."
    #     )

    video_title = None
    try:
        video_title = fetch_video_title(video_id, api_key)
    except HttpError as exc:
        logging.warning("Unable to fetch title for %s: %s", video_id, exc)

    try:
        rows.extend(fetch_comments(video_id, api_key, max_comments, order, video_title))
    except HttpError as exc:
        raise RuntimeError(f"YouTube API error: {exc}") from exc

    for row in rows:
        row["extracted_at"] = extracted_at

    df = build_dataframe(rows)
    
    # Initialize database and insert data
    db = YouTubeCommentsDatabase(str(db_path))
    inserted_count = db.insert_sentiment_data(df, table_name='youtube_comments_sentiment')
    db.close()

    logging.info(f"Inserted {inserted_count} comments into {db_path}")
    return db_path


def run_playlist_pipeline(
    playlist_id: str,
    max_videos: int,
    max_comments: int,
    order: str,
    db_path: Path,
    api_key: str, # api_key is now passed directly
) -> Path:
    # Removed redundant load_api_key call
    # api_key = load_api_key(api_key_file)
    # if not api_key:
    #     raise ValueError(
    #         "Missing API key. Set YOUTUBE_API_KEY/GOOGLE_API_KEY or use --api-key-file."
    #     )

    videos = fetch_playlist_videos(playlist_id, api_key, max_videos)
    if not videos:
        raise ValueError(f"No videos found for playlist {playlist_id}.")

    extracted_at = datetime.now(timezone.utc).isoformat()
    rows: List[Dict] = []

    for video in videos:
        video_id = video["video_id"]
        video_title = video.get("title")
        try:
            rows.extend(
                fetch_comments(video_id, api_key, max_comments, order, video_title)
            )
        except HttpError as exc:
            logging.warning(
                "Skipping comments for %s due to API error: %s",
                video_id,
                exc,
            )

    for row in rows:
        row["extracted_at"] = extracted_at

    df = build_dataframe(rows)

    # Initialize database and insert data
    db = YouTubeCommentsDatabase(str(db_path))
    inserted_count = db.insert_sentiment_data(df, table_name='youtube_comments_sentiment')
    db.close()

    logging.info(f"Inserted {inserted_count} comments into {db_path}")
    return db_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch YouTube comments and save sentiment-ready data to a SQL database using the official API."
    )
    parser.add_argument("--video-id", help="YouTube video id")
    parser.add_argument("--playlist-id", help="YouTube playlist id")
    parser.add_argument(
        "--max-videos",
        type=int,
        default=0,
        help="Max videos to fetch from playlist (0 for all)",
    )
    parser.add_argument(
        "--max-comments",
        type=int,
        default=0, # Defaulted to 0 to pull all by default, adjust as needed
        help="Max comments to fetch per video (0 for all)",
    )
    parser.add_argument(
        "--comment-order",
        choices=["relevance", "time"],
        default="relevance",
        help="Ordering for comments",
    )
    parser.add_argument("--api-key-file", help="Path to API key file")
    parser.add_argument(
        "--output-db",
        default=os.path.join(Path(__file__).resolve().parent.parent, "CAR_DATA_OUTPUT", "CAR_YOUTUBE_COMMENTS.db"),
        help="Output SQLite database path (default: CAR_DATA_OUTPUT/CAR_YOUTUBE_COMMENTS.db)"
    )

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

    db_path = Path(args.output_db) # Convert to Path object

    # Load API key once at the beginning of main
    api_key = load_api_key(args.api_key_file)
    if not api_key:
        raise ValueError(
            "Missing API key. Set YOUTUBE_API_KEY/GOOGLE_API_KEY or use --api-key-file."
        )

    if bool(args.video_id) == bool(args.playlist_id):
        if args.video_id or args.playlist_id:
            raise ValueError("Provide exactly one of --video-id or --playlist-id.")
        if DEFAULT_PLAYLIST_IDS:
            for playlist_id in DEFAULT_PLAYLIST_IDS:
                output_target = run_playlist_pipeline(
                    playlist_id=playlist_id,
                    max_videos=args.max_videos,
                    max_comments=args.max_comments,
                    order=args.comment_order,
                    db_path=db_path,
                    api_key=api_key, # Pass the loaded API key
                )
                print(f"Wrote data to DB: {output_target}")
            return
        if DEFAULT_VIDEO_IDS:
            for video_id in DEFAULT_VIDEO_IDS:
                output_target = run_pipeline(
                    video_id=video_id,
                    max_comments=args.max_comments,
                    order=args.comment_order,
                    db_path=db_path,
                    api_key=api_key, # Pass the loaded API key
                )
                print(f"Wrote data to DB: {output_target}")
            return
        raise ValueError(
            "Provide --video-id/--playlist-id or populate DEFAULT_PLAYLIST_IDS/DEFAULT_VIDEO_IDS."
        )

    if args.playlist_id:
        output_target = run_playlist_pipeline(
            playlist_id=args.playlist_id,
            max_videos=args.max_videos,
            max_comments=args.max_comments,
            order=args.comment_order,
            db_path=db_path,
            api_key=api_key, # Pass the loaded API key
        )
    else:
        output_target = run_pipeline(
            video_id=args.video_id,
            max_comments=args.max_comments,
            order=args.comment_order,
            db_path=db_path,
            api_key=api_key, # Pass the loaded API key
        )

    print(f"Wrote data to DB: {output_target}")


if __name__ == "__main__":
    main()