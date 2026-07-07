import tempfile
import unittest
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from DataPipeline.SentimentAnalysis import QuotaExceededError, run_queue
from DataPipeline.absa_pipeline import apply_weights, load_data, run_phase4_aggregation
from DataPipeline.database import YouTubeCommentsDatabase


def make_comment_rows(video_id: str, playlist_id: str, count: int) -> list[dict]:
    rows = []
    for idx in range(count):
        rows.append(
            {
                "video_id": video_id,
                "playlist_id": playlist_id,
                "video_title": "2024 Toyota Camry Review",
                "source": "comment",
                "text": f"This car has great reliability and comfort number {idx}",
                "extracted_at": "2026-07-06T00:00:00+00:00",
                "comment_id": f"{video_id}_comment_{idx}",
                "author": "tester",
                "like_count": idx,
                "reply_count": 0,
                "published_at": "2026-07-06T00:00:00+00:00",
                "updated_at": "2026-07-06T00:00:00+00:00",
            }
        )
    return rows


class SentimentIncrementalTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "CAR_YOUTUBE_COMMENTS.db"
        self.db = YouTubeCommentsDatabase(str(self.db_path))

    def tearDown(self):
        self.db.close()
        self.tmp.cleanup()

    def test_unseen_videos_sort_ahead_of_stale_completed(self):
        self.db.ensure_video_fetch_state("new_video", playlist_id="playlist_a", video_title="New")
        self.db.ensure_video_fetch_state("stale_video", playlist_id="playlist_a", video_title="Stale")
        self.db.update_video_fetch_outcome(
            "stale_video",
            status=YouTubeCommentsDatabase.FETCH_STATUS_COMPLETE,
            comments_seen_count=3,
            refresh_days=0,
        )

        candidates = self.db.get_candidate_videos(
            refresh_days=30,
            force_recheck=False,
            now_iso="2999-01-01T00:00:00+00:00",
        )
        self.assertGreaterEqual(len(candidates), 2)
        self.assertEqual(candidates[0]["video_id"], "new_video")
        self.assertEqual(candidates[0]["priority_bucket"], 1)
        self.assertEqual(candidates[1]["video_id"], "stale_video")
        self.assertEqual(candidates[1]["priority_bucket"], 5)

    def test_never_ingested_videos_sort_ahead_of_pending_with_existing_comments(self):
        self.db.ensure_video_fetch_state("already_ingested", playlist_id="playlist_a", video_title="Old")
        self.db.ensure_video_fetch_state("never_ingested", playlist_id="playlist_a", video_title="New")
        self.db.insert_sentiment_data(pd.DataFrame(make_comment_rows("already_ingested", "playlist_a", 2)))

        candidates = self.db.get_candidate_videos(playlist_ids=["playlist_a"])

        self.assertEqual([row["video_id"] for row in candidates], ["never_ingested", "already_ingested"])
        self.assertEqual(candidates[0]["has_existing_comments"], 0)
        self.assertEqual(candidates[0]["priority_bucket"], 1)
        self.assertEqual(candidates[1]["has_existing_comments"], 1)
        self.assertEqual(candidates[1]["priority_bucket"], 3)

    def test_partially_completed_playlist_surfaces_pending_videos(self):
        playlist_id = "playlist_partial"
        videos = [{"video_id": "done_video", "title": "Done"}, {"video_id": "pending_video", "title": "Pending"}]
        self.db.upsert_playlist_discovery(playlist_id, videos)
        self.db.update_video_fetch_outcome(
            "done_video",
            status=YouTubeCommentsDatabase.FETCH_STATUS_COMPLETE,
            comments_seen_count=5,
            refresh_days=30,
        )

        candidates = self.db.get_candidate_videos(playlist_ids=[playlist_id])
        self.assertEqual([row["video_id"] for row in candidates], ["pending_video"])

    def test_zero_comment_videos_are_not_retried_immediately(self):
        self.db.ensure_video_fetch_state("zero_video", playlist_id="playlist_zero")
        self.db.update_video_fetch_outcome(
            "zero_video",
            status=YouTubeCommentsDatabase.FETCH_STATUS_ZERO_COMMENTS,
            comments_seen_count=0,
            refresh_days=30,
        )

        candidates = self.db.get_candidate_videos(force_recheck=False)
        self.assertNotIn("zero_video", [row["video_id"] for row in candidates])

    def test_quota_exhaustion_stops_queue_and_preserves_resume_state(self):
        self.db.ensure_video_fetch_state("quota_video", playlist_id="playlist_q", video_title="Quota")
        self.db.ensure_video_fetch_state("next_video", playlist_id="playlist_q", video_title="Next")

        with patch("DataPipeline.SentimentAnalysis.fetch_comments", side_effect=QuotaExceededError("quota hit")), patch(
            "DataPipeline.SentimentAnalysis.fetch_video_title", return_value="Quota title"
        ):
            summary = run_queue(
                db=self.db,
                api_key="dummy",
                max_comments=10,
                order="relevance",
                refresh_days=30,
                force_recheck=False,
                stop_on_quota=True,
                playlist_ids=["playlist_q"],
            )

        quota_state = self.db.get_video_fetch_state("quota_video")
        next_state = self.db.get_video_fetch_state("next_video")
        self.assertTrue(summary["stopped_on_quota"])
        statuses = {quota_state["last_status"], next_state["last_status"]}
        self.assertIn(YouTubeCommentsDatabase.FETCH_STATUS_QUOTA_EXHAUSTED, statuses)
        self.assertIn(YouTubeCommentsDatabase.FETCH_STATUS_PENDING, statuses)

    def test_incremental_loader_returns_only_unscored_comments(self):
        raw_df = pd.DataFrame(make_comment_rows("video_a", "playlist_a", 3))
        self.db.insert_sentiment_data(raw_df)

        scored_df = raw_df.iloc[:1].copy()
        scored_df["Vehicle_Entity"] = "2024 Toyota Camry"
        scored_df["original_text"] = scored_df["text"]
        for aspect in ["reliability", "value", "performance", "comfort"]:
            scored_df[f"{aspect}_sentiment"] = 0.5
            scored_df[f"{aspect}_mentioned"] = 1
            scored_df[f"{aspect}_confidence"] = 0.7
        scored_df = apply_weights(scored_df)
        scored_df["processed_at"] = "2026-07-06T00:00:00+00:00"
        scored_df["model_name"] = "test-model"
        scored_df["aspect_version"] = "test-version"
        self.db.upsert_scored_comments(scored_df)

        pending = load_data(str(self.db_path), force_reprocess=False)
        self.assertEqual(len(pending), 2)
        self.assertNotIn(raw_df.iloc[0]["comment_id"], set(pending["comment_id"]))

    def test_scored_comment_upserts_avoid_duplicates(self):
        raw_df = pd.DataFrame(make_comment_rows("video_b", "playlist_b", 1))
        self.db.insert_sentiment_data(raw_df)

        scored_df = raw_df.copy()
        scored_df["Vehicle_Entity"] = "2024 Toyota Camry"
        scored_df["original_text"] = scored_df["text"]
        for aspect in ["reliability", "value", "performance", "comfort"]:
            scored_df[f"{aspect}_sentiment"] = 0.2
            scored_df[f"{aspect}_mentioned"] = 1
            scored_df[f"{aspect}_confidence"] = 0.8
        scored_df = apply_weights(scored_df)
        scored_df["processed_at"] = "2026-07-06T00:00:00+00:00"
        scored_df["model_name"] = "test-model"
        scored_df["aspect_version"] = "test-version"

        self.db.upsert_scored_comments(scored_df)
        self.db.upsert_scored_comments(scored_df)

        df = self.db.load_comments_for_absa(force_reprocess=True)
        self.assertEqual(len(df), 1)

        conn = self.db._get_connection()
        scored_count = conn.execute("SELECT COUNT(*) FROM youtube_comments_scored").fetchone()[0]
        self.assertEqual(scored_count, 1)

    def test_aggregation_rebuilds_from_all_scored_rows(self):
        raw_df = pd.DataFrame(make_comment_rows("video_c", "playlist_c", 2))
        self.db.insert_sentiment_data(raw_df)

        scored_df = raw_df.copy()
        scored_df["Vehicle_Entity"] = "2024 Toyota Camry"
        scored_df["original_text"] = scored_df["text"]
        for aspect in ["reliability", "value", "performance", "comfort"]:
            scored_df[f"{aspect}_sentiment"] = 0.4
            scored_df[f"{aspect}_mentioned"] = 1
            scored_df[f"{aspect}_confidence"] = 0.85
        scored_df = apply_weights(scored_df)
        scored_df["processed_at"] = "2026-07-06T00:00:00+00:00"
        scored_df["model_name"] = "test-model"
        scored_df["aspect_version"] = "test-version"
        self.db.upsert_scored_comments(scored_df)

        all_scored = pd.read_sql_query("SELECT * FROM youtube_comments_scored", self.db._get_connection())
        df_agg, _ = run_phase4_aggregation(all_scored, str(self.db_path.parent))

        self.assertEqual(len(df_agg), 1)
        self.assertEqual(df_agg.iloc[0]["Vehicle_Entity"], "2024 Toyota Camry")
        self.assertEqual(int(df_agg.iloc[0]["Sample_Size"]), 2)

    def test_legacy_scored_table_is_upgraded_before_upsert(self):
        legacy_db_path = Path(self.tmp.name) / "legacy_comments.db"
        conn = sqlite3.connect(legacy_db_path)
        try:
            conn.execute(
                """
                CREATE TABLE youtube_comments_scored
                (
                    video_id TEXT,
                    playlist_id TEXT,
                    video_title TEXT,
                    source TEXT,
                    text TEXT,
                    extracted_at TEXT,
                    comment_id TEXT,
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
                    Weighted_Comfort_Score REAL
                )
                """
            )
            conn.commit()
        finally:
            conn.close()

        legacy_db = YouTubeCommentsDatabase(str(legacy_db_path))
        try:
            raw_df = pd.DataFrame(make_comment_rows("video_legacy", "playlist_legacy", 1))
            legacy_db.insert_sentiment_data(raw_df)

            scored_df = raw_df.copy()
            scored_df["Vehicle_Entity"] = "2024 Toyota Camry"
            scored_df["original_text"] = scored_df["text"]
            for aspect in ["reliability", "value", "performance", "comfort"]:
                scored_df[f"{aspect}_sentiment"] = 0.3
                scored_df[f"{aspect}_mentioned"] = 1
                scored_df[f"{aspect}_confidence"] = 0.75
            scored_df = apply_weights(scored_df)
            scored_df["processed_at"] = "2026-07-06T00:00:00+00:00"
            scored_df["model_name"] = "test-model"
            scored_df["aspect_version"] = "test-version"

            inserted = legacy_db.upsert_scored_comments(scored_df)
            self.assertEqual(inserted, 1)

            columns = {
                row[1]
                for row in legacy_db._get_connection().execute(
                    "PRAGMA table_info(youtube_comments_scored)"
                ).fetchall()
            }
            self.assertTrue({"processed_at", "model_name", "aspect_version"}.issubset(columns))
            indexes = legacy_db._get_connection().execute(
                "PRAGMA index_list(youtube_comments_scored)"
            ).fetchall()
            self.assertTrue(any(row[2] for row in indexes))
        finally:
            legacy_db.close()


if __name__ == "__main__":
    unittest.main()
