import logging
import tempfile
import unittest
import sqlite3
from pathlib import Path
from unittest.mock import Mock, patch

import pandas as pd

from DataPipeline.SentimentAnalysis import QuotaExceededError, run_queue
from DataPipeline.absa_pipeline import (
    ASPECT_LABELS,
    apply_weights,
    load_data,
    migrate_make_grain,
    rebuild_make_sentiment_tables,
    run_absa_on_comments,
    run_incremental_batches,
    run_phase4_aggregation,
)
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
    def test_incremental_batches_commit_and_skip_persisted_comments(self):
        raw = pd.DataFrame(make_comment_rows("batch_video", "playlist", 3))
        self.db.insert_sentiment_data(raw)

        def unattributed(frame, **kwargs):
            result = frame.copy()
            result["original_text"] = result["text"]
            result["sentiment_make"] = None
            result["make_attribution_source"] = "unknown_make"
            result["make_attribution_version"] = "test"
            result["sentiment_status"] = "unknown_make"
            return result

        with patch("DataPipeline.absa_pipeline.run_phase1_preprocessing", side_effect=unattributed) as preprocess, \
                patch("DataPipeline.absa_pipeline.run_absa_on_comments") as inference:
            run_incremental_batches(str(self.db_path), {"TOYOTA": "TOYOTA"}, batch_size=2)
            self.assertEqual(preprocess.call_count, 2)
            run_incremental_batches(str(self.db_path), {"TOYOTA": "TOYOTA"}, batch_size=2)
            self.assertEqual(preprocess.call_count, 2)
            inference.assert_not_called()
        self.assertEqual(self.db._get_connection().execute("SELECT COUNT(*) FROM youtube_comments_scored").fetchone()[0], 3)

    def setUp(self):
        for handler in list(logging.getLogger().handlers):
            if isinstance(handler, logging.FileHandler) and not Path(handler.baseFilename).parent.exists():
                logging.getLogger().removeHandler(handler)
                handler.close()
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
        scored_df["sentiment_make"] = "TOYOTA"
        scored_df["make_attribution_source"] = "video_title"
        scored_df["make_attribution_version"] = "test"
        scored_df["sentiment_status"] = "scored"
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
        scored_df["sentiment_make"] = "TOYOTA"
        scored_df["make_attribution_source"] = "video_title"
        scored_df["make_attribution_version"] = "test"
        scored_df["sentiment_status"] = "scored"
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
        scored_df["sentiment_make"] = "TOYOTA"
        scored_df["make_attribution_source"] = "video_title"
        scored_df["make_attribution_version"] = "test"
        scored_df["sentiment_status"] = "scored"
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
        self.assertEqual(df_agg.iloc[0]["sentiment_make"], "TOYOTA")
        self.assertEqual(int(df_agg.iloc[0]["sentiment_comment_count"]), 2)

    def test_absa_can_reuse_classifier_across_batches(self):
        classifier = Mock()
        classifier.model.config._commit_hash = 'test-revision'
        labels = [label for pair in ASPECT_LABELS.values() for label in pair.values()]
        classifier.return_value = [{'labels': labels, 'scores': [0.8, 0.2] * 4}]
        for text in ['Reliable vehicle with comfortable seats.', 'Good value and handling.']:
            result = run_absa_on_comments(
                pd.DataFrame({'text': [text]}), model_name='test-model',
                model_revision='test-revision', classifier=classifier,
            )
            self.assertEqual(result.iloc[0]['model_revision'], 'test-revision')
            self.assertEqual(result.iloc[0]['sentiment_status'], 'scored')
            self.assertAlmostEqual(result.iloc[0]['reliability_sentiment'], 0.6)
        self.assertEqual(classifier.call_count, 2)
        self.assertTrue(classifier.call_args.kwargs['multi_label'])

    def test_monthly_video_counts_are_distinct_across_months_and_per_make(self):
        rows = [
            ('a', 'shared', 'TOYOTA', 'scored', '2026-01-01', 0.2, 1.0),
            ('b', 'shared', 'TOYOTA', 'scored', '2026-01-15', 0.6, 1.0),
            ('c', 'shared', 'TOYOTA', 'scored', '2026-02-01', 0.8, 1.0),
            ('d', 'new', 'TOYOTA', 'scored', '02-20-2026', 0.4, 1.0),
            ('e', None, 'TOYOTA', 'scored', '2026-02-21', 0.0, 1.0),
            ('f', 'shared', 'TOYOTA', 'scored', '2026-03-01', 1.0, 1.0),
            ('g', 'shared', 'HONDA', 'scored', '2026-02-01', -0.5, 1.0),
            ('h', 'undated', 'TOYOTA', 'scored', 'unparseable', 0.0, 1.0),
            ('i', 'excluded', 'TOYOTA', 'unknown', '2026-01-01', 0.0, 1.0),
        ]
        conn = self.db._get_connection()
        conn.executemany('''
            INSERT INTO youtube_comments_scored
                (comment_id, video_id, sentiment_make, sentiment_status,
                 published_at, overall_sentiment, comment_weight)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', rows)
        conn.commit()

        for _ in range(2):
            rebuild_make_sentiment_tables(self.db_path)
            monthly = conn.execute('''
                SELECT sentiment_make, sentiment_month, sentiment_video_count,
                       sentiment_comment_count, sentiment_overall_score
                FROM make_sentiment_monthly
                ORDER BY sentiment_make, sentiment_month
            ''').fetchall()
            self.assertEqual([tuple(row[:4]) for row in monthly], [
                ('HONDA', '2026-02-01', 1, 1),
                ('TOYOTA', '2026-01-01', 1, 2),
                ('TOYOTA', '2026-02-01', 2, 5),
                ('TOYOTA', '2026-03-01', 2, 6),
            ])
            for row, expected in zip(monthly, [-0.5, 0.4, 0.4, 0.5]):
                self.assertAlmostEqual(row[4], expected)
            current = conn.execute('''
                SELECT sentiment_video_count, sentiment_comment_count
                FROM make_sentiment_index WHERE sentiment_make = 'TOYOTA'
            ''').fetchone()
            self.assertEqual(tuple(current), (3, 7))

    def test_make_migration_preserves_rows_and_builds_monthly_cutoffs(self):
        raw_df = pd.DataFrame(make_comment_rows("video_migration", "playlist_migration", 2))
        self.db.insert_sentiment_data(raw_df)
        scored_df = raw_df.copy()
        scored_df["Vehicle_Entity"] = "2024 Toyota Camry"
        scored_df["original_text"] = scored_df["text"]
        for aspect in ["reliability", "value", "performance", "comfort"]:
            scored_df[f"{aspect}_sentiment"] = 0.5
            scored_df[f"{aspect}_mentioned"] = 1
            scored_df[f"{aspect}_confidence"] = 0.8
        scored_df = apply_weights(scored_df)
        scored_df["processed_at"] = "2026-07-06T00:00:00+00:00"
        scored_df["model_name"] = "test-model"
        scored_df["aspect_version"] = "legacy-version"
        self.db.upsert_scored_comments(scored_df)
        with self.db._get_connection() as conn:
            before = conn.execute(
                "SELECT COUNT(*) FROM youtube_comments_scored"
            ).fetchone()[0]

        migrated = migrate_make_grain(self.db_path)
        aggregate = rebuild_make_sentiment_tables(self.db_path)
        with self.db._get_connection() as conn:
            after = conn.execute(
                "SELECT COUNT(*) FROM youtube_comments_scored"
            ).fetchone()[0]
            monthly = pd.read_sql_query("SELECT * FROM make_sentiment_monthly", conn)

        self.assertEqual(migrated, 2)
        self.assertEqual(before, after)
        self.assertEqual(aggregate.iloc[0]["sentiment_make"], "TOYOTA")
        self.assertAlmostEqual(float(aggregate.iloc[0]["sentiment_overall_score"]), 0.5)
        self.assertEqual(monthly.iloc[0]["sentiment_month"], "2026-07-01")

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
            scored_df["sentiment_make"] = "TOYOTA"
            scored_df["make_attribution_source"] = "video_title"
            scored_df["make_attribution_version"] = "test"
            scored_df["sentiment_status"] = "scored"
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
            self.assertTrue(
                {
                    "sentiment_make",
                    "make_attribution_source",
                    "make_attribution_version",
                    "overall_sentiment",
                    "overall_confidence",
                    "sentiment_status",
                    "processed_at",
                    "model_name",
                    "model_revision",
                    "aspect_version",
                }.issubset(columns)
            )
            indexes = legacy_db._get_connection().execute(
                "PRAGMA index_list(youtube_comments_scored)"
            ).fetchall()
            self.assertTrue(any(row[2] for row in indexes))
        finally:
            legacy_db.close()


if __name__ == "__main__":
    unittest.main()
