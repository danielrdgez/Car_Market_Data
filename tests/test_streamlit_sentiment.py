import sqlite3
import tempfile
import unittest
from pathlib import Path

import streamlit_app as app


class StreamlitSentimentTests(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.original_path = app.SENTIMENT_DB_PATH
        app.SENTIMENT_DB_PATH = Path(self.tempdir.name) / "sentiment.db"
        with sqlite3.connect(app.SENTIMENT_DB_PATH) as conn:
            conn.execute("""CREATE TABLE make_sentiment_index (
                sentiment_make TEXT, sentiment_overall_score REAL, sentiment_reliability_score REAL,
                sentiment_value_score REAL, sentiment_performance_score REAL, sentiment_comfort_score REAL,
                sentiment_comment_count INTEGER, sentiment_video_count INTEGER, sentiment_aspect_coverage REAL
            )""")
            conn.execute("""CREATE TABLE youtube_comments_scored (
                sentiment_make TEXT, sentiment_status TEXT, text TEXT, video_title TEXT, published_at TEXT, like_count INTEGER, comment_weight REAL,
                reliability_sentiment REAL, value_sentiment REAL, performance_sentiment REAL, comfort_sentiment REAL
            )""")
            conn.execute("INSERT INTO make_sentiment_index VALUES ('HONDA', .60, .50, .40, .70, .30, 20, 2, .75)")
            conn.executemany(
                "INSERT INTO youtube_comments_scored VALUES (?, 'scored', ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    ('HONDA', 'newer, lighter comment', 'Video', '2026-02-02', 2, 3.0, 1, 1, 1, 1),
                    ('HONDA', 'top weighted comment', 'Video', '2026-02-01', 4, 5.0, 1, 1, 1, 1),
                    ('TOYOTA', 'other make comment', 'Video', '2026-02-03', 1, 9.0, 1, 1, 1, 1),
                ],
            )
            conn.commit()
        app.load_cohort_sentiment.clear()

    def tearDown(self):
        app.load_cohort_sentiment.clear()
        app.SENTIMENT_DB_PATH = self.original_path
        self.tempdir.cleanup()

    def test_make_match_and_weighted_comment_order(self):
        index, comments, label = app.load_cohort_sentiment("HONDA", "CIVIC", 2022, "SI")
        self.assertEqual(label, "make-wide")
        self.assertEqual(index.iloc[0]["sentiment_comment_count"], 20)
        self.assertEqual(comments.iloc[0]["text"], "top weighted comment")

    def test_missing_make_is_labeled(self):
        _, comments, label = app.load_cohort_sentiment("FORD", "MUSTANG", 2022, "GT")
        self.assertEqual(label, "no matching sentiment")
        self.assertTrue(comments.empty)


if __name__ == "__main__":
    unittest.main()
