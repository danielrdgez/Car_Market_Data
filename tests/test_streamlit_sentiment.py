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
            conn.execute("CREATE TABLE vehicle_sentiment_index (Vehicle_Entity TEXT, Sample_Size INTEGER, Reliability_Index REAL, General_Enthusiast_Score REAL, Confidence_Level TEXT)")
            conn.execute("""CREATE TABLE youtube_comments_scored (
                Vehicle_Entity TEXT, text TEXT, video_title TEXT, published_at TEXT, like_count INTEGER, comment_weight REAL,
                reliability_sentiment REAL, value_sentiment REAL, performance_sentiment REAL, comfort_sentiment REAL
            )""")
            conn.execute("INSERT INTO vehicle_sentiment_index VALUES ('2022 Honda Civic Si', 20, 75, 80, 'Low Confidence')")
            conn.executemany(
                "INSERT INTO youtube_comments_scored VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [
                    ('2022 Honda Civic Si', 'newer, lighter comment', 'Video', '2026-02-02', 2, 3.0, 1, 1, 1, 1),
                    ('2022 Honda Civic Si', 'top weighted comment', 'Video', '2026-02-01', 4, 5.0, 1, 1, 1, 1),
                    ('2022 Honda Civic Type R', 'fallback comment', 'Video', '2026-02-03', 1, 9.0, 1, 1, 1, 1),
                ],
            )
            conn.commit()
        app.load_cohort_sentiment.clear()

    def tearDown(self):
        app.load_cohort_sentiment.clear()
        app.SENTIMENT_DB_PATH = self.original_path
        self.tempdir.cleanup()

    def test_exact_trim_match_and_weighted_comment_order(self):
        index, comments, label = app.load_cohort_sentiment("HONDA", "CIVIC", 2022, "SI")
        self.assertEqual(label, "exact canonical cohort")
        self.assertEqual(index.iloc[0]["Sample_Size"], 20)
        self.assertEqual(comments.iloc[0]["text"], "top weighted comment")

    def test_model_fallback_is_labeled(self):
        _, comments, label = app.load_cohort_sentiment("HONDA", "CIVIC", 2022, "SPORT")
        self.assertEqual(label, "make/model/year fallback")
        self.assertGreaterEqual(len(comments), 1)


if __name__ == "__main__":
    unittest.main()
