import sqlite3
import sys
from contextlib import closing
import tempfile
import unittest
from unittest.mock import patch
from pathlib import Path

import numpy as np
import pandas as pd

from DataPipeline.NHTSA_text_features import (
    FEATURE_COLUMNS, LABELS, attach_features, build_query_features, documents,
    initialize, score_text, text_hash,
    main as nhtsa_text_main,
)
from ML.Price_ML_Models import engineer_current_price_features, split_train_test, train_current_price_models
from sklearn.dummy import DummyRegressor
from sklearn.pipeline import Pipeline
from ML.Time_Series_Price import (_cohort_price_series, forecast_latest_cohorts_recursive,
    clean_history_frame, build_cohort_monthly_frame, rolling_recursive_backtest, train_cohort_models)


def synthetic_history():
    rows = []
    for index, month in enumerate(pd.date_range("2024-01-01", periods=10, freq="MS")):
        for vehicle in range(2):
            row = dict(vin=f"VIN{vehicle}", history_date=month, price=30000 - index * 200,
                       mileage=10000 + index * 500, canonical_make="FORD", canonical_model="FOCUS",
                       canonical_year=2020, canonical_trim="SE", sellerType="Dealer", sourceName="Test")
            for column in ["nhtsa_BodyClass", "nhtsa_DriveType", "nhtsa_FuelTypePrimary",
                           "nhtsa_ElectrificationLevel", "nhtsa_EngineHP", "nhtsa_EngineCylinders"]:
                row[column] = None
            for label in ["overall_score", "reliability_score", "value_score", "performance_score",
                          "comfort_score", "aspect_coverage", "comment_count", "video_count"]:
                row["sentiment_" + label] = None
            rows.append(row)
    return pd.DataFrame(rows)


class NHTSATextTests(unittest.TestCase):
    def test_cli_pilot_cached_scoring_and_feature_build_on_fixture(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            source_path, output_path, pilot_path = root / "source.db", root / "features.db", root / "pilot.csv"
            with closing(sqlite3.connect(source_path)) as source:
                source.execute("CREATE TABLE nhtsa_vehicle_queries(query_id,query_type,make,model,model_year,response_status,fetched_at)")
                source.executemany("INSERT INTO nhtsa_vehicle_queries VALUES(?,?,?,?,?,?,?)", [
                    (1, "complaints", "FORD", "FOCUS", 2020, "success", "2026-08-01"),
                    (2, "recalls", "FORD", "FOCUS", 2020, "success", "2026-08-02"),
                ])
                source.execute("CREATE TABLE nhtsa_complaints(query_id,record_key,odi_number,summary)")
                source.execute("INSERT INTO nhtsa_complaints VALUES(1,'a','100','The engine stalled.')")
                source.execute("CREATE TABLE nhtsa_recalls(query_id,record_key,nhtsa_campaign_number,summary,consequence,remedy)")
                source.execute("INSERT INTO nhtsa_recalls VALUES(2,'b','campaign','Defect','Possible fire','Replace part')")
                source.commit()
            common = ["--source-db", str(source_path), "--output-db", str(output_path), "--pilot-csv", str(pilot_path)]
            with patch.object(sys, "argv", ["nhtsa", "pilot", "--limit", "3"] + common):
                nhtsa_text_main()
            self.assertEqual(len(pd.read_csv(pilot_path)), 3)
            with patch("DataPipeline.NHTSA_text_features.create_classifier", return_value=object()) as factory, patch("DataPipeline.NHTSA_text_features.score_text",
                    side_effect=lambda classifier, text, role: dict.fromkeys(LABELS[role], 0.7)) as scorer:
                for _ in range(2):
                    with patch.object(sys, "argv", ["nhtsa", "score", "--pilot-only"] + common):
                        nhtsa_text_main()
                self.assertEqual(scorer.call_count, 3)
                self.assertEqual(factory.call_count, 1)
            review_path = root / "review.csv"
            with patch.object(sys, "argv", ["nhtsa", "review", "--review-csv", str(review_path)] + common):
                nhtsa_text_main()
            self.assertIn("score_loss_of_propulsion", pd.read_csv(review_path).columns)
            with patch.object(sys, "argv", ["nhtsa", "build"] + common):
                nhtsa_text_main()
            with closing(sqlite3.connect(output_path)) as output:
                count, coverage = output.execute("SELECT nhtsa_complaints_report_count,nhtsa_complaints_text_coverage FROM query_features WHERE query_id=1").fetchone()
                self.assertEqual((count, coverage), (1, 1))

    def test_deduplication_preserves_event_identity_and_component_text(self):
        with closing(sqlite3.connect(":memory:")) as source:
            source.row_factory = sqlite3.Row
            source.execute("CREATE TABLE nhtsa_complaints(query_id,record_key,odi_number,summary)")
            source.executemany("INSERT INTO nhtsa_complaints VALUES(?,?,?,?)", [
                (1, "a", "100", "Engine stalled"), (1, "b", "100", "Repair delayed"),
                (1, "c", "101", "Engine stalled"), (2, "d", "100", "Future revision"),
            ])
            result = documents(source, {"query_id": 1, "query_type": "complaints"})
            self.assertEqual(len(result), 2)
            self.assertIn("Repair delayed", result[0]["texts"]["complaint"])
            self.assertNotIn("Future revision", str(result))

    def test_missing_empty_failed_and_unscored_are_distinct(self):
        with closing(sqlite3.connect(":memory:")) as output:
            initialize(output, "model", "revision")
            empty = build_query_features({"query_type": "complaints", "response_status": "empty"}, [], output)
            failed = build_query_features({"query_type": "complaints", "response_status": "request_failed"}, [], output)
            unscored = build_query_features({"query_type": "complaints", "response_status": "success"},
                                           [{"texts": {"complaint": "stalled"}}], output)
            self.assertEqual(empty["nhtsa_complaints_report_count"], 0)
            self.assertIsNone(failed["nhtsa_complaints_report_count"])
            self.assertEqual(failed["nhtsa_complaints_known"], 0)
            self.assertEqual(unscored["nhtsa_complaints_text_coverage"], 0)
            self.assertIsNone(unscored["nhtsa_complaint_loss_of_propulsion_score"])
            with self.assertRaises(ValueError):
                initialize(output, "different-model", "revision")

    def test_collected_before_month_join_does_not_multiply_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "features.db"
            with sqlite3.connect(path) as conn:
                initialize(conn, "model", "revision")
                conn.executemany("""INSERT INTO query_features
                    (query_id,source,make,model,model_year,available_at,response_status,
                     nhtsa_complaints_report_count,nhtsa_complaints_known)
                    VALUES(?,?,?,?,?,?,?,?,?)""", [
                        (1, "complaints", "FORD", "F 150", 2020, "2026-08-15T00:00:00+00:00", "success", 4, 1),
                        (2, "complaints", "FORD", "F 150", 2020, "2026-09-01T00:00:00+00:00", "success", 9, 1),
                        (3, "complaints", "FORD", "F 150", 2020, "2026-09-15T00:00:00+00:00", "request_failed", None, 0),
                    ])
            conn.close()
            frame = pd.DataFrame({"canonical_make": ["Ford"] * 4, "canonical_model": ["F-150"] * 4,
                                  "canonical_year": [2020] * 4,
                                  "date": ["2026-08-31", "2026-09-20", "2026-10-20", None]})
            result = attach_features(frame, "date", path)
            self.assertEqual(len(result), len(frame))
            counts = result["nhtsa_complaints_report_count"]
            self.assertTrue(pd.isna(counts.iloc[0]))
            self.assertEqual(counts.iloc[1], 4)
            self.assertTrue(pd.isna(counts.iloc[2]))
            self.assertEqual(result["nhtsa_complaints_known"].iloc[2], 0)
            self.assertTrue(pd.isna(counts.iloc[3]))

    def test_topic_scoring_visits_late_chunks(self):
        class Tokenizer:
            def encode(self, text, **kwargs):
                return list(range(600))
            def decode(self, ids, **kwargs):
                return str(max(ids))
        class Classifier:
            tokenizer = Tokenizer()
            def __call__(self, text, candidate_labels, **kwargs):
                return {"labels": candidate_labels, "scores": [float(int(text) > 500)] * len(candidate_labels)}
        scores = score_text(Classifier(), "narrative", "recall_remedy")
        self.assertEqual(set(scores), set(LABELS["recall_remedy"]))
        self.assertTrue(all(value == 1 for value in scores.values()))
        self.assertEqual(text_hash("same   text"), text_hash("same text"))


class TemporalSafeguardTests(unittest.TestCase):
    def test_current_price_partition_and_interval_report_on_synthetic_rows(self):
        rows = pd.DataFrame({"vin": [f"VIN{i}" for i in range(400)],
                             "price": np.arange(400) * 10 + 20000, "mileage": 10000,
                             "canonical_make": "FORD", "canonical_model": "FOCUS",
                             "canonical_year": 2020, "canonical_trim": "SE",
                             "loaddate": pd.date_range("2022-01-01", periods=400)})
        def candidates(tree, linear):
            return {"MedianBaseline": (Pipeline([("preprocessor", tree), ("model", DummyRegressor(strategy="median"))]), {})}
        with tempfile.TemporaryDirectory() as tmp, \
                patch("ML.Price_ML_Models.load_modeling_frame", return_value=(rows, {})), \
                patch("ML.Price_ML_Models.model_candidates", side_effect=candidates):
            report = train_current_price_models(output_dir=tmp, sample_size=400,
                                                nhtsa_feature_db=Path(tmp) / "absent.db")
            counts = report["row_counts"]
            self.assertEqual(sum(counts[c] for c in ["train_rows", "validation_rows", "calibration_rows", "test_rows"]), 400)
            self.assertEqual(report["split"]["selection_metric"], "validation_mae")
            self.assertIn("prediction_interval", report["models"]["MedianBaseline"])

    def test_calendar_targets_and_single_vin_month_weight(self):
        raw = synthetic_history()
        raw = raw[~raw.history_date.eq(pd.Timestamp("2024-02-01"))]
        duplicate = raw.iloc[[0]].copy()
        duplicate["price"] = 90000
        duplicate["history_date"] += pd.Timedelta(days=1)
        monthly = build_cohort_monthly_frame(clean_history_frame(pd.concat([raw, duplicate]), None), [1, 2], 1, 1)
        january = monthly.iloc[0]
        self.assertEqual(january.volume, 2)
        self.assertEqual(january.median_price, 60000)
        self.assertTrue(pd.isna(january.target_median_price_1m))
        self.assertEqual(january.target_median_price_2m, 29600)
        self.assertTrue(pd.isna(monthly.iloc[1].lag_median_price_1))

    def test_recursive_backtest_uses_observed_targets_and_calendar_origins(self):
        monthly = build_cohort_monthly_frame(clean_history_frame(synthetic_history(), None), [1], 1, 1)
        calls = []
        class FixedModel:
            def fit(self, X, y):
                calls.append(len(X))
                return self
            def predict(self, X):
                return np.full(len(X), -0.01)
        with patch("ML.Time_Series_Price.build_regression_pipeline", return_value=(FixedModel(), "fixed")):
            result = rolling_recursive_backtest(monthly, ["median_price"], 6, 6)
        self.assertTrue(calls)
        self.assertTrue(result.forecast_date.gt(result.origin_month_start).all())
        self.assertTrue(result.forecast_month.max() > 1)
        self.assertEqual(set(result.actual_median_price) - set(monthly.median_price), set())

    def test_synthetic_baseline_run_preserves_forecast_output_contracts(self):
        with tempfile.TemporaryDirectory() as tmp, patch("ML.Time_Series_Price.load_history_frame", return_value=synthetic_history()):
            report = train_cohort_models(Path("unused"), None, Path(tmp), 100, [1], 12, None, 1, 1,
                                         None, [], 0, "unused", nhtsa_feature_db=Path(tmp) / "missing.db")
            self.assertGreater(report["backtest_rows"], 0)
            forecasts = pd.read_csv(Path(tmp) / "cohort_future_forecasts.csv")
            self.assertEqual(set(forecasts.model_family), {"naive", "drift"})
            self.assertEqual(forecasts.forecast_month.max(), 12)
            self.assertTrue((Path(tmp) / "cohort_backtesting_matched_kpis.csv").exists())

    def test_time_split_works_with_fully_populated_dates(self):
        frame = pd.DataFrame({"vin": [str(i) for i in range(1000)], "price": 20000,
                              "loaddate": pd.date_range("2020-01-01", periods=1000)})
        train, test, info = split_train_test(frame)
        self.assertEqual(info["split_strategy"], "time_cutoff_plus_vin_exclusion")
        self.assertLess(train.loaddate.max(), test.loaddate.min())
        with self.assertRaises(ValueError):
            split_train_test(frame, "2030-01-01")

    def test_age_and_calendar_reference_are_batch_invariant(self):
        frame = pd.DataFrame({"vin": ["a", "b"], "price": [20000, 30000], "mileage": [10000, 20000],
                              "canonical_make": ["FORD"] * 2, "canonical_model": ["F150"] * 2,
                              "canonical_year": [2020] * 2, "canonical_trim": ["XLT"] * 2,
                              "loaddate": ["2022-01-01", "2026-01-01"]})
        together = engineer_current_price_features(frame)
        alone = engineer_current_price_features(frame.iloc[:1])
        self.assertEqual(together.vehicle_age.iloc[0], 2)
        self.assertEqual(together.listing_recency_days.iloc[0], alone.listing_recency_days.iloc[0])

    def test_gap_remains_missing_and_multi_month_recursion_is_rejected(self):
        series = _cohort_price_series(pd.DataFrame({"month_start": pd.to_datetime(["2020-01-01", "2020-03-01"]),
                                                   "median_price": [10000, 9000]}))
        self.assertEqual(len(series), 3)
        self.assertTrue(np.isnan(series.iloc[1]))
        with self.assertRaises(ValueError):
            forecast_latest_cohorts_recursive(pd.DataFrame(), None, [], 3, 60)


if __name__ == "__main__":
    unittest.main()
