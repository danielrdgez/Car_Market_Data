import logging
import sqlite3
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from DataPipeline.DataCleaning import DataCleaningPipeline
from ML.Price_ML_Models import (
    MAX_PARALLEL_TUNING_ROWS,
    engineer_current_price_features,
    split_train_test,
    stratified_tuning_sample_positions,
    tuning_search_n_jobs,
)
from ML.Time_Series_Price import load_gap_frame
from ML.Time_Series_Price import stratified_depreciation_tuning_sample_positions


def create_raw_fixture_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE listings (
                vin TEXT,
                loaddate DATE,
                date DATE,
                location TEXT,
                locationCode TEXT,
                countryCode TEXT,
                pendingSale BOOLEAN,
                currentBid REAL,
                bids INTEGER,
                distance REAL,
                priceRecentChange BOOLEAN,
                price INTEGER,
                mileage INTEGER,
                year INTEGER,
                title TEXT,
                details TEXT,
                sellerType TEXT,
                vehicleTitle TEXT,
                listingType TEXT,
                vehicleTitleDesc TEXT,
                sourceName TEXT,
                img TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE listing_history (
                id INTEGER PRIMARY KEY,
                vin TEXT,
                history_date DATE,
                mileage REAL,
                price INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE price_history (
                id INTEGER PRIMARY KEY,
                vin TEXT,
                history_date DATE,
                mileage INTEGER,
                price INTEGER,
                trend TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE nhtsa_enrichment (
                vin TEXT PRIMARY KEY,
                nhtsa_Make TEXT,
                nhtsa_Model TEXT,
                nhtsa_ModelYear TEXT,
                nhtsa_BodyClass TEXT,
                nhtsa_DriveType TEXT,
                nhtsa_FuelTypePrimary TEXT,
                nhtsa_EngineHP TEXT,
                nhtsa_EngineCylinders TEXT,
                nhtsa_total_recalls INTEGER,
                nhtsa_total_complaints INTEGER
            )
            """
        )
        conn.execute(
            """
            INSERT INTO listings VALUES (
                'VIN00000000000001', '2026-01-03', '2026-01-02', 'Miami, FL',
                '33186', 'US', 0, NULL, NULL, 12.4, 1, 25000, 30000, 2021,
                '2021 Toyota Camry LE', 'clean sedan', 'Dealer', 'Clean',
                'used', 'Clean title', 'autotempest', 'img'
            )
            """
        )
        conn.execute(
            "INSERT INTO listing_history VALUES (1, 'VIN00000000000001', '2026-01-02', 30000, 25500)"
        )
        conn.execute(
            "INSERT INTO price_history VALUES (1, 'VIN00000000000001', '2026-01-03', 30000, 25000, 'down')"
        )
        conn.execute(
            """
            INSERT INTO nhtsa_enrichment VALUES (
                'VIN00000000000001', 'TOYOTA', 'CAMRY', '2021', 'Sedan/Saloon',
                '4x2', 'Gasoline', '203', '4', 2, 10
            )
            """
        )
        conn.commit()
    finally:
        conn.close()


def create_cleaned_gap_fixture_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute("CREATE TABLE listings (vin TEXT, loaddate DATE, price INTEGER, mileage INTEGER, vehicleTitle TEXT)")
        conn.execute(
            """
            CREATE TABLE price_history (
                vin TEXT,
                history_date DATE,
                mileage INTEGER,
                price INTEGER,
                trend TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE listing_history (
                vin TEXT,
                history_date DATE,
                mileage INTEGER,
                price INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE nhtsa_enrichment (
                vin TEXT PRIMARY KEY,
                nhtsa_Make TEXT,
                nhtsa_Model TEXT,
                nhtsa_ModelYear INTEGER,
                nhtsa_BodyClass TEXT,
                nhtsa_DriveType TEXT,
                nhtsa_FuelTypePrimary TEXT
            )
            """
        )
        conn.execute("INSERT INTO listings VALUES ('VIN1', '2026-01-05', 20000, 40000, 'Clean')")
        conn.execute("INSERT INTO listing_history VALUES ('VIN1', '2026-01-04', 40000, 21000)")
        conn.execute("INSERT INTO listing_history VALUES ('VIN1', '2026-01-05', 40000, 20000)")
        conn.execute("INSERT INTO price_history VALUES ('VIN1', '2026-01-05', 40000, 20000, 'none')")
        conn.execute("INSERT INTO nhtsa_enrichment VALUES ('VIN1', 'TOYOTA', 'CAMRY', 2020, 'Sedan', '4x2', 'Gasoline')")
        conn.commit()
    finally:
        conn.close()


class DataCleaningUpgradeTests(unittest.TestCase):
    def test_cleaning_preserves_predictive_listing_fields_and_indexes(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_db = Path(tmp) / "raw.db"
            cleaned_db = Path(tmp) / "cleaned.db"
            create_raw_fixture_db(raw_db)

            DataCleaningPipeline(raw_db, cleaned_db).run()
            logging.shutdown()

            conn = sqlite3.connect(cleaned_db)
            try:
                listing_cols = [row[1] for row in conn.execute("PRAGMA table_info('listings')")]
                self.assertIn("title", listing_cols)
                self.assertIn("location", listing_cols)
                self.assertIn("priceRecentChange", listing_cols)
                self.assertIn("listingType", listing_cols)

                indexes = {row[1] for row in conn.execute("PRAGMA index_list('price_history')")}
                self.assertIn("idx_price_history_vin_date", indexes)
                self.assertIn("idx_price_history_date", indexes)

                row = conn.execute("SELECT title, distance, priceRecentChange FROM listings").fetchone()
                self.assertEqual(row[0], "2021 Toyota Camry LE")
                self.assertAlmostEqual(row[1], 12.4)
                self.assertIn(row[2], (1, "1", True))
            finally:
                conn.close()


class ModelingUpgradeTests(unittest.TestCase):
    def test_current_price_split_has_no_vin_overlap(self):
        start = date(2026, 1, 1)
        rows = []
        for idx in range(40):
            rows.append(
                {
                    "vin": f"VIN{idx:03d}",
                    "loaddate": start + timedelta(days=idx),
                    "date": start + timedelta(days=idx),
                    "price": 20_000 + idx * 250,
                    "mileage": 10_000 + idx * 500,
                    "nhtsa_Make": "TOYOTA",
                    "nhtsa_Model": "CAMRY",
                    "nhtsa_ModelYear": 2020 + (idx % 3),
                    "nhtsa_BodyClass": "Sedan",
                    "nhtsa_FuelTypePrimary": "Gasoline",
                }
            )
        df = engineer_current_price_features(pd.DataFrame(rows))
        train_df, test_df, metadata = split_train_test(df)
        self.assertGreater(len(train_df), 0)
        self.assertGreater(len(test_df), 0)
        self.assertEqual(metadata["vin_overlap"], 0)
        self.assertFalse(set(train_df["vin"]).intersection(set(test_df["vin"])))

    def test_large_cv_search_uses_single_process_to_avoid_pickling_pressure(self):
        self.assertEqual(tuning_search_n_jobs(MAX_PARALLEL_TUNING_ROWS), -1)
        self.assertEqual(tuning_search_n_jobs(MAX_PARALLEL_TUNING_ROWS + 1), 1)

    def test_tuning_sample_is_bounded_and_stratified(self):
        segments = pd.DataFrame(
            {
                "price_band": ["under_25k"] * 500 + ["25k_50k"] * 300 + ["150k_plus"] * 200,
                "nhtsa_Make": ["TOYOTA"] * 500 + ["FORD"] * 300 + ["PORSCHE"] * 200,
                "nhtsa_ModelYear": [2020] * 500 + [2022] * 300 + [2024] * 200,
            }
        )

        positions = stratified_tuning_sample_positions(segments, max_rows=100)
        sampled = segments.iloc[positions]

        self.assertEqual(len(positions), 100)
        self.assertEqual(set(sampled["price_band"]), {"under_25k", "25k_50k", "150k_plus"})
        self.assertEqual(set(sampled["nhtsa_Make"]), {"TOYOTA", "FORD", "PORSCHE"})

    def test_gap_loader_labels_duplicate_like_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "cleaned.db"
            create_cleaned_gap_fixture_db(db_path)

            gap_df, validation = load_gap_frame(db_path, sample_size=10)

            self.assertEqual(validation["gap_label"], "duplicate_like_price_trajectory")
            self.assertEqual(validation["same_day_same_price_rate"], 1.0)
            self.assertEqual(len(gap_df), 1)
            self.assertEqual(gap_df.iloc[0]["absolute_gap"], 0)
            self.assertIn("listing_date", gap_df.columns)

    def test_depreciation_tuning_sample_is_bounded_and_stratified(self):
        frame = pd.DataFrame(
            {
                "make": ["TOYOTA"] * 500 + ["FORD"] * 300 + ["PORSCHE"] * 200,
                "model": ["CAMRY"] * 500 + ["F-150"] * 300 + ["911"] * 200,
                "model_year": [2020] * 500 + [2022] * 300 + [2024] * 200,
                "trim_proxy": ["LE"] * 500 + ["XLT"] * 300 + ["BASE"] * 200,
                "quarter": [1] * 500 + [2] * 300 + [3] * 200,
            }
        )

        positions = stratified_depreciation_tuning_sample_positions(frame, max_rows=100)
        sampled = frame.iloc[positions]

        self.assertEqual(len(positions), 100)
        self.assertEqual(set(sampled["make"]), {"TOYOTA", "FORD", "PORSCHE"})
        self.assertEqual(set(sampled["trim_proxy"]), {"LE", "XLT", "BASE"})


if __name__ == "__main__":
    unittest.main()
