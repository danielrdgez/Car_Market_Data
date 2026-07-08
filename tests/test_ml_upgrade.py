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
    PRICE_LEAKAGE_FEATURE_COLUMNS as CURRENT_PRICE_LEAKAGE_FEATURE_COLUMNS,
    build_preprocessors,
    engineer_current_price_features,
    get_preprocessor_feature_names,
    make_feature_matrix,
    split_train_test,
    stratified_tuning_sample_positions,
    tuning_search_n_jobs,
)
from ML.Time_Series_Price import CATEGORICAL_FEATURES as DEPRECIATION_CATEGORICAL_FEATURES
from ML.Time_Series_Price import NUMERIC_FEATURES as DEPRECIATION_NUMERIC_FEATURES
from ML.Time_Series_Price import PRICE_LEAKAGE_FEATURE_COLUMNS as DEPRECIATION_PRICE_LEAKAGE_FEATURE_COLUMNS
from ML.Time_Series_Price import build_cohort_monthly_frame
from ML.Time_Series_Price import clean_history_frame
from ML.Time_Series_Price import load_gap_frame
from ML.Time_Series_Price import clean_history_frame
from ML.Time_Series_Price import load_history_frame
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


def create_outlier_trim_fixture_db(path: Path) -> None:
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
                distance REAL,
                priceRecentChange BOOLEAN,
                price INTEGER,
                mileage INTEGER,
                title TEXT,
                sellerType TEXT,
                vehicleTitle TEXT,
                listingType TEXT,
                vehicleTitleDesc TEXT,
                sourceName TEXT
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
                nhtsa_FuelTypePrimary TEXT,
                nhtsa_Trim TEXT,
                nhtsa_Trim2 TEXT
            )
            """
        )

        listing_rows = []
        nhtsa_rows = []
        price_rows = []
        listing_history_rows = []
        for idx, price in enumerate([32000, 33000, 34000, 35000, 36000, 37000, 38000, 39000]):
            vin = f"VINTRIMNORMAL{idx:02d}"
            listing_rows.append(
                (
                    vin,
                    "2026-01-05",
                    "2026-01-04",
                    "Miami, FL",
                    "33186",
                    "US",
                    0,
                    12.4,
                    0,
                    price,
                    30000 + idx * 500,
                    "2020 Ford Mustang GT",
                    "Dealer",
                    "Clean",
                    "used",
                    "Clean title",
                    "autotempest",
                )
            )
            nhtsa_rows.append((vin, "FORD", "MUSTANG", "2020", "Coupe", "Gasoline", "GT", None))
            history_price = 444444 if idx == 0 else price
            price_rows.append((idx + 1, vin, "2026-01-05", 30000 + idx * 500, history_price, "none"))
            listing_history_rows.append((idx + 1, vin, "2026-01-04", 30000 + idx * 500, history_price))

        listing_rows.extend(
            [
                (
                    "VINREPEAT4444444",
                    "2026-01-05",
                    "2026-01-04",
                    "Miami, FL",
                    "33186",
                    "US",
                    0,
                    12.4,
                    0,
                    444444,
                    31000,
                    "2020 Ford Mustang GT",
                    "Dealer",
                    "Clean",
                    "used",
                    "Clean title",
                    "autotempest",
                ),
                (
                    "VINREPEAT1111111",
                    "2026-01-05",
                    "2026-01-04",
                    "Miami, FL",
                    "33186",
                    "US",
                    0,
                    12.4,
                    0,
                    1111,
                    31000,
                    "2020 Ford Mustang GT",
                    "Dealer",
                    "Clean",
                    "used",
                    "Clean title",
                    "autotempest",
                ),
                (
                    "VINTITLEGT350001",
                    "2026-01-05",
                    "2026-01-04",
                    "Miami, FL",
                    "33186",
                    "US",
                    0,
                    12.4,
                    0,
                    65000,
                    12000,
                    "2020 Ford Mustang GT350 Shelby",
                    "Dealer",
                    "Clean",
                    "used",
                    "Clean title",
                    "autotempest",
                ),
                (
                    "VINLISTINGBASE001",
                    "2026-01-05",
                    "2026-01-04",
                    "Miami, FL",
                    "33186",
                    "US",
                    0,
                    12.4,
                    0,
                    42000,
                    18000,
                    "2020 Ford Mustang GT",
                    "Dealer",
                    "Clean",
                    "used",
                    "Clean title",
                    "autotempest",
                ),
            ]
        )
        nhtsa_rows.extend(
            [
                ("VINREPEAT4444444", "FORD", "MUSTANG", "2020", "Coupe", "Gasoline", "GT", None),
                ("VINREPEAT1111111", "FORD", "MUSTANG", "2020", "Coupe", "Gasoline", "GT", None),
                ("VINTITLEGT350001", "FORD", "MUSTANG", "2020", "Coupe", "Gasoline", None, None),
                ("VINLISTINGBASE001", "FORD", "MUSTANG", "2020", "Coupe", "Gasoline", "GT", None),
            ]
        )
        price_rows.extend(
            [
                (20, "VINREPEAT4444444", "2026-01-05", 31000, 444444, "none"),
                (21, "VINREPEAT1111111", "2026-01-05", 31000, 1111, "none"),
                (22, "VINTITLEGT350001", "2026-01-05", 12000, 65000, "none"),
            ]
        )
        listing_history_rows.extend(
            [
                (20, "VINREPEAT4444444", "2026-01-04", 31000, 444444),
                (21, "VINREPEAT1111111", "2026-01-04", 31000, 1111),
                (22, "VINTITLEGT350001", "2026-01-04", 12000, 65000),
                (23, "VINLISTINGBASE001", "2026-01-02", 18000, 43000),
            ]
        )

        conn.executemany(
            """
            INSERT INTO listings VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            listing_rows,
        )
        conn.executemany("INSERT INTO nhtsa_enrichment VALUES (?, ?, ?, ?, ?, ?, ?, ?)", nhtsa_rows)
        conn.executemany("INSERT INTO price_history VALUES (?, ?, ?, ?, ?, ?)", price_rows)
        conn.executemany("INSERT INTO listing_history VALUES (?, ?, ?, ?, ?)", listing_history_rows)
        conn.commit()
    finally:
        conn.close()


def create_cleaned_gap_fixture_db(path: Path) -> None:
    conn = sqlite3.connect(path)
    try:
        conn.execute(
            """
            CREATE TABLE listings (
                vin TEXT,
                loaddate DATE,
                date DATE,
                price INTEGER,
                mileage INTEGER,
                vehicleTitle TEXT,
                title TEXT,
                vehicleTitleDesc TEXT,
                title_trim TEXT,
                trim_combined TEXT,
                trim_source TEXT
            )
            """
        )
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
                nhtsa_Trim TEXT,
                nhtsa_Trim2 TEXT,
                nhtsa_trim_combined TEXT,
                nhtsa_BodyClass TEXT,
                nhtsa_DriveType TEXT,
                nhtsa_FuelTypePrimary TEXT
            )
            """
        )
        conn.execute(
            """
            INSERT INTO listings VALUES (
                'VIN1', '2026-01-05', '2026-01-05', 20000, 40000, 'Clean',
                '2020 Toyota Camry LE', 'Clean sedan', 'LE', 'LE', 'title'
            )
            """
        )
        conn.execute("INSERT INTO listing_history VALUES ('VIN1', '2026-01-04', 40000, 21000)")
        conn.execute("INSERT INTO listing_history VALUES ('VIN1', '2026-01-05', 40000, 20000)")
        conn.execute("INSERT INTO price_history VALUES ('VIN1', '2026-01-05', 40000, 20000, 'none')")
        conn.execute(
            """
            INSERT INTO nhtsa_enrichment VALUES (
                'VIN1', 'TOYOTA', 'CAMRY', 2020, NULL, NULL, NULL, 'Sedan', '4x2', 'Gasoline'
            )
            """
        )
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

    def test_cleaning_drops_contextual_repeated_digit_price_outliers(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_db = Path(tmp) / "raw.db"
            cleaned_db = Path(tmp) / "cleaned.db"
            create_outlier_trim_fixture_db(raw_db)

            DataCleaningPipeline(raw_db, cleaned_db).run()
            logging.shutdown()

            conn = sqlite3.connect(cleaned_db)
            try:
                listing_vins = {
                    row[0] for row in conn.execute("SELECT vin FROM listings").fetchall()
                }
                self.assertNotIn("VINREPEAT4444444", listing_vins)
                self.assertNotIn("VINREPEAT1111111", listing_vins)
                self.assertIn("VINTRIMNORMAL00", listing_vins)

                history_prices = {
                    row[0] for row in conn.execute("SELECT price FROM price_history").fetchall()
                }
                self.assertNotIn(444444, history_prices)
            finally:
                conn.close()

    def test_cleaning_adds_title_trim_and_fills_missing_nhtsa_trim(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_db = Path(tmp) / "raw.db"
            cleaned_db = Path(tmp) / "cleaned.db"
            create_outlier_trim_fixture_db(raw_db)

            DataCleaningPipeline(raw_db, cleaned_db).run()
            logging.shutdown()

            conn = sqlite3.connect(cleaned_db)
            try:
                listing_row = conn.execute(
                    """
                    SELECT title_trim, trim_combined, trim_source
                    FROM listings
                    WHERE vin = 'VINTITLEGT350001'
                    """
                ).fetchone()
                self.assertEqual(listing_row, ("GT350", "GT350", "title"))

                nhtsa_row = conn.execute(
                    """
                    SELECT nhtsa_Trim, nhtsa_Trim_source, nhtsa_trim_combined
                    FROM nhtsa_enrichment
                    WHERE vin = 'VINTITLEGT350001'
                    """
                ).fetchone()
                self.assertEqual(nhtsa_row, ("GT350", "title", "GT350"))
            finally:
                conn.close()

    def test_cleaning_fills_missing_nhtsa_base_price_from_earliest_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_db = Path(tmp) / "raw.db"
            cleaned_db = Path(tmp) / "cleaned.db"
            create_outlier_trim_fixture_db(raw_db)

            DataCleaningPipeline(raw_db, cleaned_db).run()
            logging.shutdown()

            conn = sqlite3.connect(cleaned_db)
            try:
                price_history_row = conn.execute(
                    """
                    SELECT nhtsa_BasePrice, nhtsa_BasePrice_source
                    FROM nhtsa_enrichment
                    WHERE vin = 'VINTITLEGT350001'
                    """
                ).fetchone()
                self.assertEqual(price_history_row, (65000, "price_history"))

                listing_history_row = conn.execute(
                    """
                    SELECT nhtsa_BasePrice, nhtsa_BasePrice_source
                    FROM nhtsa_enrichment
                    WHERE vin = 'VINLISTINGBASE001'
                    """
                ).fetchone()
                self.assertEqual(listing_history_row, (43000, "listing_history"))
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

    def test_current_price_features_use_cleaned_trim_columns(self):
        df = engineer_current_price_features(
            pd.DataFrame(
                [
                    {
                        "vin": "VINTRIMPOINT0001",
                        "loaddate": date(2026, 1, 5),
                        "date": date(2026, 1, 4),
                        "price": 65000,
                        "mileage": 12000,
                        "title": "2020 Ford Mustang GT350 Shelby",
                        "vehicleTitle": "Clean",
                        "vehicleTitleDesc": "Clean title",
                        "nhtsa_Make": "FORD",
                        "nhtsa_Model": "MUSTANG",
                        "nhtsa_ModelYear": 2020,
                        "nhtsa_BodyClass": "Coupe",
                        "nhtsa_FuelTypePrimary": "Gasoline",
                        "title_trim": "GT350",
                        "trim_combined": "GT350",
                        "trim_source": "title",
                        "nhtsa_Trim": None,
                        "nhtsa_trim_combined": "GT350",
                    }
                ]
            )
        )

        self.assertEqual(df.loc[0, "trim_proxy"], "GT350")
        self.assertEqual(df.loc[0, "make_model_year_trim"], "FORD_MUSTANG_2020_GT350")
        self.assertEqual(df.loc[0, "title_mentions_luxury_trim"], 1)

    def test_current_price_feature_matrix_excludes_nhtsa_base_price(self):
        df = engineer_current_price_features(
            pd.DataFrame(
                [
                    {
                        "vin": "VINBASEPRICE0001",
                        "loaddate": date(2026, 1, 5),
                        "date": date(2026, 1, 4),
                        "price": 42000,
                        "mileage": 18000,
                        "nhtsa_Make": "TOYOTA",
                        "nhtsa_Model": "CAMRY",
                        "nhtsa_ModelYear": 2022,
                        "nhtsa_BodyClass": "Sedan",
                        "nhtsa_FuelTypePrimary": "Gasoline",
                        "nhtsa_BasePrice": 41000,
                        "nhtsa_BasePrice_source": "price_history",
                    }
                ]
            )
        )

        X, _, _ = make_feature_matrix(df)

        self.assertTrue(CURRENT_PRICE_LEAKAGE_FEATURE_COLUMNS.isdisjoint(X.columns))

    def test_depreciation_feature_allowlists_exclude_nhtsa_base_price(self):
        depreciation_features = set(DEPRECIATION_CATEGORICAL_FEATURES + DEPRECIATION_NUMERIC_FEATURES)

        self.assertTrue(DEPRECIATION_PRICE_LEAKAGE_FEATURE_COLUMNS.isdisjoint(depreciation_features))

    def test_target_encoded_feature_names_map_to_source_columns(self):
        X = pd.DataFrame(
            {
                "mileage": [10_000, 20_000, 30_000, 40_000],
                "nhtsa_Make": ["TOYOTA", "FORD", "HONDA", "BMW"],
                "nhtsa_Model": ["CAMRY", "F-150", "ACCORD", "X5"],
                "trim_proxy": ["LE", "XLT", "EX", "M SPORT"],
                "body_class": ["Sedan", "Truck", "Sedan", "SUV"],
            }
        )
        y = pd.Series([25_000, 42_000, 28_000, 65_000])
        tree_preprocessor, _, _ = build_preprocessors(X)
        tree_preprocessor.fit(X, y)

        feature_names = get_preprocessor_feature_names(
            tree_preprocessor,
            tree_preprocessor.transform(X).shape[1],
        )

        self.assertIn("target_encoded__nhtsa_Make", feature_names)
        self.assertIn("target_encoded__nhtsa_Model", feature_names)
        self.assertIn("target_encoded__trim_proxy", feature_names)
        self.assertFalse(any(name.startswith("cat_high__") and name.split("__", 1)[1].isdigit() for name in feature_names))

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

    def test_depreciation_history_uses_cleaned_trim_columns(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "cleaned.db"
            create_cleaned_gap_fixture_db(db_path)

            raw = load_history_frame(db_path, sample_size=10)
            self.assertIn("title_trim", raw.columns)
            self.assertIn("trim_combined", raw.columns)

            history = clean_history_frame(raw, max_price=250_000)
            self.assertEqual(set(history["trim_proxy"]), {"LE"})

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

    def test_depreciation_frame_uses_cleaned_trim_and_monthly_target(self):
        rows = []
        for idx, month in enumerate(pd.date_range("2025-01-10", periods=4, freq="MS")):
            rows.append(
                {
                    "vin": f"VINMONTH{idx}",
                    "history_date": month,
                    "mileage": 10000 + idx * 1000,
                    "price": 30000 - idx * 500,
                    "trend": "down",
                    "title": "2022 Toyota Camry LE",
                    "vehicleTitle": "Clean",
                    "vehicleTitleDesc": "Clean title",
                    "trim_combined": "XSE",
                    "nhtsa_trim_combined": "LE",
                    "title_trim": "LE",
                    "nhtsa_Trim": "LE",
                    "nhtsa_Trim2": None,
                    "nhtsa_Make": "TOYOTA",
                    "nhtsa_Model": "CAMRY",
                    "nhtsa_ModelYear": 2022,
                    "nhtsa_BodyClass": "Sedan",
                    "nhtsa_DriveType": "4x2",
                    "nhtsa_FuelTypePrimary": "Gasoline",
                    "nhtsa_ElectrificationLevel": None,
                    "sellerType": "Dealer",
                    "sourceName": "autotempest",
                    "nhtsa_EngineHP": 203,
                    "nhtsa_EngineCylinders": 4,
                    "nhtsa_total_recalls": 0,
                    "nhtsa_total_complaints": 0,
                    "sentiment_score": None,
                    "sentiment_comment_count": 0,
                    "sentiment_video_count": 0,
                }
            )

        history = clean_history_frame(pd.DataFrame(rows), max_price=250000)
        monthly = build_cohort_monthly_frame(
            history,
            target_months=[1],
            min_monthly_vins=1,
            min_cohort_months=3,
        )

        self.assertEqual(set(monthly["trim_proxy"]), {"XSE"})
        self.assertIn("month_start", monthly.columns)
        self.assertIn("target_depreciation_pct_1m", monthly.columns)
        self.assertNotIn("target_depreciation_pct_30d", monthly.columns)
        self.assertEqual(monthly["month_start"].dt.day.unique().tolist(), [1])


if __name__ == "__main__":
    unittest.main()
