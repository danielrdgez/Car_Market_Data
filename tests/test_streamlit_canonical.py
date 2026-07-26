import __main__
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

import streamlit_app as app


class StreamlitCanonicalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.original_db_path = app.DB_PATH
        self.original_models_dir = app.MODELS_DIR
        app.DB_PATH = Path(self.temporary.name) / "fixture.db"
        app.MODELS_DIR = Path(self.temporary.name) / "models"
        app.MODELS_DIR.mkdir()
        for model_name in ["Ridge", "Broken"]:
            (app.MODELS_DIR / f"{model_name}.joblib").touch()
        conn = sqlite3.connect(app.DB_PATH)
        try:
            conn.execute(
                """
                CREATE TABLE vehicle_identity (
                    vin TEXT PRIMARY KEY,
                    canonical_make TEXT,
                    canonical_model TEXT,
                    canonical_year INTEGER,
                    canonical_trim TEXT,
                    normalization_version TEXT
                )
                """
            )
            conn.executemany(
                "INSERT INTO vehicle_identity VALUES (?, ?, ?, ?, ?, ?)",
                [
                    ("VIN1", "FORD", "MUSTANG", 2018, "GT350", "title_epa_v1"),
                    ("VIN2", "FORD", "MUSTANG", 2018, "GT350R", "title_epa_v1"),
                    ("VIN3", "TOYOTA", "TACOMA", 2023, "TRD_OFF_ROAD", "title_epa_v1"),
                ],
            )
            conn.commit()
        finally:
            conn.close()
        for function in [
            app.get_makes,
            app.get_models,
            app.get_years,
            app.get_trim_options,
            app.database_normalization_versions,
        ]:
            function.clear()

    def tearDown(self) -> None:
        for function in [app.get_makes, app.get_models, app.get_years, app.get_trim_options]:
            function.clear()
        app.DB_PATH = self.original_db_path
        app.MODELS_DIR = self.original_models_dir
        self.temporary.cleanup()

    def test_filter_options_come_from_canonical_identity(self) -> None:
        self.assertEqual(app.get_makes(), ["FORD", "TOYOTA"])
        self.assertEqual(app.get_models("FORD"), ["MUSTANG"])
        self.assertEqual(app.get_years("FORD", "MUSTANG"), [2018])
        self.assertEqual(app.get_trim_options("FORD", "MUSTANG", 2018), ["GT350", "GT350R"])

    def test_trim_proxy_ignores_conflicting_nhtsa_trim(self) -> None:
        frame = pd.DataFrame(
            {"canonical_trim": ["GT350R"], "nhtsa_Trim": ["SHELBY"], "trim_combined": ["SHELBY"]}
        )
        result = app.add_trim_proxy(frame)
        self.assertEqual(result.loc[0, "trim_proxy"], "GT350R")

    def test_current_price_joblib_symbols_are_registered(self) -> None:
        self.assertIs(
            __main__.normalize_categorical_missing_values,
            app.normalize_categorical_missing_values,
        )

    def test_filtered_metrics_keep_schema_and_successful_models(self) -> None:
        class PredictsCurrentPrice:
            def predict(self, features):
                if features.shape[0] != 2:
                    raise AssertionError(f"expected 2 scoring rows, received {features.shape[0]}")
                return [110.0, 190.0]

        def load_model(model_name, modified_ns):
            self.assertGreater(modified_ns, 0)
            if model_name == "Broken":
                raise AttributeError("missing serialized helper")
            return PredictsCurrentPrice()

        raw = pd.DataFrame({"raw": [1, 2]})
        features = pd.DataFrame({"feature": [1.0, 2.0]})
        targets = pd.Series([100.0, 200.0])
        with (
            patch.object(app, "load_vehicle_model_rows", return_value=raw),
            patch.object(app, "engineer_current_price_features", return_value=raw),
            patch.object(
                app,
                "make_feature_matrix",
                return_value=(features, targets, pd.DataFrame()),
            ),
            patch.object(app, "load_current_model", side_effect=load_model),
        ):
            metrics, errors = app.filtered_current_model_metrics(
                pd.DataFrame({"vin": ["VIN1", "VIN2"]}),
                ["Ridge", "Broken"],
            )

        self.assertEqual(metrics["model"].tolist(), ["Ridge"])
        self.assertAlmostEqual(metrics.loc[0, "mae"], 10.0)
        self.assertAlmostEqual(metrics.loc[0, "rmse"], 10.0)
        self.assertIn("Broken: missing serialized helper", errors)

    def test_filtered_metrics_do_not_mask_model_load_errors(self) -> None:
        raw = pd.DataFrame({"raw": [1]})
        with (
            patch.object(app, "load_vehicle_model_rows", return_value=raw),
            patch.object(app, "engineer_current_price_features", return_value=raw),
            patch.object(
                app,
                "make_feature_matrix",
                return_value=(
                    pd.DataFrame({"feature": [1.0]}),
                    pd.Series([100.0]),
                    pd.DataFrame(),
                ),
            ),
            patch.object(
                app,
                "load_current_model",
                side_effect=AttributeError("missing serialized helper"),
            ),
        ):
            metrics, errors = app.filtered_current_model_metrics(
                pd.DataFrame({"vin": ["VIN1"]}),
                ["Broken"],
            )

        self.assertTrue(metrics.empty)
        self.assertIn("mae", metrics.columns)
        self.assertEqual(errors, ["Broken: missing serialized helper"])

    def test_empty_current_report_keeps_metric_schema(self) -> None:
        metrics = app.current_metrics_table({})
        self.assertTrue(metrics.empty)
        self.assertIn("mae", metrics.columns)

    def test_forecast_cohort_selection_maximizes_model_coverage(self) -> None:
        rows = []
        for forecast_month in [1, 2]:
            rows.append(
                {
                    "trim_proxy": "LE",
                    "forecast_method": "recursive_global_ml_model",
                    "forecast_month": forecast_month,
                    "unique_vins": 20,
                    "volume": 40,
                }
            )
            for method in [
                "recursive_global_ml_model",
                "sarimax_local_model",
                "prophet_local_model",
                "timesfm_local_model",
            ]:
                rows.append(
                    {
                        "trim_proxy": "BASE",
                        "forecast_method": method,
                        "forecast_month": forecast_month,
                        "unique_vins": 10,
                        "volume": 30,
                    }
                )

        selected, match_label = app.select_forecast_cohort_rows(pd.DataFrame(rows), "LE")

        self.assertEqual(match_label, "Coverage fallback")
        self.assertEqual(set(selected["trim_proxy"]), {"BASE"})
        self.assertEqual(
            set(selected["forecast_method"]),
            {
                "recursive_global_ml_model",
                "sarimax_local_model",
                "prophet_local_model",
                "timesfm_local_model",
            },
        )


if __name__ == "__main__":
    unittest.main()
