import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

import streamlit_app as app
from ML.Price_ML_Models import engineer_current_price_features, make_inference_feature_matrix
from ML.Vehicle_Scenario import (
    apply_monthly_scenario_overrides,
    apply_price_anchor,
    build_resolved_vehicle_row,
    engine_profile_options,
    select_scenario_pool,
    select_stored_reference_rows,
    transmission_profile_options,
)


class StreamlitVehicleScenarioTests(unittest.TestCase):
    def setUp(self) -> None:
        self.rows = pd.DataFrame(
            [
                {
                    "vin": "VIN1",
                    "loaddate": "2026-08-01",
                    "date": "2026-08-01",
                    "canonical_make": "FORD",
                    "canonical_model": "MUSTANG",
                    "canonical_year": 2018,
                    "canonical_trim": "GT350",
                    "mileage": 30000,
                    "price": 55000,
                    "nhtsa_BodyClass": "Coupe",
                    "nhtsa_TransmissionStyle": "Manual",
                    "nhtsa_TransmissionSpeeds": 6,
                    "nhtsa_EngineModel": "Voodoo",
                    "nhtsa_EngineConfiguration": "V-Shaped",
                    "nhtsa_EngineHP": 526,
                    "nhtsa_EngineCylinders": 8,
                },
                {
                    "vin": "VIN2",
                    "loaddate": "2026-08-02",
                    "date": "2026-08-02",
                    "canonical_make": "FORD",
                    "canonical_model": "MUSTANG",
                    "canonical_year": 2018,
                    "canonical_trim": "GT350",
                    "mileage": 35000,
                    "price": 52000,
                    "nhtsa_BodyClass": "Coupe",
                    "nhtsa_TransmissionStyle": "Manual",
                    "nhtsa_TransmissionSpeeds": 6,
                    "nhtsa_EngineModel": "Voodoo",
                    "nhtsa_EngineConfiguration": "V-Shaped",
                    "nhtsa_EngineHP": 526,
                    "nhtsa_EngineCylinders": 8,
                },
                {
                    "vin": "VIN3",
                    "loaddate": "2026-08-03",
                    "date": "2026-08-03",
                    "canonical_make": "FORD",
                    "canonical_model": "MUSTANG",
                    "canonical_year": 2018,
                    "canonical_trim": "GT350R",
                    "mileage": 20000,
                    "price": 70000,
                    "nhtsa_BodyClass": "Coupe",
                    "nhtsa_TransmissionStyle": "Manual",
                    "nhtsa_TransmissionSpeeds": 6,
                    "nhtsa_EngineModel": "Voodoo",
                    "nhtsa_EngineConfiguration": "V-Shaped",
                    "nhtsa_EngineHP": 526,
                    "nhtsa_EngineCylinders": 8,
                },
                {
                    "vin": "VIN4",
                    "loaddate": "2026-08-04",
                    "date": "2026-08-04",
                    "canonical_make": "FORD",
                    "canonical_model": "MUSTANG",
                    "canonical_year": 2019,
                    "canonical_trim": "GT350R",
                    "mileage": 10000,
                    "price": 80000,
                    "nhtsa_BodyClass": "Coupe",
                    "nhtsa_TransmissionStyle": "Manual",
                    "nhtsa_TransmissionSpeeds": 6,
                    "nhtsa_EngineModel": "Voodoo",
                    "nhtsa_EngineConfiguration": "V-Shaped",
                    "nhtsa_EngineHP": 526,
                    "nhtsa_EngineCylinders": 8,
                },
            ]
        )

    def test_exact_modal_profile_and_user_overrides(self) -> None:
        pool, metadata = select_scenario_pool(self.rows, "FORD", "MUSTANG", 2018, "GT350")
        self.assertFalse(metadata["fallback_used"])
        self.assertEqual(len(pool), 2)

        transmission = transmission_profile_options(pool)[0]
        engine = engine_profile_options(pool)[0]
        resolved, provenance, _ = build_resolved_vehicle_row(
            pool,
            make="FORD",
            model="MUSTANG",
            model_year=2018,
            trim="GT350",
            mileage=12345,
            transmission_profile=transmission,
            engine_profile=engine,
        )

        self.assertEqual(resolved.loc[0, "mileage"], 12345)
        self.assertEqual(resolved.loc[0, "nhtsa_TransmissionStyle"], "Manual")
        self.assertEqual(resolved.loc[0, "nhtsa_EngineHP"], 526)
        self.assertNotIn("price", resolved.columns)
        self.assertEqual(
            provenance.loc[provenance["field"].eq("mileage"), "source"].iloc[0],
            "user input",
        )

    def test_trim_fallback_stays_within_model_year(self) -> None:
        pool, metadata = select_scenario_pool(self.rows, "FORD", "MUSTANG", 2018, "DOES_NOT_EXIST")
        self.assertTrue(metadata["fallback_used"])
        self.assertEqual(set(pool["canonical_year"]), {2018})
        self.assertEqual(set(pool["canonical_trim"]), {"GT350", "GT350R"})

    def test_inference_features_have_no_target_or_leakage_columns(self) -> None:
        pool, _ = select_scenario_pool(self.rows, "FORD", "MUSTANG", 2018, "GT350")
        resolved, _, _ = build_resolved_vehicle_row(
            pool,
            "FORD",
            "MUSTANG",
            2018,
            "GT350",
            25000,
        )
        engineered = engineer_current_price_features(resolved, require_target=False)
        features = make_inference_feature_matrix(engineered)
        self.assertNotIn("price", features.columns)
        self.assertNotIn("price_band", features.columns)
        self.assertNotIn("nhtsa_BasePrice", features.columns)
        self.assertIn("canonical_make", features.columns)

    def test_monthly_overrides_and_price_anchor(self) -> None:
        latest = pd.DataFrame(
            [
                {
                    "make": "FORD",
                    "model": "MUSTANG",
                    "model_year": 2018,
                    "trim_proxy": "GT350",
                    "month_start": pd.Timestamp("2026-07-01"),
                    "median_price": 50000.0,
                    "avg_price": 51000.0,
                    "price_p25": 47000.0,
                    "price_p75": 54000.0,
                    "median_mileage": 30000.0,
                    "avg_mileage": 31000.0,
                    "rolling_avg_mileage_3m": 29000.0,
                    "avg_vehicle_age_months": 90.0,
                    "avg_miles_per_year": 4000.0,
                    "engine_hp": 500.0,
                    "engine_cylinders": 8.0,
                    "cohort_first_median_price": 80000.0,
                    "price_index_vs_cohort_first": 0.625,
                    "cumulative_depreciation_pct": -0.375,
                    "lag_median_price_1": 50500.0,
                    "rolling_median_price_3m": 50500.0,
                }
            ]
        )
        raw = self.rows.iloc[[0]].copy()
        overridden = apply_monthly_scenario_overrides(latest, raw, 12345, as_of=pd.Timestamp("2026-08-01"))
        anchored = apply_price_anchor(overridden, 60000)
        self.assertEqual(overridden.loc[0, "median_mileage"], 12345)
        self.assertEqual(overridden.loc[0, "engine_hp"], 526)
        self.assertEqual(anchored.loc[0, "median_price"], 60000)
        self.assertAlmostEqual(anchored.loc[0, "price_index_vs_cohort_first"], 0.75)

    def test_stored_reference_prefers_exact_then_falls_back(self) -> None:
        references = pd.DataFrame(
            [
                {
                    "trim_proxy": "GT350",
                    "forecast_method": "sarimax_local_model",
                    "unique_vins": 10,
                    "volume": 20,
                },
                {
                    "trim_proxy": "BASE",
                    "forecast_method": "prophet_local_model",
                    "unique_vins": 12,
                    "volume": 30,
                },
            ]
        )
        exact, exact_label = select_stored_reference_rows(references, "GT350")
        fallback, fallback_label = select_stored_reference_rows(references, "GT500")
        self.assertEqual(exact_label, "Exact stored cohort")
        self.assertEqual(set(exact["trim_proxy"]), {"GT350"})
        self.assertEqual(fallback_label, "Coverage fallback")
        self.assertEqual(set(fallback["trim_proxy"]), {"BASE"})

    def test_scenario_model_errors_do_not_hide_successful_models(self) -> None:
        class Predicts:
            def predict(self, frame):
                assert frame.shape[0] == 1
                return np.array([12345.0])

        with tempfile.TemporaryDirectory() as tempdir:
            original_models_dir = app.MODELS_DIR
            app.MODELS_DIR = Path(tempdir)
            (app.MODELS_DIR / "Ridge.joblib").touch()
            (app.MODELS_DIR / "Broken.joblib").touch()
            raw = pd.DataFrame(
                [{
                    "canonical_make": "FORD",
                    "canonical_model": "MUSTANG",
                    "canonical_year": 2018,
                    "canonical_trim": "GT350",
                    "mileage": 1000,
                }]
            )
            try:
                with (
                    patch.object(app, "engineer_current_price_features", return_value=raw),
                    patch.object(app, "make_inference_feature_matrix", return_value=pd.DataFrame({"x": [1]})),
                    patch.object(app, "load_current_model", side_effect=[Predicts(), AttributeError("broken artifact")]),
                ):
                    predictions, errors = app.current_scenario_predictions(raw, ["Ridge", "Broken"])
            finally:
                app.MODELS_DIR = original_models_dir

        self.assertEqual(predictions["model"].tolist(), ["Ridge"])
        self.assertIn("Broken: broken artifact", errors)


if __name__ == "__main__":
    unittest.main()
