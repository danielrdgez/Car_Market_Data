import unittest

import pandas as pd

from ML.Price_ML_Models import assert_canonical_identity_contract
from ML.Time_Series_Price import (
    COHORT_COLUMNS,
    eligible_rolling_origins,
    rolling_origin_metadata,
)


class CanonicalBacktestingTests(unittest.TestCase):
    def test_price_features_reject_legacy_trim_fields(self):
        valid = pd.DataFrame({"canonical_trim": ["GT350"], "canonical_make": ["FORD"]})
        assert_canonical_identity_contract(valid)
        with self.assertRaises(AssertionError):
            assert_canonical_identity_contract(valid.assign(nhtsa_Trim="SHELBY"))

    def test_rolling_origins_require_six_canonical_cohort_months(self):
        months = pd.date_range("2025-01-01", periods=8, freq="MS")
        frame = pd.DataFrame(
            {
                "make": "FORD",
                "model": "MUSTANG",
                "model_year": 2020,
                "trim_proxy": "GT350",
                "month_start": months,
                "target_median_price_1m": list(range(7)) + [None],
            }
        )
        origins = eligible_rolling_origins(frame, horizon=1, min_history_months=6)
        self.assertEqual(origins, [pd.Timestamp("2025-06-01"), pd.Timestamp("2025-07-01")])

    def test_rolling_metadata_records_expanding_window(self):
        rows = pd.DataFrame({column: ["x"] for column in COHORT_COLUMNS})
        stamped = rolling_origin_metadata(rows, pd.Timestamp("2025-08-01"), pd.Timestamp("2025-01-01"), 6)
        self.assertEqual(stamped.loc[0, "backtest_scheme"], "expanding_rolling_origin")
        self.assertEqual(stamped.loc[0, "minimum_history_months"], 6)
        self.assertLessEqual(stamped.loc[0, "train_window_end"], stamped.loc[0, "rolling_origin_month"])


if __name__ == "__main__":
    unittest.main()
