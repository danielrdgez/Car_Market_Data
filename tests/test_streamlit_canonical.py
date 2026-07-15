import sqlite3
import tempfile
import unittest
from pathlib import Path

import pandas as pd

import streamlit_app as app


class StreamlitCanonicalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.original_db_path = app.DB_PATH
        app.DB_PATH = Path(self.temporary.name) / "fixture.db"
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


if __name__ == "__main__":
    unittest.main()
