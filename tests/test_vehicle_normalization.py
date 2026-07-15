import sqlite3
import tempfile
import unittest
import zipfile
from pathlib import Path

import polars as pl

from DataPipeline.VehicleNormalization import (
    NORMALIZATION_VERSION,
    VehicleNormalizer,
    build_vehicle_identity,
    build_epa_metadata_frame,
    download_epa_catalog,
    import_epa_catalog_sqlite,
    load_epa_catalog,
)


class VehicleNormalizationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.normalizer = VehicleNormalizer()

    def test_specific_performance_trim_wins_longest_match(self) -> None:
        parsed = self.normalizer.parse_title(
            "2018 Ford Mustang Shelby GT350R Coupe",
            nhtsa_make="FORD",
            nhtsa_model="MUSTANG",
            nhtsa_year=2018,
            nhtsa_trim="SHELBY",
            nhtsa_trim2="COUPE",
        )
        self.assertEqual(parsed.canonical_trim, "GT350R")
        self.assertEqual(parsed.canonical_trim_source, "title_alias")
        self.assertFalse(parsed.nhtsa_trim_agrees)

    def test_nhtsa_make_model_anchor_and_title_agreement_are_separate(self) -> None:
        parsed = self.normalizer.parse_title(
            "2023 Toyota Tacoma TRD Off-Road",
            nhtsa_make="TOYOTA",
            nhtsa_model="TACOMA",
            nhtsa_year=2023,
            nhtsa_trim="SR5",
        )
        self.assertEqual((parsed.canonical_make, parsed.canonical_model), ("TOYOTA", "TACOMA"))
        self.assertEqual(parsed.canonical_trim, "TRD_OFF_ROAD")
        self.assertTrue(parsed.nhtsa_make_agrees)
        self.assertTrue(parsed.nhtsa_model_agrees)
        self.assertFalse(parsed.nhtsa_trim_agrees)

        disagreement = self.normalizer.parse_title(
            "2023 Toyota Tundra Limited",
            nhtsa_make="TOYOTA",
            nhtsa_model="TACOMA",
            nhtsa_year=2023,
        )
        self.assertEqual(disagreement.canonical_model, "TACOMA")
        self.assertFalse(disagreement.nhtsa_model_agrees)

    def test_empty_title_trim_is_explicitly_unresolved(self) -> None:
        parsed = self.normalizer.parse_title(
            "2022 Honda Civic Sedan",
            nhtsa_make="HONDA",
            nhtsa_model="CIVIC",
            nhtsa_year=2022,
        )
        self.assertEqual(parsed.canonical_trim, "UNKNOWN_TRIM")
        self.assertEqual(parsed.canonical_match_status, "unresolved")

    def test_epa_archive_load_and_cached_mode(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            cache_dir = Path(tmp)
            archive = cache_dir / "vehicles.csv.zip"
            csv_text = (
                "id,year,make,model,baseModel\n"
                "1,2022,BMW,M3 Competition xDrive,M3\n"
                "2,2023,Toyota,Tacoma,Tacoma\n"
            )
            with zipfile.ZipFile(archive, "w", zipfile.ZIP_DEFLATED) as bundle:
                bundle.writestr("vehicles.csv", csv_text)

            cached_path, metadata = download_epa_catalog(cache_dir, refresh=False)
            catalog = load_epa_catalog(cached_path)
            self.assertEqual(metadata["cache_status"], "cached")
            self.assertEqual(catalog.height, 2)
            self.assertIn("normalized_base_model", catalog.columns)

            parsed = VehicleNormalizer(catalog).parse_title(
                "2022 BMW M3 Competition xDrive",
                nhtsa_make="BMW",
                nhtsa_model="M3",
                nhtsa_year=2022,
            )
            self.assertEqual(parsed.canonical_trim, "M3_COMPETITION_XDRIVE")
            self.assertIn(parsed.canonical_trim_source, {"title_alias", "title_epa"})
            self.assertEqual(parsed.normalization_version, NORMALIZATION_VERSION)

            db_path = cache_dir / "catalog.db"
            conn = sqlite3.connect(db_path)
            conn.close()
            import_epa_catalog_sqlite(
                db_path,
                catalog,
                build_epa_metadata_frame(metadata, catalog),
            )
            conn = sqlite3.connect(db_path)
            try:
                self.assertEqual(conn.execute("SELECT COUNT(*) FROM epa_vehicle_catalog").fetchone()[0], 2)
                indexes = {row[1] for row in conn.execute("PRAGMA index_list('epa_vehicle_catalog')")}
                self.assertIn("idx_epa_catalog_normalized_identity", indexes)
            finally:
                conn.close()

    def test_vin_consensus_prefers_confidence_then_recency(self) -> None:
        listings = pl.DataFrame(
            {
                "vin": ["VIN1", "VIN1"],
                "loaddate": ["2026-01-01", "2026-02-01"],
                "canonical_title": ["2020 FORD MUSTANG GT", "2020 FORD MUSTANG GT350"],
                "canonical_year": [2020, 2020],
                "canonical_make": ["FORD", "FORD"],
                "canonical_model": ["MUSTANG", "MUSTANG"],
                "canonical_trim": ["GT", "GT350"],
                "canonical_trim_raw": ["GT", "GT350"],
                "canonical_trim_source": ["title_remainder", "title_alias"],
                "canonical_match_confidence": ["high", "medium"],
                "canonical_match_status": ["confirmed", "best_candidate"],
                "epa_vehicle_id": [1, None],
                "epa_match_status": ["exact", "none"],
                "normalization_version": [NORMALIZATION_VERSION, NORMALIZATION_VERSION],
            }
        )
        identity = build_vehicle_identity(listings)
        self.assertEqual(identity.row(0, named=True)["canonical_trim"], "GT")
        self.assertEqual(identity.row(0, named=True)["supporting_listing_count"], 2)
        self.assertEqual(identity.row(0, named=True)["conflict_count"], 1)


if __name__ == "__main__":
    unittest.main()
