import sqlite3
import tempfile
import unittest
from pathlib import Path

from DataPipeline.NHTSA_enrichment import NHTSAEnricher
from DataPipeline.database import CarDatabase, NHTSADataStore, backup_sqlite_database
from Utilities.verify_schema import verify_nhtsa_schema


VIN = "1HGCM82633A004352"


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.headers = {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class NHTSAEnrichmentTests(unittest.TestCase):
    def make_enricher(self, directory):
        car_path = Path(directory) / "CAR_DATA.db"
        database = CarDatabase(str(car_path))
        database.insert_rows([{
            "vin": VIN,
            "loaddate": "2024-01-01",
            "year": 2003,
            "title": "2003 Honda Accord EX",
            "price": 10000,
            "mileage": 100000,
        }])
        database.close()
        return NHTSAEnricher(
            output_dir=str(directory),
            db_path=str(car_path),
            rate_limit_delay=0,
            refresh_days=30,
        ), car_path

    def fake_request(self, method, url, **kwargs):
        if method == "POST":
            payload = {
                "Count": 1,
                "Message": "Results returned successfully",
                "Results": [{
                    "VIN": VIN,
                    "Make": "HONDA",
                    "Model": "ACCORD",
                    "ModelYear": "2003",
                    "BodyClass": "Sedan",
                    "NewFutureVariable": "preserve me",
                    "ErrorCode": "0",
                }],
            }
        elif "VehicleId" in url:
            payload = {"Count": 1, "Results": [{
                "VehicleId": 7520,
                "OverallRating": "5",
                "OverallFrontCrashRating": "5",
                "OverallSideCrashRating": "5",
                "RolloverRating": "4",
                "NewSafetyField": "preserve me",
            }]}
        elif "SafetyRatings" in url:
            payload = {"Count": 1, "Results": [{
                "VehicleId": 7520,
                "VehicleDescription": "Accord EX",
            }]}
        elif "recallsByVehicle" in url:
            payload = {"Count": 1, "results": [{
                "Manufacturer": "Honda",
                "NHTSACampaignNumber": "24V001000",
                "ReportReceivedDate": "01/01/2024",
                "Component": "AIR BAGS",
                "Summary": "Recall summary",
                "Consequence": "Consequence",
                "Remedy": "Remedy",
                "NewRecallField": "preserve me",
            }]}
        else:
            payload = {"count": 1, "results": [{
                "odiNumber": "1234567",
                "manufacturer": "Honda",
                "numberOfInjuries": 1,
                "numberOfDeaths": 0,
                "crash": "Yes",
                "fire": "No",
                "components": "ENGINE",
                "summary": "Complaint summary",
                "NewComplaintField": "preserve me",
                "products": [{
                    "type": "Vehicle", "productYear": "2003",
                    "productMake": "HONDA", "productModel": "ACCORD",
                    "manufacturer": "Honda",
                }],
            }]}
        return FakeResponse(payload)

    def test_batch_uses_documented_payload_and_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            enricher, _ = self.make_enricher(directory)
            calls = []

            def request(method, url, **kwargs):
                calls.append((method, url, kwargs))
                return FakeResponse({"Count": 0, "Results": []})

            enricher.session.request = request
            response = enricher.decode_vins_batch([VIN], {VIN: 2003})
            self.assertIsNotNone(response)
            method, _, kwargs = calls[0]
            self.assertEqual(method, "POST")
            self.assertEqual(kwargs["data"], {"data": f"{VIN},2003", "format": "json"})
            self.assertNotIn("DATA", kwargs["data"])
            enricher.decode_vins_batch([VIN] * 51, {VIN: 2003})
            self.assertEqual(len(calls[-1][2]["data"]["data"].split(";")), 50)
            enricher.close()

    def test_full_enrichment_persists_normalized_fields_without_raw_json(self):
        with tempfile.TemporaryDirectory() as directory:
            enricher, car_path = self.make_enricher(directory)
            enricher.session.request = self.fake_request
            self.assertEqual(enricher.run(refresh_all=True, max_vins=1), 1)
            self.assertEqual(enricher.run(refresh_all=True, max_vins=1), 1)
            enricher.close()

            connection = sqlite3.connect(car_path)
            row = connection.execute(
                """
                SELECT nhtsa_Make, nhtsa_Model, nhtsa_overall_rating,
                       nhtsa_total_recalls, nhtsa_total_complaints,
                       nhtsa_safety_status
                FROM nhtsa_enrichment WHERE vin = ?
                """,
                (VIN,),
            ).fetchone()
            connection.close()
            self.assertEqual(row[:5], ("HONDA", "ACCORD", "5", 1, 1))
            self.assertEqual(row[5], "success")

            nhtsa_path = Path(directory) / "CAR_DATA_NHTSA.db"
            connection = sqlite3.connect(nhtsa_path)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM nhtsa_vpic_decodes").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM nhtsa_vin_identity_resolution").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM nhtsa_vpic_values").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM nhtsa_safety_variants").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM nhtsa_vehicle_queries WHERE query_type = 'safety_detail'").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM nhtsa_safety_details").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM nhtsa_recalls").fetchone()[0], 1)
            self.assertEqual(connection.execute("SELECT COUNT(*) FROM nhtsa_complaints").fetchone()[0], 1)
            stored_product = connection.execute(
                """
                SELECT product_type, product_year, product_make, product_model, manufacturer
                FROM nhtsa_complaint_products
                """
            ).fetchone()[0]
            future_value = connection.execute(
                """
                SELECT NewFutureVariable FROM nhtsa_vpic_values
                """
            ).fetchone()[0]
            safety_extra = connection.execute(
                """
                SELECT field_value FROM nhtsa_safety_rating_values
                WHERE field_name = 'NewSafetyField'
                """
            ).fetchone()[0]
            api_extras = dict(connection.execute(
                """
                SELECT field_name, field_value FROM nhtsa_api_extra_fields
                WHERE field_name IN ('NewRecallField', 'NewComplaintField')
                """
            ).fetchall())
            forbidden_columns = {
                "response_json", "result_json", "record_json", "raw_json",
                "variant_json", "detail_json", "value_json",
            }
            stored_columns = {
                row[1]
                for table in ("nhtsa_vpic_decodes", "nhtsa_vpic_values", "nhtsa_vehicle_queries",
                              "nhtsa_safety_variants", "nhtsa_safety_details",
                              "nhtsa_safety_rating_values", "nhtsa_recalls",
                              "nhtsa_complaints", "nhtsa_bulk_rows")
                for row in connection.execute(f"PRAGMA table_info('{table}')")
            }
            connection.close()
            self.assertEqual(future_value, "preserve me")
            self.assertEqual(safety_extra, "preserve me")
            self.assertEqual(api_extras, {
                "NewRecallField": "preserve me",
                "NewComplaintField": "preserve me",
            })
            self.assertFalse(forbidden_columns & stored_columns)
            self.assertEqual(stored_product, "Vehicle")
            self.assertTrue(verify_nhtsa_schema(nhtsa_path))

    def test_sqlite_backup_is_recoverable_and_not_overwritten(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory) / "source.db"
            backup = Path(directory) / "backup.db"
            database = CarDatabase(str(source))
            database.insert_rows([{"vin": VIN, "loaddate": "2024-01-01", "year": 2003}])
            database.close()

            backup_sqlite_database(source, backup)
            connection = sqlite3.connect(backup)
            try:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM listings").fetchone()[0], 1)
            finally:
                connection.close()
            with self.assertRaises(FileExistsError):
                backup_sqlite_database(source, backup)

    def test_bulk_import_streams_rows_and_preserves_source_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            bulk_file = Path(directory) / "investigations.csv"
            bulk_file.write_text("case_id,component\n1,ENGINE\n2,BRAKES\n", encoding="utf-8")
            nhtsa_path = Path(directory) / "CAR_DATA_NHTSA.db"
            store = NHTSADataStore(nhtsa_path)
            try:
                self.assertEqual(store.ingest_bulk_file(bulk_file, "investigations"), 2)
            finally:
                store.close()
            connection = sqlite3.connect(nhtsa_path)
            try:
                self.assertEqual(connection.execute("SELECT row_count FROM nhtsa_bulk_datasets").fetchone()[0], 2)
                rows = connection.execute(
                    """
                    SELECT source_row_number, field_name, field_value
                    FROM nhtsa_bulk_fields
                    ORDER BY source_row_number, field_name
                    """
                ).fetchall()
            finally:
                connection.close()
            self.assertEqual(rows, [
                (1, "case_id", "1"), (1, "component", "ENGINE"),
                (2, "case_id", "2"), (2, "component", "BRAKES"),
            ])


if __name__ == "__main__":
    unittest.main()
