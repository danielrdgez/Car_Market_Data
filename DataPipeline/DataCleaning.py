import logging
import os
import sqlite3
from datetime import date
from pathlib import Path
from typing import Iterable

import polars as pl

from database import CarDatabase


MAKE_WHITELIST_UPPER = {
    "TOYOTA", "NISSAN", "FORD", "CHEVROLET", "CADILLAC",
    "HONDA", "VOLVO", "MASERATI", "PORSCHE", "ACURA", "LEXUS", "TESLA",
    "KIA", "BMW", "MERCEDES", "HYUNDAI", "INFINITI", "DODGE", "LOTUS",
    "SUZUKI", "MAZDA", "FIAT", "LINCOLN", "SUBARU", "GMC",
    "GENESIS", "JEEP", "VOLKSWAGEN", "LANDROVER", "AUDI", "RAM",
    "CHRYSLER", "JAGUAR", "FERRARI", "LAMBORGHINI", "MCLAREN",
}

LISTINGS_KEEP_COLUMNS = [
    "vin", "date", "loaddate", "locationCode", "price", "mileage",
    "title", "sourceName", "sellerType", "vehicleTitle",
]

NHTSA_DROP_COLUMNS = {
    "nhtsa_latest_recall_date", "nhtsa_recall_components", "nhtsa_common_complaint_areas",
    "nhtsa_complaint_deaths", "nhtsa_complaint_crash_related", "nhtsa_complaint_fire_related",
    "nhtsa_complaint_injuries", "nhtsa_side_crash_rating", "nhtsa_rollover_rating",
    "nhtsa_front_crash_rating", "nhtsa_overall_rating", "nhtsa_safety_ratings_count",
    "nhtsa_FuelTankMaterial", "nhtsa_FuelTankType",
    "nhtsa_AdditionalErrorText", "nhtsa_DisplacementCC", "nhtsa_DisplacementCI",
    "nhtsa_ModelID", "nhtsa_ManufacturerId", "nhtsa_MakeID", 'nhtsa_SemiautomaticHeadlampBeamSwitching',
    'nhtsa_LowerBeamHeadlampLightSource', 'nhtsa_EntertainmentSystem', 'nhtsa_DestinationMarket',
    'nhtsa_BrakeSystemDesc'
}

PRICE_MIN = 1000
PRICE_MAX = 1_000_000

VIN_BLACKLIST = {
    "1GCUY6ED0LF228114",
    "1FTFW6L8XSFB63087",
}


class DataCleaningPipeline:
    """Builds a filtered SQLite clone for cleaned analysis output."""

    def __init__(self, source_db_path: Path, target_db_path: Path):
        self.source_db_path = Path(source_db_path)
        self.target_db_path = Path(target_db_path)
        self.output_dir = self.target_db_path.parent
        self._setup_logging()

    def _setup_logging(self) -> None:
        os.makedirs(self.output_dir, exist_ok=True)
        log_file = self.output_dir / f"cleaning_{date.today()}.log"
        logging.basicConfig(
            filename=str(log_file),
            level=logging.INFO,
            format="%(asctime)s - %(levelname)s - %(message)s",
        )

    @staticmethod
    def _read_table(conn: sqlite3.Connection, table_name: str) -> pl.DataFrame:
        cursor = conn.execute(f"SELECT * FROM {table_name}")
        rows = cursor.fetchall()
        columns = [d[0] for d in cursor.description]
        return pl.DataFrame(rows, schema=columns, orient="row", infer_schema_length=None)

    @staticmethod
    def _filter_non_empty(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                df = df.filter(
                    pl.col(col).is_not_null()
                    & (pl.col(col).cast(pl.String).str.strip_chars() != "")
                )
        return df

    @staticmethod
    def _normalize_date_columns(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                base = pl.col(col).cast(pl.String).str.strip_chars()
                df = df.with_columns(
                    pl.coalesce(
                        [
                            base.str.to_date("%Y-%m-%d", strict=False),
                            base.str.to_date("%m/%d/%Y", strict=False),
                            base.str.to_date("%Y/%m/%d", strict=False),
                            base.str.to_date("%b %d %Y", strict=False),
                            base.str.to_date("%B %d %Y", strict=False),
                            base.str.to_datetime("%Y-%m-%d %H:%M:%S", strict=False).dt.date(),
                            base.str.to_datetime("%Y-%m-%d %H:%M:%S%.f", strict=False).dt.date(),
                            base.str.to_datetime("%Y-%m-%dT%H:%M:%S", strict=False).dt.date(),
                            base.str.to_datetime("%Y-%m-%dT%H:%M:%S%.f", strict=False).dt.date(),
                            base.str.to_datetime("%Y-%m-%dT%H:%M:%SZ", strict=False).dt.date(),
                        ]
                    ).alias(col)
                )
        return df

    @staticmethod
    def _drop_null_dates(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                df = df.filter(pl.col(col).is_not_null())
        return df

    @staticmethod
    def _normalize_numeric_to_int_ceil(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col)
                    .cast(pl.String)
                    .str.strip_chars()
                    .str.replace_all(r"[\$,]", "")
                    .cast(pl.Float64, strict=False)
                    .ceil()
                    .cast(pl.Int64, strict=False)
                    .alias(col)
                )
        return df

    @staticmethod
    def _filter_price_range(df: pl.DataFrame, price_col: str = "price") -> pl.DataFrame:
        if price_col not in df.columns:
            return df
        return df.filter(
            pl.col(price_col).is_not_null()
            & (pl.col(price_col) >= PRICE_MIN)
            & (pl.col(price_col) <= PRICE_MAX)
        )

    @staticmethod
    def _filter_non_null_numeric(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                df = df.filter(pl.col(col).is_not_null())
        return df

    @staticmethod
    def _cast_to_int(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.String).str.strip_chars().cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).alias(col)
                )
        return df

    @staticmethod
    def _cast_to_float(df: pl.DataFrame, columns: Iterable[str]) -> pl.DataFrame:
        for col in columns:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.String).str.strip_chars().cast(pl.Float64, strict=False).alias(col)
                )
        return df

    @staticmethod
    def _insert_dataframe(
        conn: sqlite3.Connection,
        table_name: str,
        df: pl.DataFrame,
        conflict_mode: str = "",
    ) -> int:
        if df.is_empty():
            return 0

        columns = df.columns
        placeholders = ", ".join(["?" for _ in columns])
        quoted_columns = ", ".join([f'"{c}"' for c in columns])
        conflict_clause = f"OR {conflict_mode} " if conflict_mode else ""
        sql = f"INSERT {conflict_clause}INTO {table_name} ({quoted_columns}) VALUES ({placeholders})"

        rows = [tuple(r) for r in df.iter_rows()]
        conn.executemany(sql, rows)
        return len(rows)

    @staticmethod
    def _create_tables_from_polars(conn: sqlite3.Connection, table_configs: dict[str, tuple[pl.DataFrame, list[str]]]) -> None:
        type_str_map = {
            pl.Int64: "INTEGER",
            pl.Float64: "REAL",
            pl.String: "TEXT",
            pl.Boolean: "BOOLEAN",
            pl.Date: "DATE",
            pl.Datetime: "TIMESTAMP"
        }
        for table_name, (df, pks) in table_configs.items():
            cols = []
            for name, dtype in df.schema.items():
                sql_type = type_str_map.get(type(dtype), "TEXT")
                cols.append(f'"{name}" {sql_type}')

            if pks:
                pk_str = ", ".join(f'"{k}"' for k in pks)
                cols.append(f"PRIMARY KEY ({pk_str})")

            columns_sql = ",\n    ".join(cols)
            conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.execute(f"CREATE TABLE {table_name} (\n    {columns_sql}\n)")

    def run(self) -> None:
        if not self.source_db_path.exists():
            raise FileNotFoundError(f"Source database not found: {self.source_db_path}")

        logging.info("Starting cleaned database build")
        logging.info("Source DB: %s", self.source_db_path)
        logging.info("Target DB: %s", self.target_db_path)

        with sqlite3.connect(str(self.source_db_path)) as source_conn:
            listings = self._read_table(source_conn, "listings")
            listing_history = self._read_table(source_conn, "listing_history")
            price_history = self._read_table(source_conn, "price_history")
            nhtsa = self._read_table(source_conn, "nhtsa_enrichment")

        listings = listings.select([c for c in LISTINGS_KEEP_COLUMNS if c in listings.columns])
        listings = self._filter_non_empty(listings, ["vin", "loaddate"])
        listings = listings.filter(~pl.col("vin").is_in(VIN_BLACKLIST))
        listings = self._normalize_date_columns(listings, ["date", "loaddate"])
        listings = self._drop_null_dates(listings, ["date", "loaddate"])
        listings = self._normalize_numeric_to_int_ceil(listings, ["price", "mileage"])
        listings = self._filter_non_null_numeric(listings, ["price", "mileage"])
        listings = self._filter_price_range(listings, "price")

        listings = self._cast_to_int(listings, ["locationCode"])

        nhtsa = self._filter_non_empty(nhtsa, ["vin", "nhtsa_Make", "nhtsa_Model", "nhtsa_ModelYear"]).with_columns(
            [
                pl.col("nhtsa_Make").cast(pl.String).str.strip_chars().str.to_uppercase().alias("nhtsa_Make"),
                pl.col("nhtsa_Model").cast(pl.String).str.strip_chars().str.to_uppercase().alias("nhtsa_Model"),
                pl.col("nhtsa_ModelYear").cast(pl.String).str.strip_chars().cast(pl.Float64, strict=False).cast(pl.Int64, strict=False).alias("nhtsa_ModelYear"),
            ]
        )
        nhtsa = nhtsa.filter(pl.col("nhtsa_Make").is_in(MAKE_WHITELIST_UPPER))

        nhtsa = self._cast_to_int(nhtsa, [
            "nhtsa_Axles", "nhtsa_BedLengthIN", "nhtsa_BasePrice", "nhtsa_ChargerPowerKW", 
            "nhtsa_CurbWeightLB", "nhtsa_Doors", "nhtsa_EngineCycles", "nhtsa_EngineCylinders", 
            "nhtsa_EngineHP", "nhtsa_EngineHP_to", "nhtsa_EngineKW", "nhtsa_SAEAutomationLevel", 
            "nhtsa_SeatRows", "nhtsa_Seats", "nhtsa_TopSpeedMPH", "nhtsa_TransmissionSpeeds", 
            "nhtsa_WheelSizeFront", "nhtsa_WheelSizeRear", "nhtsa_Windows", "nhtsa_WheelBaseLong", 
            "nhtsa_WheelBaseShort"
        ])
        
        nhtsa = self._cast_to_float(nhtsa, [
            "nhtsa_DisplacementL", "nhtsa_TrackWidth"
        ])

        scoped_listings = listings.join(nhtsa.select("vin"), on="vin", how="inner")
        scoped_vins = scoped_listings.select("vin").unique()

        keep_nhtsa_columns = [c for c in nhtsa.columns if c not in NHTSA_DROP_COLUMNS]
        scoped_nhtsa = nhtsa.join(scoped_vins, on="vin", how="inner").select(keep_nhtsa_columns)

        listing_history = listing_history.select([c for c in listing_history.columns if c != "id"])
        price_history = price_history.select([c for c in price_history.columns if c != "id"])

        listing_history = self._normalize_date_columns(listing_history, ["history_date"])
        listing_history = self._drop_null_dates(listing_history, ["history_date"])
        listing_history = self._normalize_numeric_to_int_ceil(listing_history, ["price", "mileage"])
        listing_history = self._filter_price_range(listing_history, "price")

        price_history = self._normalize_date_columns(price_history, ["history_date"])
        price_history = self._drop_null_dates(price_history, ["history_date"])
        price_history = self._normalize_numeric_to_int_ceil(price_history, ["price", "mileage"])
        price_history = self._filter_price_range(price_history, "price")

        # Keep all history rows, but restrict them to VINs in the make-filtered scope.
        listing_history = listing_history.join(scoped_vins, on="vin", how="inner")
        price_history = price_history.join(scoped_vins, on="vin", how="inner")

        with sqlite3.connect(str(self.target_db_path)) as target_conn:
            self._create_tables_from_polars(target_conn, {
                "listings": (scoped_listings, ["vin", "loaddate"]),
                "nhtsa_enrichment": (scoped_nhtsa, ["vin"]),
                "listing_history": (listing_history, ["vin", "history_date", "price", "mileage"]),
                "price_history": (price_history, ["vin", "history_date", "price"]),
            })

            inserted_listings = self._insert_dataframe(target_conn, "listings", scoped_listings, conflict_mode="REPLACE")
            inserted_nhtsa = self._insert_dataframe(target_conn, "nhtsa_enrichment", scoped_nhtsa, conflict_mode="REPLACE")
            inserted_listing_hist = self._insert_dataframe(target_conn, "listing_history", listing_history, conflict_mode="IGNORE")
            inserted_price_hist = self._insert_dataframe(target_conn, "price_history", price_history, conflict_mode="IGNORE")
            target_conn.commit()

        logging.info("Finished cleaned database build")
        logging.info("Inserted listings: %d", inserted_listings)
        logging.info("Inserted nhtsa_enrichment: %d", inserted_nhtsa)
        logging.info("Inserted listing_history: %d", inserted_listing_hist)
        logging.info("Inserted price_history: %d", inserted_price_hist)

        print("Cleaned database build complete")
        print(f"Target DB: {self.target_db_path}")
        print(f"listings rows: {inserted_listings}")
        print(f"nhtsa_enrichment rows: {inserted_nhtsa}")
        print(f"listing_history rows: {inserted_listing_hist}")
        print(f"price_history rows: {inserted_price_hist}")


def main() -> None:
    print("Running data cleaning pipeline...")
    repo_root = Path(__file__).resolve().parent.parent
    source_db = repo_root / "CAR_DATA_OUTPUT" / "CAR_DATA.db"
    target_db = repo_root / "CAR_DATA_OUTPUT" / "CAR_DATA_CLEANED.db"

    cleaner = DataCleaningPipeline(source_db_path=source_db, target_db_path=target_db)
    cleaner.run()


if __name__ == "__main__":
    main()

