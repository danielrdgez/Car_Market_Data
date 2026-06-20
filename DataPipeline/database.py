import sqlite3
import logging
import threading
from datetime import date
from typing import Optional, List, Set

import pandas as pd  # Import pandas for to_sql method


class CarDatabase:
    def __init__(self, db_path, thread_safe=False):
        self.db_path = db_path
        self.conn = None
        self._thread_safe = thread_safe
        self._lock = threading.Lock() if thread_safe else None
        self._local = threading.local() if thread_safe else None
        self._init_db()

    def _get_connection(self):
        if self._thread_safe:
            conn = getattr(self._local, 'conn', None)
            if conn is None:
                conn = sqlite3.connect(self.db_path, timeout=30)
                self._local.conn = conn
            return conn
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path, timeout=30)
        return self.conn

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # 1. Main Listings table (Snapshots over time)
            # Using (vin, loaddate) as composite primary key as requested.
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS listings
                           (
                               vin
                               TEXT,
                               loaddate
                               DATE,
                               year
                               INTEGER,
                               title
                               TEXT,
                               details
                               TEXT,
                               price
                               INTEGER,
                               mileage
                               INTEGER,
                               date
                               DATE,
                               location
                               TEXT,
                               locationCode
                               TEXT,
                               countryCode
                               TEXT,
                               pendingSale
                               BOOLEAN,
                               currentBid
                               REAL,
                               bids
                               INTEGER,
                               distance
                               REAL,
                               priceRecentChange
                               BOOLEAN,
                               sellerType
                               TEXT,
                               vehicleTitle
                               TEXT,
                               listingType
                               TEXT,
                               vehicleTitleDesc
                               TEXT,
                               sourceName
                               TEXT,
                               img
                               TEXT,
                               PRIMARY
                               KEY
                           (
                               vin,
                               loaddate
                           )
                               )
                           ''')

            # 2. Price History table (Normalized)
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS price_history
                           (
                               id
                               INTEGER
                               PRIMARY
                               KEY
                               AUTOINCREMENT,
                               vin
                               TEXT,
                               history_date
                               DATE,
                               mileage
                               INTEGER,
                               price
                               INTEGER,
                               trend
                               TEXT,
                               UNIQUE
                           (
                               vin,
                               history_date,
                               price
                           ),
                               FOREIGN KEY
                           (
                               vin
                           ) REFERENCES listings
                           (
                               vin
                           )
                               )
                           ''')

            # 3. Listing History table (Normalized)
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS listing_history
                           (
                               id
                               INTEGER
                               PRIMARY
                               KEY
                               AUTOINCREMENT,
                               vin
                               TEXT,
                               history_date
                               DATE,
                               mileage
                               REAL,
                               price
                               INTEGER,
                               UNIQUE
                           (
                               vin,
                               history_date,
                               price,
                               mileage
                           ),
                               FOREIGN KEY
                           (
                               vin
                           ) REFERENCES listings
                           (
                               vin
                           )
                               )
                           ''')

            # 4. NHTSA Enrichment table
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS nhtsa_enrichment (
                                                                           vin TEXT PRIMARY KEY,
                                                                           nhtsa_ABS TEXT,
                                                                           nhtsa_ActiveSafetySysNote TEXT,
                                                                           nhtsa_AdaptiveCruiseControl TEXT,
                                                                           nhtsa_AdaptiveDrivingBeam TEXT,
                                                                           nhtsa_AdaptiveHeadlights TEXT,
                                                                           nhtsa_AdditionalErrorText TEXT,
                                                                           nhtsa_AirBagLocCurtain TEXT,
                                                                           nhtsa_AirBagLocFront TEXT,
                                                                           nhtsa_AirBagLocKnee TEXT,
                                                                           nhtsa_AirBagLocSeatCushion TEXT,
                                                                           nhtsa_AirBagLocSide TEXT,
                                                                           nhtsa_AutoReverseSystem TEXT,
                                                                           nhtsa_AutomaticPedestrianAlertingSound TEXT,
                                                                           nhtsa_AxleConfiguration TEXT,
                                                                           nhtsa_Axles TEXT,
                                                                           nhtsa_BasePrice TEXT,
                                                                           nhtsa_BedLengthIN TEXT,
                                                                           nhtsa_BedType TEXT,
                                                                           nhtsa_BlindSpotIntervention TEXT,
                                                                           nhtsa_BlindSpotMon TEXT,
                                                                           nhtsa_BodyCabType TEXT,
                                                                           nhtsa_BodyClass TEXT,
                                                                           nhtsa_BrakeSystemDesc TEXT,
                                                                           nhtsa_BrakeSystemType TEXT,
                                                                           nhtsa_ChargerLevel TEXT,
                                                                           nhtsa_ChargerPowerKW TEXT,
                                                                           nhtsa_CombinedBrakingSystem TEXT,
                                                                           nhtsa_CoolingType TEXT,
                                                                           nhtsa_CurbWeightLB TEXT,
                                                                           nhtsa_DaytimeRunningLight TEXT,
                                                                           nhtsa_DestinationMarket TEXT,
                                                                           nhtsa_DisplacementCC TEXT,
                                                                           nhtsa_DisplacementCI TEXT,
                                                                           nhtsa_DisplacementL TEXT,
                                                                           nhtsa_Doors TEXT,
                                                                           nhtsa_DriveType TEXT,
                                                                           nhtsa_DriverAssist TEXT,
                                                                           nhtsa_DynamicBrakeSupport TEXT,
                                                                           nhtsa_EDR TEXT,
                                                                           nhtsa_ESC TEXT,
                                                                           nhtsa_EVDriveUnit TEXT,
                                                                           nhtsa_ElectrificationLevel TEXT,
                                                                           nhtsa_EngineConfiguration TEXT,
                                                                           nhtsa_EngineCycles TEXT,
                                                                           nhtsa_EngineCylinders TEXT,
                                                                           nhtsa_EngineHP TEXT,
                                                                           nhtsa_EngineHP_to TEXT,
                                                                           nhtsa_EngineKW TEXT,
                                                                           nhtsa_EngineManufacturer TEXT,
                                                                           nhtsa_EngineModel TEXT,
                                                                           nhtsa_EntertainmentSystem TEXT,
                                                                           nhtsa_ForwardCollisionWarning TEXT,
                                                                           nhtsa_FuelInjectionType TEXT,
                                                                           nhtsa_FuelTankMaterial TEXT,
                                                                           nhtsa_FuelTankType TEXT,
                                                                           nhtsa_FuelTypePrimary TEXT,
                                                                           nhtsa_FuelTypeSecondary TEXT,
                                                                           nhtsa_KeylessIgnition TEXT,
                                                                           nhtsa_LaneCenteringAssistance TEXT,
                                                                           nhtsa_LaneDepartureWarning TEXT,
                                                                           nhtsa_LaneKeepSystem TEXT,
                                                                           nhtsa_LowerBeamHeadlampLightSource TEXT,
                                                                           nhtsa_Make TEXT,
                                                                           nhtsa_MakeID TEXT,
                                                                           nhtsa_Manufacturer TEXT,
                                                                           nhtsa_ManufacturerId TEXT,
                                                                           nhtsa_Model TEXT,
                                                                           nhtsa_ModelID TEXT,
                                                                           nhtsa_ModelYear TEXT,
                                                                           nhtsa_OtherEngineInfo TEXT,
                                                                           nhtsa_ParkAssist TEXT,
                                                                           nhtsa_PedestrianAutomaticEmergencyBraking TEXT,
                                                                           nhtsa_RearAutomaticEmergencyBraking TEXT,
                                                                           nhtsa_RearCrossTrafficAlert TEXT,
                                                                           nhtsa_RearVisibilitySystem TEXT,
                                                                           nhtsa_SAEAutomationLevel TEXT,
                                                                           nhtsa_SAEAutomationLevel_to TEXT,
                                                                           nhtsa_SeatRows TEXT,
                                                                           nhtsa_Seats TEXT,
                                                                           nhtsa_SemiautomaticHeadlampBeamSwitching TEXT,
                                                                           nhtsa_TPMS TEXT,
                                                                           nhtsa_TopSpeedMPH TEXT,
                                                                           nhtsa_TrackWidth TEXT,
                                                                           nhtsa_TractionControl TEXT,
                                                                           nhtsa_TransmissionSpeeds TEXT,
                                                                           nhtsa_TransmissionStyle TEXT,
                                                                           nhtsa_Trim TEXT,
                                                                           nhtsa_Trim2 TEXT,
                                                                           nhtsa_WheelSizeFront TEXT,
                                                                           nhtsa_WheelSizeRear TEXT,
                                                                           nhtsa_Windows TEXT,
                                                                           nhtsa_VehicleType TEXT,
                                                                           nhtsa_WheelBaseLong TEXT,
                                                                           nhtsa_WheelBaseShort TEXT,
                                                                           nhtsa_WheelBaseType TEXT,
                                                                           nhtsa_safety_ratings_count INTEGER,
                                                                           nhtsa_overall_rating TEXT,
                                                                           nhtsa_front_crash_rating TEXT,
                                                                           nhtsa_rollover_rating TEXT,
                                                                           nhtsa_side_crash_rating TEXT,
                                                                           nhtsa_total_recalls INTEGER,
                                                                           nhtsa_recall_components TEXT,
                                                                           nhtsa_latest_recall_date DATE,
                                                                           nhtsa_total_complaints INTEGER,
                                                                           nhtsa_complaint_injuries INTEGER,
                                                                           nhtsa_complaint_deaths INTEGER,
                                                                           nhtsa_complaint_crash_related INTEGER,
                                                                           nhtsa_complaint_fire_related INTEGER,
                                                                           nhtsa_common_complaint_areas TEXT,
                                                                           FOREIGN KEY (vin) REFERENCES listings (vin)
                               )
                           ''')

            conn.commit()

    def insert_rows(self, rows, vin_cache=None):
        if not rows:
            return 0

        lock = self._lock if self._thread_safe else None
        if lock:
            lock.acquire()
        try:
            return self._insert_rows_impl(rows, vin_cache=vin_cache)
        finally:
            if lock:
                lock.release()

    def _insert_rows_impl(self, rows, vin_cache=None):
        inserted_count = 0
        today = date.today().isoformat()

        with self._get_connection() as conn:
            cursor = conn.cursor()

            for row in rows:
                vin = row.get('vin')
                loaddate = row.get('loaddate')
                if not vin or not loaddate:
                    continue

                # Always process normalized history tables first. UNIQUE constraints
                # + INSERT OR IGNORE prevent duplicates automatically.
                self._insert_price_history(cursor, vin, row.get('priceHistory'))
                self._insert_listing_history(cursor, vin, row.get('listingHistory'))

                # Skip listing snapshot if we have the VIN cached and its price/mileage hasn't changed.
                if vin_cache is not None and hasattr(vin_cache, "should_insert"):
                    if not vin_cache.should_insert(vin, row.get('price'), row.get('mileage')):
                        continue
                elif vin_cache is not None and hasattr(vin_cache, "contains") and vin_cache.contains(vin, today):
                    continue

                try:
                    cursor.execute('''
                        INSERT OR REPLACE INTO listings (
                            vin, loaddate, year, title, details, price, mileage, date,
                            location, locationCode, countryCode, pendingSale,
                            currentBid, bids, distance, priceRecentChange,
                            sellerType, vehicleTitle, listingType, vehicleTitleDesc,
                            sourceName, img
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        vin, loaddate, row.get('year'), row.get('title'), row.get('details'),
                        row.get('price'), row.get('mileage'), row.get('date'),
                        row.get('location'), row.get('locationCode'), row.get('countryCode'),
                        row.get('pendingSale'), row.get('currentBid'), row.get('bids'),
                        row.get('distance'), row.get('priceRecentChange'),
                        row.get('sellerType'), row.get('vehicleTitle'), row.get('listingType'),
                        row.get('vehicleTitleDesc'), row.get('sourceName'), row.get('img')
                    ))
                    if cursor.rowcount > 0:
                        inserted_count += 1
                except sqlite3.Error as e:
                    logging.error(f"Failed to insert listing for VIN {vin}, loaddate {loaddate}: {e}")

            conn.commit()

        return inserted_count

    def _insert_price_history(self, cursor, vin: str, history: Optional[List]):
        if not history or not isinstance(history, list):
            return
        for h in history:
            try:
                cursor.execute('''
                               INSERT
                               OR IGNORE INTO price_history (vin, history_date, mileage, price, trend)
                    VALUES (?, ?, ?, ?, ?)
                               ''', (vin, h.get('date'), h.get('mileage'), h.get('price'), h.get('trend')))
            except sqlite3.Error as e:
                logging.warning(f"Failed to insert priceHistory for VIN {vin}: {e}")

    def _insert_listing_history(self, cursor, vin: str, history: Optional[List]):
        if not history or not isinstance(history, list):
            return
        for h in history:
            try:
                cursor.execute('''
                               INSERT
                               OR IGNORE INTO listing_history (vin, history_date, mileage, price)
                    VALUES (?, ?, ?, ?)
                               ''', (vin, h.get('date'), h.get('mileage'), h.get('price')))
            except sqlite3.Error as e:
                logging.warning(f"Failed to insert listingHistory for VIN {vin}: {e}")

    def get_seen_vins(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            # Get the latest price and mileage for each VIN
            cursor.execute('SELECT vin, price, mileage, MAX(loaddate) FROM listings GROUP BY vin')
            return {row[0]: {'price': row[1], 'mileage': row[2]} for row in cursor.fetchall()}

    def get_vins_for_enrichment(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                           SELECT DISTINCT l.vin
                           FROM listings l
                                    LEFT JOIN nhtsa_enrichment n ON l.vin = n.vin
                           WHERE n.vin IS NULL
                           ''')
            return [row[0] for row in cursor.fetchall()]

    def insert_nhtsa_enrichment(self, vin, enrichment_data):
        """Insert a single record into nhtsa_enrichment"""
        self.insert_nhtsa_enrichment_batch({vin: enrichment_data})

    def insert_nhtsa_enrichment_batch(self, enrichment_dict):
        """Insert multiple records into nhtsa_enrichment in a single transaction"""
        if not enrichment_dict:
            return

        with self._get_connection() as conn:
            cursor = conn.cursor()

            for vin, enrichment_data in enrichment_dict.items():
                if not enrichment_data:
                    continue

                data = enrichment_data.copy()
                data['vin'] = vin

                columns = ', '.join(data.keys())
                placeholders = ', '.join(['?' for _ in data])
                values = list(data.values())

                try:
                    cursor.execute(f'''
                        INSERT OR REPLACE INTO nhtsa_enrichment ({columns})
                        VALUES ({placeholders})
                    ''', values)
                except sqlite3.Error as e:
                    logging.error(f"Failed to insert NHTSA enrichment for VIN {vin}: {e}")

            conn.commit()

    def close(self):
        """Close the database connection(s)"""
        if self._thread_safe and self._local:
            conn = getattr(self._local, 'conn', None)
            if conn:
                try:
                    conn.close()
                except Exception as e:
                    logging.error(f"Error closing thread-local database connection: {e}")
                self._local.conn = None
        if hasattr(self, 'conn') and self.conn:
            try:
                self.conn.close()
                logging.info("Database connection closed")
            except Exception as e:
                logging.error(f"Error closing database connection: {e}")
            self.conn = None


class YouTubeCommentsDatabase:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self._init_db()

    def _get_connection(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path, timeout=30)
        return self.conn

    def _init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                           CREATE TABLE IF NOT EXISTS youtube_comments_sentiment
                           (
                               video_id
                               TEXT,
                               playlist_id
                               TEXT, -- Added playlist_id column
                               video_title
                               TEXT,
                               source
                               TEXT,
                               text
                               TEXT,
                               extracted_at
                               TEXT,
                               comment_id
                               TEXT
                               PRIMARY
                               KEY,
                               author
                               TEXT,
                               like_count
                               INTEGER,
                               reply_count
                               INTEGER,
                               published_at
                               TEXT,
                               updated_at
                               TEXT
                           )
                           ''')
            conn.commit()

    def insert_sentiment_data(self, df: pd.DataFrame, table_name: str = 'youtube_comments_sentiment'):
        """
        Inserts a pandas DataFrame into the specified SQL table,
        only adding new comments and avoiding duplicates based on comment_id.
        """
        if df.empty:
            logging.info(f"No data to insert into {table_name}.")
            return 0

        with self._get_connection() as conn:
            # Format date columns to MM-DD-YYYY for TEXT type
            for col in ['extracted_at', 'published_at', 'updated_at']:
                if col in df.columns and not df[col].empty:
                    df[col] = pd.to_datetime(df[col], errors='coerce', utc=True).dt.strftime('%m-%d-%Y')

            try:
                # Fetch existing comment_ids
                existing_comment_ids = pd.read_sql(f"SELECT comment_id FROM {table_name}", conn)['comment_id'].tolist()

                # Filter out comments that already exist in the database
                new_comments_df = df[~df['comment_id'].isin(existing_comment_ids)]

                # DROP DUPLICATES IN INCOMING BATCH
                new_comments_df = new_comments_df.drop_duplicates(subset=['comment_id'])

                if new_comments_df.empty:
                    logging.info(f"No new comments to insert into {table_name}.")
                    return 0

                # Insert only new comments
                new_comments_df.to_sql(table_name, conn, if_exists='append', index=False)
                logging.info(f"Successfully inserted {len(new_comments_df)} new rows into {table_name}.")
                return len(new_comments_df)
            except pd.io.sql.DatabaseError as e:
                # This error can occur if the table does not exist yet,
                # which is handled by _init_db, but might happen if table was dropped externally.
                # In this case, just append all data.
                logging.warning(f"Table {table_name} might not exist or other DB error. Attempting full insert: {e}")

                # CLEAN FULL DATAFRAME BEFORE FALLBACK INSERT
                clean_df = df.drop_duplicates(subset=['comment_id'])
                clean_df.to_sql(table_name, conn, if_exists='append', index=False)

                logging.info(f"Successfully inserted {len(clean_df)} rows into {table_name} (full insert).")
                return len(clean_df)
            except Exception as e:
                logging.error(f"Failed to insert sentiment data into {table_name}: {e}")
                return 0

    def get_processed_video_ids(self, table_name: str = 'youtube_comments_sentiment') -> Set[str]:
        """Retrieves a set of all video_ids already present in the sentiment table."""
        with self._get_connection() as conn:
            try:
                return set(pd.read_sql(f"SELECT DISTINCT video_id FROM {table_name} WHERE video_id IS NOT NULL", conn)[
                               'video_id'].tolist())
            except pd.io.sql.DatabaseError:
                # Table might not exist yet, return empty set
                return set()

    def get_processed_playlist_ids(self, table_name: str = 'youtube_comments_sentiment') -> Set[str]:
        """Retrieves a set of all playlist_ids already present in the sentiment table."""
        with self._get_connection() as conn:
            try:
                return set(
                    pd.read_sql(f"SELECT DISTINCT playlist_id FROM {table_name} WHERE playlist_id IS NOT NULL", conn)[
                        'playlist_id'].tolist())
            except pd.io.sql.DatabaseError:
                # Table might not exist yet, return empty set
                return set()

    def close(self):
        """Close the database connection"""
        if self.conn:
            try:
                self.conn.close()
                logging.info("YouTube Comments Database connection closed")
            except Exception as e:
                logging.error(f"Error closing YouTube Comments Database connection: {e}")
            self.conn = None