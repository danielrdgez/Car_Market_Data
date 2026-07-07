import sqlite3
import logging
import threading
from datetime import date, datetime, timedelta, timezone
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
    FETCH_STATUS_PENDING = "pending"
    FETCH_STATUS_COMPLETE = "complete"
    FETCH_STATUS_ZERO_COMMENTS = "zero_comments"
    FETCH_STATUS_COMMENTS_DISABLED = "comments_disabled"
    FETCH_STATUS_QUOTA_EXHAUSTED = "quota_exhausted"
    FETCH_STATUS_API_ERROR = "api_error"
    RETRYABLE_STATUSES = {FETCH_STATUS_PENDING, FETCH_STATUS_QUOTA_EXHAUSTED, FETCH_STATUS_API_ERROR}

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self._init_db()

    def _get_connection(self):
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path, timeout=30)
        return self.conn

    def _ensure_columns(self, table_name: str, required_columns: dict[str, str]) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            existing_columns = {
                row[1]
                for row in cursor.execute(f"PRAGMA table_info({table_name})").fetchall()
            }
            missing_columns = [
                (column_name, column_type)
                for column_name, column_type in required_columns.items()
                if column_name not in existing_columns
            ]
            for column_name, column_type in missing_columns:
                cursor.execute(
                    f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
                )
            if missing_columns:
                conn.commit()

    def _ensure_unique_index(self, table_name: str, column_name: str, index_name: str) -> None:
        with self._get_connection() as conn:
            cursor = conn.cursor()
            existing_indexes = {
                row[1]
                for row in cursor.execute(f"PRAGMA index_list({table_name})").fetchall()
            }
            if index_name in existing_indexes:
                return

            duplicate_row = cursor.execute(
                f'''
                SELECT {column_name}, COUNT(*)
                FROM {table_name}
                GROUP BY {column_name}
                HAVING COUNT(*) > 1 OR {column_name} IS NULL
                LIMIT 1
                '''
            ).fetchone()
            if duplicate_row is not None:
                raise sqlite3.IntegrityError(
                    f"Cannot create unique index {index_name} on {table_name}({column_name}) "
                    "because duplicate or NULL values already exist."
                )

            cursor.execute(
                f"CREATE UNIQUE INDEX IF NOT EXISTS {index_name} ON {table_name}({column_name})"
            )
            conn.commit()

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
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS youtube_playlist_fetch_state
                (
                    playlist_id TEXT PRIMARY KEY,
                    last_discovered_at TEXT,
                    last_status TEXT,
                    last_error TEXT
                )
                '''
            )
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS youtube_video_fetch_state
                (
                    video_id TEXT PRIMARY KEY,
                    playlist_id TEXT,
                    video_title TEXT,
                    discovered_at TEXT,
                    last_attempted_at TEXT,
                    last_succeeded_at TEXT,
                    last_status TEXT,
                    last_error TEXT,
                    comments_seen_count INTEGER DEFAULT 0,
                    next_eligible_at TEXT
                )
                '''
            )
            cursor.execute(
                '''
                CREATE TABLE IF NOT EXISTS youtube_comments_scored
                (
                    video_id TEXT,
                    playlist_id TEXT,
                    video_title TEXT,
                    source TEXT,
                    text TEXT,
                    extracted_at TEXT,
                    comment_id TEXT PRIMARY KEY,
                    author TEXT,
                    like_count REAL,
                    reply_count INTEGER,
                    published_at TEXT,
                    updated_at TEXT,
                    Vehicle_Entity TEXT,
                    original_text TEXT,
                    reliability_sentiment REAL,
                    reliability_mentioned INTEGER,
                    reliability_confidence REAL,
                    value_sentiment REAL,
                    value_mentioned INTEGER,
                    value_confidence REAL,
                    performance_sentiment REAL,
                    performance_mentioned INTEGER,
                    performance_confidence REAL,
                    comfort_sentiment REAL,
                    comfort_mentioned INTEGER,
                    comfort_confidence REAL,
                    consensus_weight REAL,
                    word_count INTEGER,
                    depth_weight REAL,
                    comment_weight REAL,
                    Weighted_Reliability_Score REAL,
                    Weighted_Value_Score REAL,
                    Weighted_Performance_Score REAL,
                    Weighted_Comfort_Score REAL,
                    processed_at TEXT,
                    model_name TEXT,
                    aspect_version TEXT
                )
                '''
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_youtube_video_fetch_state_status ON youtube_video_fetch_state(last_status)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_youtube_video_fetch_state_next_eligible ON youtube_video_fetch_state(next_eligible_at)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_youtube_video_fetch_state_playlist ON youtube_video_fetch_state(playlist_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_youtube_comments_sentiment_video_id ON youtube_comments_sentiment(video_id)"
            )
            conn.commit()
        self._ensure_columns(
            "youtube_comments_sentiment",
            {
                "playlist_id": "TEXT",
            },
        )
        self._ensure_columns(
            "youtube_comments_scored",
            {
                "processed_at": "TEXT",
                "model_name": "TEXT",
                "aspect_version": "TEXT",
            },
        )
        self._ensure_unique_index(
            "youtube_comments_scored",
            "comment_id",
            "idx_youtube_comments_scored_comment_id_unique",
        )

    @staticmethod
    def _utcnow_iso() -> str:
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat()

    @staticmethod
    def _coerce_int(value: object, default: int = 0) -> int:
        try:
            return int(value) if value is not None else default
        except (TypeError, ValueError):
            return default

    @classmethod
    def _next_eligible_timestamp(
        cls,
        status: str,
        refresh_days: int = 30,
        backoff_hours: int = 6,
    ) -> str:
        now = datetime.now(timezone.utc).replace(microsecond=0)
        if status in {cls.FETCH_STATUS_COMPLETE, cls.FETCH_STATUS_ZERO_COMMENTS, cls.FETCH_STATUS_COMMENTS_DISABLED}:
            return (now + timedelta(days=max(refresh_days, 0))).isoformat()
        if status in {cls.FETCH_STATUS_QUOTA_EXHAUSTED, cls.FETCH_STATUS_API_ERROR}:
            return (now + timedelta(hours=max(backoff_hours, 0))).isoformat()
        return now.isoformat()

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

    def ensure_video_fetch_state(
        self,
        video_id: str,
        playlist_id: Optional[str] = None,
        video_title: Optional[str] = None,
        discovered_at: Optional[str] = None,
    ) -> None:
        discovered_at = discovered_at or self._utcnow_iso()
        with self._get_connection() as conn:
            conn.execute(
                '''
                INSERT INTO youtube_video_fetch_state (
                    video_id,
                    playlist_id,
                    video_title,
                    discovered_at,
                    last_status,
                    comments_seen_count,
                    next_eligible_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    playlist_id = COALESCE(excluded.playlist_id, youtube_video_fetch_state.playlist_id),
                    video_title = COALESCE(excluded.video_title, youtube_video_fetch_state.video_title),
                    discovered_at = COALESCE(youtube_video_fetch_state.discovered_at, excluded.discovered_at),
                    next_eligible_at = COALESCE(youtube_video_fetch_state.next_eligible_at, excluded.next_eligible_at)
                ''',
                (
                    video_id,
                    playlist_id,
                    video_title,
                    discovered_at,
                    self.FETCH_STATUS_PENDING,
                    0,
                    discovered_at,
                ),
            )
            conn.commit()

    def upsert_playlist_discovery(
        self,
        playlist_id: str,
        videos: List[dict],
        status: str = FETCH_STATUS_COMPLETE,
        error: Optional[str] = None,
    ) -> None:
        discovered_at = self._utcnow_iso()
        with self._get_connection() as conn:
            conn.execute(
                '''
                INSERT INTO youtube_playlist_fetch_state (
                    playlist_id,
                    last_discovered_at,
                    last_status,
                    last_error
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(playlist_id) DO UPDATE SET
                    last_discovered_at = excluded.last_discovered_at,
                    last_status = excluded.last_status,
                    last_error = excluded.last_error
                ''',
                (playlist_id, discovered_at, status, error),
            )
            for video in videos:
                conn.execute(
                    '''
                    INSERT INTO youtube_video_fetch_state (
                        video_id,
                        playlist_id,
                        video_title,
                        discovered_at,
                        last_status,
                        comments_seen_count,
                        next_eligible_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(video_id) DO UPDATE SET
                        playlist_id = COALESCE(excluded.playlist_id, youtube_video_fetch_state.playlist_id),
                        video_title = COALESCE(excluded.video_title, youtube_video_fetch_state.video_title),
                        discovered_at = COALESCE(youtube_video_fetch_state.discovered_at, excluded.discovered_at)
                    ''',
                    (
                        video.get("video_id"),
                        playlist_id,
                        video.get("title"),
                        discovered_at,
                        self.FETCH_STATUS_PENDING,
                        0,
                        discovered_at,
                    ),
                )
            conn.commit()

    def mark_playlist_discovery_error(self, playlist_id: str, status: str, error: str) -> None:
        with self._get_connection() as conn:
            conn.execute(
                '''
                INSERT INTO youtube_playlist_fetch_state (
                    playlist_id,
                    last_discovered_at,
                    last_status,
                    last_error
                )
                VALUES (?, ?, ?, ?)
                ON CONFLICT(playlist_id) DO UPDATE SET
                    last_discovered_at = excluded.last_discovered_at,
                    last_status = excluded.last_status,
                    last_error = excluded.last_error
                ''',
                (playlist_id, self._utcnow_iso(), status, error),
            )
            conn.commit()

    def update_video_fetch_outcome(
        self,
        video_id: str,
        status: str,
        comments_seen_count: Optional[int] = None,
        error: Optional[str] = None,
        refresh_days: int = 30,
        backoff_hours: int = 6,
        playlist_id: Optional[str] = None,
        video_title: Optional[str] = None,
    ) -> None:
        attempted_at = self._utcnow_iso()
        succeeded_at = attempted_at if status in {
            self.FETCH_STATUS_COMPLETE,
            self.FETCH_STATUS_ZERO_COMMENTS,
            self.FETCH_STATUS_COMMENTS_DISABLED,
        } else None
        next_eligible_at = self._next_eligible_timestamp(
            status=status,
            refresh_days=refresh_days,
            backoff_hours=backoff_hours,
        )
        with self._get_connection() as conn:
            conn.execute(
                '''
                INSERT INTO youtube_video_fetch_state (
                    video_id,
                    playlist_id,
                    video_title,
                    discovered_at,
                    last_attempted_at,
                    last_succeeded_at,
                    last_status,
                    last_error,
                    comments_seen_count,
                    next_eligible_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(video_id) DO UPDATE SET
                    playlist_id = COALESCE(excluded.playlist_id, youtube_video_fetch_state.playlist_id),
                    video_title = COALESCE(excluded.video_title, youtube_video_fetch_state.video_title),
                    last_attempted_at = excluded.last_attempted_at,
                    last_succeeded_at = COALESCE(excluded.last_succeeded_at, youtube_video_fetch_state.last_succeeded_at),
                    last_status = excluded.last_status,
                    last_error = excluded.last_error,
                    comments_seen_count = COALESCE(excluded.comments_seen_count, youtube_video_fetch_state.comments_seen_count),
                    next_eligible_at = excluded.next_eligible_at
                ''',
                (
                    video_id,
                    playlist_id,
                    video_title,
                    attempted_at,
                    attempted_at,
                    succeeded_at,
                    status,
                    error,
                    comments_seen_count,
                    next_eligible_at,
                ),
            )
            conn.commit()

    def get_video_fetch_state(self, video_id: str) -> Optional[dict]:
        with self._get_connection() as conn:
            row = conn.execute(
                '''
                SELECT video_id, playlist_id, video_title, discovered_at, last_attempted_at,
                       last_succeeded_at, last_status, last_error, comments_seen_count, next_eligible_at
                FROM youtube_video_fetch_state
                WHERE video_id = ?
                ''',
                (video_id,),
            ).fetchone()
        if row is None:
            return None
        columns = [
            "video_id",
            "playlist_id",
            "video_title",
            "discovered_at",
            "last_attempted_at",
            "last_succeeded_at",
            "last_status",
            "last_error",
            "comments_seen_count",
            "next_eligible_at",
        ]
        return dict(zip(columns, row))

    def get_candidate_videos(
        self,
        refresh_days: int = 30,
        force_recheck: bool = False,
        limit: Optional[int] = None,
        playlist_ids: Optional[List[str]] = None,
        video_ids: Optional[List[str]] = None,
        now_iso: Optional[str] = None,
    ) -> List[dict]:
        now_iso = now_iso or self._utcnow_iso()
        filters = []
        params: List[object] = [
            self.FETCH_STATUS_PENDING,
            now_iso,
            self.FETCH_STATUS_PENDING,
            now_iso,
            now_iso,
            1 if force_recheck else 0,
        ]

        if playlist_ids:
            placeholders = ", ".join("?" for _ in playlist_ids)
            filters.append(f"playlist_id IN ({placeholders})")
            params.extend(playlist_ids)
        if video_ids:
            placeholders = ", ".join("?" for _ in video_ids)
            filters.append(f"video_id IN ({placeholders})")
            params.extend(video_ids)

        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        query = f'''
            SELECT
                video_id,
                playlist_id,
                video_title,
                discovered_at,
                last_attempted_at,
                last_succeeded_at,
                last_status,
                last_error,
                comments_seen_count,
                next_eligible_at,
                CASE
                    WHEN EXISTS (
                        SELECT 1
                        FROM youtube_comments_sentiment AS comments
                        WHERE comments.video_id = youtube_video_fetch_state.video_id
                        LIMIT 1
                    ) THEN 1
                    ELSE 0
                END AS has_existing_comments,
                CASE
                    WHEN (last_status IS NULL OR last_status = ?)
                         AND NOT EXISTS (
                             SELECT 1
                             FROM youtube_comments_sentiment AS comments
                             WHERE comments.video_id = youtube_video_fetch_state.video_id
                             LIMIT 1
                         ) THEN 1
                    WHEN last_status IN ('quota_exhausted', 'api_error')
                         AND (next_eligible_at IS NULL OR next_eligible_at <= ?)
                         AND NOT EXISTS (
                             SELECT 1
                             FROM youtube_comments_sentiment AS comments
                             WHERE comments.video_id = youtube_video_fetch_state.video_id
                             LIMIT 1
                         ) THEN 2
                    WHEN last_status IS NULL OR last_status = ? THEN 3
                    WHEN last_status IN ('quota_exhausted', 'api_error')
                         AND (next_eligible_at IS NULL OR next_eligible_at <= ?) THEN 3
                    WHEN last_status IN ('complete', 'zero_comments', 'comments_disabled')
                         AND (next_eligible_at IS NULL OR next_eligible_at <= ?) THEN 5
                    WHEN ? = 1 THEN 6
                    ELSE 99
                END AS priority_bucket
            FROM youtube_video_fetch_state
            {where_sql}
            ORDER BY priority_bucket ASC,
                     has_existing_comments ASC,
                     COALESCE(next_eligible_at, discovered_at, '') ASC,
                     COALESCE(last_attempted_at, discovered_at, '') ASC,
                     video_id ASC
        '''

        with self._get_connection() as conn:
            rows = conn.execute(query, params).fetchall()

        columns = [
            "video_id",
            "playlist_id",
            "video_title",
            "discovered_at",
            "last_attempted_at",
            "last_succeeded_at",
            "last_status",
            "last_error",
            "comments_seen_count",
            "next_eligible_at",
            "has_existing_comments",
            "priority_bucket",
        ]
        candidates = [dict(zip(columns, row)) for row in rows if row[-1] < 99]
        if not force_recheck:
            candidates = [row for row in candidates if row["priority_bucket"] < 6]
        if limit is not None:
            return candidates[:limit]
        return candidates

    def load_comments_for_absa(self, force_reprocess: bool = False, limit: Optional[int] = None) -> pd.DataFrame:
        query = '''
            SELECT raw.*
            FROM youtube_comments_sentiment AS raw
        '''
        if not force_reprocess:
            query += '''
                LEFT JOIN youtube_comments_scored AS scored
                    ON raw.comment_id = scored.comment_id
                WHERE scored.comment_id IS NULL
            '''
        query += ' ORDER BY raw.published_at ASC, raw.comment_id ASC'
        if limit is not None:
            query += f' LIMIT {int(limit)}'
        with self._get_connection() as conn:
            return pd.read_sql_query(query, conn)

    def upsert_scored_comments(self, df: pd.DataFrame) -> int:
        if df.empty:
            return 0
        rows = df.drop_duplicates(subset=["comment_id"]).to_dict(orient="records")
        if not rows:
            return 0
        columns = list(rows[0].keys())
        placeholders = ", ".join("?" for _ in columns)
        update_assignments = ", ".join(
            f"{column}=excluded.{column}" for column in columns if column != "comment_id"
        )
        values = [tuple(row.get(column) for column in columns) for row in rows]
        with self._get_connection() as conn:
            conn.executemany(
                f'''
                INSERT INTO youtube_comments_scored ({", ".join(columns)})
                VALUES ({placeholders})
                ON CONFLICT(comment_id) DO UPDATE SET
                    {update_assignments}
                ''',
                values,
            )
            conn.commit()
        return len(rows)

    def summarize_playlist_completion(self, playlist_id: str) -> dict:
        with self._get_connection() as conn:
            row = conn.execute(
                '''
                SELECT
                    COUNT(*) AS total_videos,
                    SUM(CASE WHEN last_status IS NULL OR last_status = 'pending' THEN 1 ELSE 0 END) AS pending_videos,
                    SUM(CASE WHEN last_status IN ('complete', 'zero_comments', 'comments_disabled') THEN 1 ELSE 0 END) AS completed_videos,
                    SUM(CASE WHEN last_status IN ('quota_exhausted', 'api_error') THEN 1 ELSE 0 END) AS retryable_videos
                FROM youtube_video_fetch_state
                WHERE playlist_id = ?
                ''',
                (playlist_id,),
            ).fetchone()
        total, pending, completed, retryable = row or (0, 0, 0, 0)
        return {
            "playlist_id": playlist_id,
            "total_videos": self._coerce_int(total),
            "pending_videos": self._coerce_int(pending),
            "completed_videos": self._coerce_int(completed),
            "retryable_videos": self._coerce_int(retryable),
        }

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
