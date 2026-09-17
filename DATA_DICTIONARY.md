# Project Data Dictionary

Verified: 2026-09-16. Dictionary version: 1.0. Maintainer: project repository maintainer.

This dictionary describes the project-relevant physical columns in the three requested SQLite databases under `CAR_DATA_OUTPUT/`: `CAR_DATA.db`, `CAR_YOUTUBE_COMMENTS.db`, and `CAR_DATA_NHTSA.db`. It supports the capstone's listing-price, depreciation, safety and consumer-sentiment research. It does not describe `CAR_DATA_CLEANED.db`, EPA reference tables in that database, model output files, or empty legacy table shells that are not active data sources.

Coverage: **28 project-relevant tables and 539 project-relevant columns**, plus `sqlite_sequence` in each database (3 internal tables and 6 internal columns). Table/column counts are schema counts for documented tables, not data row counts. Repeated table names in different databases are documented separately when their populated data, types, constraints and purpose differ. No SQL views were included in the table inventory.

Physical metadata was read directly using SQLite read-only URI connections, `sqlite_master`, `PRAGMA table_info`, `PRAGMA index_list`, `PRAGMA index_info`, and `PRAGMA foreign_key_list`. Definitions were checked against the current working-tree acquisition, persistence, enrichment and ABSA code. NHTSA schema metadata reports version `2`. The working tree already contained edits to `DataPipeline/database.py`, `DataPipeline/NHTSA_enrichment.py` and `tests/test_nhtsa_enrichment.py`; this document reflects inspected behavior without assuming those changes have been run against historical records.

Only schema metadata, the schema-version row, single-row existence checks for bulk metadata/rows, targeted row-count checks for suspected empty legacy tables, and a bounded sample of up to 10,000 field names from each of three NHTSA field/value tables were read. No full data profiling, training, ingestion, migrations or database writes were performed. Definitions/expected domains are not proof that every stored value passes validation. No identifying example records are reproduced.

## Navigation

- [Conventions and missing values](#conventions-and-missing-values)
- [Database and table inventory](#database-and-table-inventory)
- [Relationships and join rules](#relationships-and-join-rules)
- [CAR_DATA.db](#car-datadb)
- [CAR_YOUTUBE_COMMENTS.db](#car-youtube-commentsdb)
- [CAR_DATA_NHTSA.db](#car-data-nhtsadb)
- [Field vocabularies and calculations](#field-vocabularies-and-calculations)
- [Known limitations and review items](#known-limitations-and-review-items)
- [Maintenance and verification](#maintenance-and-verification)
- [Sources](#sources)

## Conventions and missing values

The structure follows [USGS data dictionary guidance](https://www.usgs.gov/data-management/data-dictionaries): identify data objects, properties, relationships, reference values, missing states and validation rules. Exact SQL identifiers are preserved, including mixed case and historical names.

| Notation | Meaning |
| --- | --- |
| Declared type | Literal SQLite column declaration, not a guarantee of the storage class of every value. DATE/BOOLEAN are not strict native date/Boolean types. |
| PK1, PK2, ... | Column's position in the declared primary key, not an independent key. |
| NN | Explicit NOT NULL declaration from PRAGMA. Absence means no explicit NOT NULL, not that missing values are analytically acceptable. |
| default=... | SQL default expression if declared. No label means no explicit default. |
| rowid key | INTEGER PRIMARY KEY behavior; NULL input obtains a generated identifier. |
| UQ / index | Actual uniqueness/index definitions are listed after each column table. UNIQUE allows multiple NULLs in SQLite. |
| Definition status | Physical names/types/constraints are schema-verified. Meanings are code/source-backed unless explicitly labeled legacy, inferred or unresolved. Expected domains are guidance, not unrecorded CHECK constraints. |

These databases use ordinary SQLite tables. In particular, a text/composite PRIMARY KEY can lack explicit NOT NULL; do not infer non-nullability solely from PK notation. Declared foreign keys require connection-level enforcement and a valid parent key. See [SQLite foreign key documentation](https://www.sqlite.org/foreignkeys.html).

- SQL NULL usually means absent, unavailable, not applicable or uncomputed; the source status determines which. Empty string and literal `N/A`, `NA`, `NONE`, `NULL`, `UNKNOWN` may also occur in source fields. Enrichment's usability helper treats these tokens case-insensitively as unusable; raw persistence does not universally convert them to NULL.
- A true zero is distinct from missing. Price 0/1 may be a placeholder. Zero mileage may be legitimate. Zero complaints with a successful empty response differs from zero records on a failed query.
- Listing price, bids and mileage lack dedicated currency/unit columns. The scraper parses numbers but does not convert currencies or distance units. Do not assert all amounts are USD or all odometers are miles without source validation.
- NHTSA fields use official source units where mapped below; numeric-looking TEXT must be parsed explicitly. Raw safety-feature categories must not be reduced to Boolean simply by testing whether a string is nonempty.
- NHTSA/fetch/ABSA processing timestamps use UTC ISO-8601. Listing `loaddate` is the scraping machine's local date. YouTube raw comment date fields are currently formatted `MM-DD-YYYY` after UTC conversion, discarding time-of-day; historical formats may differ. Source recall/complaint/history dates are generally passed through.
- Physical NHTSA normalized tables preserve source property spelling, while the compatibility projection adds `nhtsa_`. For example `nhtsa_vpic_values.Make` maps to `nhtsa_enrichment.nhtsa_Make`; normalized `nhtsa_vin_identity_resolution.nhtsa_make` is a separate field with normalization/fallback context.

## Database and table inventory

| Database | Role | Project tables | Project columns | Producers |
| --- | --- | ---: | ---: | --- |
| CAR_DATA.db | Raw market snapshots/history and latest NHTSA projection | 4 | 155 | Playwright scraper, CarDatabase, NHTSADataEnricher |
| CAR_YOUTUBE_COMMENTS.db | Raw comments, scores, make aggregates, fetch progress and populated legacy sentiment output | 7 | 100 | SentimentAnalysis, YouTubeCommentsDatabase, absa_pipeline |
| CAR_DATA_NHTSA.db | Normalized NHTSA response history and source metadata | 17 | 284 | NHTSADataEnricher, NHTSADatabase |

Refresh is command/scheduler driven, not guaranteed continuous. The scheduled core pipeline scrapes, enriches, refreshes EPA reference data, then cleans. NHTSA uses a 30-day freshness window by default; YouTube fetch state also defaults to 30-day completed-video refresh. ABSA processes unscored IDs unless explicitly reprocessed; aggregate tables are rebuilt from stored scores.

| Database | Table | Columns | Declared primary key |
| --- | --- | ---: | --- |
| CAR_DATA.db | [listing_history](#car_data-listing_history) | 5 | id |
| CAR_DATA.db | [listings](#car_data-listings) | 22 | vin, loaddate |
| CAR_DATA.db | [nhtsa_enrichment](#car_data-nhtsa_enrichment) | 122 | vin |
| CAR_DATA.db | [price_history](#car_data-price_history) | 6 | id |
| CAR_DATA.db | [sqlite_sequence](#car_data-sqlite_sequence) | 2 | None |
| CAR_YOUTUBE_COMMENTS.db | [make_sentiment_index](#car_youtube_comments-make_sentiment_index) | 12 | sentiment_make |
| CAR_YOUTUBE_COMMENTS.db | [make_sentiment_monthly](#car_youtube_comments-make_sentiment_monthly) | 11 | sentiment_make, sentiment_month |
| CAR_YOUTUBE_COMMENTS.db | [sqlite_sequence](#car_youtube_comments-sqlite_sequence) | 2 | None |
| CAR_YOUTUBE_COMMENTS.db | [vehicle_sentiment_index](#car_youtube_comments-vehicle_sentiment_index) | 7 | None |
| CAR_YOUTUBE_COMMENTS.db | [youtube_comments_scored](#car_youtube_comments-youtube_comments_scored) | 44 | None |
| CAR_YOUTUBE_COMMENTS.db | [youtube_comments_sentiment](#car_youtube_comments-youtube_comments_sentiment) | 12 | comment_id |
| CAR_YOUTUBE_COMMENTS.db | [youtube_playlist_fetch_state](#car_youtube_comments-youtube_playlist_fetch_state) | 4 | playlist_id |
| CAR_YOUTUBE_COMMENTS.db | [youtube_video_fetch_state](#car_youtube_comments-youtube_video_fetch_state) | 10 | video_id |
| CAR_DATA_NHTSA.db | [nhtsa_api_extra_fields](#car_data_nhtsa-nhtsa_api_extra_fields) | 5 | query_id, record_type, record_key, field_name |
| CAR_DATA_NHTSA.db | [nhtsa_bulk_datasets](#car_data_nhtsa-nhtsa_bulk_datasets) | 8 | dataset_id |
| CAR_DATA_NHTSA.db | [nhtsa_bulk_fields](#car_data_nhtsa-nhtsa_bulk_fields) | 4 | dataset_id, source_row_number, field_name |
| CAR_DATA_NHTSA.db | [nhtsa_bulk_rows](#car_data_nhtsa-nhtsa_bulk_rows) | 3 | dataset_id, source_row_number |
| CAR_DATA_NHTSA.db | [nhtsa_complaint_products](#car_data_nhtsa-nhtsa_complaint_products) | 8 | query_id, record_key, product_index |
| CAR_DATA_NHTSA.db | [nhtsa_complaints](#car_data_nhtsa-nhtsa_complaints) | 13 | query_id, record_key |
| CAR_DATA_NHTSA.db | [nhtsa_ingestion_runs](#car_data_nhtsa-nhtsa_ingestion_runs) | 9 | run_id |
| CAR_DATA_NHTSA.db | [nhtsa_recalls](#car_data_nhtsa-nhtsa_recalls) | 17 | query_id, record_key |
| CAR_DATA_NHTSA.db | [nhtsa_safety_details](#car_data_nhtsa-nhtsa_safety_details) | 2 | query_id, vehicle_id |
| CAR_DATA_NHTSA.db | [nhtsa_safety_rating_values](#car_data_nhtsa-nhtsa_safety_rating_values) | 4 | query_id, vehicle_id, field_name |
| CAR_DATA_NHTSA.db | [nhtsa_safety_variants](#car_data_nhtsa-nhtsa_safety_variants) | 3 | query_id, vehicle_id |
| CAR_DATA_NHTSA.db | [nhtsa_schema_meta](#car_data_nhtsa-nhtsa_schema_meta) | 2 | key |
| CAR_DATA_NHTSA.db | [nhtsa_source_catalog](#car_data_nhtsa-nhtsa_source_catalog) | 6 | source_name |
| CAR_DATA_NHTSA.db | [nhtsa_vehicle_queries](#car_data_nhtsa-nhtsa_vehicle_queries) | 14 | query_id |
| CAR_DATA_NHTSA.db | [nhtsa_vin_identity_resolution](#car_data_nhtsa-nhtsa_vin_identity_resolution) | 19 | identity_id |
| CAR_DATA_NHTSA.db | [nhtsa_vpic_decodes](#car_data_nhtsa-nhtsa_vpic_decodes) | 12 | decode_id |
| CAR_DATA_NHTSA.db | [nhtsa_vpic_values](#car_data_nhtsa-nhtsa_vpic_values) | 155 | decode_id |
| CAR_DATA_NHTSA.db | [sqlite_sequence](#car_data_nhtsa-sqlite_sequence) | 2 | None |

## Relationships and join rules

Relationships below describe analytical joins. Each table also lists its actual FK declarations, including invalid legacy declarations. Cross-database joins are logical relationships and have no cross-file FK enforcement.

| Parent/context | Child/target | Join and cardinality | Required handling |
| --- | --- | --- | --- |
| CAR_DATA listings | nhtsa_enrichment | Many snapshots to at most one projection, on vin | Left join preserves snapshots; latest projection is not historically time-safe by itself. |
| Distinct listing VINs | price_history / listing_history | One VIN to many history events | Joining history directly to all snapshots creates many-to-many multiplication. First select one listing identity per VIN or aggregate history. |
| CAR_DATA listing VIN | CAR_DATA_NHTSA vpic_decodes / identity_resolution | One VIN to many retained records | Select a justified version by fetched_at/resolved_at and use deterministic ID tie-breaks. Avoid future enrichment in historical evaluation. |
| nhtsa_ingestion_runs | decodes, identities, vehicle_queries | run_id, one run to many records | Deduplicated/cache-reused records may remain associated with their original run. |
| nhtsa_vpic_decodes | nhtsa_vpic_values | decode_id, one to zero-or-one | Current failure helper writes minimal VIN/ErrorText values too; inspect response_status. |
| nhtsa_vpic_decodes | nhtsa_vin_identity_resolution | decode_id, one to many candidate resolutions | Nullable decode_id; retain listing fallback provenance. |
| nhtsa_vehicle_queries | safety_variants / recalls / complaints | query_id, one to many | Filter query_type and choose a response version before counting. |
| nhtsa_safety_variants | safety_details / safety_rating_values | (query_id, vehicle_id), one to zero-or-one marker / many fields | Detail fields use the variant lookup query_id. The separate safety_detail metadata row is matched by query context, VehicleId and time, not by equating query IDs. |
| nhtsa_complaints | nhtsa_complaint_products | (query_id, record_key), one to many | Count complaint rows before expanding products to avoid inflated counts. |
| typed NHTSA record | nhtsa_api_extra_fields | query_id + record_type + record_key, one to many fields | record_type prevents collisions between kinds; safety_variant record_key is string VehicleId. |
| nhtsa_bulk_datasets | nhtsa_bulk_rows | dataset_id, one to many | Preserve dataset version/checksum. |
| nhtsa_bulk_rows | nhtsa_bulk_fields | (dataset_id, source_row_number), one to many | Pivot only with explicit per-field type definitions. |
| youtube_comments_sentiment | youtube_comments_scored | comment_id, intended one to zero-or-one | Rejected or not-yet-processed comments may lack scores; scored unique index enforces non-NULL ID uniqueness. |
| youtube_video_fetch_state | comments / scores | video_id, one to many | No declared FK; direct-video collection can have NULL playlist_id. |
| youtube_playlist_fetch_state | youtube_video_fetch_state | playlist_id, one to many stored associations | Not an exhaustive playlist/video membership table. |
| attributed scored comments | make_sentiment_index / monthly | sentiment_make plus publication month, many to one aggregate | Filter sentiment_status='scored' and non-NULL make; monthly also requires a recognized date. |
| Canonical listing/history make and month | make_sentiment_monthly | Latest make/month strictly before observation month in the ML scripts | Month labels summarize the full month; use strictly prior month for within-month prediction unless cutoff is month-end. Never join all prior months and duplicate observations. |

NHTSA recall/complaint lookup grain is resolved make/model/year, not VIN-specific risk. The same query can enrich many VINs. Aggregating projected counts across VINs double-counts shared source evidence. Matching safety variants also does not prove that one rating applies to every trim/body configuration.


<a id="car-datadb"></a>

## CAR_DATA.db

Path: `CAR_DATA_OUTPUT/CAR_DATA.db`.

<a id="car_data-listing_history"></a>

### listing_history

- **Purpose:** Supplementary historical price/odometer observations.
- **Row grain:** One retained source listing-history event, with surrogate id and uniqueness on (vin, history_date, price, mileage).
- **Producer/lineage:** CarDatabase._insert_listing_history from queue-results listingHistory.
- **Update/history behavior:** INSERT OR IGNORE; no collection timestamp; do not union with price_history without overlap handling.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `id` | INTEGER | PK1, rowid key | SQLite-generated surrogate identifier for the history record. | INTEGER PRIMARY KEY AUTOINCREMENT; not a VIN or chronology guarantee. |
| `vin` | TEXT | - | Listing VIN or vehicle identifier supplied by acquisition; enrichment normalizes case/whitespace. | Identifier text; partial/masked values may exist; VIN rule V1. |
| `history_date` | TEXT | - | Date on a source priceHistory/listingHistory element; copied from its date field. | Source event date; no ingestion timestamp in this table; format not standardized here. |
| `mileage` | REAL | - | Source historical mileage from the listingHistory element, copied without scraper numeric normalization. | Nonnegative numeric expected; distance unit not explicitly stored; NULL differs from zero. Historical source values can remain strings. |
| `price` | REAL | - | Source historical price from the listingHistory element, copied without scraper numeric normalization. | Monetary numeric after parsing; currency not stored/guaranteed; zero/1 may be placeholders. Historical source values can remain strings. |

**Indexes and uniqueness (observed):**

- `idx_listing_history_vin`: nonunique (vin); origin=c, partial=0.
- `sqlite_autoindex_listing_history_1`: UNIQUE (vin, history_date, price, mileage); origin=u, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (vin) -> `cars` (vin); ON UPDATE NO ACTION, ON DELETE NO ACTION.

<a id="car_data-listings"></a>

### listings

- **Purpose:** Market observations for price prediction and listing context.
- **Row grain:** One retained listing snapshot per (vin, loaddate).
- **Producer/lineage:** CarDatabase._insert_rows_impl; active Playwright queue-results extraction.
- **Update/history behavior:** INSERT OR REPLACE on snapshot key; unchanged price/mileage can be skipped by VINCache. Within-day changes replace the same key; not every scrape is retained.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `vin` | TEXT | PK1 | Listing VIN or vehicle identifier supplied by acquisition; enrichment normalizes case/whitespace. | Identifier text; partial/masked values may exist; VIN rule V1. |
| `loaddate` | TEXT | PK2 | Local calendar date assigned by the scraper with date.today().isoformat(); snapshot collection date, not the seller's event date. | Date, YYYY-MM-DD; no time or timezone offset retained. |
| `date` | TEXT | - | Source listing date passed through from queue-results item.date; precise source event interpretation is unresolved. | Source date/text; parse explicitly; not guaranteed ISO. |
| `location` | TEXT | - | Source location label passed through from the listing API. | Text; geography granularity is source-dependent. |
| `locationCode` | TEXT | - | Source location code passed through without a canonical geographic lookup. | Text identifier; preserve leading zeros; coding system unresolved. |
| `countryCode` | TEXT | - | Source country code passed through without validation or conversion. | Text; do not assume a particular ISO code length without verification. |
| `pendingSale` | BOOLEAN | - | Source pending-sale indicator. | Boolean-like; expected 0/1, but not constrained; NULL means unavailable. |
| `currentBid` | REAL | - | Source current auction bid at collection time. | Monetary amount; not a final sale price; currency unspecified. |
| `bids` | INTEGER | - | Source auction bid count at collection time. | Nonnegative integer expected; NULL means unavailable. |
| `distance` | REAL | - | Source search-distance value, distinct from vehicle odometer mileage. | Numeric; unit and search origin not persisted; interpretation requires source context. |
| `priceRecentChange` | BOOLEAN | - | Source flag indicating a recent price change. | Boolean-like; lookback window/direction unresolved; not a price-change amount. |
| `price` | TEXT | - | Advertised listing price, not a completed transaction price; scraper strips dollar signs/commas and parses float. | Monetary numeric after parsing; currency not stored/guaranteed; zero/1 may be placeholders. |
| `mileage` | TEXT | - | Odometer value from listing; scraper removes commas and truncates numeric values to integer. | Nonnegative numeric expected; distance unit not explicitly stored; NULL differs from zero. |
| `title` | TEXT | - | Source listing headline used downstream for identity and title-only trim parsing. | Free text; not verified canonical identity. |
| `listingType` | TEXT | - | Source listing classification. | Open text categories; interpret per sourceName. |
| `sourceName` | TEXT | - | Name of the upstream listing provider carried in the aggregator response. | Text provenance label; not a seller identifier. |
| `year` | INTEGER | - | Model year supplied by the listing source, before NHTSA identity resolution. | Integer year; may disagree with decode; no SQL range constraint. |
| `sellerType` | TEXT | - | Source seller classification. | Open text categories; no enforced vocabulary. |
| `vehicleTitleDesc` | TEXT | - | Source description associated with vehicle-title information. | Source text; exact title-status vocabulary unresolved. |
| `img` | TEXT | - | Image reference selected from img, then imgSource, then imgFallback. | URL/reference text; image binary not stored. |
| `details` | TEXT | - | Concatenation of detailsShort, detailsMid and detailsLong, without inserted separators. | Free text; NULL if unavailable. |
| `vehicleTitle` | TEXT | - | Source vehicle-title field, preserved independently of listing title. | Source text/code; precise domain unresolved; do not assume canonical trim. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_listings_1`: UNIQUE (vin, loaddate); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (vin) -> `cars` (vin); ON UPDATE NO ACTION, ON DELETE NO ACTION.

<a id="car_data-nhtsa_enrichment"></a>

### nhtsa_enrichment

- **Purpose:** Convenient decode/specification and make/model/year summary join to listings.
- **Row grain:** One latest compatibility projection per vin.
- **Producer/lineage:** NHTSADataEnricher._process_result and CarDatabase.insert_nhtsa_enrichment_batch.
- **Update/history behavior:** Upsert provided fields by vin. Failure-only updates can leave older values in unprovided columns; read per-source statuses and timestamps.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `vin` | TEXT | PK1 | Listing VIN or vehicle identifier supplied by acquisition; enrichment normalizes case/whitespace. | Identifier text; partial/masked values may exist; VIN rule V1. |
| `nhtsa_ABS` | TEXT | - | Antilock braking equipment that controls wheel slip during braking. vPIC variable 86. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 86; blank/NULL is not absence. |
| `nhtsa_ActiveSafetySysNote` | TEXT | - | Additional source information about active safety systems. vPIC variable 169. Stable compatibility projection of the unprefixed decode field. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `nhtsa_AdaptiveCruiseControl` | TEXT | - | Cruise control that adjusts speed to maintain a selected following distance. vPIC variable 81. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 81; blank/NULL is not absence. |
| `nhtsa_AdaptiveDrivingBeam` | TEXT | - | Headlamp system that adapts the upper-beam pattern around other road users. vPIC variable 180. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 180; blank/NULL is not absence. |
| `nhtsa_AdaptiveHeadlights` | TEXT | - | Legacy/source AdaptiveHeadlights property retained as delivered. Stable compatibility projection of the unprefixed decode field. | Source category text; current variable-list mapping unresolved; do not equate automatically to AdaptiveDrivingBeam. |
| `nhtsa_AdditionalErrorText` | TEXT | - | Additional decode diagnostic information beyond the main error text. vPIC variable 156. Stable compatibility projection of the unprefixed decode field. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `nhtsa_AirBagLocCurtain` | TEXT | - | Occupant positions/rows with curtain airbags. vPIC variable 55. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 55; blank/NULL is not absence. |
| `nhtsa_AirBagLocFront` | TEXT | - | Occupant positions/rows with frontal airbags. vPIC variable 65. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 65; blank/NULL is not absence. |
| `nhtsa_AirBagLocKnee` | TEXT | - | Occupant positions/rows with knee airbags. vPIC variable 69. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 69; blank/NULL is not absence. |
| `nhtsa_AirBagLocSeatCushion` | TEXT | - | Occupant positions/rows with seat-cushion airbags. vPIC variable 56. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 56; blank/NULL is not absence. |
| `nhtsa_AirBagLocSide` | TEXT | - | Occupant positions/rows with side airbags. vPIC variable 107. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 107; blank/NULL is not absence. |
| `nhtsa_AutoReverseSystem` | TEXT | - | Window/sunroof closing system that reverses when an obstruction is detected. vPIC variable 172. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 172; blank/NULL is not absence. |
| `nhtsa_AutomaticPedestrianAlertingSound` | TEXT | - | External alert sound equipment for hybrid/electric vehicle pedestrian awareness. vPIC variable 173. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 173; blank/NULL is not absence. |
| `nhtsa_AxleConfiguration` | TEXT | - | Vehicle axle arrangement/configuration reported by the source. vPIC variable 145. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 145; blank/NULL is not absence. |
| `nhtsa_Axles` | TEXT | - | Number of vehicle axles. vPIC variable 41. Stable compatibility projection of the unprefixed decode field. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `nhtsa_BasePrice` | TEXT | - | Manufacturer base price supplied by vPIC; not observed market price. vPIC variable 136. Stable compatibility projection of the unprefixed decode field. | Numeric text; US dollars; manufacturer base price, not listing price; parse without replacing missing with zero. |
| `nhtsa_BedLengthIN` | TEXT | - | Length of the pickup cargo bed. vPIC variable 49. Stable compatibility projection of the unprefixed decode field. | Numeric text; inches; parse without replacing missing with zero. |
| `nhtsa_BedType` | TEXT | - | Pickup cargo-bed configuration. vPIC variable 3. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 3; blank/NULL is not absence. |
| `nhtsa_BlindSpotIntervention` | TEXT | - | System that can intervene with braking or steering to avoid a blind-spot collision. vPIC variable 193. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 193; blank/NULL is not absence. |
| `nhtsa_BlindSpotMon` | TEXT | - | Blind-spot warning equipment that alerts the driver to adjacent vehicles. vPIC variable 88. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 88; blank/NULL is not absence. |
| `nhtsa_BodyCabType` | TEXT | - | Truck cab configuration, including passenger-space and door arrangement. vPIC variable 4. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 4; blank/NULL is not absence. |
| `nhtsa_BodyClass` | TEXT | - | General body configuration or shape, such as sedan, wagon or pickup. vPIC variable 5. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 5; blank/NULL is not absence. |
| `nhtsa_BrakeSystemDesc` | TEXT | - | Additional description of the braking system. vPIC variable 52. Stable compatibility projection of the unprefixed decode field. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `nhtsa_BrakeSystemType` | TEXT | - | Type of system used to stop and hold the vehicle. vPIC variable 42. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 42; blank/NULL is not absence. |
| `nhtsa_ChargerLevel` | TEXT | - | Source classification of electric-vehicle charger level. vPIC variable 127. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 127; blank/NULL is not absence. |
| `nhtsa_ChargerPowerKW` | TEXT | - | Electric-vehicle charger power. vPIC variable 128. Stable compatibility projection of the unprefixed decode field. | Numeric text; kW; parse without replacing missing with zero. |
| `nhtsa_CombinedBrakingSystem` | TEXT | - | Motorcycle system that applies front and rear brakes together from one control. vPIC variable 202. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 202; blank/NULL is not absence. |
| `nhtsa_CoolingType` | TEXT | - | Engine cooling-system type, such as air or liquid cooling. vPIC variable 122. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 122; blank/NULL is not absence. |
| `nhtsa_CurbWeightLB` | TEXT | - | Vehicle weight with standard equipment/operating fluids, without occupants or cargo. vPIC variable 54. Stable compatibility projection of the unprefixed decode field. | Numeric text; pounds; parse without replacing missing with zero. |
| `nhtsa_DaytimeRunningLight` | TEXT | - | Daytime illumination intended to increase vehicle visibility. vPIC variable 177. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 177; blank/NULL is not absence. |
| `nhtsa_DestinationMarket` | TEXT | - | Market where the vehicle is intended to be sold. vPIC variable 10. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 10; blank/NULL is not absence. |
| `nhtsa_DisplacementCC` | TEXT | - | Total engine cylinder swept volume in cubic centimeters. vPIC variable 11. Stable compatibility projection of the unprefixed decode field. | Numeric text; cm^3; parse without replacing missing with zero. |
| `nhtsa_DisplacementCI` | TEXT | - | Total engine cylinder swept volume in cubic inches. vPIC variable 12. Stable compatibility projection of the unprefixed decode field. | Numeric text; in^3; parse without replacing missing with zero. |
| `nhtsa_DisplacementL` | TEXT | - | Total engine cylinder swept volume in liters. vPIC variable 13. Stable compatibility projection of the unprefixed decode field. | Numeric text; liters; parse without replacing missing with zero. |
| `nhtsa_Doors` | TEXT | - | Number of vehicle doors. vPIC variable 14. Stable compatibility projection of the unprefixed decode field. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `nhtsa_DriveType` | TEXT | - | Drivetrain configuration, such as front-, rear-, all- or four-wheel drive. vPIC variable 15. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 15; blank/NULL is not absence. |
| `nhtsa_DriverAssist` | TEXT | - | Legacy/source DriverAssist property retained as delivered. Stable compatibility projection of the unprefixed decode field. | Source description/category; exact modern variable-list mapping unresolved. |
| `nhtsa_DynamicBrakeSupport` | TEXT | - | Braking assistance that supplements driver braking in an emergency. vPIC variable 170. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 170; blank/NULL is not absence. |
| `nhtsa_EDR` | TEXT | - | Event data recorder equipment for crash-related vehicle data. vPIC variable 175. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 175; blank/NULL is not absence. |
| `nhtsa_ESC` | TEXT | - | Electronic stability control equipment that intervenes to reduce skidding. vPIC variable 99. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 99; blank/NULL is not absence. |
| `nhtsa_EVDriveUnit` | TEXT | - | Electric drive motor configuration, such as single or dual motor. vPIC variable 72. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 72; blank/NULL is not absence. |
| `nhtsa_ElectrificationLevel` | TEXT | - | Source classification of hybrid/electric propulsion configuration. vPIC variable 126. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 126; blank/NULL is not absence. |
| `nhtsa_EngineConfiguration` | TEXT | - | Arrangement of engine cylinders, such as inline or V-shaped. vPIC variable 64. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 64; blank/NULL is not absence. |
| `nhtsa_EngineCycles` | TEXT | - | Number of strokes used to complete an engine power cycle. vPIC variable 17. Stable compatibility projection of the unprefixed decode field. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `nhtsa_EngineCylinders` | TEXT | - | Number of engine cylinders. vPIC variable 9. Stable compatibility projection of the unprefixed decode field. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `nhtsa_EngineHP` | TEXT | - | Engine output-shaft horsepower; lower endpoint when a range is reported. vPIC variable 71. Stable compatibility projection of the unprefixed decode field. | Numeric text; hp; lower bound when a range; parse without replacing missing with zero. |
| `nhtsa_EngineHP_to` | TEXT | - | Upper endpoint of reported engine output-shaft horsepower range. vPIC variable 125. Stable compatibility projection of the unprefixed decode field. | Numeric text; hp; upper bound; parse without replacing missing with zero. |
| `nhtsa_EngineKW` | TEXT | - | Engine power expressed in kilowatts. vPIC variable 21. Stable compatibility projection of the unprefixed decode field. | Numeric text; kW; parse without replacing missing with zero. |
| `nhtsa_EngineManufacturer` | TEXT | - | Manufacturer of the engine, which may differ from vehicle manufacturer. vPIC variable 146. Stable compatibility projection of the unprefixed decode field. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `nhtsa_EngineModel` | TEXT | - | Manufacturer-assigned engine family/model name. vPIC variable 18. Stable compatibility projection of the unprefixed decode field. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `nhtsa_EntertainmentSystem` | TEXT | - | Source classification of vehicle entertainment equipment. vPIC variable 23. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 23; blank/NULL is not absence. |
| `nhtsa_ForwardCollisionWarning` | TEXT | - | System that warns of an impending forward collision. vPIC variable 101. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 101; blank/NULL is not absence. |
| `nhtsa_FuelInjectionType` | TEXT | - | Mechanism used to deliver/inject fuel to the engine. vPIC variable 67. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 67; blank/NULL is not absence. |
| `nhtsa_FuelTankMaterial` | TEXT | - | Material used to construct the fuel tank. vPIC variable 201. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 201; blank/NULL is not absence. |
| `nhtsa_FuelTankType` | TEXT | - | Fuel-tank mounting/configuration type, particularly for motorcycles. vPIC variable 200. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 200; blank/NULL is not absence. |
| `nhtsa_FuelTypePrimary` | TEXT | - | Primary energy/fuel source used to power the vehicle. vPIC variable 24. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 24; blank/NULL is not absence. |
| `nhtsa_FuelTypeSecondary` | TEXT | - | Secondary energy/fuel source for vehicles with multiple power sources. vPIC variable 66. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 66; blank/NULL is not absence. |
| `nhtsa_KeylessIgnition` | TEXT | - | Ignition/start equipment that operates without inserting a conventional key. vPIC variable 176. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 176; blank/NULL is not absence. |
| `nhtsa_LaneCenteringAssistance` | TEXT | - | System that continuously assists steering to keep the vehicle centered in its lane. vPIC variable 194. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 194; blank/NULL is not absence. |
| `nhtsa_LaneDepartureWarning` | TEXT | - | System that warns when the vehicle unintentionally departs its lane. vPIC variable 102. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 102; blank/NULL is not absence. |
| `nhtsa_LaneKeepSystem` | TEXT | - | System that assists to prevent unintentional lane departure. vPIC variable 103. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 103; blank/NULL is not absence. |
| `nhtsa_LowerBeamHeadlampLightSource` | TEXT | - | Headlamp illumination technology/light source. vPIC variable 178. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 178; blank/NULL is not absence. |
| `nhtsa_Make` | TEXT | - | Vehicle make named by the manufacturer. vPIC variable 26. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 26; blank/NULL is not absence. |
| `nhtsa_MakeID` | TEXT | - | vPIC identifier for decoded make. Stable compatibility projection of the unprefixed decode field. | Identifier stored as text; not a quantity. |
| `nhtsa_Manufacturer` | TEXT | - | Vehicle manufacturer name. vPIC variable 27. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 27; blank/NULL is not absence. |
| `nhtsa_ManufacturerId` | TEXT | - | vPIC identifier for the vehicle manufacturer. Stable compatibility projection of the unprefixed decode field. | Identifier stored as text; not a quantity. |
| `nhtsa_Model` | TEXT | - | Vehicle model named by the manufacturer. vPIC variable 28. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 28; blank/NULL is not absence. |
| `nhtsa_ModelID` | TEXT | - | vPIC identifier for decoded model. Stable compatibility projection of the unprefixed decode field. | Identifier stored as text; not a quantity. |
| `nhtsa_ModelYear` | TEXT | - | Model year in the decode response; a supplied model-year hint may control this value. vPIC variable 29. Stable compatibility projection of the unprefixed decode field. | Numeric text; model year; supplied hint may determine returned year; parse without replacing missing with zero. |
| `nhtsa_OtherEngineInfo` | TEXT | - | Additional engine information not captured in dedicated fields. vPIC variable 129. Stable compatibility projection of the unprefixed decode field. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `nhtsa_ParkAssist` | TEXT | - | Equipment that assists steering or other functions during parking. vPIC variable 105. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 105; blank/NULL is not absence. |
| `nhtsa_PedestrianAutomaticEmergencyBraking` | TEXT | - | Automatic braking intended to avoid or mitigate collisions with pedestrians. vPIC variable 171. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 171; blank/NULL is not absence. |
| `nhtsa_RearAutomaticEmergencyBraking` | TEXT | - | System that automatically brakes to avoid an imminent collision while reversing. vPIC variable 192. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 192; blank/NULL is not absence. |
| `nhtsa_RearCrossTrafficAlert` | TEXT | - | System that warns of crossing traffic behind the vehicle while reversing. vPIC variable 183. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 183; blank/NULL is not absence. |
| `nhtsa_RearVisibilitySystem` | TEXT | - | Backup-camera/rearview video system equipment. vPIC variable 104. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 104; blank/NULL is not absence. |
| `nhtsa_SAEAutomationLevel` | TEXT | - | Lower bound of source-reported SAE driving automation level. vPIC variable 181. Stable compatibility projection of the unprefixed decode field. | Numeric text; integer level 0..5; lower bound; parse without replacing missing with zero. |
| `nhtsa_SAEAutomationLevel_to` | TEXT | - | Upper bound of source-reported SAE driving automation level. vPIC variable 182. Stable compatibility projection of the unprefixed decode field. | Numeric text; integer level 0..5; upper bound; parse without replacing missing with zero. |
| `nhtsa_SeatRows` | TEXT | - | Number of rows of seats. vPIC variable 61. Stable compatibility projection of the unprefixed decode field. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `nhtsa_Seats` | TEXT | - | Number of seats. vPIC variable 33. Stable compatibility projection of the unprefixed decode field. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `nhtsa_SemiautomaticHeadlampBeamSwitching` | TEXT | - | Equipment that automatically switches upper/lower headlamp beams when enabled. vPIC variable 179. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 179; blank/NULL is not absence. |
| `nhtsa_TPMS` | TEXT | - | Type of tire-pressure monitoring system. vPIC variable 168. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 168; blank/NULL is not absence. |
| `nhtsa_TopSpeedMPH` | TEXT | - | Source-reported maximum speed. vPIC variable 139. Stable compatibility projection of the unprefixed decode field. | Numeric text; mph; parse without replacing missing with zero. |
| `nhtsa_TrackWidth` | TEXT | - | Source-reported lateral track width. vPIC variable 159. Stable compatibility projection of the unprefixed decode field. | Numeric text; inches; parse without replacing missing with zero. |
| `nhtsa_TractionControl` | TEXT | - | Equipment that limits driven-wheel spin to maintain traction. vPIC variable 100. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 100; blank/NULL is not absence. |
| `nhtsa_TransmissionSpeeds` | TEXT | - | Number of transmission speeds/gears. vPIC variable 63. Stable compatibility projection of the unprefixed decode field. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `nhtsa_TransmissionStyle` | TEXT | - | Transmission type, such as manual, automatic, CVT or dual clutch. vPIC variable 37. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 37; blank/NULL is not absence. |
| `nhtsa_Trim` | TEXT | - | Manufacturer trim designation; comparison-only in this project, never the source of canonical_trim. vPIC variable 38. Stable compatibility projection of the unprefixed decode field. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `nhtsa_Trim2` | TEXT | - | Additional manufacturer trim information; comparison-only, never the source of canonical_trim. vPIC variable 109. Stable compatibility projection of the unprefixed decode field. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `nhtsa_WheelSizeFront` | TEXT | - | Diameter of the front wheel. vPIC variable 119. Stable compatibility projection of the unprefixed decode field. | Numeric text; inches, wheel diameter; parse without replacing missing with zero. |
| `nhtsa_WheelSizeRear` | TEXT | - | Diameter of the rear wheel. vPIC variable 120. Stable compatibility projection of the unprefixed decode field. | Numeric text; inches, wheel diameter; parse without replacing missing with zero. |
| `nhtsa_Windows` | TEXT | - | Number of vehicle windows. vPIC variable 40. Stable compatibility projection of the unprefixed decode field. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `nhtsa_VehicleType` | TEXT | - | Vehicle classification based on the World Manufacturer Identifier. vPIC variable 39. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 39; blank/NULL is not absence. |
| `nhtsa_WheelBaseLong` | TEXT | - | Upper bound of distance between front and rear axle/wheel centers. vPIC variable 112. Stable compatibility projection of the unprefixed decode field. | Numeric text; inches; upper bound; parse without replacing missing with zero. |
| `nhtsa_WheelBaseShort` | TEXT | - | Lower bound of distance between front and rear axle/wheel centers. vPIC variable 111. Stable compatibility projection of the unprefixed decode field. | Numeric text; inches; lower bound; parse without replacing missing with zero. |
| `nhtsa_WheelBaseType` | TEXT | - | Relative wheelbase variant, such as short, standard or long. vPIC variable 60. Stable compatibility projection of the unprefixed decode field. | Source categorical label; official value list for variable 60; blank/NULL is not absence. |
| `nhtsa_safety_ratings_count` | INTEGER | - | Number of safety detail dictionaries aggregated for the resolved make/model/year query. | Count of variant detail results, not unique VINs; inspect safety status. |
| `nhtsa_overall_rating` | TEXT | - | Sorted distinct usable OverallRating values across retrieved safety variants, joined with semicolon-space. | Text; may hold multiple ratings; not a numeric mean or VIN-specific rating. |
| `nhtsa_front_crash_rating` | TEXT | - | Sorted distinct OverallFrontCrashRating values, with FrontCrashRating fallback, across variants. | Semicolon-space-separated rating text; preserve variant ambiguity. |
| `nhtsa_rollover_rating` | TEXT | - | Sorted distinct usable RolloverRating values across variants. | Semicolon-space-separated rating text; not a rollover probability. |
| `nhtsa_side_crash_rating` | TEXT | - | Sorted distinct OverallSideCrashRating values, with SideCrashRating fallback, across variants. | Semicolon-space-separated rating text; preserve variant ambiguity. |
| `nhtsa_total_recalls` | INTEGER | - | Number of recall source records returned for the resolved make/model/year. | Count; not affected VINs or necessarily distinct campaigns; inspect recalls status. |
| `nhtsa_recall_components` | TEXT | - | Three most frequent nonblank Component strings, frequency descending then lexical order, joined with semicolon-space. | Text summary; whole component strings counted; not exhaustive. |
| `nhtsa_latest_recall_date` | TEXT | - | Latest usable ReportReceivedDate chosen using supported date parsing, retaining its original representation. | Source date text; NULL when no usable date; failure status still required. |
| `nhtsa_total_complaints` | INTEGER | - | Number of complaint source records for the resolved make/model/year. | Count; not a VIN-specific failure rate or exposure-adjusted risk. |
| `nhtsa_complaint_injuries` | INTEGER | - | Sum of numberOfInjuries across returned complaints; missing/invalid values contribute zero in this summary. | Count; zero may conceal missing counts; consult normalized records. |
| `nhtsa_complaint_deaths` | INTEGER | - | Sum of numberOfDeaths across returned complaints; missing/invalid values contribute zero in this summary. | Count; zero may conceal missing counts; consult normalized records. |
| `nhtsa_complaint_crash_related` | INTEGER | - | Count of complaint records whose crash value matches the writer's truthy vocabulary. | Count; 1/true/yes/y/t recognized case-insensitively, all others false for this calculation. |
| `nhtsa_complaint_fire_related` | INTEGER | - | Count of complaint records whose fire value matches the writer's truthy vocabulary. | Count; 1/true/yes/y/t recognized case-insensitively, all others false for this calculation. |
| `nhtsa_common_complaint_areas` | TEXT | - | Three most frequent nonblank components strings, frequency descending then lexical order, joined with semicolon-space. | Text summary of complete source strings; not exhaustive. |
| `nhtsa_decode_status` | TEXT | - | Latest projected application-level decode outcome. | success, invalid_vin, missing_result, request_failed; success can still have source decode errors. |
| `nhtsa_decode_error` | TEXT | - | Projected ErrorText from decode, or validation/request failure message. | Nullable diagnostic text; may accompany successful HTTP response. |
| `nhtsa_decode_fetched_at` | TEXT | - | Timestamp assigned when a returned decode result is projected; failure-only updates do not replace it. | UTC ISO-8601; not guaranteed identical to normalized record fetched_at; may remain stale after failure. |
| `nhtsa_identity_source` | TEXT | - | Semicolon-separated make/source, model/source and year/source pairs. | Example make:nhtsa_decode;model:listing;year:nhtsa_decode; not JSON. |
| `nhtsa_identity_confidence` | TEXT | - | Projected rule-based identity completeness grade. | high, medium, low, unknown; rule I1, not a probability. |
| `nhtsa_identity_conflict` | INTEGER | - | Whether usable listing and NHTSA identity disagree on any compared field. | Integer 0/1; missing projection remains NULL. |
| `nhtsa_source_run_id` | TEXT | - | Enrichment run responsible for the projected identity/specification update when supplied. | Logical cross-database join to nhtsa_ingestion_runs.run_id; failure-only updates may retain prior value. |
| `nhtsa_last_updated_at` | TEXT | - | Timestamp of latest compatibility update recorded by enrichment. | UTC ISO-8601; not source event time or freshness proof for every field. |
| `nhtsa_safety_status` | TEXT | - | Status of aggregated safety retrieval for the resolved identity. | success, partial, request_failed, missing_identity; success may represent zero variants. |
| `nhtsa_safety_vehicle_ids` | TEXT | - | Semicolon-separated VehicleId values returned by variant lookup. | Text identifier list; empty string possible; not guaranteed one variant. |
| `nhtsa_recalls_status` | TEXT | - | Outcome of recall lookup for resolved make/model/year. | success, empty, request_failed, missing_identity. |
| `nhtsa_complaints_status` | TEXT | - | Outcome of complaint lookup for resolved make/model/year. | success, empty, request_failed, missing_identity. |

**Indexes and uniqueness (observed):**

- `idx_nhtsa_model`: nonunique (nhtsa_Model); origin=c, partial=0.
- `idx_nhtsa_make`: nonunique (nhtsa_Make); origin=c, partial=0.
- `sqlite_autoindex_nhtsa_enrichment_1`: UNIQUE (vin); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (vin) -> `listings` (vin); ON UPDATE NO ACTION, ON DELETE NO ACTION.

<a id="car_data-price_history"></a>

### price_history

- **Purpose:** Source price trajectories for depreciation research.
- **Row grain:** One retained source price-history event, with surrogate id and uniqueness on (vin, history_date, price).
- **Producer/lineage:** CarDatabase._insert_price_history from queue-results priceHistory.
- **Update/history behavior:** INSERT OR IGNORE; mileage and trend are not part of the unique key. Events may predate collection.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `id` | INTEGER | PK1, rowid key | SQLite-generated surrogate identifier for the history record. | INTEGER PRIMARY KEY AUTOINCREMENT; not a VIN or chronology guarantee. |
| `vin` | TEXT | - | Listing VIN or vehicle identifier supplied by acquisition; enrichment normalizes case/whitespace. | Identifier text; partial/masked values may exist; VIN rule V1. |
| `history_date` | TEXT | - | Date on a source priceHistory/listingHistory element; copied from its date field. | Source event date; no ingestion timestamp in this table; format not standardized here. |
| `mileage` | TEXT | - | Source historical mileage from the priceHistory element, copied without scraper numeric normalization. | Nonnegative numeric expected; distance unit not explicitly stored; NULL differs from zero. Historical source values can remain strings. |
| `price` | TEXT | - | Source historical price from the priceHistory element, copied without scraper numeric normalization. | Monetary numeric after parsing; currency not stored/guaranteed; zero/1 may be placeholders. Historical source values can remain strings. |
| `trend` | TEXT | - | Source price-history trend label, copied without recalculation. | Text; values and time window unresolved; potential price-derived leakage. |

**Indexes and uniqueness (observed):**

- `idx_price_history_vin`: nonunique (vin); origin=c, partial=0.
- `sqlite_autoindex_price_history_1`: UNIQUE (vin, history_date, price); origin=u, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (vin) -> `cars` (vin); ON UPDATE NO ACTION, ON DELETE NO ACTION.

<a id="car_data-sqlite_sequence"></a>

### sqlite_sequence

- **Purpose:** SQLite internal sequence bookkeeping; not research data.
- **Row grain:** One internal AUTOINCREMENT state row per tracked table.
- **Producer/lineage:** SQLite engine.
- **Update/history behavior:** Maintained by SQLite. Included separately for completeness; never use seq as table row count.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `name` | (undeclared) | - | Table name whose AUTOINCREMENT high-water mark SQLite tracks. | SQLite internal identifier; not a project entity. |
| `seq` | (undeclared) | - | Largest AUTOINCREMENT rowid tracked by SQLite for the table. | Internal integer value despite no declared SQL type; not a row count. |

**Indexes and uniqueness (observed):**

- No separate indexes declared; an INTEGER PRIMARY KEY, when present, still supplies the rowid key.

**Foreign keys (declared, not proof of valid enforcement):**

- None.


<a id="car-youtube-commentsdb"></a>

## CAR_YOUTUBE_COMMENTS.db

Path: `CAR_DATA_OUTPUT/CAR_YOUTUBE_COMMENTS.db`.

The local database also contains four empty legacy vehicle-table shells: `listing_history`, `listings`, `nhtsa_enrichment`, and `price_history`. They are excluded from this dictionary because they contain no rows, are not written by current YouTube ingestion, and duplicate authoritative concepts documented under `CAR_DATA.db`.

<a id="car_youtube_comments-make_sentiment_index"></a>

### make_sentiment_index

- **Purpose:** Current make sentiment overview and support diagnostics.
- **Row grain:** One current aggregate per sentiment_make.
- **Producer/lineage:** absa_pipeline.rebuild_make_sentiment_tables from attributed scored comments.
- **Update/history behavior:** Rebuilt transactionally with monthly table; all eligible stored scored rows, including those with unparseable publication dates for score/count calculations. Not for historical feature joins.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `sentiment_make` | TEXT | PK1 | One unambiguous canonical make attributed from comment text, otherwise video title. | Canonical make text; NULL excludes a scored row from make aggregates. |
| `sentiment_overall_score` | REAL | - | comment_weight-weighted mean of nonmissing overall_sentiment over eligible scored rows. | Numeric [-1,1], NULL if denominator zero; current or cumulative window per table. |
| `sentiment_reliability_score` | REAL | - | comment_weight-weighted mean of reliability_sentiment where mentioned=1 and sentiment is nonmissing. | Numeric [-1,1]; NULL with no support; current/cumulative window per table. |
| `sentiment_value_score` | REAL | - | comment_weight-weighted mean of value_sentiment where mentioned=1 and sentiment is nonmissing. | Numeric [-1,1]; NULL with no support; current/cumulative window per table. |
| `sentiment_performance_score` | REAL | - | comment_weight-weighted mean of performance_sentiment where mentioned=1 and sentiment is nonmissing. | Numeric [-1,1]; NULL with no support; current/cumulative window per table. |
| `sentiment_comfort_score` | REAL | - | comment_weight-weighted mean of comfort_sentiment where mentioned=1 and sentiment is nonmissing. | Numeric [-1,1]; NULL with no support; current/cumulative window per table. |
| `sentiment_comment_count` | INTEGER | - | Count of eligible scored rows, including rows with no usable overall score. | Nonnegative integer; not total raw comments or distinct people. |
| `sentiment_video_count` | INTEGER | - | Count of distinct non-NULL video_id values among eligible rows. | Nonnegative integer; monthly count is cumulative distinct videos, not sum of monthly counts. |
| `sentiment_aspect_coverage` | REAL | - | Sum of four mention indicators divided by 4 times eligible comment count. | Fraction [0,1]; coverage of comment/aspect pairs, not fraction with any mention. |
| `sentiment_latest_comment_at` | TEXT | - | Maximum normalized publication date among eligible comments. | YYYY-MM-DD; excludes unparseable dates from MAX. |
| `sentiment_model_versions` | TEXT | - | Comma-separated distinct model_name@model_revision combinations in the current rollup. | Text provenance; ordering not guaranteed; unknown substitutes for missing pieces. |
| `updated_at` | TEXT | - | UTC time the make aggregate tables were rebuilt from stored scored rows. | UTC ISO-8601 timestamp; not comment update time. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_make_sentiment_index_1`: UNIQUE (sentiment_make); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- None.

<a id="car_youtube_comments-make_sentiment_monthly"></a>

### make_sentiment_monthly

- **Purpose:** Monthly historical make sentiment feature source.
- **Row grain:** One cumulative aggregate per (sentiment_make, sentiment_month) with a represented comment month.
- **Producer/lineage:** absa_pipeline.rebuild_make_sentiment_tables from dated, attributed scored comments.
- **Update/history behavior:** Rebuilt transactionally; cumulative through entire labeled month; months without comments are not filled. Publication-time reconstruction is not collection-time availability.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `sentiment_make` | TEXT | PK1 | One unambiguous canonical make attributed from comment text, otherwise video title. | Canonical make text; NULL excludes a scored row from make aggregates. |
| `sentiment_month` | TEXT | PK2 | Month label for cumulative scores through the end of that month. | YYYY-MM-01; month-start label does not imply start-of-month availability. |
| `sentiment_overall_score` | REAL | - | comment_weight-weighted mean of nonmissing overall_sentiment over eligible scored rows. | Numeric [-1,1], NULL if denominator zero; current or cumulative window per table. |
| `sentiment_reliability_score` | REAL | - | comment_weight-weighted mean of reliability_sentiment where mentioned=1 and sentiment is nonmissing. | Numeric [-1,1]; NULL with no support; current/cumulative window per table. |
| `sentiment_value_score` | REAL | - | comment_weight-weighted mean of value_sentiment where mentioned=1 and sentiment is nonmissing. | Numeric [-1,1]; NULL with no support; current/cumulative window per table. |
| `sentiment_performance_score` | REAL | - | comment_weight-weighted mean of performance_sentiment where mentioned=1 and sentiment is nonmissing. | Numeric [-1,1]; NULL with no support; current/cumulative window per table. |
| `sentiment_comfort_score` | REAL | - | comment_weight-weighted mean of comfort_sentiment where mentioned=1 and sentiment is nonmissing. | Numeric [-1,1]; NULL with no support; current/cumulative window per table. |
| `sentiment_comment_count` | INTEGER | - | Count of eligible scored rows, including rows with no usable overall score. | Nonnegative integer; not total raw comments or distinct people. |
| `sentiment_video_count` | INTEGER | - | Count of distinct non-NULL video_id values among eligible rows. | Nonnegative integer; monthly count is cumulative distinct videos, not sum of monthly counts. |
| `sentiment_aspect_coverage` | REAL | - | Sum of four mention indicators divided by 4 times eligible comment count. | Fraction [0,1]; coverage of comment/aspect pairs, not fraction with any mention. |
| `sentiment_latest_comment_at` | TEXT | - | Maximum normalized publication date among eligible comments. | YYYY-MM-DD; excludes unparseable dates from MAX. |

**Indexes and uniqueness (observed):**

- `idx_make_sentiment_monthly_lookup`: nonunique (sentiment_make, sentiment_month); origin=c, partial=0.
- `sqlite_autoindex_make_sentiment_monthly_1`: UNIQUE (sentiment_make, sentiment_month); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- None.

<a id="car_youtube_comments-sqlite_sequence"></a>

### sqlite_sequence

- **Purpose:** SQLite internal sequence bookkeeping; not research data.
- **Row grain:** One internal AUTOINCREMENT state row per tracked table.
- **Producer/lineage:** SQLite engine.
- **Update/history behavior:** Maintained by SQLite. Included separately for completeness; never use seq as table row count.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `name` | (undeclared) | - | Table name whose AUTOINCREMENT high-water mark SQLite tracks. | SQLite internal identifier; not a project entity. |
| `seq` | (undeclared) | - | Largest AUTOINCREMENT rowid tracked by SQLite for the table. | Internal integer value despite no declared SQL type; not a row count. |

**Indexes and uniqueness (observed):**

- No separate indexes declared; an INTEGER PRIMARY KEY, when present, still supplies the rowid key.

**Foreign keys (declared, not proof of valid enforcement):**

- None.

<a id="car_youtube_comments-vehicle_sentiment_index"></a>

### vehicle_sentiment_index

- **Purpose:** Legacy output retained in local database; superseded conceptually by make aggregates.
- **Row grain:** Historical vehicle-entity aggregate; no key declared.
- **Producer/lineage:** No current Python writer found for this table; original generating version is unresolved.
- **Update/history behavior:** Legacy retention; not rebuilt by current make-grain aggregation. Definitions of formulas, scales and thresholds remain unverified.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `Vehicle_Entity` | TEXT | - | Historical title-derived vehicle entity retained for compatibility; current make-grain pipeline does not populate it for new scoring. | Legacy text; not the canonical make/model/year/trim join key. |
| `Sample_Size` | INTEGER | - | Historical vehicle-entity sample-size statistic. | Legacy count; exact inclusion criteria unresolved in current code. |
| `Reliability_Index` | REAL | - | Historical vehicle-entity reliability aggregate. | Legacy numeric; original formula/scale unresolved; not current make reliability score. |
| `General_Enthusiast_Score` | REAL | - | Historical vehicle-entity enthusiasm aggregate. | Legacy numeric; original formula/scale unresolved. |
| `Sentiment_Volatility_StdDev` | REAL | - | Historical sentiment dispersion statistic named as a standard deviation. | Legacy numeric; input variable, weighting and ddof unresolved. |
| `Sentiment_Trend_Slope` | REAL | - | Historical sentiment trend slope. | Legacy numeric; time unit, fit window and estimator unresolved. |
| `Confidence_Level` | TEXT | - | Historical qualitative confidence label. | Legacy text; threshold definitions unresolved. |

**Indexes and uniqueness (observed):**

- No separate indexes declared; an INTEGER PRIMARY KEY, when present, still supplies the rowid key.

**Foreign keys (declared, not proof of valid enforcement):**

- None.

<a id="car_youtube_comments-youtube_comments_scored"></a>

### youtube_comments_scored

- **Purpose:** Aspect polarity, make attribution, weighting and inference provenance.
- **Row grain:** Intended one processing record per comment_id, including unattributable audit rows.
- **Producer/lineage:** absa_pipeline.py -> YouTubeCommentsDatabase.upsert_scored_comments.
- **Update/history behavior:** Incremental unseen comment IDs by default; explicit reprocessing upserts. Cleaning-rejected comments need not have a scored row. Unique index exists although declared PRIMARY KEY is absent.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `video_id` | TEXT | - | YouTube video identifier associated with the comment or fetch state. | Identifier text; logical join to video fetch state. |
| `playlist_id` | TEXT | - | Playlist through which a video/comment was discovered, when provided. | Nullable identifier; one stored playlist association is not full playlist membership. |
| `video_title` | TEXT | - | Video title supplied during discovery/comment ingestion. | Text snapshot; not a canonical vehicle identity. |
| `source` | TEXT | - | Provenance label assigned by YouTube ingestion and retained through scoring. | Current literal comment; not video/channel identity. |
| `text` | TEXT | - | Cleaned copy of raw comment text after URL/HTML removal, spam/short-comment filtering and punctuation normalization. | Free text; formula S1; raw content retained in original_text. |
| `extracted_at` | TEXT | - | Comment collection timestamp assigned before fetching, then reduced to a UTC calendar date by insert_sentiment_data. | Stored MM-DD-YYYY in current writer; original time-of-day lost. |
| `comment_id` | TEXT | - | YouTube comment-thread item.id used by this collector as its stored top-level comment identifier. | Nonempty thread identifier expected; deduplication/upsert key; not author identity or a separately extracted reply ID. |
| `author` | TEXT | - | YouTube author's display name at collection, not a unique account identifier. | Free text; potentially identifying; not a grouping key for unique people. |
| `like_count` | REAL | - | Likes observed for the top-level comment when collected. | Nonnegative count; scored writer fills missing with 0 and casts float. |
| `reply_count` | INTEGER | - | totalReplyCount reported for the comment thread. | Nonnegative count; reply texts are not ingested by this path. |
| `published_at` | TEXT | - | Comment publication time from YouTube publishedAt, reduced by the database writer to a UTC calendar date. | Current stored MM-DD-YYYY; legacy ISO also handled; not collection time. |
| `updated_at` | TEXT | - | YouTube comment updatedAt at collection, reduced to a UTC calendar date. | Current stored MM-DD-YYYY; old comments are not refreshed by insert-only raw persistence. |
| `Vehicle_Entity` | TEXT | - | Historical title-derived vehicle entity retained for compatibility; current make-grain pipeline does not populate it for new scoring. | Legacy text; not the canonical make/model/year/trim join key. |
| `original_text` | TEXT | - | Copy of raw text made before ABSA cleaning; not the YouTube textOriginal API property. | Free text; may retain content removed from scored text. |
| `reliability_sentiment` | REAL | - | ABSA polarity for reliability, aggregated across mentioned text chunks with confidence weights. | Numeric [-1,1]; NULL if aspect unmentioned; formula S1. |
| `reliability_mentioned` | INTEGER | - | Whether at least one text chunk meets the reliability mention threshold. | Integer 0/1; maximum positive/negative label score >= 0.40. |
| `reliability_confidence` | REAL | - | Maximum qualifying chunk confidence for reliability. | Numeric [0,1]; 0 if no mentioned chunk; not calibrated reliability. |
| `value_sentiment` | REAL | - | ABSA polarity for value, aggregated across mentioned text chunks with confidence weights. | Numeric [-1,1]; NULL if aspect unmentioned; formula S1. |
| `value_mentioned` | INTEGER | - | Whether at least one text chunk meets the value mention threshold. | Integer 0/1; maximum positive/negative label score >= 0.40. |
| `value_confidence` | REAL | - | Maximum qualifying chunk confidence for value. | Numeric [0,1]; 0 if no mentioned chunk; not calibrated reliability. |
| `performance_sentiment` | REAL | - | ABSA polarity for performance, aggregated across mentioned text chunks with confidence weights. | Numeric [-1,1]; NULL if aspect unmentioned; formula S1. |
| `performance_mentioned` | INTEGER | - | Whether at least one text chunk meets the performance mention threshold. | Integer 0/1; maximum positive/negative label score >= 0.40. |
| `performance_confidence` | REAL | - | Maximum qualifying chunk confidence for performance. | Numeric [0,1]; 0 if no mentioned chunk; not calibrated reliability. |
| `comfort_sentiment` | REAL | - | ABSA polarity for comfort, aggregated across mentioned text chunks with confidence weights. | Numeric [-1,1]; NULL if aspect unmentioned; formula S1. |
| `comfort_mentioned` | INTEGER | - | Whether at least one text chunk meets the comfort mention threshold. | Integer 0/1; maximum positive/negative label score >= 0.40. |
| `comfort_confidence` | REAL | - | Maximum qualifying chunk confidence for comfort. | Numeric [0,1]; 0 if no mentioned chunk; not calibrated reliability. |
| `consensus_weight` | REAL | - | 1 + log10(like_count + 1), after missing likes are replaced by zero. | Weight >= 1 for valid nonnegative likes; observed popularity, not statistical confidence. |
| `word_count` | INTEGER | - | Number of whitespace-separated words in cleaned text. | Nonnegative integer; len(str(text).split()). |
| `depth_weight` | REAL | - | Length multiplier applied to cleaned comments. | 1.2 when word_count >= 20; otherwise 1.0. |
| `comment_weight` | REAL | - | consensus_weight multiplied by depth_weight. | Positive aggregation weight; no upper cap in current code. |
| `Weighted_Reliability_Score` | REAL | - | reliability_sentiment multiplied by comment_weight. | Weighted contribution; may exceed [-1,1]; NULL when sentiment unavailable. |
| `Weighted_Value_Score` | REAL | - | value_sentiment multiplied by comment_weight. | Weighted contribution; may exceed [-1,1]; NULL when sentiment unavailable. |
| `Weighted_Performance_Score` | REAL | - | performance_sentiment multiplied by comment_weight. | Weighted contribution; may exceed [-1,1]; NULL when sentiment unavailable. |
| `Weighted_Comfort_Score` | REAL | - | comfort_sentiment multiplied by comment_weight. | Weighted contribution; may exceed [-1,1]; NULL when sentiment unavailable. |
| `processed_at` | TEXT | - | Time ABSA processing completed for the row. | UTC ISO-8601 timestamp; NULL possible for historical rows. |
| `model_name` | TEXT | - | Hugging Face model identifier used for aspect inference. | Current default facebook/bart-large-mnli; historical rows can differ. |
| `aspect_version` | TEXT | - | Version of aspect definitions and scoring implementation. | Current v3_make_grain_zero_shot; do not assume all existing rows use it. |
| `sentiment_make` | TEXT | - | One unambiguous canonical make attributed from comment text, otherwise video title. | Canonical make text; NULL excludes a scored row from make aggregates. |
| `make_attribution_source` | TEXT | - | Rule outcome identifying where the make came from or why attribution failed. | comment, video_title, ambiguous_comment, ambiguous_video_title, unknown. |
| `make_attribution_version` | TEXT | - | Version of make-alias and attribution rules used on the row. | Current make_attribution_v1; retain historical versions. |
| `overall_sentiment` | REAL | - | Confidence-weighted mean of usable mentioned aspect sentiments. | Numeric [-1,1]; NULL if no usable aspect; formula S1. |
| `overall_confidence` | REAL | - | Arithmetic mean of confidence across usable mentioned aspects. | Numeric [0,1]; NULL if none; not a calibrated probability of correctness. |
| `sentiment_status` | TEXT | - | ABSA/attribution state; only scored rows with non-NULL make enter aggregates. | scored, ambiguous_comment, ambiguous_video_title, unknown; ready is preprocessing-only. |
| `model_revision` | TEXT | - | Resolved model commit, requested revision, or provenance sentinel. | Commit/ref text; unresolved or legacy_unpinned explicitly indicates incomplete provenance. |

**Indexes and uniqueness (observed):**

- `idx_youtube_comments_scored_make`: nonunique (sentiment_make); origin=c, partial=0.
- `idx_youtube_comments_scored_comment_id_unique`: UNIQUE (comment_id); origin=c, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- None.

<a id="car_youtube_comments-youtube_comments_sentiment"></a>

### youtube_comments_sentiment

- **Purpose:** Raw comment corpus; despite its name this table contains no sentiment score.
- **Row grain:** One retained top-level comment per comment_id.
- **Producer/lineage:** SentimentAnalysis.fetch_comments -> YouTubeCommentsDatabase.insert_sentiment_data.
- **Update/history behavior:** New IDs only; existing text, likes and updates are not refreshed by raw insertion. Replies are counted but not collected as separate rows.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `video_id` | TEXT | - | YouTube video identifier associated with the comment or fetch state. | Identifier text; logical join to video fetch state. |
| `playlist_id` | TEXT | - | Playlist through which a video/comment was discovered, when provided. | Nullable identifier; one stored playlist association is not full playlist membership. |
| `video_title` | TEXT | - | Video title supplied during discovery/comment ingestion. | Text snapshot; not a canonical vehicle identity. |
| `source` | TEXT | - | Provenance label assigned by YouTube ingestion and retained through scoring. | Current literal comment; not video/channel identity. |
| `text` | TEXT | - | Top-level snippet.textOriginal when available, otherwise textDisplay, otherwise empty string; request uses plainText. | Free text; raw table retains collected text; see scored-table transformation. |
| `extracted_at` | TEXT | - | Comment collection timestamp assigned before fetching, then reduced to a UTC calendar date by insert_sentiment_data. | Stored MM-DD-YYYY in current writer; original time-of-day lost. |
| `comment_id` | TEXT | PK1 | YouTube comment-thread item.id used by this collector as its stored top-level comment identifier. | Nonempty thread identifier expected; deduplication/upsert key; not author identity or a separately extracted reply ID. |
| `author` | TEXT | - | YouTube author's display name at collection, not a unique account identifier. | Free text; potentially identifying; not a grouping key for unique people. |
| `like_count` | INTEGER | - | Likes observed for the top-level comment when collected. | Nonnegative count; scored writer fills missing with 0 and casts float. |
| `reply_count` | INTEGER | - | totalReplyCount reported for the comment thread. | Nonnegative count; reply texts are not ingested by this path. |
| `published_at` | TEXT | - | Comment publication time from YouTube publishedAt, reduced by the database writer to a UTC calendar date. | Current stored MM-DD-YYYY; legacy ISO also handled; not collection time. |
| `updated_at` | TEXT | - | YouTube comment updatedAt at collection, reduced to a UTC calendar date. | Current stored MM-DD-YYYY; old comments are not refreshed by insert-only raw persistence. |

**Indexes and uniqueness (observed):**

- `idx_youtube_comments_sentiment_video_id`: nonunique (video_id); origin=c, partial=0.
- `sqlite_autoindex_youtube_comments_sentiment_1`: UNIQUE (comment_id); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- None.

<a id="car_youtube_comments-youtube_playlist_fetch_state"></a>

### youtube_playlist_fetch_state

- **Purpose:** Resume playlist discovery and record failures.
- **Row grain:** One latest discovery state per playlist_id.
- **Producer/lineage:** YouTubeCommentsDatabase.upsert_playlist_discovery / mark_playlist_discovery_error.
- **Update/history behavior:** Upsert per playlist; not a history of every discovery attempt.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `playlist_id` | TEXT | PK1 | Playlist through which a video/comment was discovered, when provided. | Nullable identifier; one stored playlist association is not full playlist membership. |
| `last_discovered_at` | TEXT | - | Most recent playlist discovery operation timestamp, including a recorded error. | UTC ISO-8601 timestamp; inspect last_status. |
| `last_status` | TEXT | - | Most recent discovery/fetch outcome. | Fetch vocabulary F1; operational status, not sentiment. |
| `last_error` | TEXT | - | Error description for most recent operation when supplied. | Nullable diagnostic text; not a categorical feature. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_youtube_playlist_fetch_state_1`: UNIQUE (playlist_id); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- None.

<a id="car_youtube_comments-youtube_video_fetch_state"></a>

### youtube_video_fetch_state

- **Purpose:** Prioritize unseen videos and control refresh/backoff.
- **Row grain:** One latest fetch state per video_id.
- **Producer/lineage:** YouTubeCommentsDatabase.ensure_video_fetch_state / update_video_fetch_outcome.
- **Update/history behavior:** Upsert latest outcome; first discovery and last success can be retained. A single playlist_id does not encode many-to-many membership.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `video_id` | TEXT | PK1 | YouTube video identifier associated with the comment or fetch state. | Identifier text; logical join to video fetch state. |
| `playlist_id` | TEXT | - | Playlist through which a video/comment was discovered, when provided. | Nullable identifier; one stored playlist association is not full playlist membership. |
| `video_title` | TEXT | - | Video title supplied during discovery/comment ingestion. | Text snapshot; not a canonical vehicle identity. |
| `discovered_at` | TEXT | - | First stored discovery timestamp for a video, preserved on upsert. | UTC ISO-8601 timestamp. |
| `last_attempted_at` | TEXT | - | Timestamp assigned when the latest video fetch outcome is recorded. | UTC ISO-8601 timestamp; not necessarily request start time. |
| `last_succeeded_at` | TEXT | - | Most recent complete, zero_comments, or comments_disabled video outcome. | UTC ISO-8601; failure preserves previous success; NULL if never successful. |
| `last_status` | TEXT | - | Most recent discovery/fetch outcome. | Fetch vocabulary F1; operational status, not sentiment. |
| `last_error` | TEXT | - | Error description for most recent operation when supplied. | Nullable diagnostic text; not a categorical feature. |
| `comments_seen_count` | INTEGER | default=0 | Number of comments returned by the latest recorded fetch, including previously stored IDs. | Count; not number newly inserted or total lifetime comments; errors may record 0. |
| `next_eligible_at` | TEXT | - | Earliest scheduled retry or refresh time computed from the operation status. | UTC ISO-8601; current defaults 30-day refresh or 6-hour error backoff. |

**Indexes and uniqueness (observed):**

- `idx_youtube_video_fetch_state_playlist`: nonunique (playlist_id); origin=c, partial=0.
- `idx_youtube_video_fetch_state_next_eligible`: nonunique (next_eligible_at); origin=c, partial=0.
- `idx_youtube_video_fetch_state_status`: nonunique (last_status); origin=c, partial=0.
- `sqlite_autoindex_youtube_video_fetch_state_1`: UNIQUE (video_id); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- None.


<a id="car-data-nhtsadb"></a>

## CAR_DATA_NHTSA.db

Path: `CAR_DATA_OUTPUT/CAR_DATA_NHTSA.db`.

<a id="car_data_nhtsa-nhtsa_api_extra_fields"></a>

### nhtsa_api_extra_fields

- **Purpose:** Retain fields not mapped into typed recall/complaint/variant columns.
- **Row grain:** One unmapped source leaf per (query_id, record_type, record_key, field_name).
- **Producer/lineage:** NHTSADatabase._store_extra_fields.
- **Update/history behavior:** INSERT OR REPLACE; current helper excludes known root names. Empty at inspection; no source vocabulary observed.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `query_id` | INTEGER | PK1, NN | Identifier of one persisted NHTSA vehicle-query response. | Integer key; never treat as a VIN identifier. |
| `record_type` | TEXT | PK2, NN | Kind of API record contributing an unmapped extra field. | safety_variant, recall, complaint in current writer. |
| `record_key` | TEXT | PK3, NN | Parent source-row key, or stringified VehicleId for safety_variant extra fields. | Join with query_id and record_type; no declared FK to the specific record table. |
| `field_name` | TEXT | PK4, NN | Exact source field name or flattened source path. | Case-sensitive label; dots for nested objects and [index] for array items. |
| `field_value` | TEXT | - | Source leaf value converted to text; None becomes SQL NULL. | Dynamic logical type/unit follows field_name; parse explicitly; no raw JSON wrapper. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_api_extra_fields_1`: UNIQUE (query_id, record_type, record_key, field_name); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (query_id) -> `nhtsa_vehicle_queries` (query_id); ON UPDATE NO ACTION, ON DELETE CASCADE.

<a id="car_data_nhtsa-nhtsa_bulk_datasets"></a>

### nhtsa_bulk_datasets

- **Purpose:** Bulk import lineage and processed record count.
- **Row grain:** One imported file registration per dataset_id; unique (dataset_name, checksum).
- **Producer/lineage:** NHTSADatabase.import_bulk_file.
- **Update/history behavior:** INSERT OR IGNORE registration; update row_count after import. Empty at inspection; source files can be CSV, JSON, ZIP or line-based fallback.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `dataset_id` | INTEGER | PK1, rowid key | Identifier of a bulk file import keyed by dataset name and file checksum. | Integer; foreign key for normalized bulk records. |
| `dataset_name` | TEXT | NN | Caller-supplied logical name for the imported bulk dataset. | Nonempty text expected; no fixed vocabulary enforced. |
| `source_url` | TEXT | - | Caller-supplied bulk dataset download URL. | Nullable/empty text; may be absent for local files. |
| `source_file` | TEXT | NN | Path of the imported bulk source file. | File path text; can be machine-specific; not a portable download link. |
| `source_version` | TEXT | - | Optional caller-supplied source version. | Nullable text; NULL does not mean latest version was verified. |
| `checksum` | TEXT | NN | SHA-256 of a bulk source file, or optional source-catalog checksum. | Hex digest when supplied; source-catalog value can be NULL. |
| `loaded_at` | TEXT | NN | Time the bulk dataset registration was inserted. | UTC ISO-8601; repeated identical import can retain prior registration time. |
| `row_count` | INTEGER | NN, default=0 | Number of source records processed by the bulk import. | Nonnegative count; not number of field rows or a completeness guarantee after interruption. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_bulk_datasets_1`: UNIQUE (dataset_name, checksum); origin=u, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- None.

<a id="car_data_nhtsa-nhtsa_bulk_fields"></a>

### nhtsa_bulk_fields

- **Purpose:** Normalized bulk record contents.
- **Row grain:** One flattened source leaf per (dataset_id, source_row_number, field_name).
- **Producer/lineage:** NHTSADatabase.import_bulk_file -> _flatten_fields.
- **Update/history behavior:** INSERT OR REPLACE in batches. Empty at inspection. Unrecognized input formats currently use a raw_line field; structured interpretation then remains unresolved.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `dataset_id` | INTEGER | PK1, NN | Identifier of a bulk file import keyed by dataset name and file checksum. | Integer; foreign key for normalized bulk records. |
| `source_row_number` | INTEGER | PK2, NN | One-based sequence of source records across a bulk import, including concatenated ZIP members. | Integer >= 1; CSV header excluded; not necessarily physical file line number. |
| `field_name` | TEXT | PK3, NN | Exact source field name or flattened source path. | Case-sensitive label; dots for nested objects and [index] for array items. |
| `field_value` | TEXT | - | Source leaf value converted to text; None becomes SQL NULL. | Dynamic logical type/unit follows field_name; parse explicitly; no raw JSON wrapper. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_bulk_fields_1`: UNIQUE (dataset_id, source_row_number, field_name); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (dataset_id, source_row_number) -> `nhtsa_bulk_rows` (dataset_id, source_row_number); ON UPDATE NO ACTION, ON DELETE CASCADE.

<a id="car_data_nhtsa-nhtsa_bulk_rows"></a>

### nhtsa_bulk_rows

- **Purpose:** Record identity/hash for bulk imports.
- **Row grain:** One source record per (dataset_id, source_row_number).
- **Producer/lineage:** NHTSADatabase.import_bulk_file.
- **Update/history behavior:** Incremental batches of 2000, INSERT OR IGNORE. Empty at inspection; no raw record JSON stored in this table.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `dataset_id` | INTEGER | PK1, NN | Identifier of a bulk file import keyed by dataset name and file checksum. | Integer; foreign key for normalized bulk records. |
| `source_row_number` | INTEGER | PK2, NN | One-based sequence of source records across a bulk import, including concatenated ZIP members. | Integer >= 1; CSV header excluded; not necessarily physical file line number. |
| `row_hash` | TEXT | NN | SHA-256 of stable sorted-key JSON serialization of the source record. | Hex digest; content identifier, not an enforced unique constraint. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_bulk_rows_1`: UNIQUE (dataset_id, source_row_number); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (dataset_id) -> `nhtsa_bulk_datasets` (dataset_id); ON UPDATE NO ACTION, ON DELETE CASCADE.

<a id="car_data_nhtsa-nhtsa_complaint_products"></a>

### nhtsa_complaint_products

- **Purpose:** Products associated with a complaint record.
- **Row grain:** One product object per (query_id, record_key, product_index).
- **Producer/lineage:** NHTSADatabase.store_complaints, enumerating products array.
- **Update/history behavior:** INSERT OR REPLACE; zero-based array position; join using both query_id and record_key before product_index.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `query_id` | INTEGER | PK1, NN | Identifier of one persisted NHTSA vehicle-query response. | Integer key; never treat as a VIN identifier. |
| `record_key` | TEXT | PK2, NN | Identifier of the parent source record within a query response. | Current recall/complaint writer uses full-row SHA-256; historical keys may differ. |
| `product_index` | INTEGER | PK3, NN | Zero-based position of product within complaint products array. | Integer >= 0; gaps possible when non-object entries are skipped. |
| `product_type` | TEXT | - | products[].type in the complaint response. | Nullable source category text. |
| `product_year` | INTEGER | - | products[].productYear converted to integer. | Nullable model/product year; no implicit join to listing year. |
| `product_make` | TEXT | - | products[].productMake in the complaint response. | Nullable source make text. |
| `product_model` | TEXT | - | products[].productModel in the complaint response. | Nullable source model text. |
| `manufacturer` | TEXT | - | Manufacturer name from the associated NHTSA record. | Nullable source text; not always equal to make. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_complaint_products_1`: UNIQUE (query_id, record_key, product_index); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (query_id, record_key) -> `nhtsa_complaints` (query_id, record_key); ON UPDATE NO ACTION, ON DELETE CASCADE.

<a id="car_data_nhtsa-nhtsa_complaints"></a>

### nhtsa_complaints

- **Purpose:** Consumer complaint evidence for a make/model/year query.
- **Row grain:** One distinct retained source complaint row per (query_id, record_key).
- **Producer/lineage:** NHTSADatabase.store_complaints.
- **Update/history behavior:** Current working-tree writer fingerprints the full source row. Older keys may use ODI number. Reports are not validated VIN-level failure rates.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `query_id` | INTEGER | PK1, NN | Identifier of one persisted NHTSA vehicle-query response. | Integer key; never treat as a VIN identifier. |
| `record_key` | TEXT | PK2, NN | Identifier of the parent source record within a query response. | Current recall/complaint writer uses full-row SHA-256; historical keys may differ. |
| `odi_number` | TEXT | - | ODI complaint/case number from odiNumber. | Text identifier; case ID can repeat across source rows/queries. |
| `manufacturer` | TEXT | - | Manufacturer name from the associated NHTSA record. | Nullable source text; not always equal to make. |
| `crash` | TEXT | - | Source complaint crash indicator. | Text representation; unknown differs from false. |
| `fire` | TEXT | - | Source complaint fire indicator. | Text representation; unknown differs from false. |
| `number_of_injuries` | INTEGER | - | Reported numberOfInjuries converted with int(float(value)). | Nonnegative count expected; missing/invalid converts to NULL in normalized store. |
| `number_of_deaths` | INTEGER | - | Reported numberOfDeaths converted with int(float(value)). | Nonnegative count expected; missing/invalid converts to NULL in normalized store. |
| `date_of_incident` | TEXT | - | Complaint dateOfIncident, as reported by source. | Source date text; not date fetched or complaint filed. |
| `date_complaint_filed` | TEXT | - | Complaint dateComplaintFiled, as reported by source. | Source date text; not necessarily incident date. |
| `vin` | TEXT | - | VIN supplied inside the complaint record; can be masked, partial, or absent and is not the queried listing VIN. | Source text; do not automatically equate to listing VIN. |
| `components` | TEXT | - | Complaint components field, preserved as source text. | Source list/category text; not a normalized component dimension. |
| `summary` | TEXT | - | Narrative summary of the recall or complaint source record. | Free text; complaint narratives may include identifying details. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_complaints_1`: UNIQUE (query_id, record_key); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (query_id) -> `nhtsa_vehicle_queries` (query_id); ON UPDATE NO ACTION, ON DELETE CASCADE.

<a id="car_data_nhtsa-nhtsa_ingestion_runs"></a>

### nhtsa_ingestion_runs

- **Purpose:** Run lifecycle and counts, including cache skips.
- **Row grain:** One ingestion invocation per run_id.
- **Producer/lineage:** NHTSADatabase.start_run / finish_run.
- **Update/history behavior:** Insert running, then update outcome and counts; interruption may leave running. Counts are VIN-level processing counts.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `run_id` | TEXT | PK1 | Identifier linking records to an NHTSA ingestion run; generated as UUID hex by start_run. | Text identifier; run metadata is operational. |
| `source` | TEXT | NN | Enrichment workflow label passed to start_run. | Current nhtsa_vehicle_enrichment; helper default nhtsa. |
| `mode` | TEXT | NN | Requested enrichment mode for the run. | incremental or refresh_all in current enrichment entry point. |
| `started_at` | TEXT | NN | Time the ingestion run was created. | UTC ISO-8601 timestamp. |
| `completed_at` | TEXT | - | Time finish_run recorded the run outcome. | UTC ISO-8601; NULL while running or after interruption before finalization. |
| `status` | TEXT | NN | Ingestion run lifecycle outcome. | running, completed, completed_with_errors, failed. |
| `requested_count` | INTEGER | default=0 | Number of VIN contexts selected for the run before freshness skips. | Count; includes cached/skipped contexts. |
| `successful_count` | INTEGER | default=0 | Successfully processed VIN contexts plus successful cache skips. | Count; does not certify all safety/recall/complaint requests succeeded. |
| `failed_count` | INTEGER | default=0 | VIN contexts that failed decoding/processing under the run accounting. | Count; not a count of every downstream API failure. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_ingestion_runs_1`: UNIQUE (run_id); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- None.

<a id="car_data_nhtsa-nhtsa_recalls"></a>

### nhtsa_recalls

- **Purpose:** Recall records returned for a make/model/year query.
- **Row grain:** One distinct retained source recall row per (query_id, record_key).
- **Producer/lineage:** NHTSADatabase.store_recalls.
- **Update/history behavior:** Current working-tree writer fingerprints the full source row. Older data may use campaign/action keys. Multiple query snapshots can repeat campaigns.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `query_id` | INTEGER | PK1, NN | Identifier of one persisted NHTSA vehicle-query response. | Integer key; never treat as a VIN identifier. |
| `record_key` | TEXT | PK2, NN | Identifier of the parent source record within a query response. | Current recall/complaint writer uses full-row SHA-256; historical keys may differ. |
| `manufacturer` | TEXT | - | Manufacturer name from the associated NHTSA record. | Nullable source text; not always equal to make. |
| `nhtsa_campaign_number` | TEXT | - | NHTSACampaignNumber from a recall record. | Text identifier; not guaranteed unique across response rows/queries. |
| `nhtsa_action_number` | TEXT | - | NHTSAActionNumber from a recall record. | Nullable text identifier; not a row key. |
| `report_received_date` | TEXT | - | ReportReceivedDate from the recall record. | Source date string; no standardized parsing on persistence. |
| `component` | TEXT | - | Recall Component field. | Source category/text; may contain multiple components. |
| `model_year` | INTEGER | - | Model year returned in recall data or supplied in query context. | Nullable integer; typed API conversion can yield NULL. |
| `make` | TEXT | - | Make returned in recall data or supplied in query context. | Source text; query context comes from resolved identity. |
| `model` | TEXT | - | Model returned in recall data or supplied in query context. | Source text; not a trim-level identity. |
| `park_it` | TEXT | - | Recall parkIt field, preserved as source text. | Source flag; preserve absent/unknown separately from false. |
| `park_outside` | TEXT | - | Recall parkOutSide field, preserved as source text. | Source flag; preserve absent/unknown separately from false. |
| `over_the_air_update` | TEXT | - | Recall overTheAirUpdate field. | Source text/flag; not proof an individual vehicle received an update. |
| `summary` | TEXT | - | Narrative summary of the recall or complaint source record. | Free text; complaint narratives may include identifying details. |
| `consequence` | TEXT | - | Recall Consequence narrative describing potential effects of the defect. | Nullable free text. |
| `remedy` | TEXT | - | Recall Remedy narrative describing corrective action. | Nullable free text; not VIN-specific completion status. |
| `notes` | TEXT | - | Recall Notes text, preserved from the source. | Nullable free text. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_recalls_1`: UNIQUE (query_id, record_key); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (query_id) -> `nhtsa_vehicle_queries` (query_id); ON UPDATE NO ACTION, ON DELETE CASCADE.

<a id="car_data_nhtsa-nhtsa_safety_details"></a>

### nhtsa_safety_details

- **Purpose:** Marks stored detail for a variant; actual fields are in nhtsa_safety_rating_values.
- **Row grain:** One detail marker per (query_id, vehicle_id).
- **Producer/lineage:** NHTSADatabase.store_safety_detail.
- **Update/history behavior:** INSERT OR REPLACE; query_id references the parent safety_variants query, not the separate safety_detail metadata query.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `query_id` | INTEGER | PK1, NN | Identifier of one persisted NHTSA vehicle-query response. | Integer key; never treat as a VIN identifier. |
| `vehicle_id` | INTEGER | PK2, NN | NHTSA SafetyRatings VehicleId identifying a rated vehicle variant. | Integer identifier; not a VIN; retain query_id for history. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_safety_details_1`: UNIQUE (query_id, vehicle_id); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (query_id, vehicle_id) -> `nhtsa_safety_variants` (query_id, vehicle_id); ON UPDATE NO ACTION, ON DELETE CASCADE.

<a id="car_data_nhtsa-nhtsa_safety_rating_values"></a>

### nhtsa_safety_rating_values

- **Purpose:** Ratings, flags, counts and media references in normalized field/value form.
- **Row grain:** One detail leaf per (query_id, vehicle_id, field_name).
- **Producer/lineage:** NHTSADatabase.store_safety_detail -> _flatten_fields.
- **Update/history behavior:** INSERT OR REPLACE; all source leaf fields stored as TEXT. Parent query_id is the variant lookup; see field vocabulary below.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `query_id` | INTEGER | PK1, NN | Identifier of one persisted NHTSA vehicle-query response. | Integer key; never treat as a VIN identifier. |
| `vehicle_id` | INTEGER | PK2, NN | NHTSA SafetyRatings VehicleId identifying a rated vehicle variant. | Integer identifier; not a VIN; retain query_id for history. |
| `field_name` | TEXT | PK3, NN | Exact source field name or flattened source path. | Case-sensitive label; dots for nested objects and [index] for array items. |
| `field_value` | TEXT | - | Source leaf value converted to text; None becomes SQL NULL. | Dynamic logical type/unit follows field_name; parse explicitly; no raw JSON wrapper. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_safety_rating_values_1`: UNIQUE (query_id, vehicle_id, field_name); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (query_id, vehicle_id) -> `nhtsa_safety_variants` (query_id, vehicle_id); ON UPDATE NO ACTION, ON DELETE CASCADE.

<a id="car_data_nhtsa-nhtsa_safety_variants"></a>

### nhtsa_safety_variants

- **Purpose:** Variants returned by a model year/make/model safety lookup.
- **Row grain:** One rated variant per (query_id, vehicle_id).
- **Producer/lineage:** NHTSADatabase.store_safety_variants.
- **Update/history behavior:** INSERT OR REPLACE; query_id is the safety_variants lookup. Multiple variants may correspond to one listing identity.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `query_id` | INTEGER | PK1, NN | Identifier of one persisted NHTSA vehicle-query response. | Integer key; never treat as a VIN identifier. |
| `vehicle_id` | INTEGER | PK2, NN | NHTSA SafetyRatings VehicleId identifying a rated vehicle variant. | Integer identifier; not a VIN; retain query_id for history. |
| `vehicle_description` | TEXT | - | NHTSA VehicleDescription, with VehicleDescriptionName fallback, for a rated variant. | Source text; may distinguish body/drivetrain variants. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_safety_variants_1`: UNIQUE (query_id, vehicle_id); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (query_id) -> `nhtsa_vehicle_queries` (query_id); ON UPDATE NO ACTION, ON DELETE CASCADE.

<a id="car_data_nhtsa-nhtsa_schema_meta"></a>

### nhtsa_schema_meta

- **Purpose:** Identify normalized NHTSA schema format.
- **Row grain:** One schema setting per key.
- **Producer/lineage:** NHTSADatabase._init_db.
- **Update/history behavior:** Upsert schema_version; observed value 2. Metadata only.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `key` | TEXT | PK1 | NHTSA schema metadata setting name. | Current known key schema_version. |
| `value` | TEXT | NN | Value of NHTSA schema metadata setting. | Text; current schema_version is 2. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_schema_meta_1`: UNIQUE (key); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- None.

<a id="car_data_nhtsa-nhtsa_source_catalog"></a>

### nhtsa_source_catalog

- **Purpose:** Endpoint/version/checksum provenance catalog.
- **Row grain:** One registered source per source_name.
- **Producer/lineage:** NHTSADataEnricher.__init__ -> NHTSADatabase.register_source.
- **Update/history behavior:** Upsert on registration; supplied metadata may be NULL. Not an immutable source-version history.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `source_name` | TEXT | PK1 | Unique registered source label. | vpic_decode_values_extended, vpic_decode_values_batch, safety_ratings, recalls, complaints in current registration. |
| `source_type` | TEXT | NN | Registered source category. | api for currently registered API sources; open text. |
| `endpoint_or_url` | TEXT | - | Source endpoint recorded by register_source. | URL text; descriptive provenance, not response content. |
| `source_version` | TEXT | - | Optional caller-supplied source version. | Nullable text; NULL does not mean latest version was verified. |
| `last_seen_at` | TEXT | - | Most recent source registration time. | UTC ISO-8601; not last successful response time. |
| `checksum` | TEXT | - | Optional checksum metadata supplied when a source is registered. | Nullable text; no automatic content hashing in register_source. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_source_catalog_1`: UNIQUE (source_name); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- None.

<a id="car_data_nhtsa-nhtsa_vehicle_queries"></a>

### nhtsa_vehicle_queries

- **Purpose:** History/cache for safety variant/detail, recall and complaint lookups.
- **Row grain:** One retained response metadata record per query_id.
- **Producer/lineage:** NHTSADataEnricher._query_mmy -> NHTSADatabase.store_vehicle_query.
- **Update/history behavior:** Deduplicated on query_type, query_key and response_hash. Source query grain is model year/make/model, or SafetyRatings vehicle variant.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `query_id` | INTEGER | PK1, rowid key | Identifier of one persisted NHTSA vehicle-query response. | Integer key; never treat as a VIN identifier. |
| `run_id` | TEXT | - | Identifier linking records to an NHTSA ingestion run; generated as UUID hex by start_run. | Text identifier; run metadata is operational. |
| `query_type` | TEXT | NN | NHTSA operation category. | safety_variants, safety_detail, recalls, complaints. |
| `query_key` | TEXT | NN | Normalized lookup key built from resolved year, make and model, plus vehicle ID for safety details. | Pipe-delimited YEAR/MAKE/MODEL; see query-key rule N2. |
| `make` | TEXT | - | Make returned in recall data or supplied in query context. | Source text; query context comes from resolved identity. |
| `model` | TEXT | - | Model returned in recall data or supplied in query context. | Source text; not a trim-level identity. |
| `model_year` | INTEGER | - | Model year returned in recall data or supplied in query context. | Nullable integer; typed API conversion can yield NULL. |
| `vehicle_id` | INTEGER | - | NHTSA SafetyRatings VehicleId identifying a rated vehicle variant. | Integer identifier; not a VIN; retain query_id for history. |
| `response_status` | TEXT | NN | Application-level outcome of the stored API operation. | NHTSA vocabulary N1; distinct from HTTP status and vPIC ErrorCode. |
| `http_status` | INTEGER | - | HTTP response status when known. | Nullable integer, e.g. 200; NULL when no HTTP response/format rejection. |
| `error_text` | TEXT | - | vPIC error description or caught request/validation error. | Nullable diagnostic text; inspect with response_status. |
| `record_count` | INTEGER | NN, default=0 | Number of elements in Results/results at query storage time. | Nonnegative count; 0 with failure is not evidence of no recalls/complaints. |
| `response_hash` | TEXT | NN | SHA-256 of stable sorted-key JSON serialization used internally for content deduplication. | 64-character hex; decoded result for vPIC, Results/results list for vehicle queries; blobs not stored. |
| `fetched_at` | TEXT | NN | UTC time assigned when the response record is inserted. | ISO-8601; deduplicated responses can retain earlier timestamps. |

**Indexes and uniqueness (observed):**

- `idx_nhtsa_vehicle_query_key`: nonunique (query_type, query_key, fetched_at); origin=c, partial=0.
- `sqlite_autoindex_nhtsa_vehicle_queries_1`: UNIQUE (query_type, query_key, response_hash); origin=u, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (run_id) -> `nhtsa_ingestion_runs` (run_id); ON UPDATE NO ACTION, ON DELETE NO ACTION.

<a id="car_data_nhtsa-nhtsa_vin_identity_resolution"></a>

### nhtsa_vin_identity_resolution

- **Purpose:** Audit field-level NHTSA versus listing fallback, completeness and conflicts.
- **Row grain:** One retained identity resolution per identity_id.
- **Producer/lineage:** NHTSADataEnricher._resolve_identity -> NHTSADatabase.store_identity_resolution.
- **Update/history behavior:** Content-oriented unique index/INSERT OR IGNORE; NULL components can allow repeats. Not the cleaned canonical identity table or title-trim parser.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `identity_id` | INTEGER | PK1, rowid key | SQLite-generated identifier for a persisted identity resolution. | INTEGER PRIMARY KEY AUTOINCREMENT. |
| `run_id` | TEXT | - | Identifier linking records to an NHTSA ingestion run; generated as UUID hex by start_run. | Text identifier; run metadata is operational. |
| `decode_id` | INTEGER | - | Identifier of a persisted vPIC decode record. | Integer key; multiple decode records can exist for one VIN. |
| `vin` | TEXT | NN | Listing VIN or vehicle identifier supplied by acquisition; enrichment normalizes case/whitespace. | Identifier text; partial/masked values may exist; VIN rule V1. |
| `nhtsa_make` | TEXT | - | Uppercased make from decoded Make before field-level fallback. | Nullable text; source identity anchor. |
| `nhtsa_model` | TEXT | - | Uppercased model from decoded Model before field-level fallback. | Nullable text; source identity anchor. |
| `nhtsa_model_year` | INTEGER | - | Parsed model year from decoded ModelYear before fallback. | Nullable integer year, 1886..2100 in parser. |
| `listing_make` | TEXT | - | Make supplied by listing context or inferred from title for enrichment fallback. | Uppercased nullable text; inference is not an NHTSA result. |
| `listing_model` | TEXT | - | Model supplied by listing context or first remaining title token after removing make/year. | Uppercased nullable text; heuristic can be incomplete. |
| `listing_model_year` | INTEGER | - | Model year from listing context or a matching title year. | Nullable integer; title-regex and numeric parser have different accepted ranges. |
| `resolved_make` | TEXT | - | NHTSA make if usable, otherwise listing make. | Nullable uppercased text; see make_source. |
| `resolved_model` | TEXT | - | NHTSA model if usable, otherwise listing model. | Nullable uppercased text; see model_source. |
| `resolved_model_year` | INTEGER | - | NHTSA model year if usable, otherwise listing model year. | Nullable integer; see model_year_source. |
| `make_source` | TEXT | NN | Origin of resolved_make after field-level fallback. | nhtsa_decode, listing, unknown. |
| `model_source` | TEXT | NN | Origin of resolved_model after field-level fallback. | nhtsa_decode, listing, unknown. |
| `model_year_source` | TEXT | NN | Origin of resolved_model_year after field-level fallback. | nhtsa_decode, listing, unknown. |
| `confidence` | TEXT | NN | Rule-based identity completeness grade, not probabilistic confidence. | high, medium, low, unknown; rule I1. |
| `conflict_flag` | INTEGER | NN, default=0 | Whether any usable listing and NHTSA identity fields disagree after normalization. | Integer 0/1; high confidence can coexist with conflict=1. |
| `resolved_at` | TEXT | NN | Timestamp assigned during identity resolution persistence. | UTC ISO-8601; deduplicated identity can retain earlier timestamp. |

**Indexes and uniqueness (observed):**

- `idx_nhtsa_identity_distinct`: UNIQUE (vin, decode_id, resolved_make, resolved_model, resolved_model_year, confidence, conflict_flag); origin=c, partial=0.
- `idx_nhtsa_identity_vin`: nonunique (vin, resolved_at); origin=c, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (decode_id) -> `nhtsa_vpic_decodes` (decode_id); ON UPDATE NO ACTION, ON DELETE NO ACTION.
- (run_id) -> `nhtsa_ingestion_runs` (run_id); ON UPDATE NO ACTION, ON DELETE NO ACTION.

<a id="car_data_nhtsa-nhtsa_vpic_decodes"></a>

### nhtsa_vpic_decodes

- **Purpose:** VIN decode history, request context and diagnostics.
- **Row grain:** One retained decode result/failure metadata record per decode_id.
- **Producer/lineage:** NHTSADatabase.store_vpic_result / store_vpic_failure.
- **Update/history behavior:** Content-deduplicated insert using vin, model_year_hint and response_hash; NULL hint weakens SQL uniqueness. Not a log of every identical request.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `decode_id` | INTEGER | PK1, rowid key | Identifier of a persisted vPIC decode record. | Integer key; multiple decode records can exist for one VIN. |
| `run_id` | TEXT | - | Identifier linking records to an NHTSA ingestion run; generated as UUID hex by start_run. | Text identifier; run metadata is operational. |
| `vin` | TEXT | NN | Listing VIN or vehicle identifier supplied by acquisition; enrichment normalizes case/whitespace. | Identifier text; partial/masked values may exist; VIN rule V1. |
| `model_year_hint` | INTEGER | - | Model-year hint supplied with the decode request. | Nullable integer; current parser accepts 1886..2100; not necessarily decoded year. |
| `endpoint` | TEXT | NN | vPIC method label used for decode provenance. | Text, e.g. DecodeVINValuesBatch; may be a method name rather than full URL. |
| `response_status` | TEXT | NN | Application-level outcome of the stored API operation. | NHTSA vocabulary N1; distinct from HTTP status and vPIC ErrorCode. |
| `http_status` | INTEGER | - | HTTP response status when known. | Nullable integer, e.g. 200; NULL when no HTTP response/format rejection. |
| `error_code` | TEXT | - | vPIC ErrorCode copied from decoded result. | Source text, potentially multiple codes; not an HTTP status code. |
| `error_text` | TEXT | - | vPIC error description or caught request/validation error. | Nullable diagnostic text; inspect with response_status. |
| `message` | TEXT | - | Top-level vPIC Message/message from response wrapper. | Nullable source text; wrapper itself is not retained. |
| `response_hash` | TEXT | NN | SHA-256 of stable sorted-key JSON serialization used internally for content deduplication. | 64-character hex; decoded result for vPIC, Results/results list for vehicle queries; blobs not stored. |
| `fetched_at` | TEXT | NN | UTC time assigned when the response record is inserted. | ISO-8601; deduplicated responses can retain earlier timestamps. |

**Indexes and uniqueness (observed):**

- `idx_nhtsa_vpic_latest`: nonunique (vin, model_year_hint, fetched_at); origin=c, partial=0.
- `sqlite_autoindex_nhtsa_vpic_decodes_1`: UNIQUE (vin, model_year_hint, response_hash); origin=u, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (run_id) -> `nhtsa_ingestion_runs` (run_id); ON UPDATE NO ACTION, ON DELETE NO ACTION.

<a id="car_data_nhtsa-nhtsa_vpic_values"></a>

### nhtsa_vpic_values

- **Purpose:** Flat/flattened vPIC result fields, including minimal VIN/ErrorText values for recorded failures.
- **Row grain:** At most one wide decoded-value row per decode_id.
- **Producer/lineage:** NHTSADatabase.store_vpic_result and _flatten_fields.
- **Update/history behavior:** Upsert by decode_id; dynamically adds TEXT columns for new fields. Current failure helper also writes a minimal values row; presence alone does not prove success. 154 source fields observed.

vPIC variable IDs below refer to the [official variable definitions](https://vpic.nhtsa.dot.gov/api/vehicles/GetVehicleVariableList?format=json), retrieved 2026-09-15. Source lookup categories are expandable through `GetVehicleVariableValuesList/{variable_id}?format=json`; the dictionary does not claim a frozen exhaustive category list. Fields without a current variable mapping are explicitly identified.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `decode_id` | INTEGER | PK1, rowid key | Identifier of a persisted vPIC decode record. | Integer key; multiple decode records can exist for one VIN. |
| `VIN` | TEXT | - | VIN echoed by the vPIC response, distinct from requested vin in decode metadata. | Identifier text; partial/masked response possible. |
| `ErrorText` | TEXT | - | vPIC decode diagnostic description. vPIC variable 191. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `ABS` | TEXT | - | Antilock braking equipment that controls wheel slip during braking. vPIC variable 86. | Source categorical label; official value list for variable 86; blank/NULL is not absence. |
| `ActiveSafetySysNote` | TEXT | - | Additional source information about active safety systems. vPIC variable 169. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `AdaptiveCruiseControl` | TEXT | - | Cruise control that adjusts speed to maintain a selected following distance. vPIC variable 81. | Source categorical label; official value list for variable 81; blank/NULL is not absence. |
| `AdaptiveDrivingBeam` | TEXT | - | Headlamp system that adapts the upper-beam pattern around other road users. vPIC variable 180. | Source categorical label; official value list for variable 180; blank/NULL is not absence. |
| `AdaptiveHeadlights` | TEXT | - | Legacy/source AdaptiveHeadlights property retained as delivered. | Source category text; current variable-list mapping unresolved; do not equate automatically to AdaptiveDrivingBeam. |
| `AdditionalErrorText` | TEXT | - | Additional decode diagnostic information beyond the main error text. vPIC variable 156. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `AirBagLocCurtain` | TEXT | - | Occupant positions/rows with curtain airbags. vPIC variable 55. | Source categorical label; official value list for variable 55; blank/NULL is not absence. |
| `AirBagLocFront` | TEXT | - | Occupant positions/rows with frontal airbags. vPIC variable 65. | Source categorical label; official value list for variable 65; blank/NULL is not absence. |
| `AirBagLocKnee` | TEXT | - | Occupant positions/rows with knee airbags. vPIC variable 69. | Source categorical label; official value list for variable 69; blank/NULL is not absence. |
| `AirBagLocSeatCushion` | TEXT | - | Occupant positions/rows with seat-cushion airbags. vPIC variable 56. | Source categorical label; official value list for variable 56; blank/NULL is not absence. |
| `AirBagLocSide` | TEXT | - | Occupant positions/rows with side airbags. vPIC variable 107. | Source categorical label; official value list for variable 107; blank/NULL is not absence. |
| `AutoReverseSystem` | TEXT | - | Window/sunroof closing system that reverses when an obstruction is detected. vPIC variable 172. | Source categorical label; official value list for variable 172; blank/NULL is not absence. |
| `AutomaticPedestrianAlertingSound` | TEXT | - | External alert sound equipment for hybrid/electric vehicle pedestrian awareness. vPIC variable 173. | Source categorical label; official value list for variable 173; blank/NULL is not absence. |
| `AxleConfiguration` | TEXT | - | Vehicle axle arrangement/configuration reported by the source. vPIC variable 145. | Source categorical label; official value list for variable 145; blank/NULL is not absence. |
| `Axles` | TEXT | - | Number of vehicle axles. vPIC variable 41. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `BasePrice` | TEXT | - | Manufacturer base price supplied by vPIC; not observed market price. vPIC variable 136. | Numeric text; US dollars; manufacturer base price, not listing price; parse without replacing missing with zero. |
| `BatteryA` | TEXT | - | Lower bound of reported battery current. vPIC variable 57. | Numeric text; amperes; lower bound; parse without replacing missing with zero. |
| `BatteryA_to` | TEXT | - | Upper bound of reported battery current. vPIC variable 132. | Numeric text; amperes; upper bound; parse without replacing missing with zero. |
| `BatteryCells` | TEXT | - | Number of battery cells per module. vPIC variable 48. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `BatteryInfo` | TEXT | - | Additional battery details not represented by dedicated battery fields. vPIC variable 1. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `BatteryKWh` | TEXT | - | Lower bound of reported battery energy capacity. vPIC variable 59. | Numeric text; kWh; lower bound; parse without replacing missing with zero. |
| `BatteryKWh_to` | TEXT | - | Upper bound of reported battery energy capacity. vPIC variable 134. | Numeric text; kWh; upper bound; parse without replacing missing with zero. |
| `BatteryModules` | TEXT | - | Number of battery modules per pack. vPIC variable 137. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `BatteryPacks` | TEXT | - | Number of battery packs per vehicle. vPIC variable 138. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `BatteryType` | TEXT | - | Battery chemistry/type reported by the source. vPIC variable 2. | Source categorical label; official value list for variable 2; blank/NULL is not absence. |
| `BatteryV` | TEXT | - | Lower bound of reported battery voltage. vPIC variable 58. | Numeric text; volts; lower bound; parse without replacing missing with zero. |
| `BatteryV_to` | TEXT | - | Upper bound of reported battery voltage. vPIC variable 133. | Numeric text; volts; upper bound; parse without replacing missing with zero. |
| `BedLengthIN` | TEXT | - | Length of the pickup cargo bed. vPIC variable 49. | Numeric text; inches; parse without replacing missing with zero. |
| `BedType` | TEXT | - | Pickup cargo-bed configuration. vPIC variable 3. | Source categorical label; official value list for variable 3; blank/NULL is not absence. |
| `BlindSpotIntervention` | TEXT | - | System that can intervene with braking or steering to avoid a blind-spot collision. vPIC variable 193. | Source categorical label; official value list for variable 193; blank/NULL is not absence. |
| `BlindSpotMon` | TEXT | - | Blind-spot warning equipment that alerts the driver to adjacent vehicles. vPIC variable 88. | Source categorical label; official value list for variable 88; blank/NULL is not absence. |
| `BodyCabType` | TEXT | - | Truck cab configuration, including passenger-space and door arrangement. vPIC variable 4. | Source categorical label; official value list for variable 4; blank/NULL is not absence. |
| `BodyClass` | TEXT | - | General body configuration or shape, such as sedan, wagon or pickup. vPIC variable 5. | Source categorical label; official value list for variable 5; blank/NULL is not absence. |
| `BrakeSystemDesc` | TEXT | - | Additional description of the braking system. vPIC variable 52. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `BrakeSystemType` | TEXT | - | Type of system used to stop and hold the vehicle. vPIC variable 42. | Source categorical label; official value list for variable 42; blank/NULL is not absence. |
| `BusFloorConfigType` | TEXT | - | Source bus-floor configuration category. vPIC variable 148. | Source categorical label; official value list for variable 148; blank/NULL is not absence. |
| `BusLength` | TEXT | - | Source-reported bus length. vPIC variable 147. | Numeric text; feet; parse without replacing missing with zero. |
| `BusType` | TEXT | - | Source bus configuration/service type. vPIC variable 149. | Source categorical label; official value list for variable 149; blank/NULL is not absence. |
| `CAN_AACN` | TEXT | - | Automatic or advanced automatic crash notification equipment. vPIC variable 174. | Source categorical label; official value list for variable 174; blank/NULL is not absence. |
| `CIB` | TEXT | - | Crash-imminent automatic braking for an impending forward collision. vPIC variable 87. | Source categorical label; official value list for variable 87; blank/NULL is not absence. |
| `CashForClunkers` | TEXT | - | Legacy/source CashForClunkers classification retained as delivered. | Source category; eligibility coding not defined by current variable metadata. |
| `ChargerLevel` | TEXT | - | Source classification of electric-vehicle charger level. vPIC variable 127. | Source categorical label; official value list for variable 127; blank/NULL is not absence. |
| `ChargerPowerKW` | TEXT | - | Electric-vehicle charger power. vPIC variable 128. | Numeric text; kW; parse without replacing missing with zero. |
| `CombinedBrakingSystem` | TEXT | - | Motorcycle system that applies front and rear brakes together from one control. vPIC variable 202. | Source categorical label; official value list for variable 202; blank/NULL is not absence. |
| `CoolingType` | TEXT | - | Engine cooling-system type, such as air or liquid cooling. vPIC variable 122. | Source categorical label; official value list for variable 122; blank/NULL is not absence. |
| `CurbWeightLB` | TEXT | - | Vehicle weight with standard equipment/operating fluids, without occupants or cargo. vPIC variable 54. | Numeric text; pounds; parse without replacing missing with zero. |
| `CustomMotorcycleType` | TEXT | - | Source custom motorcycle design classification. vPIC variable 151. | Source categorical label; official value list for variable 151; blank/NULL is not absence. |
| `DaytimeRunningLight` | TEXT | - | Daytime illumination intended to increase vehicle visibility. vPIC variable 177. | Source categorical label; official value list for variable 177; blank/NULL is not absence. |
| `DestinationMarket` | TEXT | - | Market where the vehicle is intended to be sold. vPIC variable 10. | Source categorical label; official value list for variable 10; blank/NULL is not absence. |
| `DisplacementCC` | TEXT | - | Total engine cylinder swept volume in cubic centimeters. vPIC variable 11. | Numeric text; cm^3; parse without replacing missing with zero. |
| `DisplacementCI` | TEXT | - | Total engine cylinder swept volume in cubic inches. vPIC variable 12. | Numeric text; in^3; parse without replacing missing with zero. |
| `DisplacementL` | TEXT | - | Total engine cylinder swept volume in liters. vPIC variable 13. | Numeric text; liters; parse without replacing missing with zero. |
| `Doors` | TEXT | - | Number of vehicle doors. vPIC variable 14. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `DriveType` | TEXT | - | Drivetrain configuration, such as front-, rear-, all- or four-wheel drive. vPIC variable 15. | Source categorical label; official value list for variable 15; blank/NULL is not absence. |
| `DriverAssist` | TEXT | - | Legacy/source DriverAssist property retained as delivered. | Source description/category; exact modern variable-list mapping unresolved. |
| `DynamicBrakeSupport` | TEXT | - | Braking assistance that supplements driver braking in an emergency. vPIC variable 170. | Source categorical label; official value list for variable 170; blank/NULL is not absence. |
| `EDR` | TEXT | - | Event data recorder equipment for crash-related vehicle data. vPIC variable 175. | Source categorical label; official value list for variable 175; blank/NULL is not absence. |
| `ESC` | TEXT | - | Electronic stability control equipment that intervenes to reduce skidding. vPIC variable 99. | Source categorical label; official value list for variable 99; blank/NULL is not absence. |
| `EVDriveUnit` | TEXT | - | Electric drive motor configuration, such as single or dual motor. vPIC variable 72. | Source categorical label; official value list for variable 72; blank/NULL is not absence. |
| `ElectrificationLevel` | TEXT | - | Source classification of hybrid/electric propulsion configuration. vPIC variable 126. | Source categorical label; official value list for variable 126; blank/NULL is not absence. |
| `EngineConfiguration` | TEXT | - | Arrangement of engine cylinders, such as inline or V-shaped. vPIC variable 64. | Source categorical label; official value list for variable 64; blank/NULL is not absence. |
| `EngineCycles` | TEXT | - | Number of strokes used to complete an engine power cycle. vPIC variable 17. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `EngineCylinders` | TEXT | - | Number of engine cylinders. vPIC variable 9. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `EngineHP` | TEXT | - | Engine output-shaft horsepower; lower endpoint when a range is reported. vPIC variable 71. | Numeric text; hp; lower bound when a range; parse without replacing missing with zero. |
| `EngineHP_to` | TEXT | - | Upper endpoint of reported engine output-shaft horsepower range. vPIC variable 125. | Numeric text; hp; upper bound; parse without replacing missing with zero. |
| `EngineKW` | TEXT | - | Engine power expressed in kilowatts. vPIC variable 21. | Numeric text; kW; parse without replacing missing with zero. |
| `EngineManufacturer` | TEXT | - | Manufacturer of the engine, which may differ from vehicle manufacturer. vPIC variable 146. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `EngineModel` | TEXT | - | Manufacturer-assigned engine family/model name. vPIC variable 18. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `EntertainmentSystem` | TEXT | - | Source classification of vehicle entertainment equipment. vPIC variable 23. | Source categorical label; official value list for variable 23; blank/NULL is not absence. |
| `ErrorCode` | TEXT | - | vPIC decode diagnostic code or code list; not an HTTP status. vPIC variable 143. | Source categorical label; official value list for variable 143; blank/NULL is not absence. |
| `ForwardCollisionWarning` | TEXT | - | System that warns of an impending forward collision. vPIC variable 101. | Source categorical label; official value list for variable 101; blank/NULL is not absence. |
| `FuelInjectionType` | TEXT | - | Mechanism used to deliver/inject fuel to the engine. vPIC variable 67. | Source categorical label; official value list for variable 67; blank/NULL is not absence. |
| `FuelTankMaterial` | TEXT | - | Material used to construct the fuel tank. vPIC variable 201. | Source categorical label; official value list for variable 201; blank/NULL is not absence. |
| `FuelTankType` | TEXT | - | Fuel-tank mounting/configuration type, particularly for motorcycles. vPIC variable 200. | Source categorical label; official value list for variable 200; blank/NULL is not absence. |
| `FuelTypePrimary` | TEXT | - | Primary energy/fuel source used to power the vehicle. vPIC variable 24. | Source categorical label; official value list for variable 24; blank/NULL is not absence. |
| `FuelTypeSecondary` | TEXT | - | Secondary energy/fuel source for vehicles with multiple power sources. vPIC variable 66. | Source categorical label; official value list for variable 66; blank/NULL is not absence. |
| `GCWR` | TEXT | - | Lower source bound for allowable combined tow vehicle, trailer, occupants and cargo weight. vPIC variable 184. | Source categorical label; official value list for variable 184; blank/NULL is not absence. |
| `GCWR_to` | TEXT | - | Upper source bound for allowable combined tow vehicle, trailer, occupants and cargo weight. vPIC variable 185. | Source categorical label; official value list for variable 185; blank/NULL is not absence. |
| `GVWR` | TEXT | - | Lower source class/bound for maximum loaded vehicle weight rating, excluding trailer. vPIC variable 25. | Source categorical label; official value list for variable 25; blank/NULL is not absence. |
| `GVWR_to` | TEXT | - | Upper source class/bound for maximum loaded vehicle weight rating, excluding trailer. vPIC variable 190. | Source categorical label; official value list for variable 190; blank/NULL is not absence. |
| `KeylessIgnition` | TEXT | - | Ignition/start equipment that operates without inserting a conventional key. vPIC variable 176. | Source categorical label; official value list for variable 176; blank/NULL is not absence. |
| `LaneCenteringAssistance` | TEXT | - | System that continuously assists steering to keep the vehicle centered in its lane. vPIC variable 194. | Source categorical label; official value list for variable 194; blank/NULL is not absence. |
| `LaneDepartureWarning` | TEXT | - | System that warns when the vehicle unintentionally departs its lane. vPIC variable 102. | Source categorical label; official value list for variable 102; blank/NULL is not absence. |
| `LaneKeepSystem` | TEXT | - | System that assists to prevent unintentional lane departure. vPIC variable 103. | Source categorical label; official value list for variable 103; blank/NULL is not absence. |
| `LowerBeamHeadlampLightSource` | TEXT | - | Headlamp illumination technology/light source. vPIC variable 178. | Source categorical label; official value list for variable 178; blank/NULL is not absence. |
| `Make` | TEXT | - | Vehicle make named by the manufacturer. vPIC variable 26. | Source categorical label; official value list for variable 26; blank/NULL is not absence. |
| `MakeID` | TEXT | - | vPIC identifier for decoded make. | Identifier stored as text; not a quantity. |
| `Manufacturer` | TEXT | - | Vehicle manufacturer name. vPIC variable 27. | Source categorical label; official value list for variable 27; blank/NULL is not absence. |
| `ManufacturerId` | TEXT | - | vPIC identifier for the vehicle manufacturer. | Identifier stored as text; not a quantity. |
| `Model` | TEXT | - | Vehicle model named by the manufacturer. vPIC variable 28. | Source categorical label; official value list for variable 28; blank/NULL is not absence. |
| `ModelID` | TEXT | - | vPIC identifier for decoded model. | Identifier stored as text; not a quantity. |
| `ModelYear` | TEXT | - | Model year in the decode response; a supplied model-year hint may control this value. vPIC variable 29. | Numeric text; model year; supplied hint may determine returned year; parse without replacing missing with zero. |
| `MotorcycleChassisType` | TEXT | - | Source motorcycle chassis classification. vPIC variable 153. | Source categorical label; official value list for variable 153; blank/NULL is not absence. |
| `MotorcycleSuspensionType` | TEXT | - | Source motorcycle suspension classification. vPIC variable 152. | Source categorical label; official value list for variable 152; blank/NULL is not absence. |
| `NCSABodyType` | TEXT | - | Internal NHTSA/NCSA body-type classification. vPIC variable 96. | Source categorical label; official value list for variable 96; blank/NULL is not absence. |
| `NCSAMake` | TEXT | - | Internal NHTSA/NCSA make classification. vPIC variable 97. | Source categorical label; official value list for variable 97; blank/NULL is not absence. |
| `NCSAMapExcApprovedBy` | TEXT | - | Source metadata naming approver of an NCSA mapping exception. | Administrative text; precise upstream workflow unresolved. |
| `NCSAMapExcApprovedOn` | TEXT | - | Source approval date for an NCSA mapping exception. | Administrative source date; format not standardized here. |
| `NCSAMappingException` | TEXT | - | Source NCSA mapping exception information. | Administrative text; exact coding unresolved; not vehicle performance. |
| `NCSAModel` | TEXT | - | Internal NHTSA/NCSA model classification. vPIC variable 98. | Source categorical label; official value list for variable 98; blank/NULL is not absence. |
| `NCSANote` | TEXT | - | Administrative explanation of NCSA mapping in special cases. vPIC variable 186. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `NonLandUse` | TEXT | - | Classification of vehicle use off land, such as air or water capability. vPIC variable 195. | Source categorical label; official value list for variable 195; blank/NULL is not absence. |
| `Note` | TEXT | - | Additional general decode information not assigned to a dedicated field. vPIC variable 114. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `OtherBusInfo` | TEXT | - | Additional bus information not captured in dedicated fields. vPIC variable 150. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `OtherEngineInfo` | TEXT | - | Additional engine information not captured in dedicated fields. vPIC variable 129. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `OtherMotorcycleInfo` | TEXT | - | Additional motorcycle information not captured in dedicated fields. vPIC variable 154. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `OtherRestraintSystemInfo` | TEXT | - | Additional restraint system information not captured in dedicated fields. vPIC variable 121. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `OtherTrailerInfo` | TEXT | - | Additional trailer information not captured in dedicated fields. vPIC variable 155. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `ParkAssist` | TEXT | - | Equipment that assists steering or other functions during parking. vPIC variable 105. | Source categorical label; official value list for variable 105; blank/NULL is not absence. |
| `PedestrianAutomaticEmergencyBraking` | TEXT | - | Automatic braking intended to avoid or mitigate collisions with pedestrians. vPIC variable 171. | Source categorical label; official value list for variable 171; blank/NULL is not absence. |
| `PlantCity` | TEXT | - | City of the manufacturing plant where the VIN is affixed. vPIC variable 31. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `PlantCompanyName` | TEXT | - | Company associated with the manufacturing plant where the VIN is affixed. vPIC variable 76. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `PlantCountry` | TEXT | - | Country of the manufacturing plant where the VIN is affixed. vPIC variable 75. | Source categorical label; official value list for variable 75; blank/NULL is not absence. |
| `PlantState` | TEXT | - | State/province of the manufacturing plant where the VIN is affixed. vPIC variable 77. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `PossibleValues` | TEXT | - | vPIC suggestions for positions/values that may resolve decode uncertainty. vPIC variable 144. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `Pretensioner` | TEXT | - | Seat-belt equipment that removes belt slack when a crash is detected. vPIC variable 78. | Source categorical label; official value list for variable 78; blank/NULL is not absence. |
| `RearAutomaticEmergencyBraking` | TEXT | - | System that automatically brakes to avoid an imminent collision while reversing. vPIC variable 192. | Source categorical label; official value list for variable 192; blank/NULL is not absence. |
| `RearCrossTrafficAlert` | TEXT | - | System that warns of crossing traffic behind the vehicle while reversing. vPIC variable 183. | Source categorical label; official value list for variable 183; blank/NULL is not absence. |
| `RearVisibilitySystem` | TEXT | - | Backup-camera/rearview video system equipment. vPIC variable 104. | Source categorical label; official value list for variable 104; blank/NULL is not absence. |
| `SAEAutomationLevel` | TEXT | - | Lower bound of source-reported SAE driving automation level. vPIC variable 181. | Numeric text; integer level 0..5; lower bound; parse without replacing missing with zero. |
| `SAEAutomationLevel_to` | TEXT | - | Upper bound of source-reported SAE driving automation level. vPIC variable 182. | Numeric text; integer level 0..5; upper bound; parse without replacing missing with zero. |
| `SeatBeltsAll` | TEXT | - | Type of seat belt fitted, such as manual or automatic. vPIC variable 79. | Source categorical label; official value list for variable 79; blank/NULL is not absence. |
| `SeatRows` | TEXT | - | Number of rows of seats. vPIC variable 61. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `Seats` | TEXT | - | Number of seats. vPIC variable 33. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `SemiautomaticHeadlampBeamSwitching` | TEXT | - | Equipment that automatically switches upper/lower headlamp beams when enabled. vPIC variable 179. | Source categorical label; official value list for variable 179; blank/NULL is not absence. |
| `Series` | TEXT | - | Manufacturer marketing subdivision of a vehicle line. vPIC variable 34. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `Series2` | TEXT | - | Additional source information about the vehicle series. vPIC variable 110. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `SteeringLocation` | TEXT | - | Side of the vehicle with the steering controls. vPIC variable 36. | Source categorical label; official value list for variable 36; blank/NULL is not absence. |
| `SuggestedVIN` | TEXT | - | vPIC suggested VIN correction; retain original identifier and decode diagnostics. vPIC variable 142. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `TPMS` | TEXT | - | Type of tire-pressure monitoring system. vPIC variable 168. | Source categorical label; official value list for variable 168; blank/NULL is not absence. |
| `TopSpeedMPH` | TEXT | - | Source-reported maximum speed. vPIC variable 139. | Numeric text; mph; parse without replacing missing with zero. |
| `TrackWidth` | TEXT | - | Source-reported lateral track width. vPIC variable 159. | Numeric text; inches; parse without replacing missing with zero. |
| `TractionControl` | TEXT | - | Equipment that limits driven-wheel spin to maintain traction. vPIC variable 100. | Source categorical label; official value list for variable 100; blank/NULL is not absence. |
| `TrailerBodyType` | TEXT | - | Trailer purpose/body configuration. vPIC variable 117. | Source categorical label; official value list for variable 117; blank/NULL is not absence. |
| `TrailerLength` | TEXT | - | Trailer length from front connector to trailer end. vPIC variable 118. | Numeric text; feet; parse without replacing missing with zero. |
| `TrailerType` | TEXT | - | Trailer connection/tongue type. vPIC variable 116. | Source categorical label; official value list for variable 116; blank/NULL is not absence. |
| `TransmissionSpeeds` | TEXT | - | Number of transmission speeds/gears. vPIC variable 63. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `TransmissionStyle` | TEXT | - | Transmission type, such as manual, automatic, CVT or dual clutch. vPIC variable 37. | Source categorical label; official value list for variable 37; blank/NULL is not absence. |
| `Trim` | TEXT | - | Manufacturer trim designation; comparison-only in this project, never the source of canonical_trim. vPIC variable 38. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `Trim2` | TEXT | - | Additional manufacturer trim information; comparison-only, never the source of canonical_trim. vPIC variable 109. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `Turbo` | TEXT | - | Source indication of engine turbocharging. vPIC variable 135. | Source categorical label; official value list for variable 135; blank/NULL is not absence. |
| `ValveTrainDesign` | TEXT | - | Engine camshaft/valve-train arrangement. vPIC variable 62. | Source categorical label; official value list for variable 62; blank/NULL is not absence. |
| `VehicleDescriptor` | TEXT | - | VIN-like vehicle descriptor with identifying sequence/check-digit positions masked; not a unique VIN. vPIC variable 196. | Source text; NULL/blank means unavailable or not applicable unless source specifies otherwise. |
| `VehicleType` | TEXT | - | Vehicle classification based on the World Manufacturer Identifier. vPIC variable 39. | Source categorical label; official value list for variable 39; blank/NULL is not absence. |
| `WheelBaseLong` | TEXT | - | Upper bound of distance between front and rear axle/wheel centers. vPIC variable 112. | Numeric text; inches; upper bound; parse without replacing missing with zero. |
| `WheelBaseShort` | TEXT | - | Lower bound of distance between front and rear axle/wheel centers. vPIC variable 111. | Numeric text; inches; lower bound; parse without replacing missing with zero. |
| `WheelBaseType` | TEXT | - | Relative wheelbase variant, such as short, standard or long. vPIC variable 60. | Source categorical label; official value list for variable 60; blank/NULL is not absence. |
| `WheelSizeFront` | TEXT | - | Diameter of the front wheel. vPIC variable 119. | Numeric text; inches, wheel diameter; parse without replacing missing with zero. |
| `WheelSizeRear` | TEXT | - | Diameter of the rear wheel. vPIC variable 120. | Numeric text; inches, wheel diameter; parse without replacing missing with zero. |
| `WheelieMitigation` | TEXT | - | Motorcycle technology that limits unintended front-wheel lift during acceleration. vPIC variable 203. | Source categorical label; official value list for variable 203; blank/NULL is not absence. |
| `Wheels` | TEXT | - | Number of vehicle wheels. vPIC variable 115. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |
| `Windows` | TEXT | - | Number of vehicle windows. vPIC variable 40. | Nonnegative count stored as text; parse explicitly; NULL/blank is unknown. |

**Indexes and uniqueness (observed):**

- No separate indexes declared; an INTEGER PRIMARY KEY, when present, still supplies the rowid key.

**Foreign keys (declared, not proof of valid enforcement):**

- (decode_id) -> `nhtsa_vpic_decodes` (decode_id); ON UPDATE NO ACTION, ON DELETE CASCADE.

<a id="car_data_nhtsa-sqlite_sequence"></a>

### sqlite_sequence

- **Purpose:** SQLite internal sequence bookkeeping; not research data.
- **Row grain:** One internal AUTOINCREMENT state row per tracked table.
- **Producer/lineage:** SQLite engine.
- **Update/history behavior:** Maintained by SQLite. Included separately for completeness; never use seq as table row count.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `name` | (undeclared) | - | Table name whose AUTOINCREMENT high-water mark SQLite tracks. | SQLite internal identifier; not a project entity. |
| `seq` | (undeclared) | - | Largest AUTOINCREMENT rowid tracked by SQLite for the table. | Internal integer value despite no declared SQL type; not a row count. |

**Indexes and uniqueness (observed):**

- No separate indexes declared; an INTEGER PRIMARY KEY, when present, still supplies the rowid key.

**Foreign keys (declared, not proof of valid enforcement):**

- None.

## Column Completeness Profiles

The following tables profile the current local SQLite files. `Filled values` counts valid, nonblank values for each column; `Filled %` divides that count by the table row count; examples show up to five distinct valid stored values after markdown-safe truncation. For `CAR_DATA.db.nhtsa_enrichment` and `CAR_DATA_NHTSA.db.nhtsa_vpic_values`, the filled calculation follows the NHTSA enrichment usability rule: SQL NULL, blank strings, and literal source tokens such as `N/A`, `NA`, `NONE`, `NULL`, and `UNKNOWN` are not counted as filled because those tokens do not represent usable vPIC/source values.

The four empty legacy vehicle-table shells in `CAR_YOUTUBE_COMMENTS.db` (`listing_history`, `listings`, `nhtsa_enrichment`, and `price_history`) are intentionally omitted here for the same reason they are omitted from the table dictionary.

### CAR_DATA.db

#### CAR_DATA.db.listing_history

Rows profiled: `30,360,390`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `id` | 30,360,390 | 100.00% | 1; 2; 3; 4; 5 |
| `vin` | 30,360,390 | 100.00% | JTEVB5BR8S5005439; JTDB4MEE0P3012554; JTDBCMFEXRJ016630; JTEBU5JR7L5784297; 4T1C11AK4PU765548 |
| `history_date` | 30,360,390 | 100.00% | 2026-03-01 01:24:41; 2026-03-03 01:20:44; 2026-03-05 01:19:53; 2026-02-24 01:24:11; 2026-03-04 01:23:54 |
| `mileage` | 30,360,390 | 100.00% | 5367.0; 5548.0; 35735.0; 31099.0; 31100.0 |
| `price` | 30,360,390 | 100.00% | -1.0; 51590.0; 17975.0; 20649.0; 20900.0 |

#### CAR_DATA.db.listings

Rows profiled: `14,564,967`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `vin` | 14,564,967 | 100.00% | JTEVB5BR8S5005439; JTDB4MEE0P3012554; JTDBCMFEXRJ016630; JTEBU5JR7L5784297; 4T1C11AK4PU765548 |
| `loaddate` | 14,564,967 | 100.00% | 2026-03-05; 2026-03-06; 2026-03-07; 2026-03-08; 2026-03-13 |
| `date` | 14,564,967 | 100.00% | 2026-02-28T15:40:09Z; 2026-02-23T15:40:09Z; 2026-02-17T15:40:09Z; 2026-02-16T15:40:09Z; 2026-02-15T15:40:09Z |
| `location` | 14,561,242 | 99.97% | Miami, FL; Homestead, FL; Lighthouse Point, FL; Margate, FL; Pompano Beach, FL |
| `locationCode` | 14,560,126 | 99.97% | 33157; 33033; 33064; 33063; 33186 |
| `countryCode` | 14,564,967 | 100.00% | US |
| `pendingSale` | 14,564,967 | 100.00% | 0; 1 |
| `currentBid` | 54,305 | 0.37% | $10,101; $3,700; $0; $34,567; $5,500 |
| `bids` | 14,131 | 0.10% | 3; 14; 12; 1; 10 |
| `distance` | 14,559,547 | 99.96% | 0.0; 26.5; 32.1; 327.2; 102.0 |
| `priceRecentChange` | 14,564,967 | 100.00% | 0; 1 |
| `price` | 14,374,943 | 98.70% | 51590.0; 17975.0; 20900.0; 33360.0; 21905.0 |
| `mileage` | 14,251,114 | 97.85% | 5548; 35735; 31100; 104700; 13043 |
| `title` | 14,564,967 | 100.00% | 2025 Toyota 4Runner TRD Off-Road HV; 2023 Toyota Corolla LE; 2024 Toyota Corolla Hybrid XLE; 2020 Toyota 4Runner TRD Off-Road Premium; 2023 Toyota Camry LE |
| `listingType` | 14,564,967 | 100.00% | regular; autotempest; promoted |
| `sourceName` | 14,560,935 | 99.97% | PrivateAuto; Cars & Bids; Revolve; Sotheby's Motorsport; CarSoup |
| `year` | 14,564,967 | 100.00% | 2025; 2023; 2024; 2020; 2022 |
| `sellerType` | 14,564,967 | 100.00% | Dealer; Owner |
| `vehicleTitleDesc` | 14,564,967 | 100.00% | A clean title indicates that a vehicle has never been deemed a total loss by...; A Salvage title is issued when a vehicle has been severely damaged and/or is...; A Rebuilt title is issued when a vehicle was declared a total loss by an insu...; This listing has an unknown title, please contact the seller for more details.; A title brand is an indicator that a vehicle has sustained previous damage an... |
| `img` | 14,564,967 | 100.00% | https://thumb.autotempest.com/ll/JTEVB5BR8S5005439_JTEVB5BR8S5005439_a08a6b13...; https://thumb.autotempest.com/ll/JTDB4MEE0P3012554_JTDB4MEE0P3012554_a08a6b13...; https://thumb.autotempest.com/ll/JTDBCMFEXRJ016630_JTDBCMFEXRJ016630_7f1151e7...; https://thumb.autotempest.com/ll/JTEBU5JR7L5784297_JTEBU5JR7L5784297_7f1151e7...; https://thumb.autotempest.com/ll/4T1C11AK4PU765548_4T1C11AK4PU765548_a08a6b13... |
| `details` | 14,482,009 | 99.43% | This Cutting Edge 2025 Toyota 4Runner TRD Off-Road HV would be a welcomed add...; This Ice Cap 2023 Toyota Corolla LE would be a welcomed addition to your fami...; Why Buy from South Dade Kia Homestead?-nWhen you choose this 2024 Toyota Coro...; Why Buy from South Dade Kia Homestead?-nWhen you choose this 2020 Toyota 4Run...; This Ice Cap 2023 Toyota Camry LE would be a welcomed addition to your family... |
| `vehicleTitle` | 14,564,967 | 100.00% | Clean; Salvage; Rebuilt; Unknown; Branded |

#### CAR_DATA.db.nhtsa_enrichment

Rows profiled: `5,467,921`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `vin` | 5,467,921 | 100.00% | 000000035848911F0; 00000004742915096; 00000009111110895; 00000009111121718; 00000009112110356 |
| `nhtsa_ABS` | 4,232,874 | 77.41% | Not Applicable; Standard; Optional |
| `nhtsa_ActiveSafetySysNote` | 345,192 | 6.31% | Lane Keep System, Lane Departure Warning, Forward Collision Warning, Crash Im...; Acura Watch Plus: Lane Keep System, Lane Departure Warning, Forward Collision...; Automatic Crash Notification: Standard for Technology Package; Technology Plus: Lane Keep System, Lane Departure Warning, Forward Collision...; Rear Cross Traffic Monitor: Standard for Premium; Back-Up Sensors: Optional f... |
| `nhtsa_AdaptiveCruiseControl` | 2,837,995 | 51.90% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_AdaptiveDrivingBeam` | 1,055,572 | 19.30% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_AdaptiveHeadlights` | 0 | 0.00% |  |
| `nhtsa_AdditionalErrorText` | 919,640 | 16.82% | Invalid character(s): 9:N.; Invalid character(s): 9:R.; Invalid character(s): 10:0.; Invalid character(s): 6:I, 9:O, 10:0, 16:Z, 17:Z.; Invalid character(s): 9:F. |
| `nhtsa_AirBagLocCurtain` | 3,552,220 | 64.96% | Not Applicable; 1st and 2nd Rows; 1st Row (Driver and Passenger); All Rows; 1st and 2nd and 3rd Rows |
| `nhtsa_AirBagLocFront` | 5,189,015 | 94.90% | Not Applicable; 1st Row (Driver and Passenger); Driver Seat Only |
| `nhtsa_AirBagLocKnee` | 2,378,650 | 43.50% | Not Applicable; 1st Row (Driver and Passenger); Driver Seat Only; Passenger Seat Only |
| `nhtsa_AirBagLocSeatCushion` | 387,104 | 7.08% | Not Applicable; 1st Row (Driver and Passenger); Passenger Seat Only; Driver Seat Only; 1st and 2nd Rows |
| `nhtsa_AirBagLocSide` | 5,122,620 | 93.68% | Not Applicable; 1st Row (Driver and Passenger); 1st and 2nd Rows; All Rows; 1st and 2nd and 3rd Rows |
| `nhtsa_AutoReverseSystem` | 4,033,910 | 73.77% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_AutomaticPedestrianAlertingSound` | 273,697 | 5.01% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_AxleConfiguration` | 139 | 0.00% | SFA - Set-Forward Axle |
| `nhtsa_Axles` | 1,009,543 | 18.46% | 8; 2; 1; 5; 3 |
| `nhtsa_BasePrice` | 639,798 | 11.70% | 25900.00; 26100; 28100; 30100; 157500.00 |
| `nhtsa_BedLengthIN` | 27,342 | 0.50% | 61; 60; 71; 74; 66 |
| `nhtsa_BedType` | 1,537,084 | 28.11% | Not Applicable; Short; Long; Extended; Standard |
| `nhtsa_BlindSpotIntervention` | 1,147,103 | 20.98% | Standard; Optional |
| `nhtsa_BlindSpotMon` | 3,425,965 | 62.66% | Not Applicable; Optional; Standard; Not Available |
| `nhtsa_BodyCabType` | 2,024,813 | 37.03% | Not Applicable; Extra/Super/Quad/Double/King/Extended; Crew/Super Crew/Crew Max; Regular; MDHD: Conventional |
| `nhtsa_BodyClass` | 5,267,048 | 96.33% | Trailer; Sedan/Saloon; Hatchback/Liftback/Notchback; Coupe; Convertible/Cabriolet |
| `nhtsa_BrakeSystemDesc` | 195,054 | 3.57% | 4-Wheel ABS; Incomplete Vehicle with Hydraulic Brake; Calipers / Front Vented Discs \| Rear Drums; Calipers / Front Vented Discs; Calipers / Ventilated Front Discs \| Solid Rear Discs |
| `nhtsa_BrakeSystemType` | 1,722,700 | 31.51% | Hydraulic; Air; Air and Hydraulic |
| `nhtsa_ChargerLevel` | 6,367 | 0.12% | Not Applicable; Level 3 DC Charger or fast charger (up to 400A, up to 600V DC, up to 240kW); Level 2 AC Charger (up to 80A, 208-240V AC, up to 20kW from single- or three-... |
| `nhtsa_ChargerPowerKW` | 21,179 | 0.39% | 40; 62; 11; 135; 44 |
| `nhtsa_CombinedBrakingSystem` | 5 | 0.00% | Standard |
| `nhtsa_CoolingType` | 711,283 | 13.01% | Not Applicable; Water; Air |
| `nhtsa_CurbWeightLB` | 974,091 | 17.81% | 3585; 2756; 4100; 4269; 4340 |
| `nhtsa_DaytimeRunningLight` | 3,993,566 | 73.04% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_DestinationMarket` | 51,917 | 0.95% | U.S., Canada, Mexico, Other Export Market (BUX); U.S., Canada, Mexico; U.S., Canada; U.S.; U.S., Mexico |
| `nhtsa_DisplacementCC` | 5,062,939 | 92.59% | 2359.737216; 2400.0; 1500.0; 2000; 3211.864544 |
| `nhtsa_DisplacementCI` | 5,062,939 | 92.59% | 144; 146.45698582735; 91.53561614209; 122.0474881894; 196 |
| `nhtsa_DisplacementL` | 5,062,940 | 92.59% | 2.359737216; 2.4; 1.5; 2; 3.211864544 |
| `nhtsa_Doors` | 4,150,727 | 75.91% | 4; 5; 2; 3; 6 |
| `nhtsa_DriveType` | 4,553,286 | 83.27% | Not Applicable; 4x2; FWD/Front-Wheel Drive; AWD/All-Wheel Drive; 4WD/4-Wheel Drive/4x4 |
| `nhtsa_DriverAssist` | 0 | 0.00% |  |
| `nhtsa_DynamicBrakeSupport` | 3,703,578 | 67.73% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_EDR` | 1,792,650 | 32.78% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_ESC` | 4,177,131 | 76.39% | Not Applicable; Standard |
| `nhtsa_EVDriveUnit` | 151,371 | 2.77% | Not Applicable; Single Motor; Dual Motor; Triple Motor; Quad Motor |
| `nhtsa_ElectrificationLevel` | 596,194 | 10.90% | Not Applicable; Strong HEV (Hybrid Electric Vehicle); HEV (Hybrid Electric Vehicle) - Level Unknown; Mild HEV (Hybrid Electric Vehicle); PHEV (Plug-in Hybrid Electric Vehicle) |
| `nhtsa_EngineConfiguration` | 3,164,574 | 57.88% | Not Applicable; In-Line; V-Shaped; Rotary; Horizontally Opposed (boxer) |
| `nhtsa_EngineCycles` | 677,687 | 12.39% | 4; 6; 2; 8 |
| `nhtsa_EngineCylinders` | 4,530,630 | 82.86% | 4; 6; 10; 8; 3 |
| `nhtsa_EngineHP` | 3,376,672 | 61.75% | 201; 200; 320; 258; 286 |
| `nhtsa_EngineHP_to` | 112,406 | 2.06% | 205; 129; 155.00; 175; 160 |
| `nhtsa_EngineKW` | 62,113 | 1.14% | 280; 107; 171; 103; 140 |
| `nhtsa_EngineManufacturer` | 2,569,870 | 47.00% | Honda; HONDA; FCA; Cummins; CMC |
| `nhtsa_EngineModel` | 3,363,314 | 61.51% | K24V7; L15CA; K20C8; J32A3; J35A8 |
| `nhtsa_EntertainmentSystem` | 61,239 | 1.12% | Not Applicable; CD + Stereo; Rear Entertainment System |
| `nhtsa_ForwardCollisionWarning` | 3,650,951 | 66.77% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_FuelInjectionType` | 442,650 | 8.10% | Not Applicable; Sequential Fuel Injection (SFI); Stoichiometric Gasoline Direct Injection (SGDI); Multipoint Fuel Injection (MPFI); Unit Injector Direct Injection Diesel (UDI) |
| `nhtsa_FuelTankMaterial` | 5 | 0.00% | Aluminum alloy; Steel |
| `nhtsa_FuelTankType` | 2 | 0.00% | Under seat |
| `nhtsa_FuelTypePrimary` | 5,239,852 | 95.83% | Not Applicable; Gasoline; Compressed Natural Gas (CNG); Diesel; Flexible Fuel Vehicle (FFV) |
| `nhtsa_FuelTypeSecondary` | 527,548 | 9.65% | Not Applicable; Electric; Compressed Natural Gas (CNG); Gasoline; Flexible Fuel Vehicle (FFV) |
| `nhtsa_KeylessIgnition` | 3,593,897 | 65.73% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_LaneCenteringAssistance` | 533,962 | 9.77% | Standard; Optional |
| `nhtsa_LaneDepartureWarning` | 3,519,176 | 64.36% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_LaneKeepSystem` | 3,269,652 | 59.80% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_LowerBeamHeadlampLightSource` | 3,261,179 | 59.64% | Not Applicable; LED; Halogen; HID; Laser |
| `nhtsa_Make` | 5,270,211 | 96.38% | SHERMAN + REILLY; KROSS KOUNTRY; TRAVALONG; FIRST PRODUCTS INC; ROYALS |
| `nhtsa_MakeID` | 5,270,211 | 96.38% | 10082; 5812; 5949; 6037; 4818 |
| `nhtsa_Manufacturer` | 5,270,807 | 96.40% | SHERMAN + REILLY, INC.; KROSS KOUNTRY INDUSTRIES; TRAVALONG INC.; FIRST PRODUCTS INC.; CARRIAGE INC. |
| `nhtsa_ManufacturerId` | 5,270,807 | 96.40% | 19306; 15204; 15382; 15488; 4720 |
| `nhtsa_Model` | 5,267,134 | 96.33% | Travalong Trailer; ILX; Integra; TL; TLX |
| `nhtsa_ModelID` | 5,267,134 | 96.33% | 15649; 2150; 5063; 1873; 5354 |
| `nhtsa_ModelYear` | 5,287,785 | 96.71% | 1978; 1974; 1971; 1972; 1973 |
| `nhtsa_OtherEngineInfo` | 2,236,448 | 40.90% | Direct Fuel Injection; Direct Fuel Injection, Sequential Multiport Fuel Injection / 36HP+36HP(Fr:Twi...; Direct Fuel Injection, Sequential Multiport Fuel Injection; 36HP+36HP(Fr:Twin Motor Unit) / 47HP(Rr:Direct Drive Motor); Direct Fuel Inje...; Sequential Multiport Fuel Injection |
| `nhtsa_ParkAssist` | 1,718,748 | 31.43% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_PedestrianAutomaticEmergencyBraking` | 2,664,767 | 48.73% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_RearAutomaticEmergencyBraking` | 1,745,664 | 31.93% | Standard; Optional |
| `nhtsa_RearCrossTrafficAlert` | 2,683,784 | 49.08% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_RearVisibilitySystem` | 4,108,723 | 75.14% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_SAEAutomationLevel` | 1,540 | 0.03% | 0; 1 |
| `nhtsa_SAEAutomationLevel_to` | 0 | 0.00% |  |
| `nhtsa_SeatRows` | 2,440,290 | 44.63% | 2; 1; 3; 4 |
| `nhtsa_Seats` | 2,334,935 | 42.70% | 5; 2; 4; 7; 6 |
| `nhtsa_SemiautomaticHeadlampBeamSwitching` | 3,975,506 | 72.71% | Not Applicable; Standard; Optional; Not Available |
| `nhtsa_TPMS` | 4,685,276 | 85.69% | Not Applicable; Direct; Indirect |
| `nhtsa_TopSpeedMPH` | 267,806 | 4.90% | 120; 110; 111; 119; 117 |
| `nhtsa_TrackWidth` | 25,640 | 0.47% | 71.6; 60.30; 64.40; 61.8; 61.80 |
| `nhtsa_TractionControl` | 4,126,026 | 75.46% | Not Applicable; Standard; Optional |
| `nhtsa_TransmissionSpeeds` | 1,859,418 | 34.01% | 8; 6; 5; 9; 4 |
| `nhtsa_TransmissionStyle` | 2,790,889 | 51.04% | Not Applicable; Dual-Clutch Transmission (DCT); Continuously Variable Transmission (CVT); Manual/Standard; Automatic |
| `nhtsa_Trim` | 4,677,125 | 85.54% | Base/Acura Watch Plus; ILX; Special Edition; Premium Package/Technology Package; Premium Package |
| `nhtsa_Trim2` | 305,649 | 5.59% | NAVI; w/ NAVI; HIGH PERFORMANCE TIRE; Honda Sensing; H (High Line) |
| `nhtsa_WheelSizeFront` | 772,639 | 14.13% | 17; 18; 19; 20; 16 |
| `nhtsa_WheelSizeRear` | 771,979 | 14.12% | 17; 18; 20; 19; 16 |
| `nhtsa_Windows` | 53,891 | 0.99% | 4; 6 |
| `nhtsa_VehicleType` | 5,270,807 | 96.40% | TRAILER; PASSENGER CAR; TRUCK; MULTIPURPOSE PASSENGER VEHICLE (MPV); INCOMPLETE VEHICLE |
| `nhtsa_WheelBaseLong` | 46,493 | 0.85% | 107.10; 106.60; 148.00; 176.00; 164.20 |
| `nhtsa_WheelBaseShort` | 993,179 | 18.16% | 105.10; 103.50; 113.00; 106.30; 106.3 |
| `nhtsa_WheelBaseType` | 156,639 | 2.86% | Long; Short; Standard; Extra Long; Medium |
| `nhtsa_safety_ratings_count` | 5,286,331 | 96.68% | 0; 2; 1; 4; 3 |
| `nhtsa_overall_rating` | 3,445,988 | 63.02% | 5; Not Rated; 4; 3; 4; 4; 5 |
| `nhtsa_front_crash_rating` | 3,445,988 | 63.02% | 4; Not Rated; 5; 2; 4; 4; 5 |
| `nhtsa_rollover_rating` | 3,445,988 | 63.02% | 4; 3; 3; 4; 5; Not Rated |
| `nhtsa_side_crash_rating` | 3,445,988 | 63.02% | 5; Not Rated; 4; 4; 5; 2; Not Rated |
| `nhtsa_total_recalls` | 4,049,347 | 74.06% | 1; 3; 2; 4; 5 |
| `nhtsa_recall_components` | 4,049,347 | 74.06% | FUEL SYSTEM, GASOLINE:FUEL INJECTION SYSTEM:FUEL RAIL; POWER TRAIN:CLUTCH ASSEMBLY:PEDAL/HAND LEVER(MOTORCYCLE); FUEL SYSTEM, GASOLINE:DELIVERY; ELECTRICAL SYSTEM:IGNITION; ELECTRICAL SYSTEM:IGNITION; VISIBILITY:DEFROSTER/DEFOGGER/HVAC SYSTEM:HEATER...; EQUIPMENT:OTHER:LABELS; VISIBILITY:DEFROSTER/DEFOGGER/HVAC SYSTEM:HEATER CORE |
| `nhtsa_latest_recall_date` | 4,049,347 | 74.06% | 28/06/1977; 29/12/1971; 08/07/1974; 05/10/1977; 27/09/1979 |
| `nhtsa_total_complaints` | 4,004,808 | 73.24% | 0; 2; 4; 1; 10 |
| `nhtsa_complaint_injuries` | 4,004,808 | 73.24% | 0; 1; 12; 14; 2 |
| `nhtsa_complaint_deaths` | 4,004,808 | 73.24% | 0; 1; 2; 47; 5 |
| `nhtsa_complaint_crash_related` | 4,004,808 | 73.24% | 0; 1; 3; 12; 20 |
| `nhtsa_complaint_fire_related` | 4,004,808 | 73.24% | 0; 1; 26; 15; 2 |
| `nhtsa_common_complaint_areas` | 3,924,231 | 71.77% | POWER TRAIN; VISIBILITY; TIRES; ELECTRICAL SYSTEM,STRUCTURE,FUEL/PROPULSION SYSTEM; FUEL SYSTEM, GASOLINE; TIRES; FUEL SYSTEM, GASOLINE; FUEL SYSTEM, GASOLINE; TIRES |
| `nhtsa_decode_status` | 5,381,208 | 98.41% | success; invalid_vin; request_failed |
| `nhtsa_decode_error` | 5,364,157 | 98.10% | 1 - Check Digit (9th position) does not calculate properly; 7 - Manufacturer...; 6 - Incomplete VIN; 7 - Manufacturer is not registered with NHTSA for sale or...; VIN failed format validation; 7 - Manufacturer is not registered with NHTSA for sale or importation in the...; 5 - VIN has errors in few positions; 6 - Incomplete VIN; 12 - Model Year Warn... |
| `nhtsa_decode_fetched_at` | 5,285,729 | 96.67% | 2026-08-29T02:41:48.286955+00:00; 2026-08-29T02:41:51.610957+00:00; 2026-08-29T02:42:10.301946+00:00; 2026-08-29T02:42:13.574184+00:00; 2026-08-29T02:42:14.110297+00:00 |
| `nhtsa_identity_source` | 5,285,729 | 96.67% | make:listing;model:listing;year:nhtsa_decode; make:listing;model:unknown;year:nhtsa_decode; make:listing;model:listing;year:listing; make:unknown;model:unknown;year:nhtsa_decode; make:nhtsa_decode;model:listing;year:nhtsa_decode |
| `nhtsa_identity_confidence` | 5,285,729 | 96.67% | medium; low; high |
| `nhtsa_identity_conflict` | 5,285,729 | 96.67% | 0; 1 |
| `nhtsa_source_run_id` | 5,285,729 | 96.67% | d25ddaf393f74456ad3a256a68781c96; f20d6a0d46cf469a8042d02feed95319; 4380504168b74e7b8b67b48ca0126058; 4c5a6a2fdebc484b9c09ec34ee92fb09; 0698c5e06b774ced994f103396e4cc90 |
| `nhtsa_last_updated_at` | 5,381,208 | 98.41% | 2026-08-29T02:41:48.286968+00:00; 2026-08-29T02:41:51.610970+00:00; 2026-08-29T02:42:10.301958+00:00; 2026-08-29T02:42:13.574230+00:00; 2026-08-29T02:42:14.110310+00:00 |
| `nhtsa_safety_status` | 5,285,729 | 96.67% | success; missing_identity; request_failed |
| `nhtsa_safety_vehicle_ids` | 3,445,988 | 63.02% | 18605;18606; 15476; 17242;17243; 5172; 10036 |
| `nhtsa_recalls_status` | 5,285,729 | 96.67% | request_failed; success; missing_identity |
| `nhtsa_complaints_status` | 5,285,729 | 96.67% | request_failed; empty; missing_identity; success |

#### CAR_DATA.db.price_history

Rows profiled: `20,733,633`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `id` | 20,733,633 | 100.00% | 1; 2; 3; 4; 5 |
| `vin` | 20,733,633 | 100.00% | JTEVB5BR8S5005439; JTDB4MEE0P3012554; JTDBCMFEXRJ016630; JTEBU5JR7L5784297; 4T1C11AK4PU765548 |
| `history_date` | 20,733,633 | 100.00% | Mar 5 2026; Mar 4 2026; Feb 24 2026; Mar 1 2026; Feb 21 2026 |
| `mileage` | 20,733,633 | 100.00% | 5,548; 35,735; 31,100; 104,700; 13,043 |
| `price` | 20,733,633 | 100.00% | $51,590; $17,975; $20,900; $20,649; $33,360 |
| `trend` | 20,733,633 | 100.00% | none; up; down |

#### CAR_DATA.db.sqlite_sequence

Rows profiled: `2`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `name` | 2 | 100.00% | price_history; listing_history |
| `seq` | 2 | 100.00% | 52675182; 102164650 |

### CAR_YOUTUBE_COMMENTS.db

#### CAR_YOUTUBE_COMMENTS.db.make_sentiment_index

Rows profiled: `62`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `sentiment_make` | 62 | 100.00% | ACURA; ALFA ROMEO; ASTON MARTIN; AUDI; BENTLEY |
| `sentiment_overall_score` | 62 | 100.00% | 0.11592737814440848; 0.13057927975258443; 0.23248250927368624; 0.07935600625412846; 0.12608594424285932 |
| `sentiment_reliability_score` | 62 | 100.00% | 0.003505516799366505; 0.018186969293399437; 0.16065037861445652; -0.05436853623484076; -0.04662658820385955 |
| `sentiment_value_score` | 62 | 100.00% | -0.03988424212654826; 0.04023209167828578; 0.005034950478861901; -0.038372954142386284; -0.08585176629295603 |
| `sentiment_performance_score` | 62 | 100.00% | 0.1530325352482135; 0.15110775895187942; 0.3248258038366793; 0.1168375030238757; 0.18283038192779472 |
| `sentiment_comfort_score` | 62 | 100.00% | 0.2092534621148073; 0.2206712520146322; 0.26828720011291673; 0.21020884848892035; 0.29703418790662084 |
| `sentiment_comment_count` | 62 | 100.00% | 19672; 2468; 246; 34703; 350 |
| `sentiment_video_count` | 62 | 100.00% | 579; 188; 126; 854; 138 |
| `sentiment_aspect_coverage` | 62 | 100.00% | 0.6847676901179341; 0.6067666126418152; 0.7073170731707317; 0.6271071665273895; 0.7635714285714286 |
| `sentiment_latest_comment_at` | 62 | 100.00% | 2026-07-17; 2026-07-06; 2026-07-08; 2026-07-16; 2026-07-01 |
| `sentiment_model_versions` | 62 | 100.00% | unknown@unknown,facebook/bart-large-mnli@legacy_unpinned |
| `updated_at` | 62 | 100.00% | 2026-08-18T22:37:26+00:00 |

#### CAR_YOUTUBE_COMMENTS.db.make_sentiment_monthly

Rows profiled: `7,956`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `sentiment_make` | 7,956 | 100.00% | ACURA; ALFA ROMEO; ASTON MARTIN; AUDI; BENTLEY |
| `sentiment_month` | 7,956 | 100.00% | 2013-05-01; 2013-06-01; 2013-07-01; 2013-08-01; 2013-09-01 |
| `sentiment_overall_score` | 7,949 | 99.91% | 0.2307975839302719; 0.33973675876756837; 0.40886795184346725; 0.40771225911855874; 0.4323934220900255 |
| `sentiment_reliability_score` | 7,923 | 99.59% | 0.46043262394235285; 0.11801464971389723; 0.34318675579659136; 0.3581817268819194; 0.3839832554394608 |
| `sentiment_value_score` | 7,929 | 99.66% | 0.03941532569786255; 0.6186599284763535; 0.6803408489473228; 0.446801455263774; 0.4940889648408056 |
| `sentiment_performance_score` | 7,939 | 99.79% | -0.0019721257550317168; 0.2499074971799447; 0.26904373736728693; 0.3354512441338331; 0.3789329005049312 |
| `sentiment_comfort_score` | 7,927 | 99.64% | 0.9847795742877913; 0.5298488283552787; 0.588795515993186; 0.6060791328644635; 0.6357456517678689 |
| `sentiment_comment_count` | 7,956 | 100.00% | 9; 47; 71; 109; 123 |
| `sentiment_video_count` | 7,956 | 100.00% | 1; 2; 4; 5; 7 |
| `sentiment_aspect_coverage` | 7,956 | 100.00% | 0.4166666666666667; 0.44680851063829785; 0.4788732394366197; 0.46559633027522934; 0.4695121951219512 |
| `sentiment_latest_comment_at` | 7,956 | 100.00% | 2013-05-31; 2013-06-29; 2013-07-27; 2013-08-29; 2013-09-25 |

#### CAR_YOUTUBE_COMMENTS.db.sqlite_sequence

Rows profiled: `0`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `name` | 0 | n/a |  |
| `seq` | 0 | n/a |  |

#### CAR_YOUTUBE_COMMENTS.db.vehicle_sentiment_index

Rows profiled: `1,826`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `Vehicle_Entity` | 1,826 | 100.00% | 2018 Mercedes-Benz E-Class Coupe; 2016 Bmw 3 Series; 2021 Hyundai Santa Fe Highlander; 2016 Ford F-150; 2016 Jeep Cherokee |
| `Sample_Size` | 1,826 | 100.00% | 1; 4; 2; 15; 14 |
| `Reliability_Index` | 1,823 | 99.84% | 99.94222023623247; 99.94695166965381; 99.871224811584; 99.91920839552235; 99.44752035475766 |
| `General_Enthusiast_Score` | 1,826 | 100.00% | 99.95456315893884; 99.91461256630912; 99.90141675821144; 99.79427047384682; 99.75593013261413 |
| `Sentiment_Volatility_StdDev` | 1,810 | 99.12% | 0.0020245736056618805; 0.09129861729663956; 0.23913522208222882; 0.4293689907551478; 0.28427311043355946 |
| `Sentiment_Trend_Slope` | 1,810 | 99.12% | 1.3158997068389997e-05; -9.173734875525602e-05; -0.0005474702825262845; 0.0001780439152906905; 0.00112876305636024 |
| `Confidence_Level` | 1,826 | 100.00% | Low Confidence; High Confidence |

#### CAR_YOUTUBE_COMMENTS.db.youtube_comments_scored

Rows profiled: `1,149,960`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `video_id` | 1,149,960 | 100.00% | -MXV5VjFV54; JH300fr1KO4; SdrvFgqY4zM; 6wX4kPl4cj0; B-tZezCzao0 |
| `playlist_id` | 1,149,960 | 100.00% | PLdmWqCdsjCu6XXTGcfwQE-v-8qZKOUbPv; PLdmWqCdsjCu4IAZtt7cFYCI-5wLOu-W5v; PLdmWqCdsjCu6vdUJzxywmjNa9IMv9E4bP; PLdmWqCdsjCu4Ikxn-hvSiATgUkX5qo2iI; PLdmWqCdsjCu7Y-UCY7jFP1f86lI-IFBES |
| `video_title` | 1,149,960 | 100.00% | 2026 Dodge Charger Daytona Review \| What Hellcat Fans Never Asked For (and th...; The Craziest Honda Ever Is The 2023 Honda Civic Type R \| First Drive Review; 2022 Toyota GR86 \| Why The Best Isn't Always The One To Get; Caddy's Not Done With Gas... Yet... 2022 Cadillac CT5-V Blackwing Review; 2023 Audi S3 & RS3 Buyers Guide \| Is it worth it? How does it compare? |
| `source` | 1,149,960 | 100.00% | comment |
| `text` | 1,149,960 | 100.00% | About the efficiency, especially my comment around 35:00. The EPA's 72 MPGe r...; very nice car. i would never use the fake exhaust tho; If you could choose the sound the speakers play, it could be hilarious.; Your Venn diagram was hysterical, Alex!; i'm glad you being you and show this as you see it too |
| `extracted_at` | 1,149,960 | 100.00% | 06-04-2026; 06-08-2026; 06-16-2026; 06-18-2026; 06-20-2026 |
| `comment_id` | 1,149,960 | 100.00% | UgzhGuCfKKwM7LgPkUN4AaABAg; Ugyf3dBDEdW_-rwDb194AaABAg; Ugxe_DDU9kw-gxc0XzF4AaABAg; UgxJxWL-6i8FouHKaFV4AaABAg; Ugy_A68z-9sHzbvmgT14AaABAg |
| `author` | 1,149,947 | 100.00% | @AAutoBuyersGuide; @AntLive29; @wpelfeta; @HerrLT; @tallll70 |
| `like_count` | 1,149,960 | 100.00% | 22.0; 27.0; 21.0; 3.0; 0.0 |
| `reply_count` | 1,149,960 | 100.00% | 10; 2; 4; 1; 0 |
| `published_at` | 1,149,960 | 100.00% | 01-14-2026; 01-15-2026; 01-20-2026; 01-19-2026; 01-16-2026 |
| `updated_at` | 1,149,960 | 100.00% | 01-14-2026; 01-15-2026; 01-20-2026; 01-19-2026; 01-16-2026 |
| `Vehicle_Entity` | 922,031 | 80.18% | 2026 Dodge Charger Daytona; 2023 Honda Ever; 2022 Toyota GR86; 2022 Cadillac CT5-V Blackwing; 2023 Audi S3 & RS3 |
| `original_text` | 1,149,960 | 100.00% | About the efficiency, especially my comment around 35:00. The EPA's 72 MPGe r...; very nice car. i would never use the fake exhaust tho; If you could choose the sound the speakers play, it could be hilarious.; Your Venn diagram was hysterical, Alex!; i'm glad you being you and show this as you see it too |
| `reliability_sentiment` | 633,016 | 55.05% | -0.8986637001755522; -0.9962773909193694; -0.17821700098959067; 0.8708481732607991; -0.43971031886418077 |
| `reliability_mentioned` | 1,149,960 | 100.00% | 0; 1 |
| `reliability_confidence` | 1,149,960 | 100.00% | 0.0; 0.7866182923316956; 0.3843032717704773; 0.7993960380554199; 0.48178014159202576 |
| `value_sentiment` | 641,656 | 55.80% | 0.9971107629968311; -0.7389895680124434; 0.9995387185716446; 0.9818068173514893; 0.2812531519792366 |
| `value_mentioned` | 1,149,960 | 100.00% | 0; 1 |
| `value_confidence` | 1,149,960 | 100.00% | 0.0; 0.3868863880634308; 0.9570116996765137; 0.5452858805656433; 0.8812388181686401 |
| `performance_sentiment` | 710,738 | 61.81% | 0.9975269753906221; 0.4519365040702249; -0.8756721737778191; 0.9737906856176768; -0.9971763097089705 |
| `performance_mentioned` | 1,149,960 | 100.00% | 0; 1 |
| `performance_confidence` | 1,149,960 | 100.00% | 0.0; 0.6678227186203003; 0.5773035883903503; 0.9053677320480347; 0.7092739343643188 |
| `comfort_sentiment` | 623,526 | 54.22% | -0.2133974885129523; 0.8909431051512315; 0.9229134398500505; -0.09190253324113062; 0.17675756950449842 |
| `comfort_mentioned` | 1,149,960 | 100.00% | 0; 1 |
| `comfort_confidence` | 1,149,960 | 100.00% | 0.0; 0.9285956025123596; 0.49415838718414307; 0.7601379752159119; 0.6763584613800049 |
| `consensus_weight` | 1,149,960 | 100.00% | 2.361727836017593; 2.4471580313422194; 2.342422680822206; 1.6020599913279625; 1.0 |
| `word_count` | 1,149,960 | 100.00% | 72; 11; 13; 6; 9 |
| `depth_weight` | 1,149,960 | 100.00% | 1.2; 1.0 |
| `comment_weight` | 1,149,960 | 100.00% | 2.8340734032211117; 2.4471580313422194; 2.342422680822206; 1.6020599913279625; 1.0 |
| `Weighted_Reliability_Score` | 633,016 | 55.05% | -1.9807626192444474; -1.4716225097236506; -0.2138604011875088; 1.395151016902168; -0.845330771551638 |
| `Weighted_Value_Score` | 641,656 | 55.80% | 2.4400876118054637; -1.6288216738304964; 1.9215851889682218; 1.5729134212918614; 0.4391025445200327 |
| `Weighted_Performance_Score` | 710,738 | 61.81% | 2.4411061493076733; 0.4519365040702249; -1.9300865364794428; 1.872085316827087; -1.4729503217740372 |
| `Weighted_Comfort_Score` | 623,526 | 54.22% | -0.333163840265653; 1.42734430331229; 1.1074961278200606; -0.13575118521305435; 0.2121090834053981 |
| `processed_at` | 706,179 | 61.41% | 2026-07-06T21:44:05+00:00; 2026-07-08T07:37:43+00:00; 2026-07-09T10:34:42+00:00; 2026-07-17T19:57:12+00:00; 2026-08-18T23:02:37+00:00 |
| `model_name` | 478,250 | 41.59% | facebook/bart-large-mnli |
| `aspect_version` | 706,179 | 61.41% | v2_zero_shot_sentence_chunks; v3_make_grain_zero_shot |
| `sentiment_make` | 835,597 | 72.66% | PORSCHE; DODGE; FORD; TESLA; HYUNDAI |
| `make_attribution_source` | 1,149,960 | 100.00% | comment; video_title; ambiguous_comment; ambiguous_video_title; unknown |
| `make_attribution_version` | 1,149,960 | 100.00% | make_attribution_v1 |
| `overall_sentiment` | 818,839 | 71.21% | 0.9973743011521229; 0.4519365040702249; -0.8331197409829544; 0.9995387185716446; 0.9737906856176768 |
| `overall_confidence` | 818,839 | 71.21% | 0.5273545533418655; 0.5773035883903503; 0.8829992413520813; 0.5452858805656433; 0.7092739343643188 |
| `sentiment_status` | 1,149,960 | 100.00% | scored; ambiguous_comment; ambiguous_video_title; unknown |
| `model_revision` | 478,250 | 41.59% | legacy_unpinned |

#### CAR_YOUTUBE_COMMENTS.db.youtube_comments_sentiment

Rows profiled: `1,981,686`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `video_id` | 1,981,686 | 100.00% | -MXV5VjFV54; JH300fr1KO4; SdrvFgqY4zM; l8WHZ_2XAkA; 6wX4kPl4cj0 |
| `playlist_id` | 1,981,686 | 100.00% | PLdmWqCdsjCu6XXTGcfwQE-v-8qZKOUbPv; PLdmWqCdsjCu4IAZtt7cFYCI-5wLOu-W5v; PLdmWqCdsjCu6vdUJzxywmjNa9IMv9E4bP; PLdmWqCdsjCu4Ikxn-hvSiATgUkX5qo2iI; PLdmWqCdsjCu7Y-UCY7jFP1f86lI-IFBES |
| `video_title` | 1,981,686 | 100.00% | 2026 Dodge Charger Daytona Review \| What Hellcat Fans Never Asked For (and th...; The Craziest Honda Ever Is The 2023 Honda Civic Type R \| First Drive Review; 2022 Toyota GR86 \| Why The Best Isn't Always The One To Get; The 2023 Integra Is Fantastic... If You Set Your Expectations Right...; Caddy's Not Done With Gas... Yet... 2022 Cadillac CT5-V Blackwing Review |
| `source` | 1,981,686 | 100.00% | comment |
| `text` | 1,981,592 | 100.00% | About the efficiency, especially my comment around 35:00. The EPA's 72 MPGe r...; very nice car. i would never use the fake exhaust tho; If you could choose the sound the speakers play, it could be hilarious.; Your Venn diagram was hysterical, Alex!; i'm glad you being you and show this as you see it too |
| `extracted_at` | 1,981,686 | 100.00% | 06-04-2026; 06-08-2026; 06-16-2026; 06-18-2026; 06-20-2026 |
| `comment_id` | 1,981,686 | 100.00% | UgzhGuCfKKwM7LgPkUN4AaABAg; Ugyf3dBDEdW_-rwDb194AaABAg; Ugxe_DDU9kw-gxc0XzF4AaABAg; UgxJxWL-6i8FouHKaFV4AaABAg; Ugy_A68z-9sHzbvmgT14AaABAg |
| `author` | 1,981,663 | 100.00% | @AAutoBuyersGuide; @AntLive29; @wpelfeta; @HerrLT; @tallll70 |
| `like_count` | 1,981,686 | 100.00% | 22; 27; 21; 3; 0 |
| `reply_count` | 1,981,686 | 100.00% | 10; 2; 4; 1; 0 |
| `published_at` | 1,981,686 | 100.00% | 01-14-2026; 01-15-2026; 01-20-2026; 01-19-2026; 01-16-2026 |
| `updated_at` | 1,981,686 | 100.00% | 01-14-2026; 01-15-2026; 01-20-2026; 01-19-2026; 01-16-2026 |

#### CAR_YOUTUBE_COMMENTS.db.youtube_playlist_fetch_state

Rows profiled: `59`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `playlist_id` | 59 | 100.00% | PLdmWqCdsjCu4xf48gB1CvyGXHApkEj8ck; PLdmWqCdsjCu6XXTGcfwQE-v-8qZKOUbPv; PLdmWqCdsjCu4IAZtt7cFYCI-5wLOu-W5v; PLdmWqCdsjCu6vdUJzxywmjNa9IMv9E4bP; PLdmWqCdsjCu4Ikxn-hvSiATgUkX5qo2iI |
| `last_discovered_at` | 59 | 100.00% | 2026-08-26T15:05:12+00:00; 2026-08-26T15:05:13+00:00; 2026-08-26T15:05:15+00:00; 2026-08-26T15:05:16+00:00; 2026-08-26T15:05:19+00:00 |
| `last_status` | 59 | 100.00% | complete; api_error |
| `last_error` | 2 | 3.39% | playlistNotFound: The playlist identified with the request's &lt;code&gt;playlistId... |

#### CAR_YOUTUBE_COMMENTS.db.youtube_video_fetch_state

Rows profiled: `4,263`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `video_id` | 4,263 | 100.00% | 1pMB13DyZOc; 6pUdGFiWWzQ; Ib7qqMJ8CFY; CWTSGo0m0SI; w7-qkt75zpk |
| `playlist_id` | 4,263 | 100.00% | PLdmWqCdsjCu6vdUJzxywmjNa9IMv9E4bP; PLdmWqCdsjCu4xf48gB1CvyGXHApkEj8ck; PLdmWqCdsjCu7Y-UCY7jFP1f86lI-IFBES; PLdmWqCdsjCu4Ikxn-hvSiATgUkX5qo2iI; PLdmWqCdsjCu6XXTGcfwQE-v-8qZKOUbPv |
| `video_title` | 4,263 | 100.00% | 2026 Hyundai Venue Review \| Looking At A Used Car? Try The Cheapest New Car I...; 2026 Nissan Sentra Review \| Nissan's Most Affordable Car Is More Than Just "C...; 2025 Nissan Kicks Review \| Kicks Gets Boxy, Finally Has AWD!; The 2024 Prius Prime Is Exactly The Kind Of Crazy Prius Toyota Needed. 10 Yea...; The Best New Small "Sedan" In America Is... A Buick? 2024 Buick Envista Review |
| `discovered_at` | 4,263 | 100.00% | 2026-07-06T17:07:01+00:00; 2026-07-06T17:07:02+00:00; 2026-07-06T17:07:03+00:00; 2026-07-06T17:07:04+00:00; 2026-07-06T17:07:05+00:00 |
| `last_attempted_at` | 4,263 | 100.00% | 2026-08-18T18:45:26+00:00; 2026-08-18T18:46:18+00:00; 2026-08-18T18:49:09+00:00; 2026-08-18T18:47:22+00:00; 2026-08-18T18:55:12+00:00 |
| `last_succeeded_at` | 4,239 | 99.44% | 2026-08-18T18:45:26+00:00; 2026-08-18T18:46:18+00:00; 2026-08-18T18:49:09+00:00; 2026-08-18T18:47:22+00:00; 2026-08-18T18:55:12+00:00 |
| `last_status` | 4,263 | 100.00% | complete; api_error; zero_comments; quota_exhausted |
| `last_error` | 25 | 0.59% | videoNotFound: The video identified by the &lt;code&gt;&lt;a href="/youtube/v3/docs/co...; Max retries exceeded for fetch_comments due to quota exhaustion. |
| `comments_seen_count` | 4,263 | 100.00% | 186; 87; 173; 220; 298 |
| `next_eligible_at` | 4,263 | 100.00% | 2026-09-17T18:45:26+00:00; 2026-09-17T18:46:18+00:00; 2026-09-17T18:49:09+00:00; 2026-09-17T18:47:22+00:00; 2026-09-17T18:55:12+00:00 |

### CAR_DATA_NHTSA.db

#### CAR_DATA_NHTSA.db.nhtsa_api_extra_fields

Rows profiled: `0`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `query_id` | 0 | n/a |  |
| `record_type` | 0 | n/a |  |
| `record_key` | 0 | n/a |  |
| `field_name` | 0 | n/a |  |
| `field_value` | 0 | n/a |  |

#### CAR_DATA_NHTSA.db.nhtsa_bulk_datasets

Rows profiled: `0`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `dataset_id` | 0 | n/a |  |
| `dataset_name` | 0 | n/a |  |
| `source_url` | 0 | n/a |  |
| `source_file` | 0 | n/a |  |
| `source_version` | 0 | n/a |  |
| `checksum` | 0 | n/a |  |
| `loaded_at` | 0 | n/a |  |
| `row_count` | 0 | n/a |  |

#### CAR_DATA_NHTSA.db.nhtsa_bulk_fields

Rows profiled: `0`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `dataset_id` | 0 | n/a |  |
| `source_row_number` | 0 | n/a |  |
| `field_name` | 0 | n/a |  |
| `field_value` | 0 | n/a |  |

#### CAR_DATA_NHTSA.db.nhtsa_bulk_rows

Rows profiled: `0`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `dataset_id` | 0 | n/a |  |
| `source_row_number` | 0 | n/a |  |
| `row_hash` | 0 | n/a |  |

#### CAR_DATA_NHTSA.db.nhtsa_complaint_products

Rows profiled: `1,219,914`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `query_id` | 1,219,914 | 100.00% | 1470; 3627; 3715; 3721; 3732 |
| `record_key` | 1,219,914 | 100.00% | 680f5a86624533b6b0a93e37812c25f4dc1844292dd22a65df73f5314ea74f51; 30b6a3dec74e32b537c07330f3cbb5231a29ceed5075ea87556e649a6ea30b47; 0115b52848141e5f0321b5564cc3e5212c79301d8958220824b826e72be8b9e3; baa075995486ef643622286f6ab90b8c913e853c1a031b88b4c8e81bf4f74a12; 76b79f9ce39d07c36fb66515634af22db2415f56c4c8e45da741e3204b3a4ffd |
| `product_index` | 1,219,914 | 100.00% | 0; 1; 2; 3 |
| `product_type` | 1,219,914 | 100.00% | Vehicle; Tire; Equipment; Child Seat |
| `product_year` | 1,219,914 | 100.00% | 2021; 9999; 2015; 2017; 1985 |
| `product_make` | 1,219,914 | 100.00% | JEEP; NEXEN; FORTUNE; TBD; UNKNOWN |
| `product_model` | 1,219,914 | 100.00% | WRANGLER; RODIAN AT PRO; TORMENTA M/T FSR310; TBD; UNKNOWN |
| `manufacturer` | 1,219,914 | 100.00% | Chrysler (FCA US, LLC); Nexen Tire Corporation; Prinx Chengshan Tire North America, Inc.; ODI Demo Co; UNKNOWN MANUFACTURER |

#### CAR_DATA_NHTSA.db.nhtsa_complaints

Rows profiled: `1,201,556`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `query_id` | 1,201,556 | 100.00% | 1470; 3627; 3715; 3721; 3732 |
| `record_key` | 1,201,556 | 100.00% | 680f5a86624533b6b0a93e37812c25f4dc1844292dd22a65df73f5314ea74f51; 30b6a3dec74e32b537c07330f3cbb5231a29ceed5075ea87556e649a6ea30b47; 0115b52848141e5f0321b5564cc3e5212c79301d8958220824b826e72be8b9e3; baa075995486ef643622286f6ab90b8c913e853c1a031b88b4c8e81bf4f74a12; 76b79f9ce39d07c36fb66515634af22db2415f56c4c8e45da741e3204b3a4ffd |
| `odi_number` | 1,201,556 | 100.00% | 11758236; 11758009; 11757585; 11757424; 11756070 |
| `manufacturer` | 1,201,556 | 100.00% | Chrysler (FCA US, LLC); Honda (American Honda Motor Co.); Volkswagen Group of America, Inc.; Ford Motor Company; General Motors, LLC |
| `crash` | 1,201,556 | 100.00% | False; True |
| `fire` | 1,201,556 | 100.00% | False; True |
| `number_of_injuries` | 1,201,556 | 100.00% | 0; 2; 1; 3; 10 |
| `number_of_deaths` | 1,201,556 | 100.00% | 0; 1; 2; 45; 3 |
| `date_of_incident` | 1,160,488 | 96.58% | 08/20/2026; 07/21/2026; 07/31/2026; 02/19/2026; 04/10/2026 |
| `date_complaint_filed` | 1,201,556 | 100.00% | 08/20/2026; 08/19/2026; 08/18/2026; 08/17/2026; 08/10/2026 |
| `vin` | 1,060,813 | 88.29% | 1C4HJXDN5MW; 1C4JJXP62MW; 1C4HJXDG3MW; 1C4JJXR6XMW; 1C4HJXCG3MW |
| `components` | 1,201,556 | 100.00% | ENGINE AND ENGINE COOLING,ELECTRICAL SYSTEM; POWER TRAIN,ELECTRICAL SYSTEM,ENGINE; ELECTRICAL SYSTEM; POWER TRAIN,UNKNOWN OR OTHER; STEERING |
| `summary` | 1,201,553 | 100.00% | The contact owns a 2021 Jeep Wrangler. The contact stated that while operatin...; When using electric mode, repeatedly and consistently, when I try to accelera...; After dealer recall 21D inspection on 7/30/2026 I have experienced 2 electric...; Acceleration lag: either bucking or temporary loss of motive power when attem...; Our Jeep has a death wobble that has almost caused me to have three wrecks wh... |

#### CAR_DATA_NHTSA.db.nhtsa_ingestion_runs

Rows profiled: `9`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `run_id` | 9 | 100.00% | 4380504168b74e7b8b67b48ca0126058; f20d6a0d46cf469a8042d02feed95319; 67ab9bde8a734f51a4ce469de26be99d; 221de68d11934fc99c8aee4c79c311e4; 66932370254d4ed9ad2628831941cf14 |
| `source` | 9 | 100.00% | nhtsa_vehicle_enrichment |
| `mode` | 9 | 100.00% | refresh_all; incremental |
| `started_at` | 9 | 100.00% | 2026-08-27T06:37:05.413221+00:00; 2026-08-28T06:02:17.010054+00:00; 2026-08-28T18:26:38.091008+00:00; 2026-08-28T21:25:21.753553+00:00; 2026-08-28T21:32:29.258105+00:00 |
| `completed_at` | 3 | 33.33% | 2026-08-28T21:28:45.483192+00:00; 2026-09-13T18:24:41.164418+00:00; 2026-09-15T15:23:40.052056+00:00 |
| `status` | 9 | 100.00% | running; completed_with_errors |
| `requested_count` | 9 | 100.00% | 0; 100; 5381270 |
| `successful_count` | 9 | 100.00% | 0; 97; 98; 5285791 |
| `failed_count` | 9 | 100.00% | 0; 3; 2; 95479 |

#### CAR_DATA_NHTSA.db.nhtsa_recalls

Rows profiled: `30,501`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `query_id` | 30,501 | 100.00% | 1469; 3460; 3464; 3468; 3472 |
| `record_key` | 30,501 | 100.00% | ac825a2a424458f4d42739457c6f03d251a1a1ae73c51dc8e1e99f5002c5b456; 96973b3f264619dff899219474600b8c647607cde41c46ba88e19a88af782dc8; 038cef5eae9d7624fec13fc353766ab1b4b4e2c7da8900e1c1c59ebb7e21c9c0; ff9f6cdc0f51c70be408f0f5b25fa860a34f019573b40d9985e02ec6a68d827a; cfaefebfc1db36f2586dbea5f6a33ffc68f01daa366c5631ea0e50e0fc06b075 |
| `manufacturer` | 30,501 | 100.00% | Chrysler (FCA US, LLC); Honda (American Honda Motor Co.); Chrysler (FCA US LLC); Chrysler Group LLC; Chrysler (FCA US, LLC) (Stellantis) |
| `nhtsa_campaign_number` | 30,501 | 100.00% | 21V028000; 22V638000; 22V766000; 22V767000; 22V865000 |
| `nhtsa_action_number` | 5,857 | 19.20% | EA15001; EQ14013; EA93031; PE88058; EQ19002 |
| `report_received_date` | 30,501 | 100.00% | 28/01/2021; 25/08/2022; 13/10/2022; 23/11/2022; 23/02/2023 |
| `component` | 30,501 | 100.00% | POWER TRAIN:CLUTCH ASSEMBLY; BACK OVER PREVENTION:DISPLAY FUNCTION; SEAT BELTS:PRETENSIONER; FUEL SYSTEM, GASOLINE:DELIVERY:FUEL PUMP; ENGINE |
| `model_year` | 30,501 | 100.00% | 2021; 2016; 2017; 2018; 2019 |
| `make` | 30,501 | 100.00% | JEEP; ACURA; DODGE; VOLKSWAGEN; CHEVROLET |
| `model` | 30,501 | 100.00% | WRANGLER; ILX; DART; BEETLE; CORVETTE |
| `park_it` | 26,394 | 86.53% | False; True |
| `park_outside` | 26,394 | 86.53% | False; True |
| `over_the_air_update` | 26,394 | 86.53% | False; True |
| `summary` | 29,927 | 98.12% | Chrysler (FCA US, LLC) is recalling certain 2018-2021 Jeep Wrangler and 2020-...; Chrysler (FCA US, LLC) is recalling certain 2020-2021 Jeep Wrangler, RAM 1500...; Chrysler (FCA US, LLC) is recalling certain 2022 Ram 1500, Jeep Gladiator, an...; Chrysler (FCA US, LLC) is recalling certain 2020-2022 Jeep Wrangler, Ram 1500...; Chrysler (FCA US, LLC) is recalling certain 2021-2023 Jeep Wrangler 4xe vehic... |
| `consequence` | 29,321 | 96.13% | Overheated clutch components may increase the risk of a fire. Additionally, d...; A rearview camera that does not display an image reduces the driver's rear vi...; A seat belt with pretensioner failure may not properly restrain an occupant d...; An engine stall can increase the risk of a crash.; An engine shutdown can cause a loss of drive power, increasing the risk of a... |
| `remedy` | 29,957 | 98.22% | FCA US LLC will notify owners, and dealers will add software to reduce engine...; Dealers will update the radio software, free of charge. Owner notification le...; Dealers will replace the front seat belt retractors. free of charge. Owner no...; Dealers will replace the HPFP and inspect and replace additional fuel system...; There is more than one involved component and calibration software. Updating... |
| `notes` | 28,101 | 92.13% | Owners may also contact the National Highway Traffic Safety Administration Ve...; VOLKSWAGEN CAMPAIGN NO T-8/73. SUPER BEETLE MODEL 133. POSSIBILITY THAT THERE...; BEETLE AND SUPERBEETLE. POSSIBILITY THAT THE MOUNTING BRACKET OF THE SEAT BEL...; VOLKSWAGEN CAMPAIGN NO BJ. SUPER BEETLE. INCORRECT CERTIFICATION LABELS WEREA...; ALSO, CUSTOMERS CAN CONTACT THE NATIONAL HIGHWAY TRAFFIC SAFETY ADMINISTRATIO... |

#### CAR_DATA_NHTSA.db.nhtsa_safety_details

Rows profiled: `8,231`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `query_id` | 8,231 | 100.00% | 10; 17; 31; 358; 1098 |
| `vehicle_id` | 8,231 | 100.00% | 13570; 8250; 8406; 5858; 3627 |

#### CAR_DATA_NHTSA.db.nhtsa_safety_rating_values

Rows profiled: `242,180`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `query_id` | 242,180 | 100.00% | 10; 17; 31; 358; 1098 |
| `vehicle_id` | 242,180 | 100.00% | 13570; 8250; 8406; 5858; 3627 |
| `field_name` | 242,180 | 100.00% | OverallRating; OverallFrontCrashRating; FrontCrashDriversideRating; FrontCrashPassengersideRating; OverallSideCrashRating |
| `field_value` | 238,593 | 98.52% | Not Rated; 0.0; Standard; No; 732 |

#### CAR_DATA_NHTSA.db.nhtsa_safety_variants

Rows profiled: `8,231`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `query_id` | 8,231 | 100.00% | 10; 17; 31; 358; 1098 |
| `vehicle_id` | 8,231 | 100.00% | 13570; 8406; 8250; 5858; 3650 |
| `vehicle_description` | 8,231 | 100.00% | 2019 Jeep Wrangler 2 DR 4WD; 2014 Ford E-150 WAGON RWD; 2014 Ford E-150 Passenger VAN RWD; 2010 Honda CR-V w/SAB; 1996 Honda Civic 4-DR. |

#### CAR_DATA_NHTSA.db.nhtsa_schema_meta

Rows profiled: `1`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `key` | 1 | 100.00% | schema_version |
| `value` | 1 | 100.00% | 2 |

#### CAR_DATA_NHTSA.db.nhtsa_source_catalog

Rows profiled: `5`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `source_name` | 5 | 100.00% | vpic_decode_values_extended; vpic_decode_values_batch; safety_ratings; recalls; complaints |
| `source_type` | 5 | 100.00% | api |
| `endpoint_or_url` | 5 | 100.00% | https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVinValuesExtended/; https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/; https://api.nhtsa.gov/SafetyRatings/; https://api.nhtsa.gov/recalls/; https://api.nhtsa.gov/complaints/ |
| `source_version` | 0 | 0.00% |  |
| `last_seen_at` | 5 | 100.00% | 2026-09-13T18:28:32.666033+00:00; 2026-09-13T18:28:32.725354+00:00; 2026-09-13T18:28:32.749967+00:00; 2026-09-13T18:28:32.775404+00:00; 2026-09-13T18:28:32.800055+00:00 |
| `checksum` | 0 | 0.00% |  |

#### CAR_DATA_NHTSA.db.nhtsa_vehicle_queries

Rows profiled: `49,176`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `query_id` | 49,176 | 100.00% | 1; 2; 3; 4; 5 |
| `run_id` | 49,176 | 100.00% | 4380504168b74e7b8b67b48ca0126058; f20d6a0d46cf469a8042d02feed95319; 4c5a6a2fdebc484b9c09ec34ee92fb09; 0698c5e06b774ced994f103396e4cc90 |
| `query_type` | 49,176 | 100.00% | safety_variants; recalls; complaints; safety_detail |
| `query_key` | 49,176 | 100.00% | 1954\|CHEVROLET\|BEL; 1973\|VOLKSWAGEN\|BEETLE; 1965\|CHEVROLET\|CORVETTE; 2019\|JEEP\|WRANGLER; 2019\|JEEP\|WRANGLER\|13570 |
| `make` | 49,176 | 100.00% | CHEVROLET; VOLKSWAGEN; JEEP; FORD; HONDA |
| `model` | 49,176 | 100.00% | BEL; BEETLE; CORVETTE; WRANGLER; F-250 |
| `model_year` | 49,176 | 100.00% | 1954; 1973; 1965; 2019; 2014 |
| `vehicle_id` | 8,256 | 16.79% | 13570; 8250; 8406; 5858; 9699 |
| `response_status` | 49,176 | 100.00% | empty; request_failed; success |
| `http_status` | 49,173 | 99.99% | 200; 400; 504 |
| `error_text` | 12,188 | 24.78% | 400 Client Error: Bad Request for url: https://api.nhtsa.gov/recalls/recallsB...; 400 Client Error: Bad Request for url: https://api.nhtsa.gov/complaints/compl...; 400 Client Error: Bad Request for url: https://api.nhtsa.gov/SafetyRatings/mo...; 504 Server Error: Gateway Time-out for url: https://api.nhtsa.gov/complaints/...; HTTPSConnectionPool(host='api.nhtsa.gov', port=443): Read timed out. (read ti... |
| `record_count` | 49,176 | 100.00% | 0; 3; 1; 9; 732 |
| `response_hash` | 49,176 | 100.00% | 4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945; 94c00cffcf0db7c2e4d0dd8acd0617410164068585dd9dd6e924e1c21428fe76; 703f7e2b06f94f43d730ab24d8067049b292a9a9d0d88aa35a11589678f78881; b850fd1bcd2fd0cd46c5a37eb27a899b188556414b1e73f5d608982cccfe7678; 45a2bd6f25927df9642c4f430d9950d9f5715e3749c96f2dae1d9b628f392054 |
| `fetched_at` | 49,176 | 100.00% | 2026-08-27T09:33:25.524927+00:00; 2026-08-27T09:33:26.125277+00:00; 2026-08-27T09:33:27.437694+00:00; 2026-08-27T09:33:28.253101+00:00; 2026-08-27T09:33:29.118602+00:00 |

#### CAR_DATA_NHTSA.db.nhtsa_vin_identity_resolution

Rows profiled: `5,286,196`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `identity_id` | 5,286,196 | 100.00% | 1; 2; 3; 4; 5 |
| `run_id` | 5,286,196 | 100.00% | 4380504168b74e7b8b67b48ca0126058; f20d6a0d46cf469a8042d02feed95319; 221de68d11934fc99c8aee4c79c311e4; 66932370254d4ed9ad2628831941cf14; d25ddaf393f74456ad3a256a68781c96 |
| `decode_id` | 5,286,196 | 100.00% | 95431; 95432; 95433; 95434; 95435 |
| `vin` | 5,286,196 | 100.00% | CS4K029638; 1532994242; 1943375S119801; 1C4HJXDG6KW510241; 1FT7W2B69KEC61871 |
| `nhtsa_make` | 5,267,758 | 99.65% | JEEP; FORD; VOLKSWAGEN; HONDA; TESLA |
| `nhtsa_model` | 5,264,879 | 99.60% | WRANGLER; F-250; E-150; JETTA SPORTWAGEN; CR-V |
| `nhtsa_model_year` | 5,285,815 | 99.99% | 1954; 1973; 1965; 2019; 2014 |
| `listing_make` | 5,193,423 | 98.24% | CHEVROLET; VOLKSWAGEN; JEEP; FORD; HONDA |
| `listing_model` | 5,192,234 | 98.22% | BEL; BEETLE; CORVETTE; WRANGLER; F-250 |
| `listing_model_year` | 5,286,194 | 100.00% | 1954; 1973; 1965; 2019; 2014 |
| `resolved_make` | 5,286,056 | 100.00% | CHEVROLET; VOLKSWAGEN; JEEP; FORD; HONDA |
| `resolved_model` | 5,285,022 | 99.98% | BEL; BEETLE; CORVETTE; WRANGLER; F-250 |
| `resolved_model_year` | 5,286,196 | 100.00% | 1954; 1973; 1965; 2019; 2014 |
| `make_source` | 5,286,196 | 100.00% | listing; nhtsa_decode; unknown |
| `model_source` | 5,286,196 | 100.00% | listing; nhtsa_decode; unknown |
| `model_year_source` | 5,286,196 | 100.00% | nhtsa_decode; listing |
| `confidence` | 5,286,196 | 100.00% | medium; high; low |
| `conflict_flag` | 5,286,196 | 100.00% | 0; 1 |
| `resolved_at` | 5,286,196 | 100.00% | 2026-08-27T09:33:27.453033+00:00; 2026-08-27T09:33:30.445182+00:00; 2026-08-27T09:33:33.452713+00:00; 2026-08-27T09:33:39.646392+00:00; 2026-08-27T09:33:42.250577+00:00 |

#### CAR_DATA_NHTSA.db.nhtsa_vpic_decodes

Rows profiled: `5,383,241`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `decode_id` | 5,383,241 | 100.00% | 1; 2; 3; 4; 5 |
| `run_id` | 5,383,241 | 100.00% | 4380504168b74e7b8b67b48ca0126058; f20d6a0d46cf469a8042d02feed95319; 67ab9bde8a734f51a4ce469de26be99d; 4c5a6a2fdebc484b9c09ec34ee92fb09; 0698c5e06b774ced994f103396e4cc90 |
| `vin` | 5,383,241 | 100.00% | 9H45Q160579; #141002067; #JTHFN48Y530044026; 00000IDCO001253ZZ; 00001Q86L6M612333 |
| `model_year_hint` | 5,383,047 | 100.00% | 1969; 1960; 2003; 1933; 1976 |
| `endpoint` | 5,383,241 | 100.00% | DecodeVINValuesBatch |
| `response_status` | 5,383,241 | 100.00% | invalid_vin; success; missing_result; request_failed |
| `http_status` | 5,285,909 | 98.19% | 200; 503 |
| `error_code` | 5,285,809 | 98.19% | ERROR: Invalid Year Submitted – Pre-1981 Year Decode Attempt; 0; 1,4,12,14,400; 0,14; 1,7,12 |
| `error_text` | 5,366,190 | 99.68% | VIN failed format validation; 0 - VIN decoded clean. Check Digit (9th position) is correct; 1 - Check Digit (9th position) does not calculate properly; 4 - VIN corrected...; 0 - VIN decoded clean. Check Digit (9th position) is correct; 14 - Unable to...; 1 - Check Digit (9th position) does not calculate properly; 7 - Manufacturer... |
| `message` | 5,383,241 | 100.00% | VIN failed format validation; Results returned successfully. NOTE: Any missing decoded values should be int...; VIN was absent from the successful batch response; HTTPSConnectionPool(host='vpic.nhtsa.dot.gov', port=443): Read timed out. (re...; 503 Server Error: Service Unavailable for url: https://vpic.nhtsa.dot.gov/api... |
| `response_hash` | 5,383,241 | 100.00% | 1f9418cc80bca12abb3665dd368c4ba38c40886adc72c73884d70eb8dfc29b95; 1a1123d2cef5c0ecdef4d9bf863701b290718260ed84bc7fd5aa1ce20cd0e918; d9484f5e168a9692193f3aceefc3d846ccb522afd1550d985ce2d8f62214bbdc; efcd1bd20f6f1d2c2303c357f2df419ce8a3361c1314211a4b34dcf49cc122d2; 29e4cb47a4d7c43d112fb2b193536d04a5ef058d1018a82d0ebdd8c84259d4d6 |
| `fetched_at` | 5,383,241 | 100.00% | 2026-08-27T06:37:05.442421+00:00; 2026-08-27T06:37:05.650619+00:00; 2026-08-27T06:37:05.815739+00:00; 2026-08-27T06:37:05.949757+00:00; 2026-08-27T06:37:06.099450+00:00 |

#### CAR_DATA_NHTSA.db.nhtsa_vpic_values

Rows profiled: `5,383,088`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `decode_id` | 5,383,088 | 100.00% | 1; 2; 3; 4; 5 |
| `VIN` | 5,383,085 | 100.00% | 9H45Q160579; #141002067; #JTHFN48Y530044026; 00000IDCO001253ZZ; 00001Q86L6M612333 |
| `ErrorText` | 5,366,037 | 99.68% | VIN failed format validation; 0 - VIN decoded clean. Check Digit (9th position) is correct; 1 - Check Digit (9th position) does not calculate properly; 4 - VIN corrected...; 0 - VIN decoded clean. Check Digit (9th position) is correct; 14 - Unable to...; 1 - Check Digit (9th position) does not calculate properly; 7 - Manufacturer... |
| `ABS` | 4,232,267 | 78.62% | Standard; Not Applicable; Optional |
| `ActiveSafetySysNote` | 345,137 | 6.41% | Lane Keep System, Lane Departure Warning, Forward Collision Warning, Crash Im...; Acura Watch Plus: Lane Keep System, Lane Departure Warning, Forward Collision...; Rear Cross Traffic Monitor: Standard for Premium; Back-Up Sensors: Optional f...; Automatic Crash Notification: Standard for Technology Package; Technology Plus: Lane Keep System, Lane Departure Warning, Forward Collision... |
| `AdaptiveCruiseControl` | 2,837,663 | 52.71% | Standard; Not Applicable; Optional; Not Available |
| `AdaptiveDrivingBeam` | 1,055,302 | 19.60% | Standard; Not Applicable; Optional; Not Available |
| `AdaptiveHeadlights` | 0 | 0.00% |  |
| `AdditionalErrorText` | 833,500 | 15.48% | The Model Year decoded for this VIN may be incorrect. If you know the Model y...; In the Possible values section, the Numeric value before the : indicates the...; Unused position(s): 8.; Invalid character(s): 9:T, 15:D.; Invalid character(s): 9:N. |
| `AirBagLocCurtain` | 3,551,527 | 65.98% | 1st Row (Driver and Passenger); 1st and 2nd Rows; Not Applicable; All Rows; 1st and 2nd and 3rd Rows |
| `AirBagLocFront` | 5,187,238 | 96.36% | 1st Row (Driver and Passenger); Not Applicable; Driver Seat Only |
| `AirBagLocKnee` | 2,378,204 | 44.18% | 1st Row (Driver and Passenger); Not Applicable; Driver Seat Only; Passenger Seat Only |
| `AirBagLocSeatCushion` | 386,938 | 7.19% | Not Applicable; 1st Row (Driver and Passenger); Passenger Seat Only; Driver Seat Only; 1st and 2nd Rows |
| `AirBagLocSide` | 5,121,274 | 95.14% | 1st Row (Driver and Passenger); 1st and 2nd Rows; Not Applicable; All Rows; 1st and 2nd and 3rd Rows |
| `AutoReverseSystem` | 4,033,359 | 74.93% | Standard; Not Applicable; Optional; Not Available |
| `AutomaticPedestrianAlertingSound` | 273,524 | 5.08% | Not Applicable; Standard; Optional; Not Available |
| `AxleConfiguration` | 139 | 0.00% | SFA - Set-Forward Axle |
| `Axles` | 1,009,332 | 18.75% | 0; 8; 1; 2; 6 |
| `BasePrice` | 639,652 | 11.88% | 25900.00; 26100; 28100; 30100; 157500.00 |
| `BatteryA` | 272 | 0.01% | 70; 244; 180; 120 |
| `BatteryA_to` | 0 | 0.00% |  |
| `BatteryCells` | 15,420 | 0.29% | 68; 34 |
| `BatteryInfo` | 95,903 | 1.78% | Onboard Charger: 11.5 kW; Dual Electric Motors w Extended Range BatterySingle Charger; Dual Electric Motors w Extended Range Battery Single Charger; Dual Electric Motors w Extended Range Battery Dual Chargers; Dual Electric Motors w Extended Range BatteryDual Chargers |
| `BatteryKWh` | 60,773 | 1.13% | 100.00; 14.40; 98.00; 89; 65 |
| `BatteryKWh_to` | 8,039 | 0.15% | 70; 82.00; 102; 40; 60 |
| `BatteryModules` | 0 | 0.00% |  |
| `BatteryPacks` | 17,381 | 0.32% | 1; 2 |
| `BatteryType` | 200,416 | 3.72% | Lithium-Ion/Li-Ion; Not Applicable; Nickel-Metal-Hydride/NiMH; Iron Phosphate/FePo; Nickle-Cobalt-Manganese/NCM |
| `BatteryV` | 77,940 | 1.45% | 440; 144; 400; 360; 259 |
| `BatteryV_to` | 5,585 | 0.10% | 336; 240 |
| `BedLengthIN` | 27,339 | 0.51% | 61; 60; 110; 71; 66 |
| `BedType` | 1,535,804 | 28.53% | Not Applicable; Short; Long; Extended; Standard |
| `BlindSpotIntervention` | 1,147,055 | 21.31% | Standard; Optional |
| `BlindSpotMon` | 3,425,511 | 63.63% | Optional; Standard; Not Applicable; Not Available |
| `BodyCabType` | 2,023,269 | 37.59% | Crew/Super Crew/Crew Max; Not Applicable; Extra/Super/Quad/Double/King/Extended; Regular; MDHD: Conventional |
| `BodyClass` | 5,264,852 | 97.80% | Sport Utility Vehicle [SUV]/Multipurpose Vehicle [MPV]; Pickup; Cargo Van; Wagon; Hatchback/Liftback/Notchback |
| `BrakeSystemDesc` | 195,041 | 3.62% | 4-Wheel ABS; Incomplete Vehicle with Hydraulic Brakes; Incomplete Vehicle with Hydraulic Brake; Calipers / Front Vented Discs; Calipers / Front Vented Discs \| Rear Drums |
| `BrakeSystemType` | 1,721,939 | 31.99% | Hydraulic; Air; Air and Hydraulic |
| `BusFloorConfigType` | 5,248,470 | 97.50% | Not Applicable |
| `BusLength` | 0 | 0.00% |  |
| `BusType` | 5,248,470 | 97.50% | Not Applicable |
| `CAN_AACN` | 1,287,256 | 23.91% | Standard; Not Applicable; Optional; Not Available |
| `CIB` | 3,487,148 | 64.78% | Standard; Not Applicable; Optional; Not Available |
| `CashForClunkers` | 0 | 0.00% |  |
| `ChargerLevel` | 6,235 | 0.12% | Not Applicable; Level 3 DC Charger or fast charger (up to 400A, up to 600V DC, up to 240kW); Level 2 AC Charger (up to 80A, 208-240V AC, up to 20kW from single- or three-... |
| `ChargerPowerKW` | 21,178 | 0.39% | 40; 62; 11; 135; 44 |
| `CombinedBrakingSystem` | 5 | 0.00% | Standard |
| `CoolingType` | 710,938 | 13.21% | Not Applicable; Water; Air |
| `CurbWeightLB` | 974,089 | 18.10% | 3585; 2756; 4100; 4269; 4340 |
| `CustomMotorcycleType` | 5,268,306 | 97.87% | Not Applicable; Side-by-Side Seating |
| `DaytimeRunningLight` | 3,993,013 | 74.18% | Standard; Not Applicable; Optional; Not Available |
| `DestinationMarket` | 51,786 | 0.96% | Canada; Mexico; U.S., Mexico, Other Export Market (BUX); U.S., Canada, Mexico; U.S., Mexico |
| `DisplacementCC` | 5,060,790 | 94.01% | 3600.0; 6200.0; 4600.0; 4000; 1968 |
| `DisplacementCI` | 5,060,790 | 94.01% | 219.68547874103; 378.34721338734; 280.70922283576; 244.0949763789; 120.094728378 |
| `DisplacementL` | 5,060,791 | 94.01% | 3.6; 6.2; 4.6; 4; 1.968000 |
| `Doors` | 4,148,988 | 77.07% | 4; 2; 5; 3; 6 |
| `DriveType` | 4,551,853 | 84.56% | 4WD/4-Wheel Drive/4x4; 4x2; AWD/All-Wheel Drive; Not Applicable; FWD/Front-Wheel Drive |
| `DriverAssist` | 0 | 0.00% |  |
| `DynamicBrakeSupport` | 3,703,068 | 68.79% | Standard; Not Applicable; Optional; Not Available |
| `EDR` | 1,792,368 | 33.30% | Standard; Not Applicable; Optional; Not Available |
| `ESC` | 4,176,576 | 77.59% | Standard; Not Applicable |
| `EVDriveUnit` | 151,234 | 2.81% | Not Applicable; Single Motor; Dual Motor; Quad Motor; Triple Motor |
| `ElectrificationLevel` | 596,014 | 11.07% | BEV (Battery Electric Vehicle); Not Applicable; Strong HEV (Hybrid Electric Vehicle); HEV (Hybrid Electric Vehicle) - Level Unknown; Mild HEV (Hybrid Electric Vehicle) |
| `EngineConfiguration` | 3,162,866 | 58.76% | V-Shaped; In-Line; Not Applicable; Rotary; Horizontally Opposed (boxer) |
| `EngineCycles` | 677,532 | 12.59% | 4; 8; 6; 2 |
| `EngineCylinders` | 4,528,602 | 84.13% | 6; 8; 4; 10; 3 |
| `EngineHP` | 3,375,537 | 62.71% | 285; 383; 225; 140.00; 180 |
| `EngineHP_to` | 112,213 | 2.08% | 205; 129; 155.00; 175; 160 |
| `EngineKW` | 62,105 | 1.15% | 54; 280; 107; 143; 171 |
| `EngineManufacturer` | 2,568,689 | 47.72% | FCA; Ford; Volkswagen; Honda; HONDA |
| `EngineModel` | 3,362,379 | 62.46% | Ford; K24Z6; 2UR-GSE; K24V7; L15CA |
| `EntertainmentSystem` | 61,102 | 1.14% | CD + Stereo; Not Applicable; Rear Entertainment System |
| `ErrorCode` | 5,285,809 | 98.19% | ERROR: Invalid Year Submitted – Pre-1981 Year Decode Attempt; 0; 1,4,12,14,400; 0,14; 1,7,12 |
| `ForwardCollisionWarning` | 3,650,484 | 67.81% | Standard; Not Applicable; Optional; Not Available |
| `FuelInjectionType` | 442,020 | 8.21% | Multipoint Fuel Injection (MPFI); Not Applicable; Sequential Fuel Injection (SFI); Stoichiometric Gasoline Direct Injection (SGDI); Throttle Body Fuel Injection (TBI) |
| `FuelTankMaterial` | 5 | 0.00% | Aluminum alloy; Steel |
| `FuelTankType` | 2 | 0.00% | Under seat |
| `FuelTypePrimary` | 5,237,681 | 97.30% | Gasoline; Diesel; Electric; Not Applicable; Compressed Natural Gas (CNG) |
| `FuelTypeSecondary` | 527,315 | 9.80% | Not Applicable; Electric; Ethanol (E85); Compressed Natural Gas (CNG); Flexible Fuel Vehicle (FFV) |
| `GCWR` | 0 | 0.00% |  |
| `GCWR_to` | 0 | 0.00% |  |
| `GVWR` | 5,203,885 | 96.67% | Class 1D: 5,001 - 6,000 lb (2,268 - 2,722 kg); Class 2H: 9,001 - 10,000 lb (4,082 - 4,536 kg); Class 2G: 8,001 - 9,000 lb (3,629 - 4,082 kg); Class 1C: 4,001 - 5,000 lb (1,814 - 2,268 kg); Class 1: 6,000 lb or less (2,722 kg or less) |
| `GVWR_to` | 2,586,022 | 48.04% | Class 1D: 5,001 - 6,000 lb (2,268 - 2,722 kg); Class 1: 6,000 lb or less (2,722 kg or less); Class 1C: 4,001 - 5,000 lb (1,814 - 2,268 kg); Class 1B: 3,001 - 4,000 lb (1,360 - 1,814 kg); Class 2E: 6,001 - 7,000 lb (2,722 - 3,175 kg) |
| `KeylessIgnition` | 3,593,425 | 66.75% | Standard; Not Applicable; Optional; Not Available |
| `LaneCenteringAssistance` | 533,932 | 9.92% | Standard; Optional |
| `LaneDepartureWarning` | 3,518,798 | 65.37% | Standard; Not Applicable; Optional; Not Available |
| `LaneKeepSystem` | 3,269,300 | 60.73% | Standard; Not Applicable; Optional; Not Available |
| `LowerBeamHeadlampLightSource` | 3,260,796 | 60.57% | LED; Not Applicable; Halogen; HID; Laser |
| `Make` | 5,267,810 | 97.86% | JEEP; FORD; VOLKSWAGEN; HONDA; TESLA |
| `MakeID` | 5,267,810 | 97.86% | 483; 460; 482; 474; 441 |
| `Manufacturer` | 5,268,380 | 97.87% | FCA US LLC; FORD MOTOR COMPANY; VOLKSWAGEN DE MEXICO SA DE CV; AMERICAN HONDA MOTOR CO., INC.; TESLA, INC. |
| `ManufacturerId` | 5,268,380 | 97.87% | 994; 976; 16478; 988; 955 |
| `Model` | 5,264,940 | 97.81% | Wrangler; F-250; E-150; Jetta SportWagen; CR-V |
| `ModelID` | 5,264,940 | 97.81% | 1943; 1805; 1796; 8364; 1865 |
| `ModelYear` | 5,285,431 | 98.19% | 1954; 1973; 1965; 2019; 2014 |
| `MotorcycleChassisType` | 5,268,303 | 97.87% | Not Applicable |
| `MotorcycleSuspensionType` | 5,268,308 | 97.87% | Not Applicable; Swingarm/Wing Fork/Pivoted Fork |
| `NCSABodyType` | 0 | 0.00% |  |
| `NCSAMake` | 0 | 0.00% |  |
| `NCSAMapExcApprovedBy` | 0 | 0.00% |  |
| `NCSAMapExcApprovedOn` | 0 | 0.00% |  |
| `NCSAMappingException` | 0 | 0.00% |  |
| `NCSAModel` | 0 | 0.00% |  |
| `NCSANote` | 0 | 0.00% |  |
| `NonLandUse` | 0 | 0.00% |  |
| `Note` | 1,713,505 | 31.83% | Body Type: Open Body; Some of the decoding information provided may vary for non-U.S. market vehicles.; 0= standard length , 2= 2ft 0" longer than standard etc and letters will repr...; 8" LED Display, Apple CarPlay, Android Auto, HD Radio, SiriusXM Satellite Rad...; Bluetooth, Satellite Radio, Auxiliary Audio Input, MP3 Player |
| `OtherBusInfo` | 0 | 0.00% |  |
| `OtherEngineInfo` | 2,235,523 | 41.53% | Displacement is 4.6L 2V; 50-St. BIN 5/ULEV II emission. Emissions Certification Test Group: CVWXV02.0U5N; P2 Tri Motor; Direct Fuel Injection; Direct Fuel Injection, Sequential Multiport Fuel Injection |
| `OtherMotorcycleInfo` | 3 | 0.00% | Heavyweight Motorcycle: 901cm3 or larger; Heavy Weight Motorcycle (901cc & Larger); Middleweight Motorcycle: 351cm3 to 900cm3 |
| `OtherRestraintSystemInfo` | 3,763,890 | 69.92% | Active Belts; Active Seat Belt Advanced Front Air Bags., Active Seat Belt Advanced Front Ai...; Rear Restraint System: Seat Belt & Side Curtain Air bag (Rr R/L outer positio...; Type 2 Manual Seatbelts (FR, SR*3), PODS; seat belts: front, rear, rear center |
| `OtherTrailerInfo` | 5 | 0.00% | NO.1 - 2090 MAX GAWR, 5.30X12-C TIRE SIZE, 12X4.0 J RIM SIZE, 80 PSI NO.2 - 2...; Fifth Wheel Trailer - Fifth Wheel Pull, Trailer Length (feet): 28' to less th...; Travel Trailer; Utility Trailer body styles include Utility Trailers that may have raised or...; Travel Trailer - Recreational |
| `ParkAssist` | 1,718,424 | 31.92% | Standard; Not Applicable; Optional; Not Available |
| `PedestrianAutomaticEmergencyBraking` | 2,664,449 | 49.50% | Not Applicable; Standard; Optional; Not Available |
| `PlantCity` | 5,066,692 | 94.12% | TOLEDO; JEFFERSON COUNTY; AVON LAKE; PUEBLA; EAST LIBERTY |
| `PlantCompanyName` | 2,858,480 | 53.10% | Toledo North Assembly; Kentucky Truck; Ohio Assembly Plant; Toledo Assembly # 2; Toyota Motor Corporation - Tahara Plant |
| `PlantCountry` | 5,261,773 | 97.75% | UNITED STATES (USA); MEXICO; JAPAN; GERMANY; VENEZUELA |
| `PlantState` | 4,089,574 | 75.97% | OHIO; KENTUCKY; CALIFORNIA; SOUTH CAROLINA; AICHI |
| `PossibleValues` | 2,368 | 0.04% | (7:34); (4:0BDFGHOT)(5:0ABDGHLOSTX)(6:ABDFHLPRST); (4:W)(5:M); (4:W)(5:M)(6:0123456789)(7:0123456789)(11:1)(12:T)(13:0123); (7:6) |
| `Pretensioner` | 547,909 | 10.18% | Yes; Not Applicable |
| `RearAutomaticEmergencyBraking` | 1,745,589 | 32.43% | Optional; Standard |
| `RearCrossTrafficAlert` | 2,683,504 | 49.85% | Standard; Not Applicable; Optional; Not Available |
| `RearVisibilitySystem` | 4,108,156 | 76.32% | Standard; Not Applicable; Optional; Not Available |
| `SAEAutomationLevel` | 1,540 | 0.03% | 0; 1 |
| `SAEAutomationLevel_to` | 0 | 0.00% |  |
| `SeatBeltsAll` | 5,184,511 | 96.31% | Manual; Manual and Automatic; Not Applicable; Automatic |
| `SeatRows` | 2,439,981 | 45.33% | 2; 1; 3; 4 |
| `Seats` | 2,334,621 | 43.37% | 6; 5; 2; 4; 7 |
| `SemiautomaticHeadlampBeamSwitching` | 3,974,958 | 73.84% | Standard; Not Applicable; Optional; Not Available |
| `Series` | 1,362,936 | 25.32% | Super Duty - Single Rear Wheel; Econoline; USE20L; w/Leather; RS |
| `Series2` | 1,561,549 | 29.01% | Open Body; Wagon body style; High; Special; Premium |
| `SteeringLocation` | 1,669,438 | 31.01% | Left-Hand Drive (LHD); Not Applicable; Right-Hand Drive (RHD) |
| `SuggestedVIN` | 12,684 | 0.24% | WBAAG5!00!EA45440; 00000000!13580!15; 0000000B!R3400438; 0000000B!R3440363; 0000000H!34000678 |
| `TPMS` | 4,684,115 | 87.02% | Direct; Indirect; Not Applicable |
| `TopSpeedMPH` | 267,731 | 4.97% | 120; 121; 110; 111; 119 |
| `TrackWidth` | 25,638 | 0.48% | 71.6; 60.30; 64.40; 61.80; 61.8 |
| `TractionControl` | 4,125,482 | 76.64% | Standard; Not Applicable; Optional |
| `TrailerBodyType` | 5,268,362 | 97.87% | Not Applicable; Boat Trailer; Equipment Trailer; Box or Van Enclosed Trailer; Camping or Travel Trailer |
| `TrailerLength` | 15 | 0.00% | 67; 16; 20; 24; 19 |
| `TrailerType` | 5,268,362 | 97.87% | Not Applicable; Ball Type Pull; Fifth Wheel; Bumper Pull; Ball Hitch |
| `TransmissionSpeeds` | 1,859,091 | 34.54% | 6; 5; 1; 8; 9 |
| `TransmissionStyle` | 2,790,269 | 51.83% | Automatic; Direct Drive; Not Applicable; Dual-Clutch Transmission (DCT); Manual/Standard |
| `Trim` | 4,675,712 | 86.86% | Unlimited Sport; Sport/TJ; EX; X6 M; F |
| `Trim2` | 305,544 | 5.68% | NAVI; w/ NAVI; HIGH PERFORMANCE TIRE; Honda Sensing; H (High Line) |
| `Turbo` | 1,497,534 | 27.82% | Not Applicable; Yes; No |
| `ValveTrainDesign` | 1,724,551 | 32.04% | Dual Overhead Cam (DOHC); Not Applicable; Single Overhead Cam (SOHC); Overhead Valve (OHV); Camless Valve Actuation (CVA) |
| `VehicleDescriptor` | 5,268,758 | 97.88% | 1C4HJXDG*KW; 1FT7W2B6*KE; 1FTNE1EW*ED; 1J4FY19S*WP; 3VWPL7AJ*CM |
| `VehicleType` | 5,268,380 | 97.87% | MULTIPURPOSE PASSENGER VEHICLE (MPV); TRUCK; PASSENGER CAR; TRAILER; INCOMPLETE VEHICLE |
| `WheelBaseLong` | 46,480 | 0.86% | 176.00; 107.10; 106.60; 148.00; 148 |
| `WheelBaseShort` | 992,955 | 18.45% | 159.80; 105.10; 103.50; 113.00; 106.30 |
| `WheelBaseType` | 156,559 | 2.91% | Short; Long; Extra Long; Standard; Medium |
| `WheelSizeFront` | 772,491 | 14.35% | 17; 18; 19; 20; 16 |
| `WheelSizeRear` | 771,832 | 14.34% | 17; 18; 20; 19; 16 |
| `WheelieMitigation` | 2 | 0.00% | Optional; Standard |
| `Wheels` | 1,046,162 | 19.43% | 4; 6; 2; 5; 1 |
| `Windows` | 53,844 | 1.00% | 4; 6 |

#### CAR_DATA_NHTSA.db.sqlite_sequence

Rows profiled: `3`

| Column | Filled values | Filled % | Up to 5 distinct valid values |
| --- | ---: | ---: | --- |
| `name` | 3 | 100.00% | nhtsa_vpic_decodes; nhtsa_vehicle_queries; nhtsa_vin_identity_resolution |
| `seq` | 3 | 100.00% | 5906465; 61752; 5514599 |

## Field vocabularies and calculations

### V1: VIN and identity rules

The inspected `_vin_status` rejects empty values, strings containing `INVALID`, more than 17 characters, and characters outside `A-H`, `J-N`, `P`, `R-Z`, digits and `*`. A 17-character unmasked identifier is classified `valid`; other allowed identifiers 3..17 characters long are `partial` and are accepted for decoding. This is format validation, not check-digit verification. Earlier data/code may have applied stricter rules. Do not treat a partial VIN as a globally unique vehicle identity.

### I1: Identity completeness

For each of make/model/model year, usable NHTSA data wins; listing context is the fallback; otherwise the value remains missing. Completeness is `high` when all three NHTSA fields are populated, `medium` when at least two resolved fields are populated, `low` when one is populated, and `unknown` when none is populated. Conflict is recorded independently. Canonical trim in downstream cleaning must come from listing title, not NHTSA Trim/Trim2. No cleaned canonical columns are being documented as though they existed in these raw databases.

### N1: NHTSA status vocabulary

| Value | Context and interpretation |
| --- | --- |
| success | A result was returned/stored. Does not certify ErrorCode=0, complete identity, all fields populated, or zero downstream failures. |
| empty | Successful vehicle query with no result records; distinguish from failed request. |
| invalid_vin | VIN failed the project's format rule; no successful decode implied. |
| missing_result | Requested VIN absent from an otherwise successful batch response. |
| request_failed | Request/transport/retry failure; HTTP status can be NULL. |
| missing_identity | Compatibility safety/recall/complaint lookup could not be made because resolved make/model/year was incomplete. |
| partial | Compatibility safety summary has incomplete detail retrieval. |

Fresh/stale is determined from stored timestamps and a caller-supplied freshness window; `stale` is not a separately enumerated persisted response status in the inspected writer. Cache hits are not new response rows. Run lifecycle uses `running`, `completed`, `completed_with_errors`, `failed`.

### N2: Query keys, record identity and source conversions

`query_key` is `YEAR|UPPERCASE_MAKE|UPPERCASE_MODEL`; safety detail metadata appends `|VehicleId`. The pipe is a delimiter inside stored text, not a relational key by itself. Current recall/complaint `record_key` hashes the complete source record, retaining distinct rows that share campaign or ODI IDs. Existing records can reflect earlier key strategies; no migration was performed for this dictionary.

Normalized integer conversion uses `int(float(value))`, returning NULL for missing/invalid values. Normalized text conversion preserves scalar text and joins nested list/dictionary values compactly in typed columns. `_flatten_fields` uses dotted paths and zero-based `[index]` suffixes; scalar values become text, NULL remains NULL, and empty arrays produce a NULL leaf. Empty objects can produce no leaf. Known-root exclusion in API extra fields is not a guarantee of fully reconstructing all nested source content.

### F1: YouTube discovery/fetch vocabulary

| Value | Meaning |
| --- | --- |
| pending | Discovered or queued for initial collection. |
| complete | Collection operation returned comments successfully; may be limited by configured max_comments, so not necessarily the entire video corpus. |
| zero_comments | Collection returned no comments. |
| comments_disabled | API reported comments disabled; counts as a handled outcome for refresh scheduling. |
| quota_exhausted | Quota-related failure; retry after backoff. |
| api_error | Other API failure; retry after backoff. |

Completion states normally get a 30-day next-eligible interval, failures a 6-hour backoff, and pending can be eligible immediately. Options can change these intervals. Progress is video-level; there is no stored per-video API page token. Comment ID deduplication supports restarts. Raw insertion does not update already collected comment text/likes even when the video is revisited.

### S1: Sentiment definitions and formulas

Current model default: `facebook/bart-large-mnli`; `aspect_version=v3_make_grain_zero_shot`; `make_attribution_version=make_attribution_v1`. Provenance columns must be consulted before applying these definitions to historical scores. The current migration recomputes attribution/overall scores without rerunning aspect inference, so historical model/aspect versions can coexist.

Text preprocessing removes URLs/HTML, rejects specified spam/bot/phone/crypto patterns and comments shorter than three words, and normalizes repeated punctuation. Sentences are split into chunks of at most 45 words. A unique make mention in the cleaned comment wins; absent one, a unique make in the video title is used. Multi-make comments or titles remain ambiguous rather than being split across makes.

| Aspect | Interpretation of positive versus negative label |
| --- | --- |
| reliability | Durability/dependability versus breakdowns, unreliability or costly repairs. |
| value | Affordability/fair pricing/cost of ownership versus overpricing, poor resale value or expensive ownership. |
| performance | Power, acceleration, handling/driving dynamics versus weak or disappointing performance. |
| comfort | Cabin/interior/space/ride/features versus discomfort, cramped space, harsh ride or poor features. |

Exact label strings and hypothesis template are defined in [absa_pipeline.py](DataPipeline/absa_pipeline.py). For each aspect/chunk, `mentioned = max(s_positive, s_negative) >= 0.40`; polarity is `(s_positive - s_negative) / (s_positive + s_negative + 1e-9)`. Unmentioned polarity is NULL, not neutral zero. Comment-level polarity is the confidence-weighted mean over mentioned chunks; aspect confidence is their maximum confidence.

`consensus_weight = 1 + log10(like_count + 1)`; `depth_weight = 1.2` for at least 20 cleaned words, otherwise `1.0`; `comment_weight = consensus_weight * depth_weight`. `Weighted_<Aspect>_Score` is polarity times this weight and is not bounded to [-1,1]. `overall_sentiment` is the confidence-weighted mean of usable mentioned aspect polarities; `overall_confidence` is their average confidence. Both are NULL when no aspect is usable.

Make score = `SUM(eligible_score * COALESCE(comment_weight,1)) / SUM(eligible_weight)`, with separate denominators for overall and each aspect. Eligibility requires `sentiment_status='scored'` and non-NULL make; each aspect additionally requires mentioned=1 and nonmissing polarity. Comment counts include eligible rows even if no score is usable. Coverage = `SUM(reliability_mentioned + value_mentioned + performance_mentioned + comfort_mentioned) / (4 * comment_count)`, treating missing mention flags as zero.

Monthly scores accumulate numerators and denominators through each represented publication month, rather than averaging month averages. Distinct videos are counted across the cumulative window. `sentiment_month=YYYY-MM-01` labels the whole month; it is not an availability timestamp. For a prediction made mid-month, use the previous completed month or reconstruct an exact publication cutoff. Publication-time reconstruction also does not account for later collection, comment edits, likes accumulated after publication, or a model introduced later. Strict real-time backtesting needs those availability limitations disclosed or additional historical snapshots.

The cumulative distinct-video calculation uses each non-NULL video's first eligible publication month within its make, followed by a cumulative sum of newly encountered videos. This is equivalent to counting distinct videos through each cutoff; it is not a sum of per-month distinct counts. NULL video IDs and unparseable publication dates do not contribute to monthly video counts. The optimization changes execution cost, not table grain, score definitions, or availability semantics.

Only `sentiment_overall_score`, four `sentiment_<aspect>_score` fields, `sentiment_comment_count`, `sentiment_video_count`, and `sentiment_aspect_coverage` are permitted sentiment model inputs by project policy. Identifiers, text, provenance and statuses are audit/join fields, not additional sentiment predictors.

### N3: Safety detail field vocabulary

The first 10,000 `nhtsa_safety_rating_values` rows in rowid order contained the field names below. This is a bounded observed vocabulary, not a claim that later rows contain no additional fields. All leaf values are physically TEXT (or NULL). Do not apply one numeric conversion to every field.

| field_name | Meaning | Logical type / caveat |
| --- | --- | --- |
| `ComplaintsCount` | SafetyRatings response ComplaintsCount | Nonnegative count text; separate endpoint/scope from dedicated recall/complaint queries. |
| `FrontCrashDriversideRating` | Frontal crash driver-side rating | Rating text; stars when numeric, typically 1..5; preserve Not Rated/NULL rather than coding zero. |
| `FrontCrashPassengersideRating` | Frontal crash passenger-side rating | Rating text; stars when numeric, typically 1..5; preserve Not Rated/NULL rather than coding zero. |
| `FrontCrashPicture` | Source crash-test image reference for FrontCrashPicture | URL/reference text, not binary media or numeric rating. |
| `FrontCrashVideo` | Source crash-test video reference for FrontCrashVideo | URL/reference text, not binary media or numeric rating. |
| `InvestigationCount` | SafetyRatings response InvestigationCount | Nonnegative count text; separate endpoint/scope from dedicated recall/complaint queries. |
| `Make` | SafetyRatings vehicle variant Make | Source identity/description; VehicleId is identifier, ModelYear is a year. |
| `Model` | SafetyRatings vehicle variant Model | Source identity/description; VehicleId is identifier, ModelYear is a year. |
| `ModelYear` | SafetyRatings vehicle variant ModelYear | Source identity/description; VehicleId is identifier, ModelYear is a year. |
| `NHTSAElectronicStabilityControl` | SafetyRatings assessment for ElectronicStabilityControl | Source assessment/category, not vPIC equipment presence; do not equate blindly. |
| `NHTSAForwardCollisionWarning` | SafetyRatings assessment for ForwardCollisionWarning | Source assessment/category, not vPIC equipment presence; do not equate blindly. |
| `NHTSALaneDepartureWarning` | SafetyRatings assessment for LaneDepartureWarning | Source assessment/category, not vPIC equipment presence; do not equate blindly. |
| `OverallFrontCrashRating` | Overall frontal crash rating | Rating text; stars when numeric, typically 1..5; preserve Not Rated/NULL rather than coding zero. |
| `OverallRating` | Overall safety rating | Rating text; stars when numeric, typically 1..5; preserve Not Rated/NULL rather than coding zero. |
| `OverallSideCrashRating` | Overall side crash rating | Rating text; stars when numeric, typically 1..5; preserve Not Rated/NULL rather than coding zero. |
| `RecallsCount` | SafetyRatings response RecallsCount | Nonnegative count text; separate endpoint/scope from dedicated recall/complaint queries. |
| `RolloverPossibility` | Source rollover likelihood field | Numeric-like text; scale and first/second-field distinction must be checked against source metadata before percent conversion. |
| `RolloverPossibility2` | Source rollover likelihood field (second source field) | Numeric-like text; scale and first/second-field distinction must be checked against source metadata before percent conversion. |
| `RolloverRating` | Rollover rating | Rating text; stars when numeric, typically 1..5; preserve Not Rated/NULL rather than coding zero. |
| `RolloverRating2` | Second source rollover rating field; variant/test distinction requires source interpretation | Rating text; stars when numeric, typically 1..5; preserve Not Rated/NULL rather than coding zero. |
| `SideCrashDriversideRating` | Side crash driver-side rating | Rating text; stars when numeric, typically 1..5; preserve Not Rated/NULL rather than coding zero. |
| `SideCrashPassengersideRating` | Side crash passenger-side rating | Rating text; stars when numeric, typically 1..5; preserve Not Rated/NULL rather than coding zero. |
| `SideCrashPicture` | Source crash-test image reference for SideCrashPicture | URL/reference text, not binary media or numeric rating. |
| `SideCrashVideo` | Source crash-test video reference for SideCrashVideo | URL/reference text, not binary media or numeric rating. |
| `SidePoleCrashRating` | Side pole crash rating | Rating text; stars when numeric, typically 1..5; preserve Not Rated/NULL rather than coding zero. |
| `SidePolePicture` | Source crash-test image reference for SidePolePicture | URL/reference text, not binary media or numeric rating. |
| `SidePoleVideo` | Source crash-test video reference for SidePoleVideo | URL/reference text, not binary media or numeric rating. |
| `VehicleDescription` | SafetyRatings vehicle variant VehicleDescription | Source identity/description; VehicleId is identifier, ModelYear is a year. |
| `VehicleId` | SafetyRatings vehicle variant VehicleId | Source identity/description; VehicleId is identifier, ModelYear is a year. |
| `combinedSideBarrierAndPoleRating-Front` | Combined side barrier/pole front-seat rating | Rating text; stars when numeric, typically 1..5; preserve Not Rated/NULL rather than coding zero. |
| `combinedSideBarrierAndPoleRating-Rear` | Combined side barrier/pole rear-seat rating | Rating text; stars when numeric, typically 1..5; preserve Not Rated/NULL rather than coding zero. |
| `dynamicTipResult` | Source dynamic rollover tip-test result | Categorical result text; not a star rating. |
| `sideBarrierRating-Overall` | Overall side barrier rating | Rating text; stars when numeric, typically 1..5; preserve Not Rated/NULL rather than coding zero. |

`nhtsa_api_extra_fields` and `nhtsa_bulk_fields` had no rows at inspection. Their physical columns are fully documented above; a populated source vocabulary cannot be invented for empty tables. On first import/new endpoint fields, add an entry for each source field with source version, meaning, logical type, units and codes. In bulk data the same field name may have different semantics across dataset_id; definitions must be keyed by dataset/version as well as name.

## Known limitations and review items

1. **Actual schema differs from constructor DDL.** CAR_DATA listings price/mileage are declared TEXT; its history types also differ from fresh CarDatabase creation. The dictionary records actual declarations. Reading constructor code alone would miss this.
2. **Legacy foreign keys are not valid integrity guarantees.** CAR_DATA listings, price_history and listing_history reference a nonexistent `cars(vin)` table. CAR_DATA nhtsa_enrichment references `listings(vin)`, which is not unique by itself. The dictionary does not repair or enable these constraints. Full FK checking was not run.
3. **Scored comment uniqueness is an index.** The current local youtube_comments_scored has no declared PK but does have a unique comment_id index. Its column lacks explicit NOT NULL. Current constructor DDL declares a PK for new databases; historical schema is different.
4. **Empty and legacy YouTube tables are separated.** Four empty vehicle-table shells in `CAR_YOUTUBE_COMMENTS.db` (`listing_history`, `listings`, `nhtsa_enrichment`, `price_history`) are intentionally omitted from the table dictionary because `CAR_DATA.db` is the authoritative vehicle source and current YouTube ingestion does not write them. `vehicle_sentiment_index` remains documented because it is populated, but it lacks a current Python writer and verified historical formulas; treat it as legacy output, not evidence of current make-grain behavior.
5. **History is content-deduplicated, not complete request/event logging.** Repeated identical NHTSA responses may retain first insert timestamps/run IDs; nullable unique-key components can admit repeated rows. Vehicle-query hashes cover the Results/results list, not HTTP/status metadata; failed and empty responses for the same query can share the hash of an empty list and reuse a record. A latest projection may mix old fields with newly updated error/status fields. Use normalized records and source-specific timestamps for audits; do not assume every request attempt is reconstructable.
6. **Raw prices and geography need source interpretation.** Amounts are asking prices/current bids, not realized sales. No dedicated currency/unit or search-origin metadata is present. Source listing date, title-status codes and trend domains remain unresolved where the writer simply passes them through.
7. **NHTSA source grain must survive joins.** Recall/complaint counts are query-level evidence, not per-VIN incidence rates. Numeric zero in compatibility injury/death summaries can result from missing source counts. Ratings can summarize multiple variants using delimited text.
8. **Sentiment time and support have limits.** Raw comment dates lose clock precision; raw insertion preserves first observations rather than refreshing edits/likes. Month labels include full-month comments. Scores can mix model revisions and reconstructed publication-time history. Comments are a selected audience, not a representative sample of all vehicle owners.
9. **Dynamic field coverage can grow.** All 154 currently present vPIC source columns are documented. The safety vocabulary was sampled; future/unsampled field names require additional definitions. Bulk fallback for unsupported formats currently stores raw_line text, so normalized field tables alone do not guarantee structured parsing of every input format.
10. **Sensitive/free-text fields need deliberate use.** Author display names, comment text, complaint narratives, VINs and source paths can identify people/assets or local context. This dictionary supplies metadata only; it neither assigns a license to third-party content nor authorizes publication of the underlying records.

## Maintenance and verification

Update this file in the same change as any relevant schema, source mapping, enum, score formula, join grain or time-semantic change. Keep README.md and PROJECT_SUMMARY.md linked to this single reference. A maintainer should review unresolved definitions before using those fields in an academic claim.

For a schema refresh, open each file with `sqlite3.connect(path.as_uri() + '?mode=ro', uri=True)`, enable query_only on that connection, and inspect sqlite_master plus table_info/index_list/index_info/foreign_key_list. Never instantiate a pipeline database class merely to inspect schema: constructors can create or migrate tables. Reconcile every `(database, table, column)` with this document, including SQLite internal tables separately. Record the review date, source/code version, new dynamic fields and changed constraints. Do not replace historical definitions with guessed interpretations.

Schema verification for this edition checks all 688 physical column entries, their declared types, PK positions, NOT NULL flags and defaults against the captured schemas, and checks all declared index/FK metadata. This is documentation coverage verification, not a full-row data-quality audit. Any profiling statistics added later should include date, query, sampling scope and denominator; dynamic data row counts do not belong in stable field definitions without an as-of label.

## Sources

Internal implementation is authoritative for project transformations; upstream documentation defines source concepts. External sources were checked on 2026-09-15.

- [database.py](DataPipeline/database.py): schemas, inserts, deduplication, source-field flattening, timestamps, history and comment state.
- [Playwright_test.py](DataPipeline/Playwright_test.py): queue-results fields, numeric parsing, details concatenation, image fallback and collection date.
- [NHTSA_enrichment.py](DataPipeline/NHTSA_enrichment.py): VIN validation, identity fallback, API/query grain, statuses and summary calculations.
- [SentimentAnalysis.py](DataPipeline/SentimentAnalysis.py): top-level comment extraction and video/playlist progress.
- [absa_pipeline.py](DataPipeline/absa_pipeline.py): make attribution, exact aspect labels, inference, weighting and current/cumulative aggregation.
- [PROJECT_SUMMARY.md](PROJECT_SUMMARY.md) and [AGENTS.md](AGENTS.md): research purpose and leakage/grain constraints.
- [USGS data dictionary guidance](https://www.usgs.gov/data-management/data-dictionaries): dictionary organization and maintenance.
- [NHTSA vPIC API](https://vpic.nhtsa.dot.gov/api/) and [official variable list](https://vpic.nhtsa.dot.gov/api/vehicles/GetVehicleVariableList?format=json): vehicle-property labels, variable IDs, units and decode conventions. Flat property names are explicitly mapped to IDs; fields not matched to the current list are marked unresolved.
- [YouTube comment resource](https://developers.google.com/youtube/v3/docs/comments) and [comment thread resource](https://developers.google.com/youtube/v3/docs/commentThreads): source comment/author/date and reply-count concepts.
- [SQLite foreign keys](https://www.sqlite.org/foreignkeys.html): enforcement and parent-key requirements. Index origin codes in this document are SQLite PRAGMA values: c=explicit CREATE INDEX, u=UNIQUE constraint, pk=PRIMARY KEY.


## Derived modeling sidecar contract (implemented 2026-09-16)

This appendix specifies a new optional derived output, not a claim that the three
profiled source databases have acquired new physical tables. Its file is
`CAR_DATA_OUTPUT/CAR_NHTSA_TEXT_FEATURES.db`, created only when the user runs
`DataPipeline/NHTSA_text_features.py`. Source schemas and stored narratives remain
unchanged. The following are declared implementation constraints, not a new
full-data profile.

| Table | Grain/key | Fields and meaning |
| --- | --- | --- |
| configuration | One expected row, application-enforced | model repository, immutable revision commit, taxonomy/chunking/hypothesis version. A mismatch refuses reuse. |
| source_identity | One expected row, application-enforced | Absolute source database path; a different source requires another sidecar. |
| text_scores | PRIMARY KEY(text_hash, role, label) | SHA-256 of whitespace-normalized text; complaint/recall_hazard/recall_remedy role; topic label; REAL score; scored_at UTC processing time. Score is uncalibrated evidence, not risk or opinion polarity. |
| query_features | query_id INTEGER PRIMARY KEY | source (complaints/recalls), normalized make/model, model_year, available_at UTC retained fetched_at, response_status, nullable REAL features listed below. Indexed by make/model/year/source/available_at. |

The query association supplies MMY. Identity normalization uppercases and replaces
non-alphanumeric separators with spaces; no fuzzy mapping, manufacturer mapping,
trim mapping, or masked-VIN join is performed. Per-query event deduplication uses
ODI number for complaints and campaign number for recalls, with record_key as the
fallback. Unique component narratives are combined per event/role. Repeated text
may reuse inference while retaining separate events. query_id selects a stored
response version; different retained queries are not summed across history.

| Derived feature family | Definition and missingness |
| --- | --- |
| nhtsa_{complaints,recalls}_report_count | Distinct events in the retained successful query. Empty query = 0; failed/missing query = NULL. Counts have MMY grain, not VIN reliability. |
| nhtsa_{complaints,recalls}_known | 1 for success/empty, 0 for a retained failed query, NULL for no eligible match. |
| nhtsa_{complaints,recalls}_text_coverage | Events whose available text roles are fully scored divided by distinct report count. No reports = NULL; existing unscored reports = 0. |
| nhtsa_complaints_{crash,fire,injury,death}_known_count | Number of deduplicated reports with an interpretable corresponding structured flag/nonnegative numeric count. |
| nhtsa_complaints_{crash,fire,injury,death}_report_share | Mean indicator among reports with that known structured field. Injury/death use count > 0; repeated components use maximum indicator. No known denominator = NULL. This is not population incidence. |
| nhtsa_complaint_{loss_of_propulsion,loss_of_control,recurring_failure,repair_delay}_score | Mean event topic score from complaint summaries; NULL until every available event text in the query is scored. |
| nhtsa_recall_hazard_{loss_of_propulsion,loss_of_control,fire_hazard}_score | Mean campaign topic evidence from summary/consequence, with the same complete-text requirement. Describes potential hazards, not observed incidents. |
| nhtsa_recall_remedy_{software_remedy,replacement_remedy}_score | Mean campaign remedy evidence; absent remedy text is not neutral. No eligible scored documents = NULL. |

For each listing/history row, the shared join selects the latest retained query
per source and exact normalized MMY with available_at strictly earlier than the
observation month's start. It preserves input row count. A later failed query
makes that source unknown rather than carrying a success forward silently.
Collection-time deduplication retains first stored timestamps and cannot recreate
every refresh state. Earlier incident/filing/report dates do not authorize
backdating text. Strict joins cannot cover prices before collection began.

YouTube source rollup formulas are unchanged, including cumulative distinct
videos. ML joins now require sentiment_month strictly earlier than the observation
month because monthly labels cover a full month. Publication-time reconstruction
still cannot establish historical edits, likes, or collection availability.

Model-only semantics also changed: vehicle_age uses observation year;
listing_recency_days retains its compatibility name but now means days since
2000-01-01. Cohort volume is the number of retained VIN/month contributions, not
listing inventory; targets/lags refer to exact calendar months. Latest raw recall
and complaint totals are excluded from historical predictors. See the reviewed
runbook in PROJECT_SUMMARY.md for feature groups, retraining, and limitations.
