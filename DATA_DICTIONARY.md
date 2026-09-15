# Project Data Dictionary

Verified: 2026-09-15. Dictionary version: 1.0. Maintainer: project repository maintainer.

This dictionary describes every physical column in the three requested SQLite databases under `CAR_DATA_OUTPUT/`: `CAR_DATA.db`, `CAR_YOUTUBE_COMMENTS.db`, and `CAR_DATA_NHTSA.db`. It supports the capstone's listing-price, depreciation, safety and consumer-sentiment research. It does not describe `CAR_DATA_CLEANED.db`, EPA reference tables in that database, or model output files.

Coverage: **32 project tables and 682 project columns**, plus `sqlite_sequence` in each database (3 internal tables and 6 internal columns). Table/column counts are schema counts, not data row counts. Repeated table names in different databases are documented separately because their types, constraints and purpose differ. No SQL views were included in the table inventory.

Physical metadata was read directly using SQLite read-only URI connections, `sqlite_master`, `PRAGMA table_info`, `PRAGMA index_list`, `PRAGMA index_info`, and `PRAGMA foreign_key_list`. Definitions were checked against the current working-tree acquisition, persistence, enrichment and ABSA code. NHTSA schema metadata reports version `2`. The working tree already contained edits to `DataPipeline/database.py`, `DataPipeline/NHTSA_enrichment.py` and `tests/test_nhtsa_enrichment.py`; this document reflects inspected behavior without assuming those changes have been run against historical records.

Only schema metadata, the schema-version row, single-row existence checks for bulk metadata/rows and a bounded sample of up to 10,000 field names from each of three NHTSA field/value tables were read. No full data profiling, training, ingestion, migrations or database writes were performed. Definitions/expected domains are not proof that every stored value passes validation. No identifying example records are reproduced.

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
| CAR_YOUTUBE_COMMENTS.db | Raw comments, scores, make aggregates and progress; also retains legacy/auxiliary vehicle tables | 11 | 243 | SentimentAnalysis, YouTubeCommentsDatabase, absa_pipeline |
| CAR_DATA_NHTSA.db | Normalized NHTSA response history and source metadata | 17 | 284 | NHTSADataEnricher, NHTSADatabase |

Refresh is command/scheduler driven, not guaranteed continuous. The scheduled core pipeline scrapes, enriches, refreshes EPA reference data, then cleans. NHTSA uses a 30-day freshness window by default; YouTube fetch state also defaults to 30-day completed-video refresh. ABSA processes unscored IDs unless explicitly reprocessed; aggregate tables are rebuilt from stored scores.

| Database | Table | Columns | Declared primary key |
| --- | --- | ---: | --- |
| CAR_DATA.db | [listing_history](#car_data-listing_history) | 5 | id |
| CAR_DATA.db | [listings](#car_data-listings) | 22 | vin, loaddate |
| CAR_DATA.db | [nhtsa_enrichment](#car_data-nhtsa_enrichment) | 122 | vin |
| CAR_DATA.db | [price_history](#car_data-price_history) | 6 | id |
| CAR_DATA.db | [sqlite_sequence](#car_data-sqlite_sequence) | 2 | None |
| CAR_YOUTUBE_COMMENTS.db | [listing_history](#car_youtube_comments-listing_history) | 5 | id |
| CAR_YOUTUBE_COMMENTS.db | [listings](#car_youtube_comments-listings) | 22 | vin, loaddate |
| CAR_YOUTUBE_COMMENTS.db | [make_sentiment_index](#car_youtube_comments-make_sentiment_index) | 12 | sentiment_make |
| CAR_YOUTUBE_COMMENTS.db | [make_sentiment_monthly](#car_youtube_comments-make_sentiment_monthly) | 11 | sentiment_make, sentiment_month |
| CAR_YOUTUBE_COMMENTS.db | [nhtsa_enrichment](#car_youtube_comments-nhtsa_enrichment) | 110 | vin |
| CAR_YOUTUBE_COMMENTS.db | [price_history](#car_youtube_comments-price_history) | 6 | id |
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
| Canonical listing/history make and month | make_sentiment_monthly | Latest available make/month no later than observation month | Month labels summarize the full month; use strictly prior month for within-month prediction unless cutoff is month-end. Never join all prior months and duplicate observations. |

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

<a id="car_youtube_comments-listing_history"></a>

### listing_history

- **Purpose:** Auxiliary/legacy vehicle table present in the comments database; not the primary market/enrichment source. Supplementary historical price/odometer observations.
- **Row grain:** One retained source listing-history event, with surrogate id and uniqueness on (vin, history_date, price, mileage).
- **Producer/lineage:** Matches a CarDatabase-style schema; original creation/population provenance is unresolved. Current YouTube ingestion does not write vehicle records here.
- **Update/history behavior:** Retained physical table; active population is not assumed. Reference vehicle-table behavior: INSERT OR IGNORE; no collection timestamp; do not union with price_history without overlap handling.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `id` | INTEGER | PK1, rowid key | SQLite-generated surrogate identifier for the history record. | INTEGER PRIMARY KEY AUTOINCREMENT; not a VIN or chronology guarantee. |
| `vin` | TEXT | - | Listing VIN or vehicle identifier supplied by acquisition; enrichment normalizes case/whitespace. | Identifier text; partial/masked values may exist; VIN rule V1. |
| `history_date` | DATE | - | Date on a source priceHistory/listingHistory element; copied from its date field. | Source event date; no ingestion timestamp in this table; format not standardized here. |
| `mileage` | REAL | - | Source historical mileage from the listingHistory element, copied without scraper numeric normalization. | Nonnegative numeric expected; distance unit not explicitly stored; NULL differs from zero. Historical source values can remain strings. |
| `price` | INTEGER | - | Source historical price from the listingHistory element, copied without scraper numeric normalization. | Monetary numeric after parsing; currency not stored/guaranteed; zero/1 may be placeholders. Historical source values can remain strings. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_listing_history_1`: UNIQUE (vin, history_date, price, mileage); origin=u, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (vin) -> `listings` (vin); ON UPDATE NO ACTION, ON DELETE NO ACTION.

<a id="car_youtube_comments-listings"></a>

### listings

- **Purpose:** Auxiliary/legacy vehicle table present in the comments database; not the primary market/enrichment source. Market observations for price prediction and listing context.
- **Row grain:** One retained listing snapshot per (vin, loaddate).
- **Producer/lineage:** Matches a CarDatabase-style schema; original creation/population provenance is unresolved. Current YouTube ingestion does not write vehicle records here.
- **Update/history behavior:** Retained physical table; active population is not assumed. Reference vehicle-table behavior: INSERT OR REPLACE on snapshot key; unchanged price/mileage can be skipped by VINCache. Within-day changes replace the same key; not every scrape is retained.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `vin` | TEXT | PK1 | Listing VIN or vehicle identifier supplied by acquisition; enrichment normalizes case/whitespace. | Identifier text; partial/masked values may exist; VIN rule V1. |
| `loaddate` | DATE | PK2 | Local calendar date assigned by the scraper with date.today().isoformat(); snapshot collection date, not the seller's event date. | Date, YYYY-MM-DD; no time or timezone offset retained. |
| `year` | INTEGER | - | Model year supplied by the listing source, before NHTSA identity resolution. | Integer year; may disagree with decode; no SQL range constraint. |
| `title` | TEXT | - | Source listing headline used downstream for identity and title-only trim parsing. | Free text; not verified canonical identity. |
| `details` | TEXT | - | Concatenation of detailsShort, detailsMid and detailsLong, without inserted separators. | Free text; NULL if unavailable. |
| `price` | INTEGER | - | Advertised listing price, not a completed transaction price; scraper strips dollar signs/commas and parses float. | Monetary numeric after parsing; currency not stored/guaranteed; zero/1 may be placeholders. |
| `mileage` | INTEGER | - | Odometer value from listing; scraper removes commas and truncates numeric values to integer. | Nonnegative numeric expected; distance unit not explicitly stored; NULL differs from zero. |
| `date` | DATE | - | Source listing date passed through from queue-results item.date; precise source event interpretation is unresolved. | Source date/text; parse explicitly; not guaranteed ISO. |
| `location` | TEXT | - | Source location label passed through from the listing API. | Text; geography granularity is source-dependent. |
| `locationCode` | TEXT | - | Source location code passed through without a canonical geographic lookup. | Text identifier; preserve leading zeros; coding system unresolved. |
| `countryCode` | TEXT | - | Source country code passed through without validation or conversion. | Text; do not assume a particular ISO code length without verification. |
| `pendingSale` | BOOLEAN | - | Source pending-sale indicator. | Boolean-like; expected 0/1, but not constrained; NULL means unavailable. |
| `currentBid` | REAL | - | Source current auction bid at collection time. | Monetary amount; not a final sale price; currency unspecified. |
| `bids` | INTEGER | - | Source auction bid count at collection time. | Nonnegative integer expected; NULL means unavailable. |
| `distance` | REAL | - | Source search-distance value, distinct from vehicle odometer mileage. | Numeric; unit and search origin not persisted; interpretation requires source context. |
| `priceRecentChange` | BOOLEAN | - | Source flag indicating a recent price change. | Boolean-like; lookback window/direction unresolved; not a price-change amount. |
| `sellerType` | TEXT | - | Source seller classification. | Open text categories; no enforced vocabulary. |
| `vehicleTitle` | TEXT | - | Source vehicle-title field, preserved independently of listing title. | Source text/code; precise domain unresolved; do not assume canonical trim. |
| `listingType` | TEXT | - | Source listing classification. | Open text categories; interpret per sourceName. |
| `vehicleTitleDesc` | TEXT | - | Source description associated with vehicle-title information. | Source text; exact title-status vocabulary unresolved. |
| `sourceName` | TEXT | - | Name of the upstream listing provider carried in the aggregator response. | Text provenance label; not a seller identifier. |
| `img` | TEXT | - | Image reference selected from img, then imgSource, then imgFallback. | URL/reference text; image binary not stored. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_listings_1`: UNIQUE (vin, loaddate); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- None.

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

<a id="car_youtube_comments-nhtsa_enrichment"></a>

### nhtsa_enrichment

- **Purpose:** Auxiliary/legacy vehicle table present in the comments database; not the primary market/enrichment source. Convenient decode/specification and make/model/year summary join to listings.
- **Row grain:** One latest compatibility projection per vin.
- **Producer/lineage:** Matches a CarDatabase-style schema; original creation/population provenance is unresolved. Current YouTube ingestion does not write vehicle records here.
- **Update/history behavior:** Retained physical table; active population is not assumed. Reference vehicle-table behavior: Upsert provided fields by vin. Failure-only updates can leave older values in unprovided columns; read per-source statuses and timestamps.

This copy has 110 columns and lacks the 12 current decode/status/provenance additions present in CAR_DATA.db. Its schema is not interchangeable with the main projection.

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
| `nhtsa_latest_recall_date` | DATE | - | Latest usable ReportReceivedDate chosen using supported date parsing, retaining its original representation. | Source date text; NULL when no usable date; failure status still required. |
| `nhtsa_total_complaints` | INTEGER | - | Number of complaint source records for the resolved make/model/year. | Count; not a VIN-specific failure rate or exposure-adjusted risk. |
| `nhtsa_complaint_injuries` | INTEGER | - | Sum of numberOfInjuries across returned complaints; missing/invalid values contribute zero in this summary. | Count; zero may conceal missing counts; consult normalized records. |
| `nhtsa_complaint_deaths` | INTEGER | - | Sum of numberOfDeaths across returned complaints; missing/invalid values contribute zero in this summary. | Count; zero may conceal missing counts; consult normalized records. |
| `nhtsa_complaint_crash_related` | INTEGER | - | Count of complaint records whose crash value matches the writer's truthy vocabulary. | Count; 1/true/yes/y/t recognized case-insensitively, all others false for this calculation. |
| `nhtsa_complaint_fire_related` | INTEGER | - | Count of complaint records whose fire value matches the writer's truthy vocabulary. | Count; 1/true/yes/y/t recognized case-insensitively, all others false for this calculation. |
| `nhtsa_common_complaint_areas` | TEXT | - | Three most frequent nonblank components strings, frequency descending then lexical order, joined with semicolon-space. | Text summary of complete source strings; not exhaustive. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_nhtsa_enrichment_1`: UNIQUE (vin); origin=pk, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (vin) -> `listings` (vin); ON UPDATE NO ACTION, ON DELETE NO ACTION.

<a id="car_youtube_comments-price_history"></a>

### price_history

- **Purpose:** Auxiliary/legacy vehicle table present in the comments database; not the primary market/enrichment source. Source price trajectories for depreciation research.
- **Row grain:** One retained source price-history event, with surrogate id and uniqueness on (vin, history_date, price).
- **Producer/lineage:** Matches a CarDatabase-style schema; original creation/population provenance is unresolved. Current YouTube ingestion does not write vehicle records here.
- **Update/history behavior:** Retained physical table; active population is not assumed. Reference vehicle-table behavior: INSERT OR IGNORE; mileage and trend are not part of the unique key. Events may predate collection.

| Column | Declared type | Constraints/default | Definition and lineage | Logical meaning, units and domain |
| --- | --- | --- | --- | --- |
| `id` | INTEGER | PK1, rowid key | SQLite-generated surrogate identifier for the history record. | INTEGER PRIMARY KEY AUTOINCREMENT; not a VIN or chronology guarantee. |
| `vin` | TEXT | - | Listing VIN or vehicle identifier supplied by acquisition; enrichment normalizes case/whitespace. | Identifier text; partial/masked values may exist; VIN rule V1. |
| `history_date` | DATE | - | Date on a source priceHistory/listingHistory element; copied from its date field. | Source event date; no ingestion timestamp in this table; format not standardized here. |
| `mileage` | INTEGER | - | Source historical mileage from the priceHistory element, copied without scraper numeric normalization. | Nonnegative numeric expected; distance unit not explicitly stored; NULL differs from zero. Historical source values can remain strings. |
| `price` | INTEGER | - | Source historical price from the priceHistory element, copied without scraper numeric normalization. | Monetary numeric after parsing; currency not stored/guaranteed; zero/1 may be placeholders. Historical source values can remain strings. |
| `trend` | TEXT | - | Source price-history trend label, copied without recalculation. | Text; values and time window unresolved; potential price-derived leakage. |

**Indexes and uniqueness (observed):**

- `sqlite_autoindex_price_history_1`: UNIQUE (vin, history_date, price); origin=u, partial=0.

**Foreign keys (declared, not proof of valid enforcement):**

- (vin) -> `listings` (vin); ON UPDATE NO ACTION, ON DELETE NO ACTION.

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
2. **Legacy foreign keys are not valid integrity guarantees.** CAR_DATA listings, price_history and listing_history reference a nonexistent `cars(vin)` table. CAR_DATA nhtsa_enrichment references `listings(vin)`, which is not unique by itself. Auxiliary history/enrichment FKs in CAR_YOUTUBE_COMMENTS also reference this nonunique single-column parent. The dictionary does not repair or enable these constraints. Full FK checking was not run.
3. **Scored comment uniqueness is an index.** The current local youtube_comments_scored has no declared PK but does have a unique comment_id index. Its column lacks explicit NOT NULL. Current constructor DDL declares a PK for new databases; historical schema is different.
4. **Legacy/auxiliary tables remain visible.** Vehicle tables in the YouTube file are not assumed authoritative. `vehicle_sentiment_index` lacks a current Python writer and verified historical formulas. Treat it as legacy documentation with unresolved semantics, not as evidence of current make-grain behavior. Table population was not comprehensively profiled.
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
