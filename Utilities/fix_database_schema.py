"""
Database Schema Migration Script
=================================
Adds missing 'year' column to the listings table to fix insertion errors.

This script:
1. Checks current schema
2. Backs up existing data
3. Adds missing columns if needed
4. Validates the fix

Author: Data Science Team
Date: 2026-02-10
"""

import sqlite3
import logging
from datetime import datetime
import shutil
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

DB_PATH = r"C:\Users\danie\Code\Car-Price-Data-Visualization-Learning\CAR_DATA_OUTPUT\CAR_DATA.db"


def backup_database():
    """Create a backup of the database before migration"""
    if not os.path.exists(DB_PATH):
        logging.error(f"Database not found: {DB_PATH}")
        return False

    backup_path = DB_PATH.replace('.db', f'_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db')
    try:
        shutil.copy2(DB_PATH, backup_path)
        logging.info(f"✓ Database backed up to: {backup_path}")
        return True
    except Exception as e:
        logging.error(f"Failed to backup database: {e}")
        return False


def check_current_schema():
    """Check the current schema of the listings table"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(listings)")
    columns = cursor.fetchall()

    logging.info("Current 'listings' table schema:")
    for col in columns:
        logging.info(f"  - {col[1]} ({col[2]})")

    column_names = [col[1] for col in columns]
    conn.close()

    return column_names


def add_missing_columns():
    """Add missing columns to the listings table"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Check which columns are missing
    cursor.execute("PRAGMA table_info(listings)")
    existing_columns = {col[1] for col in cursor.fetchall()}

    required_columns = {
        'year': 'INTEGER',
        'title': 'TEXT',
        'details': 'TEXT',
        'price': 'TEXT',
        'mileage': 'TEXT',
        'date': 'TEXT',
        'location': 'TEXT',
        'locationCode': 'TEXT',
        'countryCode': 'TEXT',
        'pendingSale': 'BOOLEAN',
        'currentBid': 'REAL',
        'bids': 'INTEGER',
        'distance': 'REAL',
        'priceRecentChange': 'BOOLEAN',
        'sellerType': 'TEXT',
        'vehicleTitle': 'TEXT',
        'listingType': 'TEXT',
        'vehicleTitleDesc': 'TEXT',
        'sourceName': 'TEXT',
        'img': 'TEXT'
    }

    missing_columns = set(required_columns.keys()) - existing_columns

    if not missing_columns:
        logging.info("✓ All required columns exist")
        conn.close()
        return True

    logging.info(f"Adding {len(missing_columns)} missing columns...")

    try:
        for col_name in missing_columns:
            col_type = required_columns[col_name]
            sql = f"ALTER TABLE listings ADD COLUMN {col_name} {col_type}"
            logging.info(f"  Executing: {sql}")
            cursor.execute(sql)

        conn.commit()
        logging.info("✓ Successfully added missing columns")
        return True

    except sqlite3.Error as e:
        logging.error(f"Error adding columns: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


def verify_schema():
    """Verify that all required columns now exist"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("PRAGMA table_info(listings)")
    columns = cursor.fetchall()
    column_names = [col[1] for col in columns]

    required = ['vin', 'loaddate', 'year', 'title', 'details', 'price', 'mileage']
    missing = [col for col in required if col not in column_names]

    conn.close()

    if missing:
        logging.error(f"❌ Still missing columns: {missing}")
        return False
    else:
        logging.info("✓ Schema validation passed - all required columns present")
        return True


def get_stats():
    """Get database statistics"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT COUNT(*) FROM listings")
    listing_count = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT vin) FROM listings")
    unique_vins = cursor.fetchone()[0]

    conn.close()

    logging.info(f"Database stats: {listing_count} listings, {unique_vins} unique VINs")


def main():
    """Main migration process"""
    logging.info("="*60)
    logging.info("Database Schema Migration Tool")
    logging.info("="*60)

    # Step 1: Check if database exists
    if not os.path.exists(DB_PATH):
        logging.error(f"Database not found: {DB_PATH}")
        logging.info("The database will be created automatically when the scraper runs.")
        return

    # Step 2: Get current stats
    try:
        get_stats()
    except Exception as e:
        logging.warning(f"Could not get stats: {e}")

    # Step 3: Backup
    logging.info("\n--- Backing up database ---")
    if not backup_database():
        logging.error("Backup failed. Aborting migration.")
        return

    # Step 4: Check current schema
    logging.info("\n--- Checking current schema ---")
    current_columns = check_current_schema()

    # Step 5: Add missing columns
    logging.info("\n--- Adding missing columns ---")
    if not add_missing_columns():
        logging.error("Migration failed!")
        return

    # Step 6: Verify
    logging.info("\n--- Verifying schema ---")
    if verify_schema():
        logging.info("\n✓✓✓ Migration completed successfully! ✓✓✓")
        logging.info("The scraper should now work without insertion errors.")
    else:
        logging.error("\n❌ Migration verification failed!")


if __name__ == "__main__":
    main()

