"""
Quick Database Schema Verification
===================================
Verifies that the database schema matches the code requirements.
"""

import sqlite3

DB_PATH = r"C:\Users\danie\Code\Car-Price-Data-Visualization-Learning\CAR_DATA_OUTPUT\CAR_DATA.db"

def verify_schema():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    print("="*60)
    print("CURRENT DATABASE SCHEMA")
    print("="*60)

    # Check listings table
    cursor.execute("PRAGMA table_info(listings)")
    columns = cursor.fetchall()

    print("\n📋 LISTINGS TABLE COLUMNS:")
    print("-" * 60)
    for col in columns:
        col_id, name, dtype, notnull, default, pk = col
        pk_indicator = " [PK]" if pk else ""
        print(f"  {name:25s} {dtype:15s}{pk_indicator}")

    print(f"\n✓ Total columns: {len(columns)}")

    # Check for required columns
    required = ['vin', 'loaddate', 'year', 'title', 'details', 'price', 'mileage']
    column_names = [col[1] for col in columns]
    missing = [col for col in required if col not in column_names]

    print("\n🔍 REQUIRED COLUMN CHECK:")
    print("-" * 60)
    if missing:
        print(f"  ❌ Missing: {', '.join(missing)}")
    else:
        print("  ✓ All required columns present!")

    # Get stats
    cursor.execute("SELECT COUNT(*) FROM listings")
    total = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT vin) FROM listings")
    unique_vins = cursor.fetchone()[0]

    cursor.execute("SELECT COUNT(DISTINCT loaddate) FROM listings")
    unique_dates = cursor.fetchone()[0]

    print("\n📊 DATABASE STATISTICS:")
    print("-" * 60)
    print(f"  Total records:        {total:,}")
    print(f"  Unique VINs:          {unique_vins:,}")
    print(f"  Unique load dates:    {unique_dates}")

    # Check other tables
    print("\n📚 OTHER TABLES:")
    print("-" * 60)
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
    tables = cursor.fetchall()
    for table in tables:
        table_name = table[0]
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        print(f"  {table_name:25s} {count:,} rows")

    conn.close()

    print("\n" + "="*60)
    if not missing:
        print("✓✓✓ Schema is READY for DataAquisition script! ✓✓✓")
    else:
        print("⚠️  Schema needs migration - run fix_database_schema.py")
    print("="*60)

if __name__ == "__main__":
    verify_schema()

