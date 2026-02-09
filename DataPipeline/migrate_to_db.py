import os
import json
import pandas as pd
from database import CarDatabase

def migrate_csv_to_db(csv_path, db_path):
    if not os.path.exists(csv_path):
        print(f"CSV file not found: {csv_path}")
        return
    
    print(f"Migrating {csv_path} to {db_path}...")
    db = CarDatabase(db_path)
    
    # Read CSV
    try:
        df = pd.read_csv(csv_path)
        
        # Determine loaddate from filename if not in CSV
        # Filename format: CAR_DATA_YYYY-MM-DD.csv
        filename = os.path.basename(csv_path)
        date_str = filename.replace("CAR_DATA_", "").replace(".csv", "")
        
        if 'loaddate' not in df.columns:
            df['loaddate'] = date_str
            
        # Convert NaN to None for database insertion
        df = df.where(pd.notnull(df), None)
        
        # Normalize columns: only take what's expected by the new schema/Aquisition script
        expected_columns = [
            "loaddate", "date", "location", "locationCode", "countryCode",
            "pendingSale", "title", "currentBid", "bids", "distance", "priceHistory",
            "priceRecentChange", "price", "listingHistory", "mileage", "year", "vin", 
            "sellerType", "vehicleTitle", "listingType", "vehicleTitleDesc", 
            "sourceName", "img", "details"
        ]
        
        # Filter columns that exist in df
        cols_to_use = [c for c in expected_columns if c in df.columns]
        df_filtered = df[cols_to_use]
        
        rows = df_filtered.to_dict('records')
        
        inserted = db.insert_rows(rows)
        print(f"Successfully migrated {inserted} rows from {csv_path}")
    except Exception as e:
        print(f"Error during migration: {e}")

if __name__ == "__main__":
    output_dir = os.path.join(os.getcwd(), "CAR_DATA_OUTPUT")
    db_path = os.path.join(output_dir, "CAR_DATA.db")
    
    # Find all CAR_DATA_*.csv files
    for filename in os.listdir(output_dir):
        if filename.startswith("CAR_DATA_") and filename.endswith(".csv"):
            csv_path = os.path.join(output_dir, filename)
            migrate_csv_to_db(csv_path, db_path)
