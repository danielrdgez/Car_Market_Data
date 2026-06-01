import sqlite3
import pandas as pd
import plotly.express as px
import os
from typing import Optional

def analyze_depreciation(db_path: str, make: str, models: list[str], years: list[str | int]) -> Optional[pd.DataFrame]:
    """
    Extracts depreciation data for specific vehicle configurations.
    Compares the first selling price (from the same year as the model year)
    with the current listed price.

    Args:
        db_path: Path to the SQLite database (e.g., CAR_DATA.db).
        make: nhtsa_Make value.
        models: List of nhtsa_Model values.
        years: List of nhtsa_ModelYear values.

    Returns:
        pd.DataFrame containing the matched vehicles and depreciation metrics.
    """
    model_placeholders = ','.join('?' for _ in models)
    year_placeholders = ','.join('?' for _ in years)

    query = f"""
    WITH TargetVehicles AS (
        SELECT vin, nhtsa_Make, nhtsa_Model, nhtsa_ModelYear
        FROM nhtsa_enrichment
        WHERE nhtsa_Make = ? 
          AND nhtsa_Model IN ({model_placeholders}) 
          AND nhtsa_ModelYear IN ({year_placeholders})
    ),
    FirstYearPrices AS (
        SELECT ph.vin, ph.price AS original_price, ph.history_date AS original_date, ph.mileage AS original_mileage,
               ROW_NUMBER() OVER(PARTITION BY ph.vin ORDER BY ph.history_date ASC) as rn
        FROM price_history ph
        JOIN TargetVehicles tv ON ph.vin = tv.vin
        WHERE strftime('%Y', ph.history_date) = CAST(tv.nhtsa_ModelYear AS TEXT)
    ),
    LatestListings AS (
        SELECT vin, title, price AS current_price, loaddate AS current_date, mileage AS current_mileage,
               ROW_NUMBER() OVER(PARTITION BY vin ORDER BY loaddate DESC) as rn
        FROM listings
    )
    SELECT 
        t.vin,
        t.nhtsa_Make,
        t.nhtsa_Model,
        t.nhtsa_ModelYear,
        f.original_price,
        f.original_date,
        f.original_mileage,
        l.title,
        l.current_price,
        l.current_date,
        l.current_mileage
    FROM TargetVehicles t
    JOIN FirstYearPrices f ON t.vin = f.vin AND f.rn = 1
    JOIN LatestListings l ON t.vin = l.vin AND l.rn = 1
    -- Filter out obvious placeholder prices (e.g., $0, $1, $1234)
    WHERE f.original_price > 1234 AND l.current_price > 1234
    """

    with sqlite3.connect(db_path) as conn:
        params = [make] + list(models) + [str(y) for y in years]
        df = pd.read_sql_query(query, conn, params=params)

    if df.empty:
        print(f"No data found for configuration: {make} | Models: {models} | Years: {years}.")
        return None

    # Date parsing logic
    df['original_date'] = pd.to_datetime(df['original_date'], errors='coerce')
    df['current_date'] = pd.to_datetime(df['current_date'], errors='coerce')

    # Handle missing mileage values by treating them as 0
    df['original_mileage'] = df['original_mileage'].fillna(0)
    df['current_mileage'] = df['current_mileage'].fillna(0)

    # Feature Engineering via Vectorization
    df['depreciation_dollar'] = df['original_price'] - df['current_price']
    df['depreciation_percent'] = (df['depreciation_dollar'] / df['original_price']) * 100
    df['mileage_added'] = df['current_mileage'] - df['original_mileage']

    # Handle zeros to avoid division errors
    df['years_passed'] = (df['current_date'] - df['original_date']).dt.days / 365.25
    df['years_passed'] = df['years_passed'].replace(0, 0.001)
    df['mileage_added_div'] = df['mileage_added'].replace(0, 1)

    df['depreciation_per_year'] = df['depreciation_dollar'] / df['years_passed']
    df['depreciation_per_mile'] = df['depreciation_dollar'] / df['mileage_added_div']

    # Group by title for analysis
    title_analysis = df.groupby('title').agg(
        count_vehicles=('vin', 'count'),
        avg_depreciation_dollar=('depreciation_dollar', 'mean'),
        avg_depreciation_percent=('depreciation_percent', 'mean'),
        avg_depreciation_per_year=('depreciation_per_year', 'mean'),
        avg_depreciation_per_mile=('depreciation_per_mile', 'mean'),
        avg_original_price=('original_price', 'mean'),
        avg_current_price=('current_price', 'mean')
    ).reset_index()

    print("\n--- Depreciation Analysis by Title ---")
    print(title_analysis.to_string(index=False))
    print("--------------------------------------\n")

    # Save output to CSV
    output_dir = r"C:\Users\danie\Code\Car-Price-Data-Visualization-Learning\CAR_DATA_OUTPUT"
    os.makedirs(output_dir, exist_ok=True)

    raw_csv_path = os.path.join(output_dir, f"depreciation_raw_{make}.csv")
    title_csv_path = os.path.join(output_dir, f"depreciation_by_title_{make}.csv")

    df.to_csv(raw_csv_path, index=False)
    title_analysis.to_csv(title_csv_path, index=False)
    print(f"Saved raw data to: {raw_csv_path}")
    print(f"Saved title analysis to: {title_csv_path}\n")

    # Generate interactive Plotly scatter plot
    fig = px.scatter(
        df,
        x='current_mileage',
        y='current_price',
        color='depreciation_percent',
        hover_data=['vin', 'original_price', 'original_date', 'current_date', 'title', 'nhtsa_Model', 'nhtsa_ModelYear'],
        title=f"Depreciation Analysis: {make} (Models: {', '.join(models)}) (Years: {', '.join(map(str, years))})",
        labels={
            'current_mileage': 'Current Mileage',
            'current_price': 'Current Listing Price ($)',
            'depreciation_percent': 'Depreciation (%)'
        },
        color_continuous_scale='Viridis'
    )

    avg_original = df['original_price'].mean()
    fig.add_hline(
        y=avg_original,
        line_dash="dash",
        line_color="red",
        annotation_text=f"Avg Original Price: ${avg_original:,.2f}"
    )

    fig.show()

    return df

if __name__ == "__main__":
    # Ensure correct relative layout path to DB
    analyze_depreciation(
        db_path="C:/Users/danie/Code/Car-Price-Data-Visualization-Learning/CAR_DATA_OUTPUT/CAR_DATA_CLEANED.db",
        make="FORD", 
        models=["MAVERICK", "F-150", "RANGER"],
        years=["2021","2022", "2023"]
    )
