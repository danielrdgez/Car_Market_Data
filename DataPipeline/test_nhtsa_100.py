import pandas as pd
import os
from datetime import date
from NHTSA_enrichment import NHTSAEnricher

def test_enrichment_100_iterations():
    """Test NHTSA enrichment with only 100 records"""
    
    # Initialize enricher
    enricher = NHTSAEnricher(rate_limit_delay=0.5)
    
    # Get the latest car data file
    input_csv = enricher.get_latest_car_data_file()
    print(f"Using input file: {input_csv}")
    
    # Read and limit to 100 records
    df = pd.read_csv(input_csv)
    print(f"Original dataset size: {len(df)} records")
    
    # Limit to 1000 records
    df_test = df[:1000].copy()
    print(f"Test dataset size: {len(df_test)} records")
    
    # Save test data
    test_input_csv = os.path.join(enricher.output_dir, f"TEST_CAR_DATA_{date.today()}.csv")
    df_test.to_csv(test_input_csv, index=False)
    print(f"Test input saved to: {test_input_csv}")
    
    # Run enrichment on test data
    print("\n" + "="*60)
    print("Running NHTSA Enrichment on 100 Test Records")
    print("="*60 + "\n")
    
    test_output_csv = os.path.join(enricher.output_dir, f"TEST_ENRICHED_CAR_DATA_{date.today()}.csv")
    
    try:
        df_enriched = enricher.enrich_data_from_csv(test_input_csv, test_output_csv)
        
        print(f"\n✓ Test completed successfully!")
        print(f"Enriched dataset shape: {df_enriched.shape}")
        print(f"Original columns: {len(df.columns)}")
        print(f"Enriched columns: {len(df_enriched.columns)}")
        print(f"New columns added: {len(df_enriched.columns) - len(df.columns)}")
        
        print(f"\nFirst 5 rows of enriched data:")
        print(df_enriched.head())
        
        print(f"\nSample enriched columns:")
        nhtsa_cols = [col for col in df_enriched.columns if col.startswith('nhtsa_')]
        print(f"Found {len(nhtsa_cols)} NHTSA columns")
        if nhtsa_cols:
            print(f"Sample: {nhtsa_cols[:]}")
        
        return df_enriched
        
    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        raise

if __name__ == "__main__":
    df_enriched = test_enrichment_100_iterations()
