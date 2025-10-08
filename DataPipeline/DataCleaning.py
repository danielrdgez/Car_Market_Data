import pandas as pd
import logging
from datetime import date
import os
from typing import Optional

class DataCleaner:
    """Class to clean and peprocess data files."""

    def __init__(self, input_dir, output_dir):
        """
        Initialize DataCleaner

        Args:
            input_dir (str): Directory containing raw CSV file
            output_dir (str): Directory to cleaned data output
        """
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.setup_logging()

    def setup_logging(self) -> None:
        """Configure logging process for data cleaning"""
        os.makedirs(self.output_dir, exist_ok=True)

        log_file = os.path.join(self.output_dir, f'cleaning_log_{date.today()}.log')

        logging.basicConfig(
            filename=log_file,
            level = logging.INFO,
            format= '%(asctime)s - %(levelname)s - %(message)s'
        )
    
    def get_latest_file(self) -> str:
        """Get the latest raw data CSV file from the input directory"""
        try:
            files = [f for f in os.listdir(self.input_dir) if f.startswith("CAR_DATA_")]
            latest_file = max(files, key=lambda x: os.path.getctime(os.path.join(self.input_dir, x)))
            return os.path.join(self.input_dir, latest_file)
        except Exception as e:
            logging.error(f"Error finding latest file: {e}")
            raise

    def clean_price(self, df:pd.DataFrame) -> pd.DataFrame:
        """"Clean the Price Column"""
        try:
            df['price'] = df['price'].replace('[\$,]', '', regex=True)
            df['price'] = pd.to_numeric(df['price'].replace("Inquire", pd.NA), errors='coerce') 
            return df
        except Exception as e:
            logging.error(f"Error cleaning price: {e}")
            raise

    def standardize_makes(self, df:pd.DataFrame) -> pd.DataFrame:
        """Standardize the makes so there arent so many variations"""
        try:
            # Convert all values to string first to handle any numeric values
            df['make'] = df['make'].astype(str)

            df['make'] = df['make'].str.strip()
            df['make'] = df['make'].str.title()

            logging.info(f"Unique makes before standardization: {df['make'].nunique()}")
            logging.info(f"Standardized makes: {sorted(df['make'].unique())}")

            return df
        except Exception as e:
            logging.error(f"Error standardizing makes: {e}")
            raise

    def standardize_models(self, df: pd.DataFrame) -> pd.DataFrame:
        """Standardize the models so there aren't so many variations"""
        try:
            # Convert all values to string first to handle any numeric values
            df['model'] = df['model'].astype(str)
            
            # Clean and standardize model names
            df['model'] = df['model'].str.strip()
            df['model'] = df['model'].str.title()
    
            logging.info(f"Unique models before standardization: {df['model'].nunique()}")
            logging.info(f"Standardized models: {sorted(df['model'].unique())}")
    
            return df
        except Exception as e:
            logging.error(f"Error standardizing models: {e}")
            raise  

    def process_data(self) -> Optional[pd.DataFrame]:
        """Main method to process the data"""
        try:
            # Get latest file
            latest_file = self.get_latest_file()
            logging.info(f"Processing {latest_file}")
            
            # Read raw data
            df = pd.read_csv(latest_file)
            initial_rows = len(df)
            logging.info(f"Initial row count: {initial_rows}")
            
            # Apply cleaning steps
            df = self.clean_price(df)
            df = self.standardize_makes(df)
            df = self.standardize_models(df)
            
            # Save cleaned data
            os.makedirs(self.output_dir, exist_ok=True)
            output_file = os.path.join(self.output_dir, f"CLEAN_CAR_DATA_{date.today()}.csv")
            df.to_csv(output_file, index=False)

            logging.info(f"Cleaned data saved to {output_file}")
            
            return df
            
        except Exception as e:
            logging.error(f"Error processing data: {e}")
            return None

def main():
    """Main function to run the data cleaning pipeline"""
    INPUT_DIR = "/Users/OneTwo/Documents/CAR_ML/CAR_DATA_OUTPUT"
    OUTPUT_DIR = "/Users/OneTwo/Documents/CAR_ML/CAR_DATA_CLEAN"
    
    cleaner = DataCleaner(INPUT_DIR, OUTPUT_DIR)
    
    try:
        cleaned_df = cleaner.process_data()
        if cleaned_df is not None:
            print("Data cleaning completed successfully!")
    except Exception as e:
        print(f"Error in data cleaning pipeline: {e}")

if __name__ == "__main__":
    main()




    
    