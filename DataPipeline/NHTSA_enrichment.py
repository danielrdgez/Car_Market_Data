import requests
import json
import pandas as pd
import logging
import time
import os
from datetime import date
from typing import List, Dict, Optional
from urllib.parse import urljoin

class NHTSAEnricher:
    """Enrich car data with NHTSA vPIC API specifications using VIN"""
    
    BASE_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/"
    DECODE_ENDPOINT = "DecodeVinValuesExtended/"
    MAX_BATCH_SIZE = 50  # NHTSA recommends max 50 VINs per request
    
    def __init__(self, rate_limit_delay: float = 0.5, output_dir: Optional[str] = None):
        """
        Initialize NHTSA Enricher
        
        Args:
            rate_limit_delay: Delay in seconds between API requests
            output_dir: Directory to save enriched data and logs
        """
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0
        self.output_dir = output_dir or os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "CAR_DATA_OUTPUT"
        )
        
        # Create output directory if it doesn't exist
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Setup logging
        self.setup_logging()
    
    def setup_logging(self) -> None:
        """Configure logging for NHTSA enrichment process"""
        log_file = os.path.join(self.output_dir, f'nhtsa_enrichment_{date.today()}.log')
        
        logging.basicConfig(
            filename=log_file,
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
    
    def _apply_rate_limit(self) -> None:
        """Enforce rate limiting between API requests"""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_delay:
            time.sleep(self.rate_limit_delay - elapsed)
        self.last_request_time = time.time()
    
    def _is_valid_vin(self, vin: str) -> bool:
        """Check if VIN is valid (not null, empty, or placeholder)"""
        if pd.isna(vin) or vin is None:
            return False
        vin_str = str(vin).strip()
        if not vin_str or len(vin_str) < 3:
            return False
        # Filter out placeholder VINs with asterisks
        if '*' in vin_str or 'invalid' in vin_str.lower():
            return False
        return True
    
    def decode_vin(self, vin: str) -> Optional[Dict]:
        """
        Query NHTSA API to decode a single VIN
        
        Args:
            vin: Vehicle Identification Number
            
        Returns:
            Dictionary with API response data or None if request fails
        """
        self._apply_rate_limit()
        
        if not self._is_valid_vin(vin):
            logging.warning(f"Invalid VIN format: {vin}")
            return None
        
        try:
            url = f"{self.BASE_URL}{self.DECODE_ENDPOINT}{vin}?format=json"
            logging.info(f"Querying NHTSA API for VIN: {vin}")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check if VIN was successfully decoded
            if data.get("Results"):
                return data
            else:
                logging.warning(f"No results returned for VIN: {vin}")
                return None
                
        except requests.exceptions.Timeout:
            logging.error(f"Timeout querying VIN {vin}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error querying VIN {vin}: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error decoding VIN {vin}: {e}")
            return None
    
    def extract_specs_from_results(self, results: List[Dict]) -> Dict:
        """
        Extract ALL vehicle specifications from NHTSA API results
        
        Args:
            results: List of result dictionaries from NHTSA API
            
        Returns:
            Dictionary with all extracted specifications
        """
        specs = {}
        
        # If results is a list, take the first item (should only be one)
        if isinstance(results, list) and len(results) > 0:
            result_item = results[0]
        else:
            result_item = results
        
        # Extract ALL fields from the NHTSA API response
        # Prefix with 'nhtsa_' to distinguish from AutoTempest data
        all_fields = [
            "ABS", "ActiveSafetySysNote", "AdaptiveCruiseControl", "AdaptiveDrivingBeam",
            "AdaptiveHeadlights", "AdditionalErrorText", "AirBagLocCurtain", "AirBagLocFront",
            "AirBagLocKnee", "AirBagLocSeatCushion", "AirBagLocSide", "AutoReverseSystem",
            "AutomaticPedestrianAlertingSound", "AxleConfiguration", "Axles", "BasePrice",
            "BatteryA", "BatteryA_to", "BatteryCells", "BatteryInfo", "BatteryKWh",
            "BatteryKWh_to", "BatteryModules", "BatteryPacks", "BatteryType", "BatteryV",
            "BatteryV_to", "BedLengthIN", "BedType", "BlindSpotIntervention", "BlindSpotMon",
            "BodyCabType", "BodyClass", "BrakeSystemDesc", "BrakeSystemType", "BusFloorConfigType",
            "BusLength", "BusType", "CAN_AACN", "CIB", "CashForClunkers", "ChargerLevel",
            "ChargerPowerKW", "CombinedBrakingSystem", "CoolingType", "CurbWeightLB",
            "CustomMotorcycleType", "DaytimeRunningLight", "DestinationMarket", "DisplacementCC",
            "DisplacementCI", "DisplacementL", "Doors", "DriveType", "DriverAssist",
            "DynamicBrakeSupport", "EDR", "ESC", "EVDriveUnit", "ElectrificationLevel",
            "EngineConfiguration", "EngineCycles", "EngineCylinders", "EngineHP", "EngineHP_to",
            "EngineKW", "EngineManufacturer", "EngineModel", "EntertainmentSystem", "ErrorCode",
            "ErrorText", "ForwardCollisionWarning", "FuelInjectionType", "FuelTankMaterial",
            "FuelTankType", "FuelTypePrimary", "FuelTypeSecondary", "GCWR", "GCWR_to",
            "KeylessIgnition", "LaneCenteringAssistance", "LaneDepartureWarning", "LaneKeepSystem",
            "LowerBeamHeadlampLightSource", "Make", "MakeID", "Manufacturer", "ManufacturerId",
            "Model", "ModelID", "ModelYear", "MotorcycleChassisType", "MotorcycleSuspensionType",
            "NCSABodyType", "NCSAMake", "NCSAMapExcApprovedBy", "NCSAMapExcApprovedOn",
            "NCSAMappingException", "NCSAModel", "NCSANote", "NonLandUse", "Note",
            "OtherBusInfo", "OtherEngineInfo", "OtherMotorcycleInfo", "OtherRestraintSystemInfo",
            "OtherTrailerInfo", "ParkAssist", "PedestrianAutomaticEmergencyBraking", "PlantCity",
            "PlantCompanyName", "PlantCountry", "PlantState", "PossibleValues", "Pretensioner",
            "RearAutomaticEmergencyBraking", "RearCrossTrafficAlert", "RearVisibilitySystem",
            "SAEAutomationLevel", "SAEAutomationLevel_to", "SeatBeltsAll", "SeatRows", "Seats",
            "SemiautomaticHeadlampBeamSwitching", "Series", "Series2", "SteeringLocation",
            "SuggestedVIN", "TPMS", "TopSpeedMPH", "TrackWidth", "TractionControl",
            "TrailerBodyType", "TrailerLength", "TrailerType", "TransmissionSpeeds",
            "TransmissionStyle", "Trim", "Trim2", "Turbo", "VIN", "ValveTrainDesign",
            "VehicleDescriptor", "VehicleType", "WheelBaseLong", "WheelBaseShort", "WheelBaseType",
            "WheelSizeFront", "WheelSizeRear", "WheelieMitigation", "Wheels", "Windows"
        ]
        
        # Extract all fields from the API result
        for field in all_fields:
            value = result_item.get(field, "")
            # Prefix with 'nhtsa_' to avoid conflicts with AutoTempest data
            specs[f"nhtsa_{field}"] = value
        
        return specs
    
    def enrich_data_from_csv(self, input_csv: str, output_csv: Optional[str] = None) -> pd.DataFrame:
        """
        Read AutoTempest data, enrich with NHTSA specs, and merge on VIN
        
        Args:
            input_csv: Path to AutoTempest CSV file
            output_csv: Path to save enriched CSV (optional)
            
        Returns:
            Enriched DataFrame
        """
        # Read input CSV
        try:
            df_autotempest = pd.read_csv(input_csv)
            logging.info(f"Loaded {len(df_autotempest)} records from {input_csv}")
        except Exception as e:
            logging.error(f"Error reading input CSV: {e}")
            raise
        
        # Filter for valid VINs
        df_valid = df_autotempest[df_autotempest['vin'].apply(self._is_valid_vin)].copy()
        logging.info(f"Found {len(df_valid)} records with valid VINs")
        
        if len(df_valid) == 0:
            logging.warning("No valid VINs found in dataset")
            return df_autotempest
        
        # Get unique VINs to query
        unique_vins = df_valid['vin'].unique().tolist()
        logging.info(f"Querying NHTSA API for {len(unique_vins)} unique VINs")
        
        # Query NHTSA API for each VIN
        nhtsa_specs = {}
        for i, vin in enumerate(unique_vins, 1):
            print(f"[{i}/{len(unique_vins)}] Querying VIN: {vin}")
            
            api_response = self.decode_vin(vin)
            
            if api_response and api_response.get("Results"):
                specs = self.extract_specs_from_results(api_response["Results"])
                nhtsa_specs[vin] = specs
            else:
                nhtsa_specs[vin] = {}
        
        # Convert specs dictionary to DataFrame
        df_nhtsa = pd.DataFrame.from_dict(nhtsa_specs, orient='index')
        df_nhtsa.index.name = 'vin'
        df_nhtsa = df_nhtsa.reset_index()
        
        logging.info(f"Successfully queried {len(df_nhtsa)} VINs from NHTSA API")
        
        # Merge on VIN
        df_enriched = df_autotempest.merge(
            df_nhtsa,
            on='vin',
            how='left'
        )
        
        logging.info(f"Merged data: {len(df_enriched)} total records")
        
        # Save enriched data
        if output_csv is None:
            output_csv = os.path.join(self.output_dir, f"ENRICHED_CAR_DATA_{date.today()}.csv")
        
        try:
            df_enriched.to_csv(output_csv, index=False)
            logging.info(f"Enriched data saved to {output_csv}")
            print(f"\n✓ Enriched data exported to: {output_csv}")
        except Exception as e:
            logging.error(f"Error saving enriched CSV: {e}")
        
        return df_enriched
    
    def get_latest_car_data_file(self) -> str:
        """Get the latest CAR_DATA CSV file from output directory"""
        try:
            files = [
                f for f in os.listdir(self.output_dir)
                if f.startswith("CAR_DATA_") and f.endswith(".csv")
            ]
            if not files:
                raise FileNotFoundError("No CAR_DATA CSV files found")
            
            latest_file = max(
                files,
                key=lambda x: os.path.getctime(os.path.join(self.output_dir, x))
            )
            return os.path.join(self.output_dir, latest_file)
        except Exception as e:
            logging.error(f"Error finding latest CAR_DATA file: {e}")
            raise
    
    def run(self, input_csv: Optional[str] = None, output_csv: Optional[str] = None) -> pd.DataFrame:
        """
        Main method to run the enrichment pipeline
        
        Args:
            input_csv: Path to input CSV (auto-detects if not provided)
            output_csv: Path to save enriched data (optional)
            
        Returns:
            Enriched DataFrame
        """
        if input_csv is None:
            input_csv = self.get_latest_car_data_file()
            print(f"Auto-detected input file: {input_csv}")
        
        logging.info("Starting NHTSA enrichment pipeline")
        print("\n" + "="*60)
        print("NHTSA Data Enrichment Pipeline")
        print("="*60)
        
        try:
            df_enriched = self.enrich_data_from_csv(input_csv, output_csv)
            logging.info("NHTSA enrichment pipeline completed successfully")
            print("\n✓ Enrichment pipeline completed successfully!")
            
            return df_enriched
        
        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            print(f"\n✗ Pipeline failed: {e}")
            raise


def main():
    """Main function to run the NHTSA enrichment pipeline"""
    enricher = NHTSAEnricher(
        rate_limit_delay=0.5,
        output_dir="/Users/OneTwo/Desktop/Car-Price-Data-Visualization-Learning/CAR_DATA_OUTPUT"
    )
    
    try:
        # Run enrichment (auto-detects latest CAR_DATA file)
        df_enriched = enricher.run()
        
        print(f"\nEnriched dataset shape: {df_enriched.shape}")
        print(f"Columns: {list(df_enriched.columns)}")
        print("\nFirst few rows:")
        print(df_enriched.head())
        
    except Exception as e:
        print(f"Error in enrichment pipeline: {e}")


if __name__ == "__main__":
    main()
