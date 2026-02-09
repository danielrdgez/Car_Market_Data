import requests
import json
import pandas as pd
import logging
import time
import os
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from typing import List, Dict, Optional
from urllib.parse import urljoin
from database import CarDatabase

class NHTSAEnricher:
    """Enrich car data with NHTSA vPIC API specifications using VIN"""
    
    BASE_URL = "https://vpic.nhtsa.dot.gov/api/vehicles/"
    DECODE_ENDPOINT = "DecodeVinValuesExtended/"
    BATCH_DECODE_ENDPOINT = "DecodeVinValuesBatch/"
    RATINGS_BASE_URL = "https://api.nhtsa.gov/SafetyRatings/"
    RECALLS_BASE_URL = "https://api.nhtsa.gov/recalls/"
    COMPLAINTS_BASE_URL = "https://api.nhtsa.gov/complaints/"
    MAX_BATCH_SIZE = 50  # NHTSA recommends max 50 VINs per request
    
    def __init__(self, rate_limit_delay: float = 0.5, output_dir: Optional[str] = None, db_path: Optional[str] = None, max_workers: int = 5):
        """
        Initialize NHTSA Enricher
        
        Args:
            rate_limit_delay: Delay in seconds between API requests (per thread)
            output_dir: Directory to save enriched data and logs
            db_path: Path to CAR_DATA.db
            max_workers: Number of parallel worker threads
        """
        self.rate_limit_delay = rate_limit_delay
        self.last_request_time = 0
        self.max_workers = max_workers
        self.output_dir = output_dir or os.path.join(
            os.path.dirname(os.path.dirname(__file__)), "CAR_DATA_OUTPUT"
        )
        
        # Database path
        if db_path is None:
            self.db_path = os.path.join(self.output_dir, "CAR_DATA.db")
        else:
            self.db_path = db_path
            
        self.db = CarDatabase(self.db_path)
        
        # Caches for MMY data
        self.cache_safety = {}
        self.cache_recalls = {}
        self.cache_complaints = {}
        self.cache_lock = threading.Lock()
        
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
        """Enforce rate limiting between API requests in a thread-safe manner"""
        with self.cache_lock:
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

    def decode_vins_batch(self, vins: List[str]) -> Optional[Dict]:
        """
        Query NHTSA API to decode multiple VINs in a single batch request
        
        Args:
            vins: List of Vehicle Identification Numbers
            
        Returns:
            Dictionary with API response data or None if request fails
        """
        self._apply_rate_limit()
        
        valid_vins = [str(v).strip() for v in vins if self._is_valid_vin(v)]
        if not valid_vins:
            return None
            
        try:
            url = f"{self.BASE_URL}{self.BATCH_DECODE_ENDPOINT}"
            # Format: DATA=vin1;vin2;...&format=json
            payload = {
                "DATA": ";".join(valid_vins),
                "format": "json"
            }
            
            logging.info(f"Querying NHTSA API Batch for {len(valid_vins)} VINs")
            
            response = requests.post(url, data=payload, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("Results"):
                return data
            else:
                logging.warning(f"No results returned for VIN batch")
                return None
                
        except Exception as e:
            logging.error(f"Error decoding VIN batch: {e}")
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
        """all_fields = [
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
        ]"""
        
        selected_fields = ["ABS", "ActiveSafetySysNote", "AdaptiveCruiseControl", "AdaptiveDrivingBeam",
            "AdaptiveHeadlights", "AdditionalErrorText", "AirBagLocCurtain", "AirBagLocFront",
            "AirBagLocKnee", "AirBagLocSeatCushion", "AirBagLocSide", "AutoReverseSystem",
            "AutomaticPedestrianAlertingSound", "AxleConfiguration", "Axles", "BasePrice", "BedLengthIN", "BedType", "BlindSpotIntervention", "BlindSpotMon",
            "BodyCabType", "BodyClass", "BrakeSystemDesc", "BrakeSystemType", "ChargerLevel",
            "ChargerPowerKW", "CombinedBrakingSystem", "CoolingType", "CurbWeightLB","DaytimeRunningLight", "DestinationMarket", "DisplacementCC",
            "DisplacementCI", "DisplacementL", "Doors", "DriveType", "DriverAssist",
            "DynamicBrakeSupport", "EDR", "ESC", "EVDriveUnit", "ElectrificationLevel",
            "EngineConfiguration", "EngineCycles", "EngineCylinders", "EngineHP", "EngineHP_to",
            "EngineKW", "EngineManufacturer", "EngineModel", "EntertainmentSystem", "ForwardCollisionWarning", "FuelInjectionType", "FuelTankMaterial",
            "FuelTankType", "FuelTypePrimary", "FuelTypeSecondary",
            "KeylessIgnition", "LaneCenteringAssistance", "LaneDepartureWarning", "LaneKeepSystem",
            "LowerBeamHeadlampLightSource", "Make", "MakeID", "Manufacturer", "ManufacturerId",
            "Model", "ModelID", "ModelYear", "OtherEngineInfo", "ParkAssist", "PedestrianAutomaticEmergencyBraking",
            "RearAutomaticEmergencyBraking", "RearCrossTrafficAlert", "RearVisibilitySystem",
            "SAEAutomationLevel", "SAEAutomationLevel_to", "SeatRows", "Seats",
            "SemiautomaticHeadlampBeamSwitching", "TPMS", "TopSpeedMPH", "TrackWidth", "TractionControl", "TransmissionSpeeds",
            "TransmissionStyle", "Trim", "Trim2",
            "WheelSizeFront", "WheelSizeRear", "Windows","VehicleType", "WheelBaseLong", "WheelBaseShort", "WheelBaseType",]
        
        # Extract all fields from the API result
        for field in selected_fields:
            value = result_item.get(field, "")
            # Prefix with 'nhtsa_' to avoid conflicts with AutoTempest data
            specs[f"nhtsa_{field}"] = value
        
        return specs
    
    def get_safety_ratings(self, model_year: str, make: str, model: str) -> Optional[Dict]:
        """
        Query NHTSA Safety Ratings API
        
        Args:
            model_year: Vehicle model year (e.g., '2013')
            make: Vehicle make (e.g., 'Acura')
            model: Vehicle model (e.g., 'RDX')
            
        Returns:
            Dictionary with API response data or None if request fails
            Sample: {"Count":2,"Message":"Results returned successfully","Results":[...]}
        """
        self._apply_rate_limit()
        
        try:
            url = f"{self.RATINGS_BASE_URL}modelyear/{model_year}/make/{make}/model/{model}?format=json"
            logging.info(f"Querying NHTSA Safety Ratings API: {model_year} {make} {model}")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("Results"):
                return data
            else:
                logging.warning(f"No safety ratings found for {model_year} {make} {model}")
                return None
                
        except requests.exceptions.Timeout:
            logging.error(f"Timeout querying safety ratings for {model_year} {make} {model}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error querying safety ratings: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error getting safety ratings: {e}")
            return None
    
    def get_recalls(self, make: str, model: str, model_year: str) -> Optional[Dict]:
        """
        Query NHTSA Recalls API
        
        Args:
            make: Vehicle make (e.g., 'acura')
            model: Vehicle model (e.g., 'rdx')
            model_year: Vehicle model year (e.g., '2012')
            
        Returns:
            Dictionary with API response data or None if request fails
            Sample: {"Count":2,"Message":"Results returned successfully","results":[...]}
        """
        self._apply_rate_limit()
        
        try:
            url = f"{self.RECALLS_BASE_URL}recallsByVehicle?make={make}&model={model}&modelYear={model_year}&format=json"
            logging.info(f"Querying NHTSA Recalls API: {model_year} {make} {model}")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("results") or data.get("Results"):
                return data
            else:
                logging.warning(f"No recalls found for {model_year} {make} {model}")
                return None
                
        except requests.exceptions.Timeout:
            logging.error(f"Timeout querying recalls for {model_year} {make} {model}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error querying recalls: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error getting recalls: {e}")
            return None
    
    def get_complaints(self, make: str, model: str, model_year: str) -> Optional[Dict]:
        """
        Query NHTSA Complaints API
        
        Args:
            make: Vehicle make (e.g., 'acura')
            model: Vehicle model (e.g., 'rdx')
            model_year: Vehicle model year (e.g., '2012')
            
        Returns:
            Dictionary with API response data or None if request fails
            Sample: {"count":15,"message":"Results returned successfully","results":[...]}
        """
        self._apply_rate_limit()
        
        try:
            url = f"{self.COMPLAINTS_BASE_URL}complaintsByVehicle?make={make}&model={model}&modelYear={model_year}&format=json"
            logging.info(f"Querying NHTSA Complaints API: {model_year} {make} {model}")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if data.get("results") or data.get("Results"):
                return data
            else:
                logging.warning(f"No complaints found for {model_year} {make} {model}")
                return None
                
        except requests.exceptions.Timeout:
            logging.error(f"Timeout querying complaints for {model_year} {make} {model}")
            return None
        except requests.exceptions.RequestException as e:
            logging.error(f"Error querying complaints: {e}")
            return None
        except Exception as e:
            logging.error(f"Unexpected error getting complaints: {e}")
            return None
    
    def extract_ratings_data(self, results: List[Dict]) -> Dict:
        """
        Extract safety ratings data from NHTSA API results
        
        Args:
            results: List of result dictionaries from NHTSA Safety Ratings API
            
        Returns:
            Dictionary with extracted safety ratings data
        """
        ratings = {}
        
        if isinstance(results, list) and len(results) > 0:
            # Store count of ratings found
            ratings['nhtsa_safety_ratings_count'] = len(results)
            
            # Extract key fields from each rating (typically just one, but handle multiple)
            for idx, result in enumerate(results[:1]):  # Usually just one result
                ratings[f'nhtsa_overall_rating'] = result.get('OverallRating', '')
                ratings[f'nhtsa_front_crash_rating'] = result.get('FrontCrashRating', '')
                ratings[f'nhtsa_rollover_rating'] = result.get('RolloverRating', '')
                ratings[f'nhtsa_side_crash_rating'] = result.get('SideCrashRating', '')
        
        return ratings
    
    def extract_recalls_data(self, results: List[Dict]) -> Dict:
        """
        Extract recalls data from NHTSA API results
        
        Args:
            results: List of result dictionaries from NHTSA Recalls API
            
        Returns:
            Dictionary with extracted recall information
        """
        recalls = {}
        
        if isinstance(results, list):
            recalls['nhtsa_total_recalls'] = len(results)
            
            if len(results) > 0:
                # Extract key recall components
                all_components = []
                for recall in results:
                    component = recall.get('Component', '')
                    if component:
                        all_components.append(component)
                
                recalls['nhtsa_recall_components'] = '; '.join(set(all_components[:3]))  # Top 3 unique
                recalls['nhtsa_latest_recall_date'] = results[0].get('ReportReceivedDate', '')
        
        return recalls
    
    def extract_complaints_data(self, results: List[Dict]) -> Dict:
        """
        Extract complaints data from NHTSA API results
        
        Args:
            results: List of result dictionaries from NHTSA Complaints API
            
        Returns:
            Dictionary with extracted complaint information
        """
        complaints = {}
        
        if isinstance(results, list):
            complaints['nhtsa_total_complaints'] = len(results)
            
            if len(results) > 0:
                # Extract complaint statistics
                injury_count = sum(r.get('numberOfInjuries', 0) for r in results)
                death_count = sum(r.get('numberOfDeaths', 0) for r in results)
                crash_count = sum(1 for r in results if r.get('crash', False))
                fire_count = sum(1 for r in results if r.get('fire', False))
                
                complaints['nhtsa_complaint_injuries'] = injury_count
                complaints['nhtsa_complaint_deaths'] = death_count
                complaints['nhtsa_complaint_crash_related'] = crash_count
                complaints['nhtsa_complaint_fire_related'] = fire_count
                
                # Extract main complaint components
                all_components = []
                for complaint in results:
                    component = complaint.get('components', '')
                    if component:
                        all_components.append(component)
                
                complaints['nhtsa_common_complaint_areas'] = '; '.join(set(all_components[:3]))
        
        return complaints
    
    def enrich_database(self) -> int:
        """
        Enrich data in CAR_DATA.db with NHTSA specs using batching and parallel processing
        
        Returns:
            Number of records enriched
        """
        # Get VINs that need enrichment
        vins_to_process = self.db.get_vins_for_enrichment()
        logging.info(f"Found {len(vins_to_process)} VINs needing enrichment in database")
        
        if not vins_to_process:
            print("No new VINs to process in database.")
            return 0
            
        print(f"Querying NHTSA API for {len(vins_to_process)} VINs using {self.max_workers} threads...")
        
        # Split into batches of 50
        batches = [vins_to_process[i:i + self.MAX_BATCH_SIZE] for i in range(0, len(vins_to_process), self.MAX_BATCH_SIZE)]
        
        total_enriched = 0
        processed_count = 0
        
        def process_batch(batch_vins):
            nonlocal processed_count, total_enriched
            
            batch_specs = {}
            api_response = self.decode_vins_batch(batch_vins)
            
            if api_response and api_response.get("Results"):
                for result in api_response["Results"]:
                    vin = result.get("VIN")
                    if not vin:
                        continue
                        
                    specs = self.extract_specs_from_results(result)
                    
                    make = result.get("Make", "")
                    manufacturer = result.get("Manufacturer", "")
                    model = result.get("Model", "")
                    model_year = result.get("ModelYear", "")
                    
                    if make and model and model_year:
                        mmy_key = f"{model_year}|{make}|{model}"
                        
                        # Use thread-safe caching for MMY-based data
                        with self.cache_lock:
                            cached_safety = self.cache_safety.get(mmy_key)
                            cached_recalls = self.cache_recalls.get(mmy_key)
                            cached_complaints = self.cache_complaints.get(mmy_key)
                        
                        # Get Safety Ratings
                        if cached_safety is None:
                            ratings_response = self.get_safety_ratings(model_year, make, model)
                            cached_safety = self.extract_ratings_data(ratings_response["Results"]) if ratings_response and ratings_response.get("Results") else {}
                            with self.cache_lock:
                                self.cache_safety[mmy_key] = cached_safety
                        specs.update(cached_safety)
                        
                        # Get Recalls
                        if cached_recalls is None:
                            recalls_response = self.get_recalls(make, model, model_year)
                            cached_recalls = self.extract_recalls_data(recalls_response.get("results", [])) if recalls_response else {}
                            with self.cache_lock:
                                self.cache_recalls[mmy_key] = cached_recalls
                        specs.update(cached_recalls)
                        
                        # Get Complaints
                        if cached_complaints is None:
                            complaints_response = self.get_complaints(make, model, model_year)
                            cached_complaints = self.extract_complaints_data(complaints_response.get("results", [])) if complaints_response else {}
                            with self.cache_lock:
                                self.cache_complaints[mmy_key] = cached_complaints
                        specs.update(cached_complaints)
                    
                    # Add to batch dictionary
                    batch_specs[vin] = specs
                
                # Store batch in database
                if batch_specs:
                    self.db.insert_nhtsa_enrichment_batch(batch_specs)
                    with self.cache_lock:
                        total_enriched += len(batch_specs)
            
            with self.cache_lock:
                processed_count += len(batch_vins)
                if processed_count % 100 == 0 or processed_count >= len(vins_to_process):
                    print(f"[{processed_count}/{len(vins_to_process)}] Processed VINs... Enriched: {total_enriched}")
            
            return len(batch_specs)

        # Use ThreadPoolExecutor for parallel batch processing
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            executor.map(process_batch, batches)
                
        return total_enriched

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
            specs = {}
            
            if api_response and api_response.get("Results"):
                specs = self.extract_specs_from_results(api_response["Results"])
                
                # Extract make, model, and model_year for additional API queries
                result = api_response["Results"][0] if isinstance(api_response["Results"], list) else api_response["Results"]
                make = result.get("Make", "").lower()
                model = result.get("Model", "").lower()
                model_year = result.get("ModelYear", "")
                
                # Query additional NHTSA APIs if we have make, model, year
                if make and model and model_year:
                    # Get Safety Ratings
                    ratings_response = self.get_safety_ratings(model_year, make.title(), model.title())
                    if ratings_response and ratings_response.get("Results"):
                        ratings_data = self.extract_ratings_data(ratings_response["Results"])
                        specs.update(ratings_data)
                    
                    # Get Recalls
                    recalls_response = self.get_recalls(make, model, model_year)
                    if recalls_response:
                        recalls_data = self.extract_recalls_data(recalls_response.get("results", []))
                        specs.update(recalls_data)
                    
                    # Get Complaints
                    complaints_response = self.get_complaints(make, model, model_year)
                    if complaints_response:
                        complaints_data = self.extract_complaints_data(complaints_response.get("results", []))
                        specs.update(complaints_data)
            
            nhtsa_specs[vin] = specs
        
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
    
    def run(self) -> int:
        """
        Main method to run the enrichment pipeline using the database
        
        Returns:
            Number of records enriched
        """
        logging.info("Starting NHTSA database enrichment pipeline")
        print("\n" + "="*60)
        print("NHTSA Database Data Enrichment Pipeline")
        print("="*60)
        
        try:
            count = self.enrich_database()
            logging.info(f"NHTSA enrichment pipeline completed successfully. Enriched {count} records.")
            print(f"\n✓ Enrichment pipeline completed! Enriched {count} new records.")
            return count
        except Exception as e:
            logging.error(f"Pipeline failed: {e}")
            print(f"\n✗ Pipeline failed: {e}")
            raise


def main():
    """Main function to run the NHTSA enrichment pipeline"""
    # Use higher parallelism and batching to speed up
    enricher = NHTSAEnricher(rate_limit_delay=0.2, max_workers=10)
    
    try:
        # Run enrichment (uses database)
        count = enricher.run()
        print(f"Total new records enriched: {count}")
        
    except Exception as e:
        print(f"Error in enrichment pipeline: {e}")


if __name__ == "__main__":
    main()
