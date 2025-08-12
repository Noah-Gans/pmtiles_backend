import json
import os
from downloading_and_geojson_processing.base_downloader import BaseDownloader
from downloading_and_geojson_processing.data_merger import DataMerger
from downloading_and_geojson_processing.data_standardizer import DataStandardizer

class BaseCounty:
    """Base county class with common ownership processing"""
    
    def __init__(self, county_name, output_dir="geojson_files", config_path="download_and_file_config.json"):
        self.county_name = county_name
        self.output_dir = output_dir
        
        # Fix config path to be relative to tile_cycle directory
        if not os.path.isabs(config_path):
            from pathlib import Path
            tile_cycle_dir = Path(__file__).parent.parent
            config_path = tile_cycle_dir / config_path
        
        # Initialize ownership attributes with default values
        self.parcel_download_type = None
        self.parcel_url = None
        self.address_download_type = None
        self.address_url = None
        self.needs_merging = False
        self.data_type = "ownership"
        
        # Initialize data storage attributes
        self.parcel_data = None
        self.address_data = None
        self.merged_data = None
        self.standardized_data = None
        
        self.config = self._load_config(str(config_path))
        self.county_config = self.config.get(county_name, {})
        # Optionally, set as attributes:
        for k, v in self.county_config.items():
            setattr(self, k, v)
        self.downloader = BaseDownloader(output_dir)
        self.merger = DataMerger(output_dir)
        self.standardizer = DataStandardizer(output_dir)
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Set up ownership-specific attributes from config
        self._setup_ownership_attributes()
    
    def collect_and_organize_county_ownership_data(self):
        """Template method that subclasses can override"""
        print(f"🚀 Processing ownership for {self.county_name}")
        
        # Step 1: Download ownership data
        self.collect_ownership_data()
        
        # Step 2: Clean and format data
        self.clean_and_normalize_names()

        # Step 2: Merge address data (if needed)
        if self.needs_merging:
            self.merge_address_data()
        
        # Step 4: Standardize data
        self.standardize_data()
        
        # Step 5: Save final data
        self.save_final_data()
        
        print(f"✅ Completed ownership processing for {self.county_name}")
        return self.standardized_data
    
    def collect_ownership_data(self):
        """Download ownership data based on config attributes"""
        print(f"📥 Collecting ownership data for {self.county_name}")
        
        # Download parcel data
        if self.parcel_download_type and self.parcel_url:
            self.collect_and_downlod_parcel_data()
        else:
            raise ValueError(f"Missing parcel download configuration for {self.county_name}")
        
        # Download address data if needed
        if self.address_download_type and self.address_url:
            self.collect_and_download_address_data()
        else:
            print(f"📝 No address data download needed for {self.county_name}")
    
    def collect_and_downlod_parcel_data(self):
        """Download parcel data based on download type"""
        print(f"📦 Downloading parcel data using {self.parcel_download_type}")
        
        if self.parcel_download_type == "signed_geojson":
            filename = f"{self.county_name}_ownership_parcel.geojson"
            self.downloader.download_signed_geojson(self.parcel_url, filename)
            
        elif self.parcel_download_type == "kmz":
            self.downloader.download_kmz(self.parcel_url, f"{self.county_name}_ownership_parcel")
            # Load the downloaded data (KMZ creates multiple files, we need the ownership one)
            
        elif self.parcel_download_type == "zip_ownership_shp":
            self.downloader.download_zip(self.parcel_url, f"{self.county_name}_ownership", expect_shp="ownership.shp") #<---- look here, figuring out how it knows to loook for ownership.shp

        else:
            raise ValueError(f"Unsupported parcel download type: {self.parcel_download_type}")
         
    def collect_and_download_address_data(self):
        """Download address data based on download type"""
        print(f"📦 Downloading address data using {self.address_download_type}")
        
        if self.address_download_type == "kmz":
            self.downloader.download_kmz(self.address_url, f"{self.county_name}_ownership_address")
            # Load the downloaded data
            address_file = self.get_file_path(f"{self.county_name}_ownership_address_ownership_address.geojson")
            if self.file_exists(f"{self.county_name}_ownership_address_ownership_address.geojson"):
                self.address_data = self.load_geojson(address_file)
            else:
                raise FileNotFoundError(f"Address file not found: {address_file}")
        
        else:
            raise ValueError(f"Unsupported address download type: {self.address_download_type}")
        
        print(f"✅ Downloaded address data: {len(self.address_data['features'])} addresses")
    
    def merge_address_data(self):
        """Override in subclasses if address merging is needed"""
        print(f"📝 No address merging needed for {self.county_name}")
        pass
    
    def clean_and_normalize_names(self, parcel_filename=None, address_filename=None):
        """
        Normalize file names in the output directory.
        If one file: rename to {county_name}_ownership_complete.<ext>
        If two files: rename to {county_name}_ownership_parcel.<ext> and {county_name}_ownership_address.<ext>
        Optionally, specify which files to rename.
        """
        files = [f for f in os.listdir(self.output_dir) if (f.endswith('.geojson') or f.endswith('.jsonl'))]
        if parcel_filename and address_filename:
            files = [parcel_filename, address_filename]
        elif parcel_filename:
            files = [parcel_filename]
        elif address_filename:
            files = [address_filename]

        if len(files) == 1:
            src = os.path.join(self.output_dir, files[0])
            ext = os.path.splitext(files[0])[1]
            dst = os.path.join(self.output_dir, f"{self.county_name}_ownership_complete{ext}")
            if src != dst:
                os.rename(src, dst)
                print(f"🔄 Renamed {files[0]} → {os.path.basename(dst)}")
        elif len(files) == 2:
            for f in files:
                src = os.path.join(self.output_dir, f)
                ext = os.path.splitext(f)[1]
                if "address" in f.lower():
                    dst = os.path.join(self.output_dir, f"{self.county_name}_ownership_address{ext}")
                else:
                    dst = os.path.join(self.output_dir, f"{self.county_name}_ownership_parcel{ext}")
                if src != dst:
                    os.rename(src, dst)
                    print(f"🔄 Renamed {f} → {os.path.basename(dst)}")
        else:
            print(f"⚠️ Expected 1 or 2 GeoJSON/JSONL files, found {len(files)}. Skipping renaming.")
    
    def standardize_data(self):
        """Standardize the data to common format"""
        complete_path = self.get_file_path(f"{self.county_name}_ownership_complete.geojson")
        if not os.path.exists(complete_path):
            raise FileNotFoundError(f"Expected file not found: {complete_path}")
        
        print(f"📊 Standardizing data for {self.county_name}")
        print(f"  📁 Reading from: {complete_path}")
        
        # Try different encodings to handle potential encoding issues
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        data_to_standardize = None
        
        for encoding in encodings:
            try:
                with open(complete_path, 'r', encoding=encoding) as f:
                    data_to_standardize = json.load(f)
                print(f"  ✅ Successfully read file with {encoding} encoding")
                break
            except UnicodeDecodeError:
                continue
            except json.JSONDecodeError:
                continue
        
        if data_to_standardize is None:
            raise ValueError(f"Could not read {complete_path} with any of the attempted encodings: {encodings}")
        
        self.standardized_data = self.standardizer.standardize_ownership(
            data_to_standardize, 
            self.county_name
        )
        
        print(f"✅ Standardization complete for {self.county_name}")
        print(f"  📊 Features processed: {len(self.standardized_data['features'])}")
    
    def save_final_data(self):
        """Save the standardized data to file"""
        if hasattr(self, 'standardized_data'):
            output_path = self.standardizer.save_standardized_data(
                self.standardized_data, 
                self.county_name
            )
            print(f"💾 Saved final data for {self.county_name} to {output_path}")
        else:
            print(f"⚠️ No standardized data to save for {self.county_name}")
    
    def load_geojson(self, file_path):
        """Helper method to load GeoJSON data"""
        # Try different encodings to handle potential encoding issues
        encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1']
        
        for encoding in encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    return json.load(f)
            except UnicodeDecodeError:
                continue
            except json.JSONDecodeError:
                continue
        
        raise ValueError(f"Could not read {file_path} with any of the attempted encodings: {encodings}")
    
    def save_geojson(self, data, file_path):
        """Helper method to save GeoJSON data"""
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"💾 Saved {file_path}")
    
    def get_file_path(self, filename):
        """Helper method to get full file path in output directory"""
        return os.path.join(self.output_dir, filename)
    
    def file_exists(self, filename):
        """Helper method to check if file exists in output directory"""
        return os.path.exists(self.get_file_path(filename))

    def _load_config(self, config_path):
        """Helper method to load configuration from a JSON file"""
        with open(config_path, 'r') as f:
            return json.load(f)

    def _setup_ownership_attributes(self):
        """Set up ownership-related attributes from config"""
        ownership_config = self.county_config.get("ownership", {})
        
        # Update attributes from config
        self.parcel_download_type = ownership_config.get("parcel_download_type")
        self.parcel_url = ownership_config.get("parcel_url")
        self.address_download_type = ownership_config.get("address_download_type", None)
        self.address_url = ownership_config.get("address_url", None)
        self.needs_merging = ownership_config.get("needs_merging", False)
        self.data_type = ownership_config.get("data_type", "ownership")
        
        print(f"📋 Configured {self.county_name} ownership attributes:")
        print(f"   Parcel download type: {self.parcel_download_type}")
        print(f"   Parcel URL: {self.parcel_url}")
        print(f"   Address download type: {self.address_download_type}")
        print(f"   Address URL: {self.address_url}")
        print(f"   Needs merging: {self.needs_merging}")
        print(f"   Data type: {self.data_type}")
