import json
import os
from counties.base_county import BaseCounty
from downloading_and_geojson_processing.lincoln_county_scraper import LincolnCountyScraper

class TetonCountyWy(BaseCounty):
    """Teton County, Wyoming - has separate address file"""
    
    def __init__(self, county_name, output_dir="geojson_files"):
        super().__init__(county_name, output_dir)

    def clean_and_normalize_names(self, parcel_filename=None, address_filename=None):
        # Remove the address file if it exists
        super().clean_and_normalize_names(parcel_filename, address_filename)
        address_file = os.path.join(self.output_dir, f"{self.county_name}_ownership_address.geojson")
        if os.path.exists(address_file):
            os.remove(address_file)
            print(f"🗑 Removed address file: {address_file}")
        super().clean_and_normalize_names(parcel_filename, address_filename)
        print(f"🔄 Parsing descriptions for Teton County...")
        parsed_complete_path = self.get_file_path(f"{self.county_name}_ownership_complete.geojson")
        self.merger.parse_description_to_properties(self.get_file_path(f"{self.county_name}_ownership_complete.geojson"), parsed_complete_path)
        print(f"✅ Parsed descriptions for Teton County")
        


class LincolnCountyWy(BaseCounty):
    """Lincoln County, Wyoming - needs web scraping"""
    
    def __init__(self, county_name, output_dir="geojson_files"):
        super().__init__(county_name, output_dir)
    
    def collect_ownership_data(self):
        """Override to add web scraping for Lincoln County"""
        # First, do the standard download
        
        super().collect_ownership_data()

        # Then run web scraper for additional address details
        print(f"🔎 Scraping Lincoln County property details from the web...")
        scraper = LincolnCountyScraper(self.output_dir)
        self.scraped_data = scraper.scrape_all_properties()
        print(f"✅ Scraped {len(self.scraped_data)} property details")
    
    def merge_address_data(self):
        """Merge scraped data into parcel data"""
        print(f"🔄 Merging scraped data for Lincoln County...")
        self.merged_data = self.merger.join_address_to_parcel(self.get_file_path(f"{self.county_name}_ownership_parcel.geojson"), self.get_file_path(f"{self.county_name}_ownership_address.jsonl"), "RWACCT", "Account #")
        print(f"✅ Merged scraped data for Lincoln County")

class SubletteCountyWy(BaseCounty):
    """Sublette County, Wyoming - simple ZIP with SHP"""
    
    def __init__(self, county_name, output_dir="geojson_files"):
        super().__init__(county_name, output_dir)
    
    def standardize_data(self):
        return super().standardize_data()

class FremontCountyWy(BaseCounty):
    """Fremont County, Wyoming - simple ZIP with SHP"""
    
    def __init__(self, county_name, output_dir="geojson_files"):
        super().__init__(county_name, output_dir)
    
    def standardize_data(self):
        return super().standardize_data()

class TetonCountyId(BaseCounty):
    """Teton County, Idaho - simple ZIP with SHP"""
    
    def __init__(self, county_name, output_dir="geojson_files"):
        super().__init__(county_name, output_dir)
    
    def clean_and_normalize_names(self):
        super().clean_and_normalize_names()
        print(f"🔄 Parsing descriptions for Teton County ID...")
        parsed_complete_path = self.get_file_path(f"{self.county_name}_ownership_complete.geojson")
        self.merger.parse_description_to_properties(self.get_file_path(f"{self.county_name}_ownership_complete.geojson"), parsed_complete_path)
        print(f"✅ Parsed descriptions for Teton County ID")
