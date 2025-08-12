#!/usr/bin/env python3
"""
Enhanced PMTiles pipeline entry point with ownership pipeline integration
"""

import os
import sys
import subprocess
import tempfile
import argparse
from pathlib import Path
from google.cloud import storage

# Add the current directory to Python path
sys.path.insert(0, str(Path(__file__).parent))

def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    """Download a file from GCS"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    print(f"✅ Downloaded {source_blob_name} to {destination_file_name}")

def run_legacy_pipeline():
    """Run the original main.py pipeline (single county from GCS)"""
    print("🔄 Running legacy single-county pipeline...")
    
    try:
        # GCS paths
        bucket_name = "teton-county-gis-bucket"
        geojson_path = "geojsons/teton_county_wy/ownership_data_20250807.geojson"
        
        # Create tiles directory for Martin to serve from
        tiles_dir = Path.home() / "tiles"
        tiles_dir.mkdir(parents=True, exist_ok=True)
        print(f"📁 Tiles will be output to: {tiles_dir}")
        
        # Download GeoJSON from GCS
        print("📥 Downloading test GeoJSON from GCS...")
        geojson_file = tiles_dir / "teton_county_wy_ownership.geojson"
        download_from_gcs(bucket_name, geojson_path, str(geojson_file))
        
        # Generate MBTiles v2 using tippecanoe with improved settings
        print(" Generating MBTiles v2 using tippecanoe...")
        mbtiles_file = tiles_dir / "teton_county_wy_ownership.mbtiles"
        
        # Remove old file if it exists
        if mbtiles_file.exists():
            mbtiles_file.unlink()
            print("🗑️ Removed old MBTiles file")
        
        cmd = [
            "tippecanoe",
            "-o", str(mbtiles_file),
            "-l", "teton_county_wy_ownership",  # Layer name
            "-n", "teton_county_wy_ownership",  # Source name
            "-Z", "7",                           # Minimum zoom (higher for better coverage)
            "-z", "15",                          # Maximum zoom (higher for detail)
            "--drop-densest-as-needed",
            "--extend-zooms-if-still-dropping",
            "--coalesce",
            "--coalesce-densest-as-needed",
            "--detect-shared-borders",
            "--force",                           # Force overwrite
            str(geojson_file)
        ]
        
        subprocess.run(cmd, check=True, timeout=7200)
        print("✅ MBTiles v2 generated successfully!")
        
        # Convert MBTiles to PMTiles using Python library
        print(" Converting MBTiles to PMTiles...")
        pmtiles_file = tiles_dir / "teton_county_wy_ownership.pmtiles"
        
        try:
            from pmtiles import convert
            print(f"Converting {mbtiles_file} to {pmtiles_file}")
            convert.mbtiles_to_pmtiles(str(mbtiles_file), str(pmtiles_file), maxzoom=15)
            
            # Verify the file was created and has content
            if pmtiles_file.exists() and pmtiles_file.stat().st_size > 0:
                print("✅ PMTiles conversion completed!")
                print(f"PMTiles file size: {pmtiles_file.stat().st_size} bytes")
                
                # Clean up MBTiles file
                if mbtiles_file.exists():
                    mbtiles_file.unlink()
                    print("🗑️ Cleaned up MBTiles file")
                
                return str(pmtiles_file)
            else:
                print("❌ PMTiles file is empty or missing!")
                return None
                
        except Exception as e:
            print(f"❌ Error converting MBTiles to PMTiles: {e}")
            import traceback
            traceback.print_exc()
            return None
        
    except Exception as e:
        print(f"❌ Error in legacy pipeline: {e}")
        return None

def run_ownership_pipeline(county_list=None, skip_data_collection=False, skip_gcs_upload=False):
    """Run the full ownership pipeline with optional flags"""
    print("🔄 Running full ownership pipeline...")
    
    try:
        from ownership_pipeline import OwnershipPipeline
        
        # Create pipeline instance
        pipeline = OwnershipPipeline()
        
        if skip_data_collection:
            print("⏭️ Skipping data collection - using existing files")
            print("🔍 Checking for existing county data files...")
            
            # Check what counties have data
            available_counties = county_list or pipeline.get_available_counties()
            counties_with_data = []
            
            for county in available_counties:
                geojson_path = pipeline.pmtiles_cycle_dir / "geojsons_for_db_upload" / f"{county}_data_files" / f"{county}_final_ownership.geojson"
                if geojson_path.exists():
                    counties_with_data.append(county)
                    size_mb = geojson_path.stat().st_size / (1024 * 1024)
                    print(f"  ✅ {county}: {size_mb:.1f} MB")
                else:
                    print(f"  ❌ {county}: No data file found")
            
            if not counties_with_data:
                print("❌ No counties have existing data files!")
                return None
            
            print(f"\n📊 Found {len(counties_with_data)} counties with data: {', '.join(counties_with_data)}")
            print("🔄 Proceeding with combination and tile generation...")
            
            # Just generate PMTiles from existing data
            pmtiles_file = pipeline.generate_pmtiles(counties_with_data)
        else:
            print("🔄 Running complete pipeline: collect data, combine, generate tiles")
            # Run the full pipeline - this handles PMTiles generation internally
            pipeline.process_all_counties(
                county_list or pipeline.get_available_counties(),
                upload_to_gcs=not skip_gcs_upload,
                skip_gcs_upload=skip_gcs_upload,
                generate_pmtiles=True  # This will generate PMTiles automatically
            )
            # Don't call generate_pmtiles again - it was already done above
            # Just get the result from the tiles directory
            tiles_dir = Path.home() / "tiles"
            pmtiles_file = tiles_dir / "combined_ownership.pmtiles"
            if pmtiles_file.exists():
                pmtiles_file = str(pmtiles_file)
            else:
                pmtiles_file = None
        
        if pmtiles_file:
            print(f"✅ Ownership pipeline completed successfully!")
            print(f" Final PMTiles file: {pmtiles_file}")
            return pmtiles_file
        else:
            print("❌ Ownership pipeline failed!")
            return None
            
    except Exception as e:
        print(f"❌ Error in ownership pipeline: {e}")
        import traceback
        traceback.print_exc()
        return None

def main():
    """Main function with enhanced pipeline options"""
    
    parser = argparse.ArgumentParser(description="Enhanced PMTiles pipeline with ownership pipeline integration")
    parser.add_argument("--legacy", action="store_true", help="Run legacy single-county pipeline (original main.py behavior)")
    parser.add_argument("--ownership", action="store_true", help="Run full ownership pipeline for all counties")
    parser.add_argument("--county", type=str, help="Single county to process with ownership pipeline")
    parser.add_argument("--counties", nargs='+', help="Specific counties to process with ownership pipeline")
    parser.add_argument("--skip-data", action="store_true", help="Skip data collection, use existing files (ownership pipeline only)")
    parser.add_argument("--skip-gcs", action="store_true", help="Skip GCS upload (ownership pipeline only)")
    
    args = parser.parse_args()
    
    print("🚀 Starting Enhanced PMTiles Pipeline...")
    
    # Determine which pipeline to run
    if args.legacy:
        # Run the original main.py pipeline
        print("🔄 Running legacy pipeline (single county from GCS)...")
        result = run_legacy_pipeline()
        
    elif args.ownership or args.county or args.counties:
        # Run ownership pipeline
        county_list = None
        if args.county:
            county_list = [args.county]
        elif args.counties:
            county_list = args.counties
        
        result = run_ownership_pipeline(
            county_list=county_list,
            skip_data_collection=args.skip_data,
            skip_gcs_upload=args.skip_gcs
        )
        
    else:
        # Default: run ownership pipeline for all counties
        print("🔄 No specific mode specified, running ownership pipeline for all counties...")
        result = run_ownership_pipeline()
    
    # Check result
    if result:
        print(f"\n✅ Pipeline completed successfully!")
        print(f"📁 Final output: {result}")
        
        # Show final directory contents
        tiles_dir = Path.home() / "tiles"
        if tiles_dir.exists():
            print(f"\n📁 Final output in tiles directory:")
            for file in tiles_dir.glob("*"):
                if file.is_file():
                    size_mb = file.stat().st_size / (1024 * 1024)
                    print(f"  {file.name}: {size_mb:.1f} MB")
    else:
        print("\n❌ Pipeline failed!")
        sys.exit(1)

if __name__ == "__main__":
    main() 