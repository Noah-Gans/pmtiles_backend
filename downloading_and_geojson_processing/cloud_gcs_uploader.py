import os
from google.cloud import storage
from datetime import datetime
import subprocess

def upload_geojson_to_gcs(local_path, county_name, bucket_name="teton-county-gis-bucket", gcs_base_folder="geojsons"):
    """
    Upload a GeoJSON file to a GCS bucket in the structure geojsons/county_name/ownership_data_YYYYMMDD.geojson
    """
    today = datetime.now().strftime("%Y%m%d")
    filename = f"ownership_data_{today}.geojson"
    gcs_path = f"{gcs_base_folder}/{county_name}/{filename}"
    gcs_uri = f"gs://{bucket_name}/{gcs_path}"

    print(f"📤 Uploading {local_path} to {gcs_uri} via gsutil...")

    try:
        subprocess.run([
            "gsutil", "-o", "GSUtil:parallel_composite_upload_threshold=150M", 
            "cp", local_path, gcs_uri
        ], check=True)
        print(f"✅ Upload successful: {gcs_uri}")
        return gcs_uri
    except subprocess.CalledProcessError as e:
        print("❌ Upload failed with error:", e)
        return None