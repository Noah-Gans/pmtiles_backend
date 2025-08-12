import os
import requests
import tempfile
import zipfile
import subprocess
import json
from osgeo import ogr
from tqdm import tqdm

class BaseDownloader:
    """Base class with common download methods (KMZ, ZIP, signed URLs, etc.)"""
    
    def __init__(self, output_dir="geojson_files"):
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)
    
    def download_kmz(self, url, base_name):
        """Download KMZ file and convert to GeoJSON"""
        kmz_path = os.path.join(self.output_dir, f"{base_name}.kmz")
        self._simple_download(url, kmz_path)
        self._convert_kmz_to_geojson(kmz_path, base_name)
    
    def download_signed_geojson(self, url, filename):
        """Download GeoJSON from a signed URL"""
        signed_url = self._get_signed_url(url)
        geojson_path = os.path.join(self.output_dir, filename)
        self._simple_download(signed_url, geojson_path)
    
    def download_zip(self, url, base_name, expect_shp=None):
        """Download ZIP file and extract/convert to GeoJSON"""
        tmp_zip = tempfile.mktemp(suffix=".zip")
        self._simple_download(url, tmp_zip)

        with zipfile.ZipFile(tmp_zip, 'r') as zip_ref:
            zip_ref.extractall(self.output_dir)
        
        if expect_shp:
            shp_path = os.path.join(self.output_dir, expect_shp)
            if not os.path.exists(shp_path):
                print(f"❌ Expected SHP {expect_shp} not found")
                return
        else:
            # Pick first SHP found
            shp_path = next((os.path.join(self.output_dir, f) for f in os.listdir(self.output_dir) if f.endswith('.shp')), None)
            if not shp_path:
                print("❌ No SHP file found in ZIP")
                return

        geojson_path = os.path.join(self.output_dir, f"{base_name}.geojson")
        subprocess.run(["ogr2ogr", "-f", "GeoJSON", geojson_path, shp_path], check=True)
        
        # Clean up all extracted files except the converted GeoJSON
        print(f"🧹 Cleaning up extracted files...")
        for file in os.listdir(self.output_dir):
            file_path = os.path.join(self.output_dir, file)
            if file_path != geojson_path and os.path.isfile(file_path):
                try:
                    os.remove(file_path)
                    print(f"🗑 Deleted {file}")
                except Exception as e:
                    print(f"⚠️ Could not delete {file}: {e}")
        
        # Clean up the temporary ZIP file
        try:
            os.remove(tmp_zip)
            print(f"🗑 Deleted temporary ZIP: {tmp_zip}")
        except Exception as e:
            print(f"⚠️ Could not delete temporary ZIP: {e}")
        
        print(f"✅ ZIP processing complete. Cleaned up all files except {os.path.basename(geojson_path)}")
    
    def _simple_download(self, url, output_path):
        """Download a file from a URL, formats GeoJSON to have properties before geometry with one-line features."""
        response = requests.get(url)
        response.raise_for_status()

        # Save raw content
        with open(output_path, 'wb') as file:
            file.write(response.content)

        print(f"✅ Downloaded file to {output_path}")

        # If it's GeoJSON, reformat with properties before geometry
        if output_path.lower().endswith('.geojson'):
            try:
                with open(output_path, 'r') as f:
                    data = json.load(f)

                # Rebuild features with properties before geometry
                reordered_features = []
                for feature in data['features']:
                    new_feature = {
                        "type": feature.get("type", "Feature"),
                        "id": feature.get("id"),
                        "properties": feature.get("properties", {}),
                        "geometry": feature.get("geometry")
                    }
                    reordered_features.append(new_feature)

                # Write out with compact one-line-per-feature
                with open(output_path, 'w') as f:
                    f.write('{\n')
                    f.write(f'  "type": "FeatureCollection",\n')
                    if 'name' in data:
                        f.write(f'  "name": {json.dumps(data["name"])},\n')
                    if 'crs' in data:
                        f.write(f'  "crs": {json.dumps(data["crs"])},\n')
                    f.write('  "features": [\n')

                    for idx, feature in enumerate(reordered_features):
                        line = json.dumps(feature, separators=(',', ':'))
                        if idx < len(reordered_features) - 1:
                            line += ','
                        f.write(f'    {line}\n')

                    f.write('  ]\n')
                    f.write('}\n')

                print(f"✨ Reordered and wrote GeoJSON to {output_path}")

            except Exception as e:
                print(f"⚠️ Could not format GeoJSON: {e}")
    
    def _get_signed_url(self, base_url):
        """Get signed URL from base URL"""
        resp = requests.get(base_url, allow_redirects=False)
        if resp.status_code == 302:
            return resp.headers['Location']
        raise RuntimeError(f"❌ Failed to get signed URL for {base_url}")
    
    def _convert_kmz_to_geojson(self, kmz_path, base_name):
        """Convert KMZ file to GeoJSON"""
        driver = ogr.GetDriverByName('LIBKML')
        datasource = driver.Open(kmz_path, 0)
        if datasource is None:
            print(f"❌ Could not open {kmz_path}")
            return

        for i in range(datasource.GetLayerCount()):
            layer = datasource.GetLayerByIndex(i)
            original_layer_name = layer.GetName().replace(" ", "_").lower()

            # Prepend county/state
            geojson_file = os.path.join(
                self.output_dir,
                f"{base_name}_{original_layer_name}.geojson"
            )

            geojson_driver = ogr.GetDriverByName('GeoJSON')
            if os.path.exists(geojson_file):
                geojson_driver.DeleteDataSource(geojson_file)

            geojson_ds = geojson_driver.CreateDataSource(geojson_file)
            geojson_layer = geojson_ds.CreateLayer(original_layer_name, layer.GetSpatialRef(), layer.GetGeomType())
            geojson_layer.CreateFields(layer.schema)

            for feature in layer:
                geojson_layer.CreateFeature(feature.Clone())

            geojson_ds = None  # Close the file
            print(f"✅ Saved {geojson_file}")

        os.remove(kmz_path)
        print(f"🗑 Deleted KMZ {kmz_path}")
