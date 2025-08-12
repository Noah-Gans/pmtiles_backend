import json
import os
from bs4 import BeautifulSoup
from tqdm import tqdm
import re
from shapely.geometry import shape
from shapely.strtree import STRtree
import collections
from shapely.geometry.base import BaseGeometry

class DataMerger:
    """Handles common merging operations across counties"""
    
    def __init__(self, output_dir="geojson_files"):
        self.output_dir = output_dir
    
    def merge_by_id(self, primary_data, secondary_data, primary_id_field, secondary_id_field):
        """Merge two datasets based on matching ID fields"""
        # Create lookup dictionary for secondary data
        secondary_lookup = {}
        for feature in secondary_data["features"]:
            props = feature.get("properties", {})
            id_value = props.get(secondary_id_field)
            if id_value:
                secondary_lookup[id_value] = props
        
        # Merge data
        merged_features = []
        for feature in primary_data["features"]:
            props = feature.get("properties", {}).copy()
            primary_id = props.get(primary_id_field)
            
            if primary_id and primary_id in secondary_lookup:
                # Merge secondary properties into primary
                secondary_props = secondary_lookup[primary_id]
                for key, value in secondary_props.items():
                    if key and value:
                        props[key] = value
            
            merged_features.append({
                "type": "Feature",
                "geometry": feature.get("geometry"),
                "properties": props
            })
        
        return {
            "type": "FeatureCollection",
            "features": merged_features
        }
    
    def spatial_join(self, parcel_data, address_data, parcel_id_field="PIN", address_id_field="FID"):
        """Spatial join addresses to parcels using spatial indexing"""
        # Prepare parcel geometries and a parallel list of features
        parcel_geoms = []
        parcel_features = []
        geom_wkt_to_idx = {}
        
        for idx, feature in enumerate(parcel_data["features"]):
            geom_data = feature.get("geometry")
            if not geom_data:
                continue
            geom = shape(geom_data)
            parcel_geoms.append(geom)
            parcel_features.append(feature)
            geom_wkt_to_idx[geom.wkt] = len(parcel_geoms) - 1
        
        tree = STRtree(parcel_geoms)
        
        # Track which parcels get which addresses
        parcel_to_addresses = collections.defaultdict(list)
        unmatched_addresses = []

        # For each address, find containing parcel using spatial index
        for addr_feature in tqdm(address_data["features"], desc="Spatially joining addresses to parcels"):
            addr_geom = shape(addr_feature["geometry"])
            candidates = tree.query(addr_geom)
            matched = False
            
            for candidate in candidates:
                # Only process if candidate is a Shapely geometry
                if not isinstance(candidate, BaseGeometry):
                    continue
                idx = geom_wkt_to_idx.get(candidate.wkt)
                if idx is None:
                    continue  # Shouldn't happen, but safety check
                parcel_feature = parcel_features[idx]
                if candidate.contains(addr_geom) or candidate.intersects(addr_geom):
                    parcel_to_addresses[idx].append(addr_feature)
                    matched = True
                    break
            if not matched:
                unmatched_addresses.append(addr_feature)
        
        # Apply address data to parcels
        total_matched_addresses = 0
        for idx, addr_list in parcel_to_addresses.items():
            parcel_feature = parcel_features[idx]
            if len(addr_list) > 1:
                pid = parcel_feature["properties"].get(parcel_id_field, parcel_feature["properties"].get("FID", idx))
                addr_fids = [a["properties"].get(address_id_field, None) for a in addr_list]
                print(f"Parcel {pid} matched multiple addresses: {addr_fids}")
            
            addr_props = addr_list[0]["properties"]
            for k, v in addr_props.items():
                if k and v:
                    parcel_feature["properties"][k] = v
            total_matched_addresses += len(addr_list)

        print(f"Total parcels with at least one address: {len(parcel_to_addresses)}")
        print(f"Total addresses matched to parcels: {total_matched_addresses}")
        print(f"Unmatched addresses: {len(unmatched_addresses)}")
        
        return parcel_data
    
    def merge_by_pidn(self, parcel_data, address_data):
        """Specialized merge for PIDN-based systems (like Teton County)"""
        # Create a mapping of PIDN -> Address Data
        address_lookup = {}
        print("Building address lookup...")
        for feature in tqdm(address_data["features"], desc="Processing addresses"):
            properties = feature.get("properties", {})
            description = properties.get("description", "")
            parsed_properties = self._extract_properties_from_description(description)
            pidn = parsed_properties.get("pidn", "").strip()
            if pidn:
                address_lookup[pidn] = parsed_properties
        
        print(f"Loaded {len(address_lookup)} addresses")

        # Create a new list for updated features
        updated_features = []
        updated_count = 0

        print("Updating ownership records...")
        for feature in tqdm(parcel_data["features"], desc="Updating ownership"):
            properties = feature.get("properties", {}).copy()
            description = properties.get("description", "")
            parsed_properties = self._extract_properties_from_description(description)
            
            # Write all parsed description details as separate properties
            for k, v in parsed_properties.items():
                if k and v:
                    properties[k] = v
            
            pidn = parsed_properties.get("pidn", "").strip()
            
            # If address data exists for this PIDN, add each address component as its own property
            if pidn and pidn in address_lookup:
                address_data = address_lookup[pidn]
                for k, v in address_data.items():
                    if k and v:
                        properties[k] = v
                updated_count += 1
            
            updated_features.append({
                "type": "Feature",
                "id": feature.get("id"),
                "properties": properties,
                "geometry": feature.get("geometry")
            })

        print(f"Updated {updated_count} ownership records with address data.")
        
        return {
            "type": "FeatureCollection",
            "features": updated_features
        }
    
    def merge_scraped_data(self, parcel_data, scraped_data, merge_field):
        """Merge scraped data into parcel data"""
        # Create lookup for scraped data
        scraped_lookup = {}
        for item in scraped_data:
            merge_value = item.get(merge_field)
            if merge_value:
                scraped_lookup[merge_value] = item
        
        # Merge into parcel data
        merged_features = []
        for feature in parcel_data["features"]:
            props = feature.get("properties", {}).copy()
            merge_value = props.get(merge_field)
            
            if merge_value and merge_value in scraped_lookup:
                scraped_item = scraped_lookup[merge_value]
                for key, value in scraped_item.items():
                    if key and value and key != merge_field:
                        props[key] = value
            
            merged_features.append({
                "type": "Feature",
                "geometry": feature.get("geometry"),
                "properties": props
            })
        
        return {
            "type": "FeatureCollection",
            "features": merged_features
        }
    
    def _extract_properties_from_description(self, html_string):
        """Parses the description field and extracts property key-value pairs."""
        soup = BeautifulSoup(html_string, "html.parser")
        rows = soup.find_all("tr")
        properties = {}
        for row in rows:
            cells = row.find_all(["th", "td"])
            if len(cells) == 2:
                key = re.sub(r"\s+", "_", cells[0].text.strip().lower())  # Normalize key
                value = cells[1].text.strip()
                properties[key] = value
        return properties

    def _load_json_any(self, path):
            if path.endswith('.jsonl'):
                with open(path) as f:
                    return [json.loads(line) for line in f]
            else:
                with open(path) as f:
                    data = json.load(f)
                    # GeoJSON or plain JSON
                    if isinstance(data, dict) and 'features' in data:
                        return data['features']
                    elif isinstance(data, list):
                        return data
                    else:
                        raise ValueError(f"Unknown JSON structure in {path}")
                        
    def join_address_to_parcel(self, parcel_file_path, address_file_path, parcel_key, address_key, output_path=None):
        """Join address data to parcel data by key, supporting GeoJSON, JSON, or JSONL."""
        print(f"🔄 Joining address data to parcel data...")
        # Load address data and build lookup
        address_features = self._load_json_any(address_file_path)
        print(len(address_features))
        address_lookup = {}
        for feature in address_features:
            props = feature.get('properties', feature)  # fallback to root if not GeoJSON
            key = props[address_key] if isinstance(address_key, str) else props[address_key]
            address_lookup[key] = props

        # Load parcel data
        parcel_features = self._load_json_any(parcel_file_path)
        updated_features = []
        for feature in parcel_features:
            props = feature.get('properties', feature)
            key = props[parcel_key] if isinstance(parcel_key, str) else props[parcel_key]
            if key in address_lookup:
                # Merge all address fields except the key itself
                for k, v in address_lookup[key].items():
                    if k != address_key:
                        props[k] = v
            if 'properties' in feature:
                feature['properties'] = props
            updated_features.append(feature)

        # Save or return
        if not output_path:
            # Infer county name from parcel_file_path
            base = os.path.basename(parcel_file_path)
            county_name = base.split('_ownership')[0]
            output_path = os.path.join(self.output_dir, f"{county_name}_ownership_complete.geojson")
        # If input was GeoJSON, output as GeoJSON
        if parcel_file_path.endswith('.geojson'):
            out = {'type': 'FeatureCollection', 'features': updated_features}
        else:
            out = updated_features
        with open(output_path, 'w') as f:
            json.dump(out, f, indent=2)
        print(f"✅ Joined data saved to {output_path}")
        return output_path

    def parse_description_to_properties(self, input_geojson_path, output_geojson_path):
        """Parse description field for each feature and update properties, then save."""
        with open(input_geojson_path, 'r') as f:
            data = json.load(f)
        features = data['features']
        for feature in tqdm(features, desc="Parsing descriptions"):
            props = feature.get('properties', {})
            description = props.get('description', '')
            parsed = self._extract_properties_from_description(description)
            # Merge parsed into properties
            props.update(parsed)
            feature['properties'] = props
        # Save new GeoJSON
        with open(output_geojson_path, 'w') as f:
            json.dump({'type': 'FeatureCollection', 'features': features}, f, indent=2)
        print(f"✅ Parsed and saved: {output_geojson_path}")
