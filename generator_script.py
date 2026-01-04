import pandas as pd
import json
import lzma
import datetime
import glob
import os

# --- CONFIGURATION ---
# We save the database in the root folder, not inside datasets/
OUTPUT_FILE = 'surveillance_db.excam'
DB_NAME = "Scarecrow-db"

# --- FLAGS ---
FLAG_ALPR = 1 << 13 
FLAG_AUDIO = 1 << 9

def get_flag_from_filename(filename):
    if 'raven' in filename.lower():
        return FLAG_AUDIO
    return FLAG_ALPR

def normalize_csv(file_path):
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"Skipping {file_path} (Not a valid CSV): {e}")
        return pd.DataFrame()

    file_flag = get_flag_from_filename(file_path)

    # Clean up column names to handle messy inputs
    df.columns = df.columns.astype(str).str.lower().str.strip()

    # Logic 1: Flock-You / Wardriving (trilat/trilong)
    if 'trilat' in df.columns and 'trilong' in df.columns:
        df = df[['trilat', 'trilong']].rename(columns={'trilat': 'lat', 'trilong': 'lon'})
    
    # Logic 2: Standard (latitude/longitude)
    elif 'latitude' in df.columns and 'longitude' in df.columns:
        df = df[['latitude', 'longitude']].rename(columns={'latitude': 'lat', 'longitude': 'lon'})
    
    # Logic 3: Pigvision / Combined Coordinates ("lat, lon")
    elif 'coordinates' in df.columns:
        try:
            split_data = df['coordinates'].astype(str).str.split(',', expand=True)
            if len(split_data.columns) >= 2:
                df['lat'] = split_data[0].str.strip().astype(float)
                df['lon'] = split_data[1].str.strip().astype(float)
                df = df[['lat', 'lon']]
            else:
                return pd.DataFrame()
        except:
            return pd.DataFrame()
    else:
        # This catches files like PresentMon that have no location data
        return pd.DataFrame()

    df['flg'] = file_flag
    return df

def normalize_osm_json(file_path):
    """
    Ingests BOTH Standard Overpass JSON and GeoJSON formats.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # MODE 1: Standard Overpass JSON (elements list)
        if 'elements' in data:
            df = pd.DataFrame(data['elements'])
            if 'lat' in df.columns and 'lon' in df.columns:
                df = df[['lat', 'lon']]
                df['flg'] = FLAG_ALPR
                return df

        # MODE 2: GeoJSON (like export.geojson)
        elif 'type' in data and data['type'] == 'FeatureCollection':
            features = data.get('features', [])
            extracted_data = []
            
            for feat in features:
                # GeoJSON coordinates are [Longitude, Latitude]
                coords = feat.get('geometry', {}).get('coordinates', [])
                if len(coords) >= 2:
                    extracted_data.append({
                        'lat': coords[1],
                        'lon': coords[0],
                        'flg': FLAG_ALPR
                    })
            
            if extracted_data:
                return pd.DataFrame(extracted_data)

        # Silently skip files that aren't map data (like raven_configurations.json)
        return pd.DataFrame()
            
    except Exception as e:
        print(f"Error parsing JSON {file_path}: {e}")
        return pd.DataFrame()

# --- MAIN EXECUTION ---
all_entries = []

# Update: Look specifically in the datasets/ subfolder
search_path = os.path.join('datasets', '*')
files = glob.glob(search_path)

print(f"Scanning 'datasets/'... Found {len(files)} files.")

for f in files:
    # Skip directories if any exist
    if os.path.isdir(f):
        continue

    if f.endswith('.json') or f.endswith('.geojson'):
        d = normalize_osm_json(f)
    elif f.endswith('.csv'):
        d = normalize_csv(f)
    else:
        continue

    if not d.empty:
        d = d.dropna(subset=['lat', 'lon'])
        all_entries.append(d)
        # Just print the filename, not the full path, for cleaner output
        fname = os.path.basename(f)
        print(f"  -> Imported {len(d):>5} cameras from {fname}")

if not all_entries:
    print("No valid camera data found in datasets/ folder.")
    exit()

# Merge and Deduplicate
final_df = pd.concat(all_entries, ignore_index=True)
final_df = final_df.round({'lat': 6, 'lon': 6}).drop_duplicates()

print(f"Total Unique Surveillance Nodes: {len(final_df)}")

def create_entry(row):
    return {
        "lat": row['lat'],
        "lon": row['lon'],
        "flg": int(row['flg']),
        "dir": None
    }

with lzma.open(OUTPUT_FILE, mode='wt', encoding='utf-8') as f:
    meta = {
        "_meta": {
            "name": DB_NAME,
            "date": datetime.date.today().strftime("%Y-%m-%d"),
            "revision": int(datetime.datetime.now().timestamp())
        }
    }
    f.write(json.dumps(meta) + '\n')
    
    for _, row in final_df.iterrows():
        f.write(json.dumps(create_entry(row)) + '\n')

print(f"Success! Database generated: {OUTPUT_FILE}")
