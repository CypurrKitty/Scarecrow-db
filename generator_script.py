import pandas as pd
import json
import lzma
import datetime
import glob
import os

# --- CONFIGURATION ---
OUTPUT_FILE = 'surveillance_db.excam'
MANIFEST_FILE = 'manifest.json'
DB_NAME = "Scarecrow-db"
# REPLACE THIS with your actual GitHub Pages URL
BASE_URL = "https://CypurrKitty.github.io/Scarecrow-db/"

# --- FLAGS ---
FLAG_ALPR = 1 << 13 
FLAG_AUDIO = 1 << 9

def get_flag_from_filename(filename):
    if 'raven' in filename.lower(): return FLAG_AUDIO
    return FLAG_ALPR

def normalize_csv(file_path):
    try:
        df = pd.read_csv(file_path)
    except: return pd.DataFrame()
    
    # Clean column names
    df.columns = df.columns.astype(str).str.lower().str.strip()
    
    file_flag = get_flag_from_filename(file_path)
    
    if 'trilat' in df.columns and 'trilong' in df.columns:
        df = df[['trilat', 'trilong']].rename(columns={'trilat': 'lat', 'trilong': 'lon'})
    elif 'latitude' in df.columns and 'longitude' in df.columns:
        df = df[['latitude', 'longitude']].rename(columns={'latitude': 'lat', 'longitude': 'lon'})
    elif 'coordinates' in df.columns:
        try:
            s = df['coordinates'].astype(str).str.split(',', expand=True)
            if len(s.columns) >= 2:
                df['lat'] = s[0].str.strip().astype(float)
                df['lon'] = s[1].str.strip().astype(float)
                df = df[['lat', 'lon']]
            else: return pd.DataFrame()
        except: return pd.DataFrame()
    else: return pd.DataFrame()

    df['flg'] = file_flag
    return df

def normalize_osm_json(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        if 'elements' in data:
            df = pd.DataFrame(data['elements'])
            if 'lat' in df.columns and 'lon' in df.columns:
                df = df[['lat', 'lon']]
                df['flg'] = FLAG_ALPR
                return df
        elif 'type' in data and data['type'] == 'FeatureCollection':
            features = data.get('features', [])
            extracted = []
            for feat in features:
                coords = feat.get('geometry', {}).get('coordinates', [])
                if len(coords) >= 2:
                    extracted.append({'lat': coords[1], 'lon': coords[0], 'flg': FLAG_ALPR})
            if extracted: return pd.DataFrame(extracted)
        return pd.DataFrame()
    except: return pd.DataFrame()

# --- MAIN EXECUTION ---
all_entries = []
search_path = os.path.join('datasets', '*')
files = glob.glob(search_path)

print(f"Scanning {len(files)} files...")

for f in files:
    if os.path.isdir(f): continue
    if f.endswith('.json') or f.endswith('.geojson'): d = normalize_osm_json(f)
    elif f.endswith('.csv'): d = normalize_csv(f)
    else: continue

    if not d.empty:
        all_entries.append(d.dropna(subset=['lat', 'lon']))
        print(f"  -> Imported {os.path.basename(f)}")

if not all_entries: exit()

final_df = pd.concat(all_entries, ignore_index=True).round({'lat': 6, 'lon': 6}).drop_duplicates()
print(f"Total Unique Nodes: {len(final_df)}")

# Generate Date/Revision for both files
today = datetime.date.today().strftime("%Y-%m-%d")
revision = int(datetime.datetime.now().timestamp())

# 1. Write the Database (.excam)
def create_entry(row):
    return {"lat": row['lat'], "lon": row['lon'], "flg": int(row['flg']), "dir": None}

with lzma.open(OUTPUT_FILE, mode='wt', encoding='utf-8') as f:
    meta = {"_meta": {"name": DB_NAME, "date": today, "revision": revision}}
    f.write(json.dumps(meta) + '\n')
    for _, row in final_df.iterrows():
        f.write(json.dumps(create_entry(row)) + '\n')

# 2. Write the Manifest (.json)
manifest = {
    "date": today,
    "revision": revision,
    "dataUrl": f"{BASE_URL}{OUTPUT_FILE}"
}
with open(MANIFEST_FILE, 'w') as f:
    json.dump(manifest, f, indent=2)

print(f"Done. Files created:\n 1. {OUTPUT_FILE} (The Data)\n 2. {MANIFEST_FILE} (The Link for the App)")
