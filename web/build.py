import os
import json
import tempfile
from datetime import date
from pathlib import Path
import sys

# Add src to python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv

from glofas.config import GAUGE_IDS, GAUGE_INFO, DATA_DIR, GEOMETRY_PATH
from glofas.s3 import (
    get_s3_client, get_last_data_date, upload_incremental_to_s3,
    download_from_s3, update_s3_metadata
)
from glofas.download import download_glofas_incremental
from glofas.process import extract_glofas_at_gauges, correct_new_data
from glofas.io import load_gauge_data_all, load_cv_results
from glofas.utils import get_missing_years, normalize_base_url

load_dotenv()

WEB_DIR = Path(__file__).parent
DATA_DIR = WEB_DIR / "src" / "_data" # Overriding for web build
CV_RESULTS_PATH = PROJECT_ROOT / "tables" / "tbl2_cv-results.csv"

def generate_gauges_json(cv_results: dict, update_date: str) -> dict:
    """Generate GeoJSON for all gauges."""
    features = []
    for gauge_id, info in GAUGE_INFO.items():
        obs_path = PROJECT_ROOT / "data" / "hydro" / "obs" / f"{gauge_id}.csv"
        if obs_path.exists():
            obs = pd.read_csv(obs_path, parse_dates=["date"])
            obs_with_data = obs.dropna(subset=["q_cms"])
            if len(obs_with_data) > 0:
                obs_start = obs_with_data["date"].min().strftime("%Y-%m-%d")
                obs_end = obs_with_data["date"].max().strftime("%Y-%m-%d")
                missing_pct = round((1 - len(obs_with_data) / len(obs)) * 100, 1)
            else:
                obs_start = obs_end = ""
                missing_pct = 100.0
        else:
            obs_start = obs_end = ""
            missing_pct = 100.0
        
        cv = cv_results.get(gauge_id, {"kge_raw": "", "kge_dqm": ""})
        
        feature = {
            "type": "Feature",
            "properties": {
                "id": gauge_id,
                "name": info["name"],
                "river": info["river"],
                "obs_start": obs_start,
                "obs_end": obs_end,
                "missing_pct": missing_pct,
                "kge_raw": cv["kge_raw"],
                "kge_dqm": cv["kge_dqm"],
            },
            "geometry": {"type": "Point", "coordinates": [info["lon"], info["lat"]]},
        }
        features.append(feature)
    
    return {
        "type": "FeatureCollection",
        "properties": {"lastUpdated": update_date},
        "features": features,
    }


def generate_timeseries_json(gauge_id: int, df: pd.DataFrame) -> dict:
    """Generate timeseries JSON for a gauge."""
    info = GAUGE_INFO[gauge_id]
    data = []
    for _, row in df.iterrows():
        date_str = row["date"].strftime("%Y-%m-%d")
        q_obs = None if pd.isna(row.get("q_obs")) else round(row["q_obs"], 2)
        q_raw = None if pd.isna(row.get("q_raw")) else round(row["q_raw"], 2)
        q_cor = None if pd.isna(row.get("q_cor")) else round(row["q_cor"], 2)
        data.append([date_str, q_obs, q_raw, q_cor])
    
    return {"id": gauge_id, "name": info["name"], "river": info["river"], "data": data}


def main():
    bucket = os.environ.get("S3_BUCKET")
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    endpoint = os.environ.get("ENDPOINT_URL")
    
    use_s3 = bucket and aws_key and aws_secret and endpoint
    
    if use_s3:
        s3_client = get_s3_client(endpoint)
        
        print("Checking S3 for last data date...")
        last_data_date = get_last_data_date(bucket, s3_client)
        print(f"Last data date in S3: {last_data_date}")
        
        today = date.today()
        days_since_last = (today - last_data_date).days
        print(f"Days since last data: {days_since_last}")
        
        missing_years = get_missing_years(last_data_date)
        
        if missing_years:
            print(f"Missing years to download: {missing_years}")
            
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir)
                print(f"Downloading to: {temp_path}")
                
                print("Downloading missing GloFAS data...")
                nc_files = download_glofas_incremental(missing_years, temp_path)
                
                if nc_files:
                    print("Loading gauge geometry...")
                    gauges_gdf = gpd.read_file(GEOMETRY_PATH)
                    gauges_gdf = gauges_gdf[gauges_gdf["id"].isin(GAUGE_IDS)]
                    
                    print("Extracting values at gauge locations...")
                    print(f"Filtering for data after: {last_data_date}")
                    all_raw_data = {}
                    for nc_file in nc_files:
                        print(f"Extracting from {nc_file.name}...")
                        extracted = extract_glofas_at_gauges(nc_file, gauges_gdf)
                        if not extracted.empty:
                            print(f"  Total extracted records: {len(extracted)}")
                            extracted["datetime"] = pd.to_datetime(extracted["datetime"])
                            print(f"  Date range: {extracted['datetime'].min().date()} to {extracted['datetime'].max().date()}")
                            before_filter = len(extracted)
                            extracted = extracted[extracted["datetime"].dt.date > last_data_date]
                            print(f"  After filtering (>{last_data_date}): {len(extracted)} records (removed {before_filter - len(extracted)})")
                            for gauge_id in GAUGE_IDS:
                                gauge_data = extracted[extracted["gauge_id"] == gauge_id]
                                if not gauge_data.empty:
                                    if gauge_id not in all_raw_data:
                                        all_raw_data[gauge_id] = []
                                    all_raw_data[gauge_id].append(gauge_data)
                        else:
                            print(f"  No data extracted from {nc_file.name}")
                    
                    if not all_raw_data:
                        print("Warning: No new data available from GloFAS")
                        print("The latest available data has already been processed")
                    else:
                        print("Applying DQM correction...")
                        update_date = today.isoformat()

                        successful_uploads = 0
                        for gauge_id in GAUGE_IDS:
                            if gauge_id in all_raw_data:
                                raw_df = pd.concat(all_raw_data[gauge_id])
                                raw_df = raw_df.sort_values("datetime")
                                raw_df = raw_df.drop_duplicates(subset=["datetime"], keep="last")

                                print(f"  Processing gauge {gauge_id}: {len(raw_df)} records")

                                corrected_df = correct_new_data(gauge_id, raw_df)

                                if not corrected_df.empty:
                                    raw_df["date"] = pd.to_datetime(raw_df["datetime"]).dt.date
                                    raw_df["date"] = pd.to_datetime(raw_df["date"]) - pd.Timedelta(days=1)

                                    merged_df = raw_df.merge(corrected_df, on="date", how="left")
                                    merged_df["gauge_id"] = gauge_id
                                    merged_df["q_obs"] = None
                                    merged_df = merged_df[["date", "gauge_id", "q_obs", "q_raw", "q_cor"]]

                                    print(f"  Uploading {len(merged_df)} records for gauge {gauge_id}")
                                    upload_incremental_to_s3(bucket, s3_client, gauge_id, merged_df, update_date)
                                    successful_uploads += 1
                                else:
                                    print(f"  Warning: No corrected data for gauge {gauge_id}")

                        if successful_uploads > 0:
                            update_s3_metadata(bucket, s3_client, update_date)
                            print(f"Updated S3 with new data (update_date={update_date}, gauges_uploaded={successful_uploads})")
                        else:
                            print("Skipping metadata update: no gauges successfully uploaded")
        else:
            print(f"No missing years detected (last data: {last_data_date}, today: {today}, days diff: {days_since_last})")
        
        print("Downloading data from S3 for website build...")
        s3_data = download_from_s3(bucket, s3_client)
        update_date = s3_data["update_date"]
        gauge_data = s3_data["gauges"]
    else:
        print("S3 not configured, using local data")
        update_date = date.today().isoformat()
        gauge_data = {}
        # load_gauge_data_all expects global DATA_DIR, but here we might want project root data.
        # In config.py: DATA_DIR = PROJECT_ROOT / "data". 
        # load_gauge_data_all uses config.DATA_DIR / "hydro".
        # This matches what was there before (PROJECT_ROOT / "data" / "hydro" / ...).
        for gauge_id in GAUGE_IDS:
            gauge_data[gauge_id] = load_gauge_data_all(gauge_id)
    
    print("Loading CV results...")
    cv_results = load_cv_results(CV_RESULTS_PATH)
    
    print("Generating website data...")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    gauges_geojson = generate_gauges_json(cv_results, update_date)
    with open(DATA_DIR / "gauges.json", "w") as f:
        json.dump(gauges_geojson, f, indent=2)
    print("Generated gauges.json")
    
    timeseries_dir = DATA_DIR / "timeseries"
    timeseries_dir.mkdir(exist_ok=True)
    for gauge_id, df in gauge_data.items():
        ts_json = generate_timeseries_json(gauge_id, df)
        with open(timeseries_dir / f"{gauge_id}.json", "w") as f:
            json.dump(ts_json, f, indent=2)
        print(f"Generated timeseries/{gauge_id}.json")
    
    site_json_path = DATA_DIR / "site.json"
    if site_json_path.exists():
        with open(site_json_path) as f:
            site_data = json.load(f)
        site_data["lastUpdated"] = update_date
        env_base = os.environ.get("SITE_BASE_URL")
        existing_base = site_data.get("baseUrl", "")
        resolved_base = env_base if env_base is not None else existing_base
        site_data["baseUrl"] = normalize_base_url(resolved_base)
        with open(site_json_path, "w") as f:
            json.dump(site_data, f, indent=2)
    
    print(f"Build complete: {update_date}")


if __name__ == "__main__":
    main()
