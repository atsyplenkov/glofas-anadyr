import os
import json
import re
import pickle
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import boto3
import cdsapi
import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from xsdba import DetrendedQuantileMapping

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent
WEB_DIR = Path(__file__).parent
DATA_DIR = WEB_DIR / "src" / "_data"
GEOMETRY_PATH = PROJECT_ROOT / "data" / "geometry" / "anadyr_gauges.gpkg"
CV_RESULTS_PATH = PROJECT_ROOT / "tables" / "tbl2_cv-results.csv"
MODELS_DIR = PROJECT_ROOT / "data" / "models"
GLOFAS_DIR = PROJECT_ROOT / "data" / "glofas"

GAUGE_IDS = [1496, 1497, 1499, 1502, 1504, 1508, 1587]

GAUGE_INFO = {
    1496: {"name": "Lamutskoe", "river": "Anadyr", "lon": 168.868195111111, "lat": 65.55792480555559},
    1497: {"name": "Novyy Yeropol", "river": "Anadyr", "lon": 168.74044325, "lat": 64.84445925},
    1499: {"name": "Snezhnoe", "river": "Anadyr", "lon": 172.9356813472321, "lat": 65.43040789253637},
    1502: {"name": "Chuvanskoe", "river": "Yeropol", "lon": 167.955277888889, "lat": 65.1809386944445},
    1504: {"name": "Vayegi", "river": "Mayn", "lon": 171.064252833333, "lat": 64.16331699999999},
    1508: {"name": "Mukhomornoe", "river": "Enmyvaam", "lon": 173.340203222222, "lat": 66.39800291666668},
    1587: {"name": "Tanyurer", "river": "Tanyurer", "lon": 174.388533611111, "lat": 64.8584099444444},
}

ANADYR_BBOX = [66.47, 167.59, 64.04, 174.82]


def extract_kge(value: str) -> str:
    """Extract KGE value from string like '0.571 [95% CI 0.204 - 0.74]'."""
    match = re.match(r"([-\d.]+)", str(value))
    return match.group(1) if match else ""


def load_cv_results() -> dict:
    """Load cross-validation results from CSV."""
    df = pd.read_csv(CV_RESULTS_PATH)
    results = {}
    for gauge_id in df["gauge_id"].unique():
        gauge_df = df[df["gauge_id"] == gauge_id]
        raw_row = gauge_df[gauge_df["type"] == "Raw"]
        dqm_row = gauge_df[gauge_df["type"] == "DQM"]
        if len(raw_row) > 0 and len(dqm_row) > 0:
            results[gauge_id] = {
                "kge_raw": extract_kge(raw_row["KGE'"].values[0]),
                "kge_dqm": extract_kge(dqm_row["KGE'"].values[0]),
            }
    return results


def get_s3_metadata(bucket: str, s3_client) -> dict:
    """Get metadata from S3."""
    try:
        metadata_obj = s3_client.get_object(Bucket=bucket, Key="metadata.json")
        metadata = json.loads(metadata_obj["Body"].read())
        return metadata
    except ClientError:
        return None


def get_last_data_date(bucket: str, s3_client) -> date:
    """Get the actual last date in the data files."""
    metadata = get_s3_metadata(bucket, s3_client)
    if not metadata or "last_update" not in metadata:
        return date(2024, 12, 31)
    
    update_date = metadata["last_update"]
    last_data_date = None
    
    for gauge_id in GAUGE_IDS:
        try:
            key = f"timeseries/update_date={update_date}/{gauge_id}.parquet"
            local_path = f"/tmp/{gauge_id}_check.parquet"
            s3_client.download_file(bucket, key, local_path)
            df = pd.read_parquet(local_path)
            if not df.empty:
                gauge_last_date = pd.to_datetime(df["date"]).max().date()
                if last_data_date is None or gauge_last_date > last_data_date:
                    last_data_date = gauge_last_date
            os.remove(local_path)
        except Exception:
            continue
    
    if last_data_date is None:
        return datetime.fromisoformat(update_date).date()
    
    return last_data_date


def get_missing_years(last_data_date: date) -> list:
    """Get list of years that need to be downloaded."""
    today = date.today()
    current_year = today.year
    last_year = last_data_date.year
    
    missing_years = []
    
    if last_year < current_year:
        missing_years = list(range(last_year + 1, current_year + 1))
    elif last_year == current_year:
        days_since_last = (today - last_data_date).days
        if days_since_last > 7:
            missing_years = [current_year]
    
    return missing_years


def download_glofas_incremental(years: list, temp_dir: Path):
    """Download missing GloFAS NetCDF files."""
    if not years:
        print("No missing years to download")
        return []
    
    ecmwf_token = os.getenv("ECMWF_TOKEN")
    if not ecmwf_token:
        raise ValueError("ECMWF_TOKEN not found in environment")
    
    client = cdsapi.Client(url="https://ewds.climate.copernicus.eu/api", key=ecmwf_token)
    dataset = "cems-glofas-historical"
    
    downloaded_files = []
    for year in years:
        target_file = temp_dir / f"{year}.nc"
        if target_file.exists():
            print(f"Skipping {year}.nc (already exists)")
            downloaded_files.append(target_file)
            continue
        
        print(f"Downloading GloFAS data for {year}...")
        request = {
            "system_version": ["version_4_0"],
            "hydrological_model": ["lisflood"],
            "product_type": ["consolidated"],
            "variable": ["river_discharge_in_the_last_24_hours"],
            "hyear": [str(year)],
            "hmonth": [f"{i:02d}" for i in range(1, 13)],
            "hday": [f"{i:02d}" for i in range(1, 32)],
            "data_format": "netcdf",
            "download_format": "unarchived",
            "area": ANADYR_BBOX
        }
        
        try:
            client.retrieve(dataset, request, str(target_file))
            downloaded_files.append(target_file)
            print(f"Downloaded {year}.nc")
        except Exception as e:
            print(f"Error downloading {year}: {e}")
    
    return downloaded_files


def extract_glofas_at_gauges(nc_file: Path, gauges_gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """Extract GloFAS discharge values at gauge locations."""
    try:
        ds = xr.open_dataset(nc_file)
    except Exception as e:
        print(f"Error opening {nc_file}: {e}")
        return pd.DataFrame()
    
    if "dis24" not in ds:
        print(f"Warning: dis24 variable not found in {nc_file}")
        ds.close()
        return pd.DataFrame()
    
    # Print date range in the file
    time_var = None
    if "time" in ds.dims:
        time_var = ds["time"]
    elif "valid_time" in ds.dims:
        time_var = ds["valid_time"]
    
    if time_var is not None:
        min_date = pd.to_datetime(time_var.values.min()).date()
        max_date = pd.to_datetime(time_var.values.max()).date()
        print(f"  NetCDF date range: {min_date} to {max_date} ({len(time_var)} timesteps)")
    
    lon_dim = "longitude" if "longitude" in ds.dims else "lon"
    lat_dim = "latitude" if "latitude" in ds.dims else "lat"
    
    results = []
    for idx, gauge in gauges_gdf.iterrows():
        gauge_id = gauge["id"]
        lon = gauge.geometry.x
        lat = gauge.geometry.y
        
        try:
            point_data = ds["dis24"].sel({lon_dim: lon, lat_dim: lat}, method="nearest")
            
            time_dim = None
            if "time" in point_data.dims:
                time_dim = "time"
            elif "valid_time" in point_data.dims:
                time_dim = "valid_time"
            
            if time_dim:
                point_series = point_data.to_series()
                for time_val, dis24_val in point_series.items():
                    if pd.notna(dis24_val):
                        results.append({
                            "gauge_id": gauge_id,
                            "datetime": pd.to_datetime(time_val),
                            "q_raw": float(dis24_val)
                        })
            else:
                single_val = float(point_data.values)
                if pd.notna(single_val):
                    results.append({
                        "gauge_id": gauge_id,
                        "datetime": pd.to_datetime("now"),
                        "q_raw": single_val
                    })
        except Exception as e:
            print(f"Error extracting data for gauge {gauge_id}: {e}")
    
    ds.close()
    return pd.DataFrame(results)


def load_dqm_model(gauge_id: int) -> DetrendedQuantileMapping:
    """Load pretrained DQM model."""
    model_path = MODELS_DIR / f"{gauge_id}_dqm.pkl"
    if not model_path.exists():
        raise FileNotFoundError(f"Model not found: {model_path}")
    
    with open(model_path, "rb") as f:
        return pickle.load(f)


def correct_new_data(gauge_id: int, raw_df: pd.DataFrame) -> pd.DataFrame:
    """Apply pretrained DQM model to new raw data."""
    if raw_df.empty:
        return pd.DataFrame(columns=["date", "q_cor"])
    
    try:
        dqm = load_dqm_model(gauge_id)
        
        raw_df["date"] = pd.to_datetime(raw_df["datetime"]).dt.date
        raw_df["date"] = pd.to_datetime(raw_df["date"]) - pd.Timedelta(days=1)
        
        epsilon = 0.01
        sim_values = raw_df["q_raw"].values + epsilon
        
        sim_da = xr.DataArray(
            sim_values,
            coords={"time": pd.to_datetime(raw_df["date"].values)},
            dims=["time"],
            attrs={"units": "m3/s"},
            name="sim"
        )
        
        corrected_da = dqm.adjust(sim_da)
        corrected_values = corrected_da.values - epsilon
        corrected_values = np.maximum(corrected_values, 0.0)
        
        result_df = pd.DataFrame({
            "date": raw_df["date"].values,
            "q_cor": corrected_values
        })
        
        return result_df
    except Exception as e:
        print(f"Error correcting data for gauge {gauge_id}: {e}")
        return pd.DataFrame(columns=["date", "q_cor"])


def upload_incremental_to_s3(bucket: str, s3_client, gauge_id: int, new_data: pd.DataFrame, update_date: str):
    """Upload new corrected data to S3, appending to existing parquet."""
    try:
        metadata = get_s3_metadata(bucket, s3_client)
        if not metadata:
            old_update_date = "2025-10-01"
        else:
            old_update_date = metadata["last_update"]
        
        old_key = f"timeseries/update_date={old_update_date}/{gauge_id}.parquet"
        
        try:
            old_path = f"/tmp/{gauge_id}_old.parquet"
            s3_client.download_file(bucket, old_key, old_path)
            old_df = pd.read_parquet(old_path)
            
            if "q_obs" not in old_df.columns:
                old_df["q_obs"] = None
            
            last_date = pd.to_datetime(old_df["date"]).max()
            new_data["date"] = pd.to_datetime(new_data["date"])
            new_data_filtered = new_data[new_data["date"] > last_date].copy()
            
            if new_data_filtered.empty:
                print(f"No new data for gauge {gauge_id}")
                os.remove(old_path)
                return
            
            if "q_obs" not in new_data_filtered.columns:
                new_data_filtered["q_obs"] = None
            
            combined_df = pd.concat([old_df, new_data_filtered]).sort_values("date")
            combined_df = combined_df.drop_duplicates(subset=["date"], keep="last")
            os.remove(old_path)
        except ClientError:
            combined_df = new_data.copy()
            if "q_obs" not in combined_df.columns:
                combined_df["q_obs"] = None
        
        combined_df["update_date"] = update_date
        combined_df = combined_df[["date", "gauge_id", "q_obs", "q_raw", "q_cor", "update_date"]]
        
        new_key = f"timeseries/update_date={update_date}/{gauge_id}.parquet"
        parquet_path = f"/tmp/{gauge_id}_new.parquet"
        combined_df.to_parquet(parquet_path, index=False)
        
        s3_client.upload_file(parquet_path, bucket, new_key)
        print(f"Uploaded updated data for gauge {gauge_id}")
        os.remove(parquet_path)
        
    except Exception as e:
        print(f"Error uploading gauge {gauge_id}: {e}")


def download_from_s3(bucket: str, s3_client) -> dict:
    """Download parquet files from S3 and return as dict of DataFrames."""
    metadata = get_s3_metadata(bucket, s3_client)
    if not metadata:
        return {"update_date": date.today().isoformat(), "gauges": {}}
    
    update_date = metadata["last_update"]
    gauges = metadata["gauges"]
    
    data = {"update_date": update_date, "gauges": {}}
    for gauge_id in gauges:
        key = f"timeseries/update_date={update_date}/{gauge_id}.parquet"
        try:
            local_path = f"/tmp/{gauge_id}.parquet"
            s3_client.download_file(bucket, key, local_path)
            data["gauges"][gauge_id] = pd.read_parquet(local_path)
            os.remove(local_path)
        except ClientError as e:
            print(f"Error downloading {gauge_id}: {e}")
    
    return data


def load_local_data(gauge_id: int) -> pd.DataFrame:
    """Load data from local CSV files when S3 is not available."""
    obs_path = PROJECT_ROOT / "data" / "hydro" / "obs" / f"{gauge_id}.csv"
    raw_path = PROJECT_ROOT / "data" / "hydro" / "raw" / f"{gauge_id}.csv"
    cor_path = PROJECT_ROOT / "data" / "hydro" / "cor" / f"{gauge_id}.csv"
    
    obs = pd.read_csv(obs_path, parse_dates=["date"]) if obs_path.exists() else pd.DataFrame(columns=["date", "q_cms"])
    obs = obs.rename(columns={"q_cms": "q_obs"})
    
    raw = pd.read_csv(raw_path, parse_dates=["datetime"]) if raw_path.exists() else pd.DataFrame(columns=["datetime", "q_raw"])
    raw["date"] = raw["datetime"].dt.normalize() if not raw.empty else pd.Series(dtype="datetime64[ns]")
    raw = raw.groupby("date")["q_raw"].mean().reset_index() if not raw.empty else pd.DataFrame(columns=["date", "q_raw"])
    
    cor = pd.read_csv(cor_path, parse_dates=["date"]) if cor_path.exists() else pd.DataFrame(columns=["date", "q_cor"])
    
    df = obs.merge(raw, on="date", how="outer").merge(cor, on="date", how="outer")
    return df.sort_values("date")


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


def normalize_base_url(value: str) -> str:
    """Normalize the base URL for site asset references."""
    if value is None:
        return ""
    base = value.strip()
    if not base:
        return ""
    base = base.rstrip("/")
    if not base.startswith("/"):
        base = f"/{base}"
    if base == "/":
        return ""
    return base


def update_s3_metadata(bucket: str, s3_client, update_date: str):
    """Update metadata.json in S3."""
    metadata = {
        "last_update": update_date,
        "gauges": GAUGE_IDS
    }
    metadata_json = json.dumps(metadata, indent=2)
    s3_client.put_object(
        Bucket=bucket,
        Key="metadata.json",
        Body=metadata_json.encode('utf-8'),
        ContentType='application/json'
    )


def main():
    bucket = os.environ.get("S3_BUCKET")
    aws_key = os.environ.get("AWS_ACCESS_KEY_ID")
    aws_secret = os.environ.get("AWS_SECRET_ACCESS_KEY")
    endpoint = os.environ.get("ENDPOINT_URL")
    
    use_s3 = bucket and aws_key and aws_secret and endpoint
    
    if use_s3:
        s3_client = boto3.client("s3", endpoint_url=endpoint)
        
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
                                else:
                                    print(f"  Warning: No corrected data for gauge {gauge_id}")
                        
                        update_s3_metadata(bucket, s3_client, update_date)
                        print(f"Updated S3 with new data (update_date={update_date})")
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
        for gauge_id in GAUGE_IDS:
            gauge_data[gauge_id] = load_local_data(gauge_id)
    
    print("Loading CV results...")
    cv_results = load_cv_results()
    
    print("Generating website data...")
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    gauges_geojson = generate_gauges_json(cv_results, update_date)
    with open(DATA_DIR / "gauges.json", "w") as f:
        json.dump(gauges_geojson, f, indent=2)
    print(f"Generated gauges.json")
    
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
