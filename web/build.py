import os
import json
import re
from datetime import date
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).parent.parent
WEB_DIR = Path(__file__).parent
DATA_DIR = WEB_DIR / "src" / "_data"
GEOMETRY_PATH = PROJECT_ROOT / "data" / "geometry" / "anadyr_gauges.gpkg"
CV_RESULTS_PATH = PROJECT_ROOT / "tables" / "tbl2_cv-results.csv"

GAUGE_INFO = {
    1496: {"name": "Lamutskoe", "river": "Anadyr", "lon": 168.868195111111, "lat": 65.55792480555559},
    1497: {"name": "Novyy Yeropol", "river": "Anadyr", "lon": 168.74044325, "lat": 64.84445925},
    1499: {"name": "Snezhnoe", "river": "Anadyr", "lon": 172.9356813472321, "lat": 65.43040789253637},
    1502: {"name": "Chuvanskoe", "river": "Yeropol", "lon": 167.955277888889, "lat": 65.1809386944445},
    1504: {"name": "Vayegi", "river": "Mayn", "lon": 171.064252833333, "lat": 64.16331699999999},
    1508: {"name": "Mukhomornoe", "river": "Enmyvaam", "lon": 173.340203222222, "lat": 66.39800291666668},
    1587: {"name": "Tanyurer", "river": "Tanyurer", "lon": 174.388533611111, "lat": 64.8584099444444},
}

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


def load_local_data(gauge_id: int) -> pd.DataFrame:
    """Load data from local CSV files when S3 is not available."""
    obs_path = PROJECT_ROOT / "data" / "hydro" / "obs" / f"{gauge_id}.csv"
    raw_path = PROJECT_ROOT / "data" / "hydro" / "raw" / f"{gauge_id}.csv"
    cor_path = PROJECT_ROOT / "data" / "hydro" / "cor" / f"{gauge_id}.csv"

    obs = pd.read_csv(obs_path, parse_dates=["date"])
    obs = obs.rename(columns={"q_cms": "q_obs"})

    raw = pd.read_csv(raw_path, parse_dates=["datetime"])
    raw["date"] = raw["datetime"].dt.normalize()
    raw = raw.groupby("date")["q_raw"].mean().reset_index()

    cor = pd.read_csv(cor_path, parse_dates=["date"])

    df = obs.merge(raw, on="date", how="outer").merge(cor, on="date", how="outer")
    return df.sort_values("date")


def download_from_s3(bucket: str, prefix: str) -> dict:
    """Download parquet files from S3 and return as dict of DataFrames."""
    s3 = boto3.client("s3")

    # Get latest update date from metadata
    metadata_obj = s3.get_object(Bucket=bucket, Key=f"{prefix}/metadata.json")
    metadata = json.loads(metadata_obj["Body"].read())
    update_date = metadata["last_update"]
    gauges = metadata["gauges"]

    data = {"update_date": update_date, "gauges": {}}
    for gauge_id in gauges:
        key = f"{prefix}/timeseries/update_date={update_date}/{gauge_id}.parquet"
        local_path = f"/tmp/{gauge_id}.parquet"
        s3.download_file(bucket, key, local_path)
        data["gauges"][gauge_id] = pd.read_parquet(local_path)
        os.remove(local_path)

    return data


def generate_gauges_json(cv_results: dict, update_date: str) -> dict:
    """Generate GeoJSON for all gauges."""
    features = []
    for gauge_id, info in GAUGE_INFO.items():
        # Load obs data to get date range and missing %
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
    update_date = date.today().isoformat()

    # Load CV results
    cv_results = load_cv_results()

    # Try S3, fall back to local
    gauge_data = {}
    use_s3 = bucket and aws_key and aws_secret
    if bucket and not use_s3:
        print("Warning: S3_BUCKET set but missing AWS credentials, using local data")
    if use_s3:
        try:
            s3_data = download_from_s3(bucket, "glofas-anadyr")
            update_date = s3_data["update_date"]
            gauge_data = s3_data["gauges"]
        except Exception as e:
            print(f"S3 download failed: {e}, using local data")

    if not gauge_data:
        for gauge_id in GAUGE_INFO.keys():
            gauge_data[gauge_id] = load_local_data(gauge_id)

    # Generate gauges.json
    gauges_geojson = generate_gauges_json(cv_results, update_date)
    with open(DATA_DIR / "gauges.json", "w") as f:
        json.dump(gauges_geojson, f)
    print(f"Generated gauges.json")

    # Generate timeseries JSON for each gauge
    timeseries_dir = DATA_DIR / "timeseries"
    timeseries_dir.mkdir(exist_ok=True)
    for gauge_id, df in gauge_data.items():
        ts_json = generate_timeseries_json(gauge_id, df)
        with open(timeseries_dir / f"{gauge_id}.json", "w") as f:
            json.dump(ts_json, f)
        print(f"Generated timeseries/{gauge_id}.json")

    # Update site.json with last updated date
    site_json_path = DATA_DIR / "site.json"
    with open(site_json_path) as f:
        site_data = json.load(f)
    site_data["lastUpdated"] = update_date
    with open(site_json_path, "w") as f:
        json.dump(site_data, f, indent=2)

    print(f"Build complete: {update_date}")


if __name__ == "__main__":
    main()
