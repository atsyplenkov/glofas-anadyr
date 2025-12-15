import os
import json
from datetime import date
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path(__file__).parent.parent / "data" / "hydro"
GAUGE_IDS = [1496, 1497, 1499, 1502, 1504, 1508, 1587]


def load_gauge_data(gauge_id: int) -> pd.DataFrame:
    """Load and merge obs, raw, cor data for a gauge."""
    obs_path = DATA_DIR / "obs" / f"{gauge_id}.csv"
    raw_path = DATA_DIR / "raw" / f"{gauge_id}.csv"
    cor_path = DATA_DIR / "cor" / f"{gauge_id}.csv"

    obs = pd.read_csv(obs_path, parse_dates=["date"])
    obs = obs.rename(columns={"q_cms": "q_obs"})

    raw = pd.read_csv(raw_path, parse_dates=["datetime"])
    raw["date"] = raw["datetime"].dt.normalize()
    raw = raw.groupby("date")["q_raw"].mean().reset_index()

    cor = pd.read_csv(cor_path, parse_dates=["date"])

    df = obs.merge(raw, on="date", how="outer").merge(cor, on="date", how="outer")
    df = df.sort_values("date")
    df["gauge_id"] = gauge_id
    return df[["date", "gauge_id", "q_obs", "q_raw", "q_cor"]]


def upload_to_s3(bucket: str, prefix: str, update_date: str):
    """Upload parquet files to S3."""
    s3 = boto3.client("s3")

    for gauge_id in GAUGE_IDS:
        df = load_gauge_data(gauge_id)
        parquet_path = f"/tmp/{gauge_id}.parquet"
        df.to_parquet(parquet_path, index=False)

        s3_key = f"{prefix}/timeseries/update_date={update_date}/{gauge_id}.parquet"
        s3.upload_file(parquet_path, bucket, s3_key)
        print(f"Uploaded {s3_key}")
        os.remove(parquet_path)

    metadata = {"last_update": update_date, "gauges": GAUGE_IDS}
    metadata_path = "/tmp/metadata.json"
    with open(metadata_path, "w") as f:
        json.dump(metadata, f)
    s3.upload_file(metadata_path, bucket, f"{prefix}/metadata.json")
    print(f"Uploaded {prefix}/metadata.json")
    os.remove(metadata_path)


def main():
    required_env = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET"]
    missing = [v for v in required_env if not os.environ.get(v)]
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")

    bucket = os.environ["S3_BUCKET"]
    prefix = "glofas-anadyr"
    update_date = date.today().isoformat()

    upload_to_s3(bucket, prefix, update_date)
    print(f"Upload complete: {update_date}")


if __name__ == "__main__":
    main()
