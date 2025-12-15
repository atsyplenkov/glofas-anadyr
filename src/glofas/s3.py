import json
import os
from datetime import date, datetime
from typing import Optional, List, Dict, Any

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from .config import GAUGE_IDS

def get_s3_client(endpoint_url: Optional[str] = None):
    """Create S3 client using environment variables."""
    return boto3.client("s3", endpoint_url=endpoint_url or os.environ.get("ENDPOINT_URL"))

def get_s3_metadata(bucket: str, s3_client) -> Optional[Dict[str, Any]]:
    """Get metadata from S3."""
    try:
        metadata_obj = s3_client.get_object(Bucket=bucket, Key="metadata.json")
        metadata = json.loads(metadata_obj["Body"].read())
        return metadata
    except ClientError:
        return None

def update_s3_metadata(bucket: str, s3_client, update_date: str, gauges: List[int] = GAUGE_IDS):
    """Update metadata.json in S3."""
    metadata = {
        "last_update": update_date,
        "gauges": gauges
    }
    metadata_json = json.dumps(metadata, indent=2)
    s3_client.put_object(
        Bucket=bucket,
        Key="metadata.json",
        Body=metadata_json.encode('utf-8'),
        ContentType='application/json'
    )

def get_last_data_date(bucket: str, s3_client) -> date:
    """Get the actual last date in the data files."""
    metadata = get_s3_metadata(bucket, s3_client)
    if not metadata or "last_update" not in metadata:
        # Default fallback if no metadata
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
            if os.path.exists(local_path):
                os.remove(local_path)
        except Exception:
            continue
    
    if last_data_date is None:
        return datetime.fromisoformat(update_date).date()
    
    return last_data_date

def upload_incremental_to_s3(bucket: str, s3_client, gauge_id: int, new_data: pd.DataFrame, update_date: str):
    """Upload new corrected data to S3, appending to existing parquet."""
    try:
        metadata = get_s3_metadata(bucket, s3_client)
        if not metadata:
            old_update_date = "2025-10-01" # TODO: Should this be a parameter or constant? Keeping from original code.
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
                if os.path.exists(old_path):
                    os.remove(old_path)
                return
            
            if "q_obs" not in new_data_filtered.columns:
                new_data_filtered["q_obs"] = None
            
            combined_df = pd.concat([old_df, new_data_filtered]).sort_values("date")
            combined_df = combined_df.drop_duplicates(subset=["date"], keep="last")
            if os.path.exists(old_path):
                os.remove(old_path)
        except ClientError:
            combined_df = new_data.copy()
            if "q_obs" not in combined_df.columns:
                combined_df["q_obs"] = None
        
        combined_df["update_date"] = update_date
        # Ensure columns exist
        for col in ["q_raw", "q_cor"]:
             if col not in combined_df.columns:
                 combined_df[col] = None

        combined_df = combined_df[["date", "gauge_id", "q_obs", "q_raw", "q_cor", "update_date"]]
        
        new_key = f"timeseries/update_date={update_date}/{gauge_id}.parquet"
        parquet_path = f"/tmp/{gauge_id}_new.parquet"
        combined_df.to_parquet(parquet_path, index=False)
        
        s3_client.upload_file(parquet_path, bucket, new_key)
        print(f"Uploaded updated data for gauge {gauge_id}")
        if os.path.exists(parquet_path):
            os.remove(parquet_path)
        
    except Exception as e:
        print(f"Error uploading gauge {gauge_id}: {e}")

def download_from_s3(bucket: str, s3_client) -> Dict[str, Any]:
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
            if os.path.exists(local_path):
                os.remove(local_path)
        except ClientError as e:
            print(f"Error downloading {gauge_id}: {e}")
    
    return data

