import os
import sys
from datetime import date
from pathlib import Path

# Add src to python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

from dotenv import load_dotenv

from glofas.config import GAUGE_IDS
from glofas.s3 import get_s3_client, update_s3_metadata
from glofas.io import load_gauge_data_all

load_dotenv()

def upload_to_s3(bucket: str, update_date: str):
    """Upload parquet files to S3."""
    s3 = get_s3_client()

    for gauge_id in GAUGE_IDS:
        # Load from default local location
        df = load_gauge_data_all(gauge_id)
        
        parquet_path = f"/tmp/{gauge_id}.parquet"
        df.to_parquet(parquet_path, index=False)

        s3_key = f"timeseries/update_date={update_date}/{gauge_id}.parquet"
        s3.upload_file(parquet_path, bucket, s3_key)
        print(f"Uploaded {s3_key}")
        os.remove(parquet_path)

    update_s3_metadata(bucket, s3, update_date, GAUGE_IDS)
    print("Uploaded metadata.json")


def main():
    required_env = ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_BUCKET", "ENDPOINT_URL"]
    missing = [v for v in required_env if not os.environ.get(v)]
    if missing:
        raise ValueError(f"Missing environment variables: {', '.join(missing)}")

    bucket = os.environ["S3_BUCKET"]
    update_date = date.today().isoformat()

    upload_to_s3(bucket, update_date)
    print(f"Upload complete: {update_date}")


if __name__ == "__main__":
    main()
