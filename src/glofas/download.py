import os
import cdsapi
from pathlib import Path
from typing import List, Optional
from .config import ANADYR_BBOX

def create_request(hyear: str, area: List[float] = ANADYR_BBOX) -> dict:
    """Create a CDS API request for a specific year."""
    return {
        "system_version": ["version_4_0"],
        "hydrological_model": ["lisflood"],
        "product_type": ["consolidated"],
        "variable": ["river_discharge_in_the_last_24_hours"],
        "hyear": [str(hyear)],
        "hmonth": [f"{i:02d}" for i in range(1, 13)],
        "hday": [f"{i:02d}" for i in range(1, 32)],
        "data_format": "netcdf",
        "download_format": "unarchived",
        "area": area
    }

def download_glofas_incremental(years: List[int], temp_dir: Path) -> List[Path]:
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
        request = create_request(str(year))
        
        try:
            client.retrieve(dataset, request, str(target_file))
            downloaded_files.append(target_file)
            print(f"Downloaded {year}.nc")
        except Exception as e:
            print(f"Error downloading {year}: {e}")
    
    return downloaded_files

