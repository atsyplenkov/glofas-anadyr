import os
import cdsapi
from dotenv import load_dotenv

load_dotenv()

ecmwf_token = os.getenv("ECMWF_TOKEN")
if not ecmwf_token:
    raise ValueError("ECMWF_TOKEN not found in .env file")

dataset = "cems-glofas-historical"
anadyr_bb_glofas = [66.47, 167.59, 64.04, 174.82]

def create_request(hyear: str) -> dict:
    return {
        "system_version": ["version_4_0"],
        "hydrological_model": ["lisflood"],
        "product_type": ["consolidated"],
        "variable": ["river_discharge_in_the_last_24_hours"],
        "hyear": [hyear],
        "hmonth": [f"{i:02d}" for i in range(1, 13)],
        "hday": [f"{i:02d}" for i in range(1, 32)],
        "data_format": "netcdf",
        "download_format": "unarchived",
        "area": anadyr_bb_glofas
    }

client = cdsapi.Client(url="https://ewds.climate.copernicus.eu/api", key=ecmwf_token)

batch_requests = [
    ("2020", "data/glofas/2020.nc"),
    ("2021", "data/glofas/2021.nc"),
    ("2022", "data/glofas/2022.nc"),
    ("2023", "data/glofas/2023.nc"),
    ("2024", "data/glofas/2024.nc"),
]

for hyear, target in batch_requests:
    request = create_request(hyear)
    client.retrieve(dataset, request, target)