from pathlib import Path

# Paths
# Assuming structure:
# repo/
#   src/
#     glofas/
#       config.py
PACKAGE_DIR = Path(__file__).parent
SRC_DIR = PACKAGE_DIR.parent
PROJECT_ROOT = SRC_DIR.parent

DATA_DIR = PROJECT_ROOT / "data"
MODELS_DIR = DATA_DIR / "models"
GEOMETRY_PATH = DATA_DIR / "geometry" / "anadyr_gauges.gpkg"

# Gauges
GAUGE_IDS = [1496, 1497, 1499, 1502, 1504, 1508, 1587]

GAUGE_INFO = {
    1496: {
        "name": "Lamutskoe",
        "river": "Anadyr",
        "lon": 168.868195111111,
        "lat": 65.55792480555559,
    },
    1497: {
        "name": "Novyy Yeropol",
        "river": "Anadyr",
        "lon": 168.74044325,
        "lat": 64.84445925,
    },
    1499: {
        "name": "Snezhnoe",
        "river": "Anadyr",
        "lon": 172.9356813472321,
        "lat": 65.43040789253637,
    },
    1502: {
        "name": "Chuvanskoe",
        "river": "Yeropol",
        "lon": 167.955277888889,
        "lat": 65.1809386944445,
    },
    1504: {
        "name": "Vayegi",
        "river": "Mayn",
        "lon": 171.064252833333,
        "lat": 64.16331699999999,
    },
    1508: {
        "name": "Mukhomornoe",
        "river": "Enmyvaam",
        "lon": 173.340203222222,
        "lat": 66.39800291666668,
    },
    1587: {
        "name": "Tanyurer",
        "river": "Tanyurer",
        "lon": 174.388533611111,
        "lat": 64.8584099444444,
    },
}

# GloFAS
ANADYR_BBOX = [66.47, 167.59, 64.04, 174.82]
