import pickle
import re
from typing import List, Dict, Any, Optional, Union
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
import hydroeval as he
from xsdba import DetrendedQuantileMapping, Grouper

from .config import MODELS_DIR


def extract_kge(value: str) -> str:
    """Extract KGE value from string like '0.571 [95% CI 0.204 - 0.74]'."""
    match = re.match(r"([-\d.]+)", str(value))
    return match.group(1) if match else ""


def as_xarray(
    data: pd.DataFrame, value_col: str, datetime_col: str, name: str = "Q"
) -> xr.DataArray:
    """Convert DataFrame to xarray DataArray."""
    da = xr.DataArray(
        data[value_col].values,
        coords={"time": pd.to_datetime(data[datetime_col].values)},
        dims=["time"],
        attrs={"units": "m3/s"},
        name=name,
    )
    return da


def calculate_metrics(obs: np.ndarray, sim: np.ndarray) -> Optional[Dict[str, float]]:
    """Calculate all required metrics."""
    obs = np.array(obs).flatten()
    sim = np.array(sim).flatten()

    mask = (obs > 0) & (sim > 0)
    obs = obs[mask]
    sim = sim[mask]

    if len(obs) < 10:
        return None

    nse = he.evaluator(he.nse, sim, obs)
    log_nse = he.evaluator(he.nse, np.log(sim), np.log(obs))
    kge, r, alpha, beta = he.evaluator(he.kgeprime, sim, obs)
    kgenp, r, alpha, beta = he.evaluator(he.kgenp, sim, obs)
    pbias_val = he.evaluator(he.pbias, sim, obs)
    rmse_val = he.evaluator(he.rmse, sim, obs)

    def to_scalar(val):
        if isinstance(val, (np.ndarray, list)):
            return float(val[0]) if len(val) > 0 else np.nan
        return float(val)

    return {
        "nse": to_scalar(nse),
        "log_nse": to_scalar(log_nse),
        "kgeprime": to_scalar(kge),
        "kgenp": to_scalar(kgenp),
        "pbias": to_scalar(pbias_val),
        "rmse": to_scalar(rmse_val),
    }


def loocv_splits(data: pd.DataFrame) -> List[Dict[str, Any]]:
    """Generate Leave-One-Out (LOO) splits."""
    year_counts = data.groupby("year")["obs"].count()
    valid_years = sorted(year_counts[year_counts > 90].index.tolist())

    splits = []

    if len(valid_years) < 2:
        return splits

    for test_year in valid_years:
        train_years = [y for y in valid_years if y != test_year]

        train_mask = data["year"].isin(train_years)
        test_mask = data["year"] == test_year

        train_data = data[train_mask].copy()
        test_data = data[test_mask].copy()

        if len(train_data) > 0 and len(test_data) > 0:
            splits.append(
                {
                    "train": train_data,
                    "test": test_data,
                    "test_year": test_year,
                    "n_train_years": len(train_years),
                }
            )

    return splits


def extract_glofas_at_gauges(
    nc_file: Path, gauges_gdf: gpd.GeoDataFrame, tz: str = "Asia/Kamchatka"
) -> pd.DataFrame:
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
                # Convert to series and handle timezone if present or assume UTC -> Kamchatka
                # xarray often loads as datetime64[ns], usually UTC-like.
                # R script logic: with_tz(valid_time, tzone = tz)

                point_series = point_data.to_series()

                # Handling timezone
                # If naive, assume UTC then convert. If aware, just convert.

                for time_val, dis24_val in point_series.items():
                    if pd.notna(dis24_val):
                        ts = pd.to_datetime(time_val)
                        if ts.tzinfo is None:
                            ts = ts.tz_localize("UTC")
                        ts = ts.tz_convert(tz)

                        results.append(
                            {
                                "gauge_id": gauge_id,
                                "datetime": ts.strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                ),  # Store as string or keep as timestamp? R saves as string in CSV.
                                "q_raw": float(dis24_val),
                            }
                        )
            else:
                single_val = float(point_data.values)
                if pd.notna(single_val):
                    # If no time dimension, use current time converted to target TZ?
                    # Or keep "now" logic?
                    ts = pd.Timestamp.now(tz=tz)
                    results.append(
                        {
                            "gauge_id": gauge_id,
                            "datetime": ts.strftime("%Y-%m-%d %H:%M:%S"),
                            "q_raw": single_val,
                        }
                    )
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
    """Apply pretrained DQM model to new raw data.

    DQM correction is only applied to months 5-9 (May-Sept) where training
    data is robust. For Oct-April, raw GloFAS values are used directly.
    """
    if raw_df.empty:
        return pd.DataFrame(columns=["date", "q_cor"])

    try:
        raw_df = raw_df.copy()
        raw_df["date"] = pd.to_datetime(raw_df["datetime"]).dt.date
        raw_df["date"] = pd.to_datetime(raw_df["date"]) - pd.Timedelta(days=1)
        raw_df["month"] = pd.to_datetime(raw_df["date"]).dt.month

        epsilon = 0.01

        # Split data: months 5-9 get DQM correction, others use raw values
        correction_months = [5, 6, 7, 8, 9]
        correction_mask = raw_df["month"].isin(correction_months)

        data_to_correct = raw_df[correction_mask].copy()
        data_raw_only = raw_df[~correction_mask].copy()

        # Process months that need correction (May-Sept)
        if len(data_to_correct) > 0:
            dqm = load_dqm_model(gauge_id)

            sim_values = data_to_correct["q_raw"].values + epsilon

            sim_da = xr.DataArray(
                sim_values,
                coords={"time": pd.to_datetime(data_to_correct["date"].values)},
                dims=["time"],
                attrs={"units": "m3/s"},
                name="sim",
            )

            corrected_da = dqm.adjust(sim_da)
            corrected_values = corrected_da.values - epsilon
            corrected_values = np.maximum(corrected_values, 0.0)

            data_to_correct["q_cor"] = corrected_values

        # For Oct-April: use raw values directly
        if len(data_raw_only) > 0:
            data_raw_only["q_cor"] = np.maximum(data_raw_only["q_raw"].values, 0.0)

        # Combine and sort by date
        result_df = pd.concat([data_to_correct, data_raw_only], ignore_index=True)
        result_df = result_df.sort_values("date").reset_index(drop=True)
        result_df = result_df[["date", "q_cor"]]

        return result_df
    except Exception as e:
        print(f"Error correcting data for gauge {gauge_id}: {e}")
        return pd.DataFrame(columns=["date", "q_cor"])
