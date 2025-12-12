import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from os.path import dirname
from xsdba import DetrendedQuantileMapping, Grouper
import warnings

warnings.filterwarnings("ignore", category=RuntimeWarning)

QUANTILES_MAP = {
    1496: 20,
    1497: 10,
    1499: 1,
    1502: 15,
    1504: 1,
    1508: 10,
    1587: 1,
}

def read_station_data(station_id, obs_dir, raw_dir):
    obs_path = Path(obs_dir) / f"{station_id}.csv"
    raw_path = Path(raw_dir) / f"{station_id}.csv"
    
    obs_df = pd.read_csv(obs_path, parse_dates=["date"])
    raw_df = pd.read_csv(raw_path, parse_dates=["datetime"])
    
    raw_df["date"] = pd.to_datetime(raw_df["datetime"]).dt.date
    raw_df["date"] = pd.to_datetime(raw_df["date"]) - pd.Timedelta(days=1)
    
    merged = obs_df.merge(raw_df, on="date", how="inner")
    merged = merged[["date", "q_cms", "q_raw"]].copy()
    merged = merged.rename(columns={"q_cms": "obs", "q_raw": "sim"})
    merged = merged.dropna()
    
    merged["year"] = merged["date"].dt.year
    merged["month"] = merged["date"].dt.month
    
    merged = merged[
        (merged["year"] >= 1979) & 
        (merged["year"] <= 1996) &
        (merged["month"].between(5, 10))
    ].copy()
    
    epsilon = 0.01
    merged["obs"] = merged["obs"] + epsilon
    merged["sim"] = merged["sim"] + epsilon
    
    return merged

def read_all_raw_data(station_id, raw_dir):
    raw_path = Path(raw_dir) / f"{station_id}.csv"
    raw_df = pd.read_csv(raw_path, parse_dates=["datetime"])
    
    raw_df["date"] = pd.to_datetime(raw_df["datetime"]).dt.date
    raw_df["date"] = pd.to_datetime(raw_df["date"]) - pd.Timedelta(days=1)
    
    raw_df = raw_df[["date", "q_raw"]].copy()
    raw_df = raw_df.rename(columns={"q_raw": "sim"})
    
    epsilon = 0.01
    raw_df["sim"] = raw_df["sim"] + epsilon
    
    return raw_df

def as_xarray(data, value_col, datetime_col, name="Q"):
    da = xr.DataArray(
        data[value_col].values,
        coords={"time": pd.to_datetime(data[datetime_col].values)},
        dims=["time"],
        attrs={"units": "m3/s"},
        name=name
    )
    return da

def correct_station(station_id, obs_dir, raw_dir, output_dir, n_quantiles):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    print(f"Processing station {station_id} with {n_quantiles} quantiles...")
    
    train_data = read_station_data(station_id, obs_dir, raw_dir)
    if len(train_data) == 0:
        print(f"  Station {station_id}: No training data available")
        return
    
    all_raw_data = read_all_raw_data(station_id, raw_dir)
    if len(all_raw_data) == 0:
        print(f"  Station {station_id}: No raw data available")
        return
    
    train_obs_da = as_xarray(train_data, "obs", "date")
    train_sim_da = as_xarray(train_data, "sim", "date")
    all_sim_da = as_xarray(all_raw_data, "sim", "date")
    
    group = Grouper("time.dayofyear", window=31)
    
    try:
        dqm = DetrendedQuantileMapping.train(
            train_obs_da,
            train_sim_da,
            nquantiles=n_quantiles,
            group=group,
            kind="*",
        )
        
        corrected_da = dqm.adjust(all_sim_da)
        corrected_values = corrected_da.values
        
        epsilon = 0.01
        corrected_values = corrected_values - epsilon
        corrected_values = np.maximum(corrected_values, 0.0)
        
        result_df = pd.DataFrame({
            "date": all_raw_data["date"].values,
            "q_cor": corrected_values
        })
        
        output_file = output_path / f"{station_id}.csv"
        result_df.to_csv(output_file, index=False)
        print(f"  Station {station_id}: Saved {len(result_df)} corrected values")
        
    except Exception as e:
        print(f"  Station {station_id}: Error - {e}")

def correct_all_stations(
    stations=None,
    obs_dir="data/hydro/obs",
    raw_dir="data/hydro/raw",
    output_dir="data/hydro/cor",
    quantiles_map=None
):
    if stations is None:
        stations = list(QUANTILES_MAP.keys())
    
    if quantiles_map is None:
        quantiles_map = QUANTILES_MAP
    
    print(f"Correcting {len(stations)} stations...")
    
    for station_id in stations:
        n_quantiles = quantiles_map.get(station_id)
        if n_quantiles is None:
            print(f"  Station {station_id}: No quantiles specified, skipping")
            continue
        
        correct_station(station_id, obs_dir, raw_dir, output_dir, n_quantiles)
    
    print("Correction complete!")

if __name__ == "__main__":
    try:
        # When run by Snakemake, snakemake object is available
        output_files = snakemake.output.cor_files
        stations = [int(Path(f).stem) for f in output_files]
        
        obs_dir = dirname(snakemake.input.obs_files[0])
        raw_dir = dirname(snakemake.input.raw_files[0])
        output_dir = dirname(snakemake.output.cor_files[0])
        
        correct_all_stations(
            stations=stations,
            obs_dir=obs_dir,
            raw_dir=raw_dir,
            output_dir=output_dir
        )
    except NameError:
        # When run standalone
        correct_all_stations()
