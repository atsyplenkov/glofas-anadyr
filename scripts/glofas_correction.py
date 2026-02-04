import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from os.path import dirname
from xsdba import DetrendedQuantileMapping, Grouper
import pickle
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

def save_dqm_model(dqm, station_id, models_dir):
    models_path = Path(models_dir)
    models_path.mkdir(parents=True, exist_ok=True)
    
    model_file = models_path / f"{station_id}_dqm.pkl"
    try:
        with open(model_file, "wb") as f:
            pickle.dump(dqm, f)
        print(f"  Station {station_id}: Saved DQM model to {model_file}")
    except Exception as e:
        print(f"  Station {station_id}: Error saving model - {e}")

def correct_station(station_id, obs_dir, raw_dir, output_dir, n_quantiles, models_dir=None):
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
    
    # Add month column for filtering
    all_raw_data["month"] = pd.to_datetime(all_raw_data["date"]).dt.month
    
    # Split data: months 5-9 get DQM correction, others use raw values
    correction_months = [5, 6, 7, 8, 9]
    correction_mask = all_raw_data["month"].isin(correction_months)
    
    data_to_correct = all_raw_data[correction_mask].copy()
    data_raw_only = all_raw_data[~correction_mask].copy()
    
    epsilon = 0.01
    
    # Process months that need correction (May-Sept)
    if len(data_to_correct) > 0:
        train_obs_da = as_xarray(train_data, "obs", "date")
        train_sim_da = as_xarray(train_data, "sim", "date")
        correct_sim_da = as_xarray(data_to_correct, "sim", "date")
        
        group = Grouper("time.dayofyear", window=31)
        
        try:
            dqm = DetrendedQuantileMapping.train(
                train_obs_da,
                train_sim_da,
                nquantiles=n_quantiles,
                group=group,
                kind="*",
            )
            
            if models_dir is not None:
                save_dqm_model(dqm, station_id, models_dir)
            
            corrected_da = dqm.adjust(correct_sim_da)
            corrected_values = corrected_da.values - epsilon
            corrected_values = np.maximum(corrected_values, 0.0)
            
            data_to_correct["q_cor"] = corrected_values
            
        except Exception as e:
            print(f"  Station {station_id}: DQM Error - {e}")
            # Fall back to raw values if DQM fails
            data_to_correct["q_cor"] = data_to_correct["sim"] - epsilon
    
    # For Oct-April: use raw values directly (subtract epsilon that was added)
    if len(data_raw_only) > 0:
        data_raw_only["q_cor"] = data_raw_only["sim"] - epsilon
        data_raw_only["q_cor"] = np.maximum(data_raw_only["q_cor"].values, 0.0)
    
    # Combine and sort by date
    result_df = pd.concat([data_to_correct, data_raw_only], ignore_index=True)
    result_df = result_df.sort_values("date").reset_index(drop=True)
    result_df = result_df[["date", "q_cor"]]
    
    output_file = output_path / f"{station_id}.csv"
    result_df.to_csv(output_file, index=False)
    
    n_corrected = len(data_to_correct)
    n_raw = len(data_raw_only)
    print(f"  Station {station_id}: Saved {len(result_df)} values ({n_corrected} DQM corrected, {n_raw} raw)")

def correct_all_stations(
    stations=None,
    obs_dir="data/hydro/obs",
    raw_dir="data/hydro/raw",
    output_dir="data/hydro/cor",
    quantiles_map=None,
    models_dir="data/models"
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
        
        correct_station(station_id, obs_dir, raw_dir, output_dir, n_quantiles, models_dir)
    
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
            output_dir=output_dir,
            models_dir="data/models"
        )
    except NameError:
        # When run standalone
        correct_all_stations()
