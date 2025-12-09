import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
import xsdba as sdba
from concurrent.futures import ProcessPoolExecutor, as_completed
import hydroeval as he
import warnings

# Suppress divide by zero warnings in logs
warnings.filterwarnings("ignore", category=RuntimeWarning)

def read_station_data(station_id, obs_dir, sim_dir):
    """Read observed and simulated data for a station."""
    obs_path = Path(obs_dir) / f"{station_id}.csv"
    sim_path = Path(sim_dir) / f"{station_id}.csv"
    
    obs_df = pd.read_csv(obs_path, parse_dates=["date"])
    sim_df = pd.read_csv(sim_path, parse_dates=["datetime"])
    
    sim_df["date"] = pd.to_datetime(sim_df["datetime"]).dt.date
    # NOTE: Justification for -1 day lag must be included in the paper method section
    # (e.g., "Simulations represent 00:00 UTC, effectively prev day average...")
    sim_df["date"] = pd.to_datetime(sim_df["date"]) - pd.Timedelta(days=1)
    
    merged = obs_df.merge(sim_df, on="date", how="inner")
    merged = merged[["date", "q_cms", "q_raw"]].copy()
    merged = merged.rename(columns={"q_cms": "obs", "q_raw": "sim"})
    merged = merged.dropna()
    
    merged["year"] = merged["date"].dt.year
    merged["month"] = merged["date"].dt.month
    
    # Filter for Anadyr open water season (May-Sept)
    merged = merged[
        (merged["year"] >= 1979) & 
        (merged["year"] <= 1996) &
        (merged["month"].between(5, 9))
    ].copy()
    
    # Add epsilon to handle zero flows in multiplicative correction and logs
    epsilon = 0.01
    merged["obs"] = merged["obs"] + epsilon
    merged["sim"] = merged["sim"] + epsilon
    
    return merged

def sliding_window_splits(data, lookback=5, assess_stop=2, step=1):
    year_non_na_counts = data.groupby("year")["obs"].apply(lambda x: x.notna().sum())
    years_with_data = sorted(year_non_na_counts[year_non_na_counts > 0].index.tolist())
    
    splits = []
    i = 0
    while i + lookback + assess_stop <= len(years_with_data):
        train_years = years_with_data[i:i + lookback]
        test_years = years_with_data[i + lookback:i + lookback + assess_stop]
        
        train_mask = data["year"].isin(train_years)
        test_mask = data["year"].isin(test_years)
        
        splits.append({
            "train": data[train_mask].copy(),
            "test": data[test_mask].copy(),
            "train_years": f"{min(train_years)}-{max(train_years)}",
            "test_years": f"{min(test_years)}-{max(test_years)}"
        })
        
        i += step
    return splits

def as_xarray(data, value_col, datetime_col):
    """Convert DataFrame to xarray DataArray with proper units for xclim."""
    da = xr.DataArray(
        data[value_col].values,
        coords={"time": pd.to_datetime(data[datetime_col].values)},
        dims=["time"],
        attrs={"units": "m3 s-1", "kind": "streamflow"},
        name=value_col
    )
    return da

def calculate_metrics(obs, sim):
    """Calculate hydrological metrics."""
    obs = np.array(obs)
    sim = np.array(sim)
    
    # Ensure no NaNs or zeros (though epsilon handled zeros earlier)
    mask = (obs > 0) & (sim > 0) & (~np.isnan(obs)) & (~np.isnan(sim))
    obs = obs[mask]
    sim = sim[mask]
    
    if len(obs) < 10: # Minimum threshold for valid calculation
        return None
    
    # Standard NSE
    nse = he.evaluator(he.nse, sim, obs)
    # Log NSE (for low flows)
    log_nse = he.evaluator(he.nse, np.log(sim), np.log(obs))
    # KGE'
    kge, r, alpha, beta = he.evaluator(he.kgeprime, sim, obs)
    # PBIAS
    pbias_val = he.evaluator(he.pbias, sim, obs)
    # RMSE
    rmse_val = he.evaluator(he.rmse, sim, obs)
    
    def to_scalar(val):
        if isinstance(val, np.ndarray):
            return float(val.item() if val.size == 1 else val[0])
        return float(val)
    
    return {
        "nse": to_scalar(nse),
        "log_nse": to_scalar(log_nse),
        "kgeprime": to_scalar(kge),
        "pbias": to_scalar(pbias_val),
        "rmse": to_scalar(rmse_val),
    }

def process_station(station_id, obs_dir, sim_dir, output_dir, quantiles_list):
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    data = read_station_data(station_id, obs_dir, sim_dir)
    if len(data) == 0:
        return station_id, None
    
    splits = sliding_window_splits(data)
    results = []
    
    for split in splits:
        train_df = split["train"]
        test_df = split["test"]
        
        # 1. Calculate Raw Metrics
        raw_metrics = calculate_metrics(test_df["obs"], test_df["sim"])
        if raw_metrics:
            raw_metrics.update({
                "type": "Raw",
                "nq": "raw",
                "train_years": split["train_years"],
                "test_years": split["test_years"]
            })
            results.append(raw_metrics)
        
        # Prepare DataArrays for DQM
        train_obs_da = as_xarray(train_df, "obs", "date")
        train_sim_da = as_xarray(train_df, "sim", "date")
        test_sim_da = as_xarray(test_df, "sim", "date")
        
        # 2. Define Seasonality Grouping
        # Group by day of year with a window (e.g., +/- 15 days) to capture seasonal drift
        # group = sdba.base.Grouper("time.dayofyear", window=31)
        # OR
        group = sdba.base.Grouper("time.month")
        
        for nq in quantiles_list:
            try:
                # DQM Training
                dqm = sdba.DetrendedQuantileMapping.train(
                    train_obs_da,
                    train_sim_da,
                    nquantiles=nq,
                    group=group, 
                    kind="*" 
                )
                
                # Adjustment
                test_qmap_da = dqm.adjust(test_sim_da)
                test_qmap = test_qmap_da.values
                
                # Calculate Metrics
                dqm_metrics = calculate_metrics(test_df["obs"], test_qmap)
                
                if dqm_metrics:
                    dqm_metrics.update({
                        "type": "DQM",
                        "nq": nq,
                        "train_years": split["train_years"],
                        "test_years": split["test_years"]
                    })
                    results.append(dqm_metrics)
                    
            except Exception as e:
                # Catch specific errors (e.g. not enough data for quantiles)
                continue
    
    if results:
        results_df = pd.DataFrame(results)
        results_df.to_csv(output_path / f"{station_id}.csv", index=False)
        return station_id, len(results)
    
    return station_id, None

def cv_glofas(stations, 
              obs_dir="data/hydro/obs", 
              sim_dir="data/hydro/raw", 
              output_dir="data/cv", 
              quantiles_range=[5, 15, 25, 35, 50, 75, 90, 100, 110],
              # quantiles_range=range(5, 111, 25),
              max_workers=None):
    """Perform cross-validation for GloFAS stations in parallel."""
    if max_workers is None:
        import os
        max_workers = min(len(stations), os.cpu_count() or 1)
    
    quantiles_list = list(quantiles_range)
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                process_station, station_id, obs_dir, sim_dir, output_dir, quantiles_list
            ): station_id
            for station_id in stations
        }
        
        for future in as_completed(futures):
            station_id = futures[future]
            try:
                result = future.result()
                print(f"Completed station {result[0]}")
            except Exception as e:
                print(f"Station {station_id} generated an exception: {e}")


if __name__ == "__main__":
    # Ensure quantiles are reasonable for the data length. 
    # With limited data, avoid nq > 50 unless window is very long.
    stations = [1496, 1497, 1499, 1502, 1504, 1508, 1587]
    cv_glofas(stations, quantiles_range=[5, 10, 15, 20, 25, 30, 50])