import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from os.path import dirname
from xsdba import DetrendedQuantileMapping, Grouper
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
        (merged["month"].between(5, 10))
    ].copy()
    
    # Add epsilon to handle zero flows in multiplicative correction and logs
    epsilon = 0.01
    merged["obs"] = merged["obs"] + epsilon
    merged["sim"] = merged["sim"] + epsilon
    
    return merged

def loocv_splits(data):
    """
    Generate Leave-One-Out (LOO) splits.
    
    Strategy:
    1. Identify all years with sufficient data.
    2. Iterate through each year:
       - Test set: The specific year being iterated.
       - Training set: All other years combined.
    
    This preserves the hydrograph structure (autocorrelation) within the test year
    while maximizing the training data size.
    """
    # Filter for years that have enough data (e.g. > 90% of the season)
    # Season is May-Sept (approx 153 days). Threshold ~130 days.
    year_counts = data.groupby("year")["obs"].count()
    valid_years = sorted(year_counts[year_counts > 90].index.tolist())
    
    splits = []
    
    # Need at least 2 years to do training/testing
    if len(valid_years) < 2:
        return splits

    for test_year in valid_years:
        # Train on everything EXCEPT the test year
        train_years = [y for y in valid_years if y != test_year]
        
        train_mask = data["year"].isin(train_years)
        test_mask = data["year"] == test_year
        
        train_data = data[train_mask].copy()
        test_data = data[test_mask].copy()
        
        if len(train_data) > 0 and len(test_data) > 0:
            splits.append({
                "train": train_data,
                "test": test_data,
                "test_year": test_year,
                "n_train_years": len(train_years)
            })
            
    return splits

def as_xarray(data, value_col, datetime_col, name="Q"):
    """Convert DataFrame to xarray DataArray."""
    da = xr.DataArray(
        data[value_col].values,
        coords={"time": pd.to_datetime(data[datetime_col].values)},
        dims=["time"],
        attrs={"units": "m3/s"},
        name=name
    )
    return da

def calculate_metrics(obs, sim):
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

def process_station(station_id, obs_dir, sim_dir, output_dir, quantiles_list):
    """Process a single station using LOYO-CV."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # print(f"Processing station {station_id}...")
    
    data = read_station_data(station_id, obs_dir, sim_dir)
    if len(data) == 0:
        return station_id, "No Data"
    
    # LOOCV
    splits = loocv_splits(data)
    
    if not splits:
        return station_id, "Insufficient Data"
    
    results = []
    
    for split in splits:
        train_df = split["train"]
        test_df = split["test"]
        test_year = split["test_year"]
        
        # 1. Raw Metrics (Benchmark)
        raw_metrics = calculate_metrics(test_df["obs"].values, test_df["sim"].values)
        if raw_metrics:
            raw_metrics.update({
                "type": "Raw",
                "nq": "raw",
                "test_year": test_year,
                "n_train_years": split["n_train_years"]
            })
            results.append(raw_metrics)
        
        # Prepare Xarray objects
        train_obs_da = as_xarray(train_df, "obs", "date")
        train_sim_da = as_xarray(train_df, "sim", "date")
        test_sim_da = as_xarray(test_df, "sim", "date")
        
        # Preserve Seasonality
        # Group by day of year with a window (e.g., +/- 15 days) to capture seasonal drift
        group = Grouper("time.dayofyear", window=31)
        # OR
        # group = sdba.base.Grouper("time.month")

        for nq in quantiles_list:
            try:
                # DQM Training
                # kind="*" (multiplicative) preserves zero bound and handles heteroscedasticity
                dqm = DetrendedQuantileMapping.train(
                    train_obs_da,
                    train_sim_da,
                    nquantiles=nq,
                    group=group,
                    kind="*",
                )
                
                test_qmap_da = dqm.adjust(test_sim_da)
                test_qmap = test_qmap_da.values
                
                # Calculate Metrics
                dqm_metrics = calculate_metrics(test_df["obs"].values, test_qmap)
                if dqm_metrics:
                    dqm_metrics.update({
                        "type": "DQM",
                        "nq": nq,
                        "test_year": test_year,
                        "n_train_years": split["n_train_years"]
                    })
                    results.append(dqm_metrics)
            except Exception as e:
                # print(f"  Error station {station_id} year {test_year} nq={nq}: {e}")
                continue
    
    if results:
        results_df = pd.DataFrame(results)
        output_file = output_path / f"{station_id}.csv"
        results_df.to_csv(output_file, index=False)
        return station_id, len(results)
    else:
        return station_id, "No Results"


def cv_glofas(stations, 
              obs_dir="data/hydro/obs", 
              sim_dir="data/hydro/raw", 
              output_dir="data/cv", 
              quantiles_range=[1, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50],
              max_workers=None):
    """Perform LOOCV for GloFAS stations in parallel."""
    if max_workers is None:
        import os
        max_workers = min(len(stations), os.cpu_count() or 1)
    
    quantiles_list = list(quantiles_range)
    
    print(f"Starting LOOCV on {len(stations)} stations.")
    print(f"Quantiles to test: {quantiles_list}")
    
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
                print(f"Completed station {result[0]} (Rows: {result[1]})")
            except Exception as e:
                print(f"Station {station_id} generated an exception: {e}")


if __name__ == "__main__":
    try:
        # When run by Snakemake, snakemake object is available
        output_files = snakemake.output.cv_files
        stations = [int(Path(f).stem) for f in output_files]
        
        obs_dir = dirname(snakemake.input.obs_files[0])
        sim_dir = dirname(snakemake.input.raw_files[0])
        output_dir = dirname(snakemake.output.cv_files[0])
        
        cv_glofas(
            stations=stations,
            obs_dir=obs_dir,
            sim_dir=sim_dir,
            output_dir=output_dir
        )
    except NameError:
        # When run standalone
        stations = [1496, 1497, 1499, 1502, 1504, 1508, 1587]
        cv_glofas(stations)