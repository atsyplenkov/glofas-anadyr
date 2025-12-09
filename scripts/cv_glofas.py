import hydroeval as he
import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from xsdba import DetrendedQuantileMapping


def read_station_data(station_id, obs_dir, sim_dir):
    """Read observed and simulated data for a station."""
    obs_path = Path(obs_dir) / f"{station_id}.csv"
    sim_path = Path(sim_dir) / f"{station_id}.csv"
    
    obs_df = pd.read_csv(obs_path, parse_dates=["date"])
    sim_df = pd.read_csv(sim_path, parse_dates=["datetime"])
    
    sim_df["date"] = pd.to_datetime(sim_df["datetime"]).dt.date
    sim_df["date"] = pd.to_datetime(sim_df["date"]) - pd.Timedelta(days=1)
    
    merged = obs_df.merge(sim_df, on="date", how="inner")
    merged = merged[["date", "q_cms", "q_raw"]].copy()
    merged = merged.rename(columns={"q_cms": "obs", "q_raw": "sim"})
    merged = merged.dropna()
    
    merged["year"] = merged["date"].dt.year
    merged["month"] = merged["date"].dt.month
    
    merged = merged[
        (merged["year"] >= 1979) & 
        (merged["year"] <= 1996) &
        (merged["month"].between(5, 9))
    ].copy()
    
    merged = merged[["date", "obs", "sim"]].copy()
    merged["obs"] = merged["obs"] + 0.01
    merged["sim"] = merged["sim"] + 0.01
    
    return merged


def sliding_window_splits(data, lookback=6, assess_stop=2, step=1):
    """Generate sliding window splits for cross-validation."""
    years = sorted(data["year"].unique())
    years = [y for y in years if y != 1979]
    
    splits = []
    i = 0
    while i + lookback + assess_stop <= len(years):
        train_years = years[i:i + lookback]
        test_years = years[i + lookback:i + lookback + assess_stop]
        
        train_mask = data["year"].isin(train_years)
        test_mask = data["year"].isin(test_years)
        
        train_data = data[train_mask].copy()
        test_data = data[test_mask].copy()
        
        if len(train_data) > 0 and len(test_data) > 0:
            splits.append({
                "train": train_data,
                "test": test_data,
                "train_start": train_data["date"].min(),
                "train_end": train_data["date"].max(),
                "test_start": test_data["date"].min(),
                "test_end": test_data["date"].max(),
            })
        
        i += step
    
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
    obs = np.array(obs)
    sim = np.array(sim)
    
    mask = (obs > 0) & (sim > 0)
    obs = obs[mask]
    sim = sim[mask]
    
    if len(obs) == 0:
        return None
    
    nse = he.evaluator(he.nse, sim, obs)
    kge, r, alpha, beta = he.evaluator(he.kgeprime, sim, obs)
    kgenp, r, alpha, beta = he.evaluator(he.kgenp, sim, obs)
    pbias_val = he.evaluator(he.pbias, sim, obs)
    rmse_val = he.evaluator(he.rmse, sim, obs)
    
    return {
        "nse": nse,
        "kgeprime": kge,
        "kgenp": kgenp,
        "pbias": pbias_val,
        "rmse": rmse_val,
    }


def cv_glofas(stations, obs_dir="data/hydro/obs", sim_dir="data/hydro/raw", 
               output_dir="data/cv", quantiles_range=range(5, 111, 25)):
    """Perform cross-validation for GloFAS stations."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    for station_id in stations:
        print(f"Processing station {station_id}...")
        
        data = read_station_data(station_id, obs_dir, sim_dir)
        if len(data) == 0:
            print(f"  No data for station {station_id}, skipping...")
            continue
        
        data["year"] = data["date"].dt.year
        splits = sliding_window_splits(data)
        
        results = []
        
        for split in splits:
            train_df = split["train"]
            test_df = split["test"]
            
            train_obs = train_df["obs"].values
            train_sim = train_df["sim"].values
            test_obs = test_df["obs"].values
            test_sim = test_df["sim"].values
            
            raw_metrics = calculate_metrics(test_obs, test_sim)
            if raw_metrics:
                raw_metrics.update({
                    "type": "Raw",
                    "N_quantiles": "raw",
                    "train_start": split["train_start"],
                    "train_end": split["train_end"],
                    "test_start": split["test_start"],
                    "test_end": split["test_end"],
                })
                results.append(raw_metrics)
            
            for nq in quantiles_range:
                try:
                    train_obs_da = as_xarray(train_df, "obs", "date", "obs")
                    train_sim_da = as_xarray(train_df, "sim", "date", "sim")
                    test_sim_da = as_xarray(test_df, "sim", "date", "sim")
                    
                    dqm = DetrendedQuantileMapping.train(
                        train_obs_da,
                        train_sim_da,
                        nquantiles=nq,
                        group="time.dayofyear",
                        kind="+",
                    )
                    
                    test_qmap_da = dqm.adjust(test_sim_da)
                    test_qmap = test_qmap_da.values
                    
                    mask = (test_qmap > 0) & (test_obs > 0) & (test_sim > 0)
                    test_qmap_filtered = test_qmap[mask]
                    test_obs_filtered = test_obs[mask]
                    
                    if len(test_qmap_filtered) > 0:
                        dqm_metrics = calculate_metrics(test_obs_filtered, test_qmap_filtered)
                        if dqm_metrics:
                            dqm_metrics.update({
                                "type": "DQM",
                                "N_quantiles": nq,
                                "train_start": split["train_start"],
                                "train_end": split["train_end"],
                                "test_start": split["test_start"],
                                "test_end": split["test_end"],
                            })
                            results.append(dqm_metrics)
                except Exception as e:
                    print(f"  Error processing n_quantiles={nq}: {e}")
                    continue
        
        if results:
            results_df = pd.DataFrame(results)
            output_file = output_path / f"{station_id}.csv"
            results_df.to_csv(output_file, index=False)
            print(f"  Saved results to {output_file}")
        else:
            print(f"  No results for station {station_id}")


if __name__ == "__main__":
    stations = [1496, 1497, 1499, 1502, 1504, 1508, 1587]
    cv_glofas(stations)
