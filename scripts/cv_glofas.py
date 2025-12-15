import numpy as np
import pandas as pd
import xarray as xr
from pathlib import Path
from os.path import dirname
from xsdba import DetrendedQuantileMapping, Grouper
from concurrent.futures import ProcessPoolExecutor, as_completed
import warnings
import sys

# Add src to python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

from glofas.io import read_station_data_cv
from glofas.process import loocv_splits, as_xarray, calculate_metrics
from glofas.config import GAUGE_IDS

# Suppress divide by zero warnings in logs
warnings.filterwarnings("ignore", category=RuntimeWarning)

def process_station(station_id, obs_dir, sim_dir, output_dir, quantiles_list):
    """Process a single station using LOYO-CV."""
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # print(f"Processing station {station_id}...")
    
    data = read_station_data_cv(station_id, Path(obs_dir), Path(sim_dir))
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
        stations = GAUGE_IDS
        cv_glofas(stations)
