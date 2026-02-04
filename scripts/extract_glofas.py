import sys
from pathlib import Path
from multiprocessing import Pool, cpu_count

import geopandas as gpd
import pandas as pd

# Add src to python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

from glofas.process import extract_glofas_at_gauges


def process_single_file(args):
    """Worker function to process a single NetCDF file."""
    nc_file, gauges_gdf = args
    try:
        df = extract_glofas_at_gauges(nc_file, gauges_gdf)
        if df.empty:
            return None
        return df
    except Exception as e:
        print(f"Error processing {nc_file}: {e}")
        return None


def main():
    try:
        # Snakemake inputs/outputs
        obs_files = snakemake.input.obs_files
        geometry_path = snakemake.input.geometry
        glofas_files = snakemake.input.glofas_files
        raw_files = snakemake.output.raw_files

        # Determine gauge IDs from obs filenames
        site_ids = [int(Path(f).stem) for f in obs_files]

        # Load gauges
        gauges = gpd.read_file(geometry_path)
        gauges = gauges[gauges["id"].isin(site_ids)]

        # Parallel processing of GloFAS files
        # Using 12 cores as in original R script, or available CPU count
        n_cores = min(12, cpu_count())

        print(f"Processing {len(glofas_files)} GloFAS files using {n_cores} cores...")

        with Pool(n_cores) as pool:
            # Create args list for starmap/map
            args = [(Path(f), gauges) for f in glofas_files]
            results = pool.map(process_single_file, args)

        # Combine results
        valid_results = [df for df in results if df is not None and not df.empty]
        if not valid_results:
            print("No data extracted!")
            return

        all_data = pd.concat(valid_results, ignore_index=True)
        all_data = all_data.sort_values("datetime")

        # Rename columns to match R output if needed (datetime, q_raw)
        # extract_glofas_at_gauges returns: gauge_id, datetime, q_raw

        # Output directory
        # raw_files is a list of expected output files, e.g. ["data/hydro/raw/1496.csv", ...]
        # We need to map site_id to output path

        output_map = {int(Path(f).stem): Path(f) for f in raw_files}

        for site_id, group in all_data.groupby("gauge_id"):
            if site_id in output_map:
                out_path = output_map[site_id]
                out_path.parent.mkdir(parents=True, exist_ok=True)

                # Format for CSV
                df_out = group[["datetime", "q_raw"]].copy()
                df_out = df_out.sort_values("datetime")

                # Write CSV
                df_out.to_csv(out_path, index=False)
                print(f"Saved {out_path}")

    except NameError:
        print("This script is intended to be run via Snakemake.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
