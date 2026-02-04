import pandas as pd
from pathlib import Path
from .config import DATA_DIR


def load_gauge_data_all(
    gauge_id: int, data_dir: Path = DATA_DIR / "hydro"
) -> pd.DataFrame:
    """Load and merge obs, raw, cor data for a gauge (all years)."""
    obs_path = data_dir / "obs" / f"{gauge_id}.csv"
    raw_path = data_dir / "raw" / f"{gauge_id}.csv"
    cor_path = data_dir / "cor" / f"{gauge_id}.csv"

    obs = (
        pd.read_csv(obs_path, parse_dates=["date"])
        if obs_path.exists()
        else pd.DataFrame(columns=["date", "q_cms"])
    )
    if "q_cms" in obs.columns:
        obs = obs.rename(columns={"q_cms": "q_obs"})

    raw = (
        pd.read_csv(raw_path, parse_dates=["datetime"])
        if raw_path.exists()
        else pd.DataFrame(columns=["datetime", "q_raw"])
    )
    if not raw.empty:
        raw["date"] = raw["datetime"].dt.normalize()
        # Group by date if multiple entries per day?
        # Usually one per day or hourly. If hourly, we need mean.
        # Assuming existing logic:
        raw = raw.groupby("date")["q_raw"].mean().reset_index()
    else:
        raw = pd.DataFrame(columns=["date", "q_raw"])

    cor = (
        pd.read_csv(cor_path, parse_dates=["date"])
        if cor_path.exists()
        else pd.DataFrame(columns=["date", "q_cor"])
    )

    df = obs.merge(raw, on="date", how="outer").merge(cor, on="date", how="outer")
    df = df.sort_values("date")

    # Ensure columns exist
    if "q_obs" not in df.columns:
        df["q_obs"] = None
    if "q_raw" not in df.columns:
        df["q_raw"] = None
    if "q_cor" not in df.columns:
        df["q_cor"] = None

    df["gauge_id"] = gauge_id
    return df[["date", "gauge_id", "q_obs", "q_raw", "q_cor"]]


def read_station_data_cv(station_id: int, obs_dir: Path, sim_dir: Path) -> pd.DataFrame:
    """Read observed and simulated data for a station (filtered for CV)."""
    obs_path = obs_dir / f"{station_id}.csv"
    sim_path = sim_dir / f"{station_id}.csv"

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
        (merged["year"] >= 1979)
        & (merged["year"] <= 1996)
        & (merged["month"].between(5, 10))
    ].copy()

    # Add epsilon to handle zero flows in multiplicative correction and logs
    epsilon = 0.01
    merged["obs"] = merged["obs"] + epsilon
    merged["sim"] = merged["sim"] + epsilon

    return merged


def load_cv_results(cv_results_path: Path) -> dict:
    """Load cross-validation results from CSV."""
    from .process import (
        extract_kge,
    )  # Avoiding circular import if possible, but extracting logic is better

    if not cv_results_path.exists():
        return {}

    df = pd.read_csv(cv_results_path)
    results = {}
    for gauge_id in df["gauge_id"].unique():
        gauge_df = df[df["gauge_id"] == gauge_id]
        raw_row = gauge_df[gauge_df["type"] == "Raw"]
        dqm_row = gauge_df[gauge_df["type"] == "DQM"]
        if len(raw_row) > 0 and len(dqm_row) > 0:
            results[gauge_id] = {
                "kge_raw": extract_kge(raw_row["KGE'"].values[0]),
                "kge_dqm": extract_kge(dqm_row["KGE'"].values[0]),
            }
    return results
