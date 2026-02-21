# ruff: noqa: E402

import argparse
import sys
from pathlib import Path
from dotenv import load_dotenv

# Add src to python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

from glofas.download import download_glofas_incremental

load_dotenv()


def parse_args():
    parser = argparse.ArgumentParser(description="Download GloFAS NetCDF files")
    parser.add_argument(
        "--year",
        type=int,
        help="Single year to download (used by Snakemake wildcard jobs)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    years = [args.year] if args.year is not None else list(range(1979, 2026))
    output_dir = PROJECT_ROOT / "data" / "glofas"
    output_dir.mkdir(parents=True, exist_ok=True)

    download_glofas_incremental(years, output_dir)


if __name__ == "__main__":
    main()
