import sys
from pathlib import Path
from dotenv import load_dotenv

# Add src to python path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT / "src"))

from glofas.download import download_glofas_incremental

load_dotenv()

def main():
    years = list(range(1979, 2026))
    output_dir = PROJECT_ROOT / "data" / "glofas"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    download_glofas_incremental(years, output_dir)

if __name__ == "__main__":
    main()
