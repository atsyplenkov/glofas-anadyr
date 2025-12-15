# Variable declarations -------------------------------------------------------
CONTAINER = "container.sif"

# Get all GLOFAS files dynamically
YEARS = list(range(1979, 2026))
GLOFAS_FILES = [f"data/glofas/{year}.nc" for year in YEARS]

# Station IDs
STATIONS = [1496, 1497, 1499, 1502, 1504, 1508, 1587]
OBS_FILES = [f"data/hydro/obs/{station}.csv" for station in STATIONS]
RAW_FILES = [f"data/hydro/raw/{station}.csv" for station in STATIONS]
COR_FILES = [f"data/hydro/cor/{station}.csv" for station in STATIONS]
CV_FILES = [f"data/cv/{station}.csv" for station in STATIONS]

# Final outputs
FIGURES = [
    "figures/fig02_lamutskoe1986.png",
    "figures/fig03_missing-data.png",
    "figures/fig04_loocv.png",
    "figures/fig05_correction_scatter.png",
    "figures/fig06_annual-trends.png",
    "figures/fig07_anadyr_adcp_glofas.png"
]
TABLES = [
    "tables/tbl2_cv-results.csv",
    "tables/tbl3_trends-results.csv"
]

# Inputs -----------------------------------------------------------
rule all:
    input:
        CONTAINER,
        FIGURES,
        TABLES

# Build compute environment -----------------------------------------------------------
rule apptainer_build:
    input:  
        def_file = "container.def",
        lock_file = "renv.lock",
        pyproject = "pyproject.toml",
        uv_lock = "uv.lock",
        env_file = ".env"
    output:
        CONTAINER
    shell:
        """
        apptainer build {output} {input.def_file}
        """

# Download GLOFAS data ------------------------------------------------
rule download_glofas:
    input:
        container = CONTAINER,
        script = "scripts/download_glofas.py"
    singularity:
        CONTAINER
    output:
        GLOFAS_FILES
    shell:
        """
        python {input.script}
        """

# Read water discharge data ----------------------------------------------------
rule read_discharge:
    input:  
        file = "data/raw/Анадырь-расходы1223.xlsx",
        container = CONTAINER
    singularity:
        CONTAINER
    output:
        obs_files = OBS_FILES,
        figure = "figures/fig03_missing-data.png"
    script:
        "scripts/read_discharge.R"

# Extract GLOFAS data at gauge locations --------------------------------------
rule extract_glofas:
    input:
        obs_files = OBS_FILES,
        glofas_files = GLOFAS_FILES,
        geometry = "data/geometry/anadyr_gauges.gpkg",
        container = CONTAINER
    singularity:
        CONTAINER
    output:
        raw_files = RAW_FILES
    script:
        "scripts/extract_glofas.py"

# Cross-validation -------------------------------------------------------------
rule cv_glofas:
    input:
        obs_files = OBS_FILES,
        raw_files = RAW_FILES,
        container = CONTAINER
    singularity:
        CONTAINER
    output:
        cv_files = CV_FILES
    script:
        "scripts/cv_glofas.py"

# CV results analysis ----------------------------------------------------------
rule cv_results:
    input:
        cv_files = CV_FILES,
        container = CONTAINER
    singularity:
        CONTAINER
    output:
        table = "tables/tbl2_cv-results.csv",
        figure = "figures/fig04_loocv.png"
    script:
        "scripts/cv_results.R"

# Apply bias correction --------------------------------------------------------
rule glofas_correction:
    input:
        obs_files = OBS_FILES,
        raw_files = RAW_FILES,
        container = CONTAINER
    singularity:
        CONTAINER
    output:
        cor_files = COR_FILES
    script:
        "scripts/glofas_correction.py"

# Correction validation plots --------------------------------------------------
rule correction_check:
    input:
        obs_files = OBS_FILES,
        cor_files = COR_FILES,
        raw_files = RAW_FILES,
        container = CONTAINER
    singularity:
        CONTAINER
    output:
        scatter = "figures/fig05_correction_scatter.png",
        adcp = "figures/fig07_anadyr_adcp_glofas.png"
    script:
        "scripts/correction_check.R"

# Time series plot -------------------------------------------------------------
rule ts_plot:
    input:
        obs_files = OBS_FILES,
        cor_files = COR_FILES,
        raw_files = RAW_FILES,
        container = CONTAINER
    singularity:
        CONTAINER
    output:
        figure = "figures/fig02_lamutskoe1986.png"
    script:
        "scripts/ts_plot.R"

# Trend analysis ---------------------------------------------------------------
rule trend_analysis:
    input:
        obs_files = OBS_FILES,
        cor_files = COR_FILES,
        raw_files = RAW_FILES,
        container = CONTAINER
    singularity:
        CONTAINER
    output:
        figure = "figures/fig06_annual-trends.png",
        table = "tables/tbl3_trends-results.csv"
    script:
        "scripts/trend_analysis.R"