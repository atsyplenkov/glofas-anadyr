# FIXME:
# The whole file is incomplete

# Variable declarations -------------------------------------------------------
CONTAINER = "container.sif"

# Get all GLOFAS files dynamically
import glob
YEARS = 1979:2025
GLOFAS_FILES = [f"data/glofas/{year}.nc" for year in YEARS]

# Inputs -----------------------------------------------------------
rule all:
    input:
        CONTAINER

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
rule read_streamflow:
    input:  
        file = "data/raw/Анадырь-расходы1223.xlsx",
        container = CONTAINER
    singularity:
        CONTAINER
    output:
        Q_DIR
    script:
        "scripts/read_discharge.R"