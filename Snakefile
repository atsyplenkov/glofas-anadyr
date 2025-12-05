# Variable declarations -------------------------------------------------------
CONTAINER = "container.sif"

# Inputs -----------------------------------------------------------
rule all:
    input:
        CONTAINER,
        "data/glofas/2020.nc",
        "data/glofas/2021.nc",
        "data/glofas/2022.nc",
        "data/glofas/2023.nc",
        "data/glofas/2024.nc",
        "out/cyl.csv",
        "out/paths.txt",
        "out/plot.png"

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

# Download GLOFAS data -----------------------------------------------------------
rule download_glofas:
    input:
        container = CONTAINER,
        script = "scripts/download_glofas.py"
    singularity:
        CONTAINER
    output:
        "data/glofas/2020.nc",
        "data/glofas/2021.nc",
        "data/glofas/2022.nc",
        "data/glofas/2023.nc",
        "data/glofas/2024.nc"
    shell:
        """
        python {input.script}
        """

# Run scripts -----------------------------------------------------------
rule run_test_script:
    input:  
        file = "in/mtcars.csv",
        container = CONTAINER
    singularity:
        CONTAINER
    output:
        "out/cyl.csv",
        "out/paths.txt"
    script:
        "scripts/test_script.R"

rule run_test_script2:
    input:  
        CONTAINER
    singularity:
        CONTAINER
    output:
        "out/plot.png"
    script:
        "scripts/test_script2.R"