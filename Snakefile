# Variable declarations -------------------------------------------------------
CONTAINER = "container.sif"

# Inputs -----------------------------------------------------------
rule all:
    input:
        CONTAINER,
        "out/cyl.csv",
        "out/paths.txt",
        "out/plot.png"

# Build compute environment -----------------------------------------------------------
rule apptainer_build:
    input:  
        def_file = "container.def",
        lock_file = "renv.lock"
    output:
        CONTAINER
    shell:
        """
        apptainer build {output} {input.def_file}
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