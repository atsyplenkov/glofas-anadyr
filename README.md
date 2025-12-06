# Reconstructing daily streamflow data for Anadyr River using GloFAS-ERA5 reanalysis

<p align="center">
     <a href="https://github.com/atsyplenkov/glofas-anadyr/.github/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/atsyplenkov/glofas-anadyr/ci.yml?style=flat&labelColor=1C2C2E&color=039475&logo=GitHub%20Actions&logoColor=white&label=CI"></a>
</p>

### 🟪 summary
TBA

### 🟩 project structure
The `Snakefile` is the backbone of the workflow. It defines the order of the steps and the dependencies between them. In the current implementation it uses the built-in [R integration of Snakemake](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html#r-and-r-markdown). That is, user can specify variables in the `Snakefile` and use them in the R scripts via `snakemake@input` and `snakemake@output` objects (see `scripts/test_script.R` for an example).

```text
.
├── container.def   # Singularity definition file
├── container.sif   # Singularity image file
├── data              # Data directory
├── renv            # renv directory
│   ├── activate.R
│   ├── library
│   ├── settings.json
│   └── staging
├── renv.lock       # renv file with R deps
├── pyproject.toml  # Python project desc
├── uv.lock         # uv file with Python deps
├── scripts         # Scripts directory, both R and Py
└── Snakefile       # Snakemake workflow file

```
### 🔷 how to use reproduce
1. Clone the repository:
```shell
git clone https://github.com/atsyplenkov/glofas-anadyr
cd glofas-anadyr
```

2. Install `miniforge3` and `apptainer` using default params as described in their docs. Then install `snakemake`. Any `snakemake` version will do, but the current template has been tested under `9.14.1`:
```shell
conda create -c conda-forge -c bioconda -n snakemake snakemake=9.10.1
```

3. Activate `snakemake` by running:
```shell
conda activate snakemake
```

4. Run the workflow with the following command:
```shell
snakemake --use-singularity --cores 1
```
If you want to run workflow in parallel, you can use the following command:
```shell
snakemake --use-singularity --cores 2
```
It will make `snakemake` to run jobs simultaneously.
