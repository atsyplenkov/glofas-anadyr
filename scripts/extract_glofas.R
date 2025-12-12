library(sf)
library(dplyr)
library(lubridate)
library(pbmcapply)

source("src/utils_glofas.R")
source("src/utils.R")

# Directory with streamflow data
Q_DIR <- dirname(snakemake@input[["obs_files"]][1])

# Load gauging station locations -----------------------------------------
sites_ids <-
  snakemake@input[["obs_files"]] |>
  fs::path_file() |>
  tools::file_path_sans_ext() |>
  as.integer()

gauges <-
  sf::st_read(
    snakemake@input[["geometry"]],
    query = "SELECT id, geom AS geometry FROM anadyr_gauges",
    quiet = TRUE
  ) |>
  dplyr::filter(id %in% sites_ids)

# Load GloFAS ------------------------------------------------------------
glofas_dir <- snakemake@input[["glofas_files"]]

# Read all GloFAS NetCDFs
glofas_ls <-
  pbmcapply::pbmclapply(
    glofas_dir,
    FUN = \(i) {
      suppressMessages(
        stars::read_ncdf(
          i,
          var = "dis24",
          make_units = FALSE
        )
      )
    },
    mc.cores = 12L
  )

# Add names
names(glofas_ls) <-
  glofas_dir |>
  fs::path_file() |>
  tools::file_path_sans_ext()

# Extract water discharge at POI
glofas_df <-
  pbmcapply::pbmclapply(
    glofas_ls,
    extract_glofas,
    y = gauges,
    mc.cores = 12L
  ) |>
  collapse::unlist2d(idcols = FALSE) |>
  dplyr::arrange(datetime) |>
  dplyr::rename(q_raw = dis24) |>
  dplyr::as_tibble()

# Save as CSVs -----------------------------------------------------------
RAW_DIR <- dirname(snakemake@output[["raw_files"]][1])
fs::dir_create(RAW_DIR)

glofas_list <-
  glofas_df |>
  split(glofas_df$id) |>
  lapply(dplyr::select, -id) |>
  lapply(dplyr::arrange, datetime) |>
  lapply(dplyr::mutate, datetime = as.character(datetime))

list_to_csv(glofas_list, RAW_DIR)
