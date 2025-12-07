library(sf)
library(dplyr)
library(lubridate)

source("src/utils_glofas.R")
source("src/utils.R")

# Directory with streamflow data
Q_DIR <- "data/hydro/obs"

# Load gauging station locations -----------------------------------------
sites_ids <-
  fs::dir_ls(Q_DIR) |>
  fs::path_file() |>
  tools::file_path_sans_ext()

gauges <-
  sf::st_read(
    "data/geometry/anadyr_gauges.gpkg",
    query = "SELECT id, geom AS geometry FROM anadyr_gauges",
    quiet = TRUE
  ) |>
  dplyr::filter(id %in% sites_ids)

# Load GloFAS ------------------------------------------------------------
glofas_dir <-
  fs::dir_ls("data/glofas/", regexp = ".nc$") |>
  head()

# Read all GloFAS NetCDFs
glofas_ls <-
  lapply(
    glofas_dir,
    FUN = \(i) {
      suppressMessages(
        stars::read_ncdf(
          i,
          var = "dis24",
          make_units = FALSE
        )
      )
    }
  )

# Add names
names(glofas_ls) <-
  glofas_dir |>
  fs::path_file() |>
  tools::file_path_sans_ext()

# Extract water discharge at POI
glofas_df <-
  lapply(
    glofas_ls,
    extract_glofas,
    y = gauges
  ) |>
  collapse::unlist2d(idcols = FALSE) |>
  dplyr::arrange(datetime) |>
  dplyr::rename(q_raw = dis24) |>
  dplyr::as_tibble()

# Save as CSVs -----------------------------------------------------------
RAW_DIR <- "data/hydro/raw"
fs::dir_create(RAW_DIR)

glofas_list <-
  glofas_df |>
  split(glofas_df$id) |>
  lapply(dplyr::select, -id) |>
  lapply(dplyr::arrange, datetime) |>
  lapply(dplyr::mutate, datetime = as.character(datetime))

list_to_csv(glofas_list, RAW_DIR)
