# TODO:
# plot using tmap

# see scripts/extract_glofas.R

# Aggregate for visualisation purposes -----------------------------------
filtered_glofas <-
  glofas_ls[["1993"]] |>
  dplyr::filter(lubridate::month(valid_time) %in% c(5:10))

glofas_1993_mean <-
  stars::st_apply(
    filtered_glofas,
    c("longitude", "latitude"),
    mean,
    na.rm = TRUE
  ) |>
  terra::rast()

terra::writeRaster(
  glofas_1993_mean,
  "data/spatial/glofas_1993_meanMayOct.tiff",
  gdal = c(
    "COPY_SRC_OVERVIEWS=YES",
    "COMPRESS=LZW",
    "TILED=YES"
  )
)
