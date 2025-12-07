library(stars)

extract_glofas <- function(x, y, tz = "Asia/Kamchatka") {
  stopifnot(inherits(x, "stars"))
  stopifnot(all(sf::st_is(y, c("POINT", "MULTIPOINT"))))

  geom_col <- attr(y, "sf_column")

  stars::st_extract(x, y) |>
    as.data.frame(long = TRUE) |>
    dplyr::mutate(
      valid_time = lubridate::with_tz(valid_time, tzone = tz),
      dis24 = as.numeric(dis24)
    ) |>
    dplyr::left_join(
      dplyr::as_tibble(y),
      by = dplyr::join_by(!!dplyr::sym(geom_col))
    ) |>
    dplyr::select(id, datetime = valid_time, dis24) |>
    dplyr::as_tibble()
}
