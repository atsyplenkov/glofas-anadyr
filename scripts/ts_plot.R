library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)

source("src/utils_ggplot.R")
theme_set(theme_mw())

# Read data -----------------------------------------------------------
obs_data <-
  snakemake@input[["obs_files"]] |>
  purrr::map_dfr(
    ~ readr::read_csv(.x, show_col_types = FALSE),
    .id = "gauge_id"
  ) |>
  mutate(
    gauge_id = as.character(factor(
      gauge_id,
      levels = 1:7,
      labels = snakemake@input[["obs_files"]]
    ))
  ) |>
  mutate(
    gauge_id = tools::file_path_sans_ext(basename(gauge_id)),
    date = lubridate::as_date(date),
    q = q_cms,
    type = "obs",
    .keep = "unused"
  ) |>
  filter(!is.na(q))

cor_data <-
  snakemake@input[["cor_files"]] |>
  purrr::map_dfr(
    ~ readr::read_csv(.x, show_col_types = FALSE),
    .id = "gauge_id"
  ) |>
  mutate(
    gauge_id = as.character(factor(
      gauge_id,
      levels = 1:7,
      labels = snakemake@input[["cor_files"]]
    ))
  ) |>
  mutate(
    gauge_id = tools::file_path_sans_ext(basename(gauge_id)),
    date = lubridate::as_date(date),
    q = q_cor,
    type = "cor",
    .keep = "unused"
  )

raw_data <-
  snakemake@input[["raw_files"]] |>
  purrr::map_dfr(
    ~ readr::read_csv(.x, show_col_types = FALSE),
    .id = "gauge_id"
  ) |>
  mutate(
    gauge_id = as.character(factor(
      gauge_id,
      levels = 1:7,
      labels = snakemake@input[["raw_files"]]
    ))
  ) |>
  mutate(
    gauge_id = tools::file_path_sans_ext(basename(gauge_id)),
    date = lubridate::as_date(datetime) - 1,
    q = q_raw,
    type = "raw",
    .keep = "unused"
  )

# Merge data -----------------------------------------------------------
all_data <-
  bind_rows(obs_data, cor_data, raw_data) |>
  mutate(year = year(date), month = month(date)) |>
  mutate(type = factor(type, levels = c("obs", "raw", "cor")))

ts_plots <-
  all_data |>
  filter(
    year == 1986,
    gauge_id == 1496,
    between(month, 5, 11)
  ) |>
  ggplot() +
  geom_line(
    aes(x = date, y = q, color = type),
    lwd = 0.6
  ) +
  scale_color_manual(
    name = "Streamflow",
    values = c(mw_black, mw_red, mw_blue),
    labels = c("Observed", "Raw GloFAS-ERA5", "Bias-corrected GloFAS-ERA5")
  ) +
  scale_x_date(
    date_breaks = "1 month",
    date_labels = "%b %Y"
  ) +
  scale_y_continuous(
    expand = expansion(add = c(5, 50))
  ) +
  labs(
    x = "",
    y = "Water discharge, m³/s"
  ) +
  # facet_wrap(~gauge_id) +
  theme(
    legend.position = "inside",
    legend.position.inside = c(0.7, 0.8)
  )

save_png(
  snakemake@output[["figure"]],
  ts_plots,
  dpi = 300
)
