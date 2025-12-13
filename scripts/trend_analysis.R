library(dplyr)
library(tidyr)
library(lubridate)

source("src/utils_ggplot.R")
source("src/utils.R")
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
    date = lubridate::as_date(date) - 1,
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

m_data <-
  all_data |>
  filter(between(month, 5, 10)) |>
  group_by(gauge_id, type, year) |>
  reframe(q_mean = mean(q), q_max = max(q)) |>
  mutate(q_mean = ifelse(q_mean < 10, NA_real_, q_mean))

annual_trends <-
  m_data |>
  ggplot(aes(x = year, y = q_mean, color = type)) +
  # Optional: add Mann-Kendall trends
  # geom_abline(
  #   data = trends,
  #   aes(slope = slope, intercept = intercept, color = type)
  # ) +
  geom_smooth(se = FALSE, method = "lm") +
  geom_line() +
  scale_color_manual(
    name = "Streamflow",
    values = c(mw_black, mw_red, mw_blue),
    labels = c("Observed", "Raw GloFAS-ERA5", "Bias-corrected GloFAS-ERA5")
  ) +
  labs(
    x = "",
    y = "Mean annual water discharge, m³/s"
  ) +
  facet_wrap(~gauge_id, scales = "free") +
  theme(
    legend.position = "inside",
    legend.position.inside = c(0.35, 0.15)
  )

save_png(
  snakemake@output[["figure"]],
  annual_trends,
  dpi = 300,
  w = 18,
  h = 12
)

# Estimate trends -----------------------------------------------------------
library(purrr)

trends <-
  m_data |>
  group_by(gauge_id, type) |>
  mutate(period = glue::glue("{min(year)}-{max(year)}")) |>
  group_by(gauge_id, period, type) |>
  nest() |>
  mutate(trend = map_dfr(data, ~ mw_kendall(.x$year, .x$q_mean))) |>
  unnest(trend) |>
  select(-data) |>
  ungroup()

# Write down Table 3
trends |>
  select(-slope:-tau) |>
  relocate(gauge_id:period, pct, p) |>
  mutate(
    type = forcats::fct_recode(
      type,
      "Observed" = "obs",
      "Raw GloFAS-ERA5" = "raw",
      "Bias-corrected GloFAS-ERA5" = "cor"
    )
  ) |>
  mutate(across(where(is.numeric), ~ mw_round(.x))) |>
  write.csv(
    snakemake@output[["table"]],
    quote = FALSE,
    na = "",
    row.names = FALSE
  )
