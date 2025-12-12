library(dplyr)
library(tidyr)
library(lubridate)

source("src/utils_ggplot.R")
source("src/utils.R")
theme_set(theme_mw())

# Read data -----------------------------------------------------------
obs_data <-
  fs::dir_ls("data/hydro/obs", regexp = ".csv$") |>
  purrr::map_dfr(
    ~ readr::read_csv(.x, show_col_types = FALSE),
    .id = "gauge_id"
  ) |>
  mutate(
    gauge_id = stringr::str_remove_all(gauge_id, "data/hydro/obs/|.csv"),
    date = lubridate::as_date(date),
    q = q_cms,
    type = "obs",
    .keep = "unused"
  ) |>
  filter(!is.na(q))

cor_data <-
  fs::dir_ls("data/hydro/cor", regexp = ".csv$") |>
  purrr::map_dfr(
    ~ readr::read_csv(.x, show_col_types = FALSE),
    .id = "gauge_id"
  ) |>
  mutate(
    gauge_id = stringr::str_remove_all(gauge_id, "data/hydro/cor/|.csv"),
    date = lubridate::as_date(date) - 1,
    q = q_cor,
    type = "cor",
    .keep = "unused"
  )

raw_data <-
  fs::dir_ls("data/hydro/raw", regexp = ".csv$") |>
  purrr::map_dfr(
    ~ readr::read_csv(.x, show_col_types = FALSE),
    .id = "gauge_id"
  ) |>
  mutate(
    gauge_id = stringr::str_remove_all(gauge_id, "data/hydro/raw/|.csv"),
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
    # legend.direction = "horizontal",
    legend.position.inside = c(0.35, 0.15)
  )

save_png(
  "figures/fig06_annual-trends.png",
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
  mutate(period = glue::glue("{min(year)}–{max(year)}")) |>
  group_by(gauge_id, period, type) |>
  nest() |>
  mutate(trend = map_dfr(data, ~ mw_kendall(.x$year, .x$q_mean))) |>
  unnest(trend) |>
  select(-data) |>
  ungroup()

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
    "tables/tbl3_trends-results.csv",
    quote = FALSE,
    na = "",
    row.names = FALSE
  )


