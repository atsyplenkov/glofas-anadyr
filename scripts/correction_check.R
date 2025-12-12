library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)
library(tidyhydro)
library(ggrastr)
library(patchwork)

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
    obs = q_cms,
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
    cor = q_cor,
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
    raw = q_raw,
    .keep = "unused"
  )

# Merge data -----------------------------------------------------------
comparison_data <-
  obs_data |>
  inner_join(
    cor_data,
    by = c("gauge_id", "date")
  ) |>
  left_join(
    raw_data,
    by = c("gauge_id", "date")
  ) |>
  filter(!is.na(obs), !is.na(cor)) |>
  mutate(
    gauge_id = as.integer(gauge_id),
    year = lubridate::year(date),
    month = lubridate::month(date)
  )

# Calculate metrics -----------------------------------------------------------
# Metrics of the corrected GloFAS streamflow for the
# whole period 1979-1996
calculate_metrics <- function(obs, sim) {
  tibble(
    nse = tidyhydro::nse_vec(obs, sim),
    kge = tidyhydro::kge2012_vec(obs, sim),
    rmse = tidyhydro::rmse_vec(obs, sim),
    pbias = tidyhydro::pbias_vec(obs, sim)
  )
}

metrics <-
  comparison_data |>
  group_by(gauge_id, year) |>
  summarize(
    calculate_metrics(obs, cor),
    .groups = "drop"
  )

# Scatter plots -----------------------------------------------------------
scatter_data <-
  comparison_data |>
  filter(
    year >= 1979,
    year <= 1996,
    month %in% 5:10,
    obs > 0,
    cor > 0,
    !(gauge_id == 1504 & obs > 6000)
  )

facet_limits <-
  scatter_data |>
  summarize(
    min_val = 0,
    max_val = max(c(obs, cor), na.rm = TRUE),
    .by = gauge_id
  ) |>
  mutate(
    range = max_val - min_val,
    padding = range * 0.05,
    lim_min = min_val - padding,
    lim_max = max_val + padding
  )

gauge_ids <- sort(unique(scatter_data$gauge_id))

scatter_plots <-
  gauge_ids |>
  purrr::map(\(gauge_id_val) {
    x <- scatter_data[scatter_data$gauge_id == gauge_id_val, ]
    lims <- facet_limits[facet_limits$gauge_id == gauge_id_val, ]

    x |>
      ggplot() +
      geom_abline(
        intercept = 0,
        slope = 1,
        color = "gray60",
        linetype = "dashed",
        linewidth = 0.3
      ) +
      rasterise(
        geom_point(
          aes(x = obs, y = cor, color = "DQM"),
          alpha = 0.4,
          size = 0.5,
        ),
        dpi = 300
      ) +
      rasterise(
        geom_point(
          aes(x = obs, y = raw, color = "Raw"),
          alpha = 0.4,
          size = 0.5,
        ),
        dpi = 300
      ) +
      scale_color_manual(
        name = "GloFAS-ERA5",
        values = c(mw_red, mw_blue),
        labels = c("Raw", "Bias-corrected")
      ) +
      guides(
        color = guide_legend(override.aes = list(alpha = 1, size = 2))
      ) +
      scale_x_continuous(
        limits = c(lims$lim_min, lims$lim_max),
        expand = expansion(mult = c(0, 0.01))
      ) +
      scale_y_continuous(
        limits = c(lims$lim_min, lims$lim_max),
        expand = expansion(mult = c(0, 0.01))
      ) +
      labs(
        x = "Observed Q, m³/s",
        y = "Predicted Q, m³/s"
      ) +
      facet_wrap(~gauge_id) +
      coord_fixed(ratio = 1, expand = FALSE) +
      theme(
        plot.title = ggtext::element_markdown(hjust = 0.5, size = 10),
        plot.margin = margin(-5, -5, -5, -5, unit = "pt")
      )
  }) |>
  append(list(guide_area())) |>
  wrap_plots(ncol = 4, guides = "collect") &
  theme(legend.position = "bottom", legend.direction = "vertical")

save_png(
  "figures/fig05_correction_scatter.png",
  scatter_plots,
  w = 20,
  h = 11,
  dpi = 300
)

# # Monthly comparison -----------------------------------------------------------
# monthly_data <-
#   comparison_data |>
#   filter(
#     year >= 1979,
#     year <= 1996,
#     month %in% 5:10
#   ) |>
#   group_by(gauge_id, month) |>
#   summarize(
#     obs_mean = mean(obs, na.rm = TRUE),
#     cor_mean = mean(cor, na.rm = TRUE),
#     obs_sd = sd(obs, na.rm = TRUE),
#     cor_sd = sd(cor, na.rm = TRUE),
#     .groups = "drop"
#   ) |>
#   mutate(
#     month_name = factor(
#       month.abb[month],
#       levels = month.abb[5:10]
#     )
#   )

# monthly_plots <-
#   monthly_data |>
#   ggplot() +
#   geom_col(
#     aes(x = month_name, y = obs_mean),
#     fill = mw_black,
#     alpha = 0.5,
#     position = position_dodge(width = 0.7)
#   ) +
#   geom_col(
#     aes(x = month_name, y = cor_mean),
#     fill = mw_blue,
#     alpha = 0.5,
#     position = position_dodge(width = 0.7)
#   ) +
#   geom_errorbar(
#     aes(
#       x = month_name,
#       ymin = obs_mean - obs_sd,
#       ymax = obs_mean + obs_sd
#     ),
#     width = 0.2,
#     color = mw_black,
#     linewidth = 0.3
#   ) +
#   geom_errorbar(
#     aes(
#       x = month_name,
#       ymin = cor_mean - cor_sd,
#       ymax = cor_mean + cor_sd
#     ),
#     width = 0.2,
#     color = mw_blue,
#     linewidth = 0.3
#   ) +
#   labs(
#     x = "Month",
#     y = "Mean discharge (m³/s)"
#   ) +
#   facet_wrap(~gauge_id, scales = "free_y", ncol = 2) +
#   theme(
#     strip.text = element_text(size = 10)
#   )

# save_png(
#   "figures/fig_correction_monthly.png",
#   monthly_plots,
#   w = 20,
#   h = 24,
#   dpi = 500
# )

# # Summary table -----------------------------------------------------------
# summary_table <-
#   metrics |>
#   mutate(
#     gauge_id = as.character(gauge_id),
#     NSE = mw_round(nse),
#     `KGE'` = mw_round(kge),
#     RMSE = mw_round(rmse),
#     `pBIAS` = mw_round(pbias)
#   ) |>
#   select(gauge_id, NSE, `KGE'`, RMSE, `pBIAS`) |>
#   arrange(gauge_id)

# fs::dir_create("tables")
# write.csv(
#   summary_table,
#   "tables/tbl_correction_metrics.csv",
#   quote = FALSE,
#   na = "",
#   row.names = FALSE
# )

# print(summary_table)
