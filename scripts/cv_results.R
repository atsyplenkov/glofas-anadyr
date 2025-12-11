library(dplyr)
library(tidyr)
library(ggplot2)

# ggplot2 settings
source("src/utils_ggplot.R")
theme_set(theme_mw())

# Read CV -----------------------------------------------------------
cv_results <-
  fs::dir_ls("data/cv", regexp = ".csv$") |>
  purrr::map_dfr(
    ~ readr::read_csv(.x, show_col_types = FALSE),
    .id = "gauge_id"
  ) |>
  mutate(
    gauge_id = stringr::str_remove_all(gauge_id, "data/cv/|.csv"),
    nq = factor(
      nq,
      levels = c(sort(as.integer(unique(nq[nq != "raw"]))), "raw")
    )
  ) |>
  rename(
    `KGE'` = kgeprime,
    `KGEnp` = kgenp,
    `NSE` = nse,
    `NSElog` = log_nse,
    `RMSE` = rmse,
    `pBIAS` = pbias
  ) |>
  select(-KGEnp) |>
  as.data.frame()

# Aggregate CV -----------------------------------------------------------
cv_tidy <-
  cv_results |>
  tidyr::pivot_longer(
    c(NSE:RMSE),
    names_to = "metric",
    values_to = "estimate"
  ) |>
  group_by(gauge_id, metric, type, nq) |>
  ggdist::median_qi(estimate) |>
  select(-.width:-.interval) |>
  group_by(gauge_id, metric, type) |>
  filter(
    (!metric %in% c("pBIAS", "RMSE") & estimate == max(estimate)) |
      (metric %in% c("pBIAS", "RMSE") & abs(estimate) == min(abs(estimate)))
  ) |>
  ungroup()

# Plot -----------------------------------------------------------
loocv_plot <-
  cv_tidy |>
  mutate(
    type = factor(type, levels = c("Raw", "DQM"))
  ) |>
  ggplot() +
  geom_line(
    aes(
      x = type,
      y = estimate,
      group = gauge_id
    ),
    color = "gray60"
  ) +
  ggrepel::geom_text_repel(
    data = \(x) filter(x, type == "Raw"),
    aes(x = type, y = estimate, label = gauge_id),
    hjust = 1,
    nudge_x = -0.2,
    direction = "y",
    segment.curvature = 0,
    segment.angle = 90,
    min.segment.length = 0,
    box.padding = 0.1,
    family = mw_font,
    size = 2.5,
    segment.size = 0.3
  ) +
  geom_point(
    aes(x = type, y = estimate, fill = type),
    size = 2.5,
    shape = 21,
    color = "black"
  ) +
  scale_fill_manual(
    name = "GloFAS-ERA5 LOOCV",
    values = c(mw_red, mw_blue),
    labels = c(
      "Raw",
      "Bias-corrected with\nDetrended Quantile Mapping"
    )
  ) +
  labs(
    x = "",
    y = "Estimate"
  ) +
  scale_x_discrete(
    expand = expansion(
      mult = c(0.2, 0)
    )
  ) +
  coord_cartesian(xlim = c(0.7, 2.3)) +
  facet_wrap(~metric, scales = "free_y") +
  theme(
    legend.position = "inside",
    legend.position.inside = c(0.7, 0.3)
  )

save_png(
  "figures/fig03_loocv.png",
  loocv_plot,
  dpi = 500
)
