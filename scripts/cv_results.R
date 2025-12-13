library(dplyr)
library(tidyr)
library(ggplot2)

# ggplot2 settings
source("src/utils.R")
source("src/utils_ggplot.R")
theme_set(theme_mw())

# Read CV -----------------------------------------------------------
cv_results <-
  snakemake@input[["cv_files"]] |>
  purrr::map_dfr(
    ~ readr::read_csv(.x, show_col_types = FALSE),
    .id = "gauge_id"
  ) |>
  mutate(
    gauge_id = stringr::str_remove_all(gauge_id, ".*/|.csv"),
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
  ungroup() |>
  mutate(
    type = factor(type, levels = c("Raw", "DQM"))
  )

# Make it pretty -----------------------------------------------------------
cv_table <-
  cv_tidy |>
  mutate(
    estimate_fmt = glue::glue(
      "{mw_round(estimate)} [95% CI {mw_round(.lower)} – {mw_round(.upper)}]"
    )
  ) |>
  select(gauge_id, type, metric, estimate_fmt) |>
  pivot_wider(
    names_from = "metric",
    values_from = "estimate_fmt"
  ) |>
  arrange(gauge_id, type)

nq_table <-
  cv_tidy |>
  filter(type == "DQM") |>
  select(gauge_id, metric, nq) |>
  mutate(
    nq_fmt = ifelse(as.character(nq) == "raw", "—", as.character(nq))
  ) |>
  group_by(gauge_id) |>
  summarize(
    type = factor("NQ", levels = c("Raw", "DQM", "NQ")),
    `KGE'` = nq_fmt[metric == "KGE'"][1],
    NSE = nq_fmt[metric == "NSE"][1],
    NSElog = nq_fmt[metric == "NSElog"][1],
    RMSE = nq_fmt[metric == "RMSE"][1],
    pBIAS = nq_fmt[metric == "pBIAS"][1],
    .groups = "drop"
  )

cv_table <-
  cv_table |>
  bind_rows(nq_table) |>
  arrange(gauge_id, type)

# write on disk
fs::dir_create(dirname(snakemake@output[["table"]]))
write.csv(
  cv_table,
  snakemake@output[["table"]],
  quote = FALSE,
  na = "",
  row.names = FALSE
)

# Estimate increase in performance ---------------------------------------------
perf_increase <-
  cv_tidy |>
  select(gauge_id, metric, type, estimate) |>
  pivot_wider(
    names_from = "type",
    values_from = "estimate"
  ) |>
  mutate(
    pct_change = case_when(
      metric == "RMSE" ~ ((Raw - DQM) / Raw) * 100,
      metric == "pBIAS" ~ ((abs(Raw) - abs(DQM)) / abs(Raw)) * 100,
      TRUE ~ ((DQM - Raw) / pmax(abs(Raw), 0.01)) * 100
    )
  ) |>
  group_by(metric) |>
  summarize(
    avg_pct_increase = mean(pct_change, na.rm = TRUE),
    .groups = "drop"
  ) |>
  arrange(metric)

perf_increase
# # # A tibble: 5 × 2
#   metric avg_pct_increase
#   <chr>             <dbl>
# 1 KGE'              17.6
# 2 NSE                8.12
# 3 NSElog           208.
# 4 RMSE               6.86
# 5 pBIAS            -10.4

# Plot -----------------------------------------------------------
loocv_plot <-
  cv_tidy |>
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
      "Bias-corrected"
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
  snakemake@output[["figure"]],
  loocv_plot,
  dpi = 500
)
