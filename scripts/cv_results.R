library(dplyr)
library(tidyr)
library(ggplot2)

# Read all CV results
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
  as.data.frame()

# cv_results |>
#   filter(gauge_id == "1496") |>
#   tidyr::pivot_longer(
#     c(nse:rmse),
#     names_to = "metric",
#     values_to = "estimate"
#   ) |>
#   ggplot(aes(x = as.factor(nq), y = estimate)) +
#   geom_point(aes(color = type)) +
#   facet_wrap(~metric, nrow = 3, scales = "free") +
#   labs(x = "Number of quantiles", y = "Estimate", fill = "") +
#   scale_fill_manual(values = c("#DC3220", "#009E73"))

cv_tidy <-
  cv_results |>
  tidyr::pivot_longer(
    c(nse:rmse),
    names_to = "metric",
    values_to = "estimate"
  ) |>
  group_by(gauge_id, metric, type, nq) |>
  ggdist::median_qi(estimate) |>
  select(-.width:-.interval) |>
  group_by(gauge_id, metric, type) |>
  filter(
    (!metric %in% c("pbias", "rmse") & estimate == max(estimate)) |
      (metric %in% c("pbias", "rmse") & abs(estimate) == min(abs(estimate)))
  ) |>
  ungroup()

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
  # FIXME: label on the right and remove vertical axes
  ggrepel::geom_text_repel(
    data = \(x) filter(x, type == "DQM"),
    aes(x = type, y = estimate, label = gauge_id),
    hjust = 0,
    nudge_x = 0.25,
    direction = "y",
    segment.curvature = 0,
    segment.angle = 90,
    min.segment.length = 0
  ) +
  geom_point(
    aes(x = type, y = estimate, fill = type),
    size = 2.5,
    shape = 21,
    color = "black"
  ) +
  facet_wrap(~metric, scales = "free_y") +
  theme_minimal() +
  theme(legend.position = "bottom")
