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
  mutate(gauge_id = stringr::str_remove_all(gauge_id, "data/cv/|.csv"))

cv_results |>
  filter(gauge_id == "1496") |>
  tidyr::pivot_longer(
    c(nse:rmse),
    names_to = "metric",
    values_to = "estimate"
  ) |>
  ggplot(aes(x = as.factor(N_quantiles), y = estimate)) +
  geom_point(aes(color = type)) +
  facet_wrap(~metric, nrow = 3, scales = "free") +
  labs(x = "Number of quantiles", y = "Estimate", fill = "") +
  scale_fill_manual(values = c("#DC3220", "#009E73"))

cv_results |>
  filter(gauge_id == "1496") |>
  count(test_start, test_end)

library(lubridate)
(ymd("1980-05-30") - ymd("1986-09-30")) / 365
