library(dplyr)
library(tidyr)

# Read all CV results
cv_results <-
  fs::dir_ls("data/cv", regexp = ".csv$") |>
  purrr::map_dfr(
    ~ readr::read_csv(.x, show_col_types = FALSE),
    .id = "gauge_id"
  ) |>
  mutate(gauge_id = stringr::str_remove_all(gauge_id, "data/cv/|.csv"))

cv_results |>
  filter(gauge_id == "1499") |> glimpse()
