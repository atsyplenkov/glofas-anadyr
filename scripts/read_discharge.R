library(openxlsx)
library(dplyr)
library(lubridate)
library(tidyr)

source("src/utils.R")

# Lookup table -----------------------------------------------------------
# Source: personal communication manually digitised by students
# from yearbooks
streamflow_excel <- "data/raw/Анадырь-расходы1223.xlsx"

# fmt: skip
lookup <- tibble::tribble(
  ~id,   ~river_en,  ~name_en,       ~sheet, ~cols,
  1496L, "Anadyr",   "Lamutskoe",     6L,     26L,  
  1502L, "Yeropol",  "Chuvanskoe",    7L,     26L,  
  1497L, "Anadyr",   "Novyy Yeropol", 3L,     78L,  
  1504L, "Mayn",     "Vayegi",        8L,     24L,  
  1499L, "Anadyr",   "Snezhnoye",     5L,     72L,
  1508L, "Enmyvaam", "Mukhomornoe",   10L,    30L,  
  1587L, "Tanyurer", "Tanyurer",      9L,     24L  
) |>
  dplyr::arrange(id)

# Read streamflow data ---------------------------------------------------
streamflow_data <-
  lapply(
    seq_len(nrow(lookup)),
    \(i) {
      raw_df <-
        openxlsx::read.xlsx(
          streamflow_excel,
          sheet = lookup$sheet[i],
          startRow = 2,
          colNames = FALSE,
          detectDates = TRUE,
          skipEmptyCols = FALSE,
          cols = seq_len(lookup$cols[i]),
          rows = c(2:367)
        ) |>
        tibble::as_tibble()

      # Cols ID's
      date_cols <- seq(1, ncol(raw_df), by = 2)
      q_cols <- seq(2, ncol(raw_df), by = 2)

      # Extract dates
      dates_long <- raw_df |>
        dplyr::select(tidyr::all_of(date_cols)) |>
        dplyr::mutate(dplyr::across(
          tidyr::everything(),
          ~ as.Date(.x)
        )) |>
        tidyr::pivot_longer(
          cols = tidyr::everything(),
          values_to = "Date",
          names_prefix = "X"
        )

      # Extract discharges
      values_long <- raw_df |>
        dplyr::select(tidyr::all_of(q_cols)) |>
        dplyr::mutate(dplyr::across(
          tidyr::everything(),
          ~ as.character(.x)
        )) |>
        tidyr::pivot_longer(
          cols = tidyr::everything(),
          values_to = "Q",
          names_prefix = "X"
        )

      # Join together
      tibble::tibble(
        id = lookup$id[i],
        date = dates_long$Date,
        q_cms = values_long$Q
      ) |>
        # Parse numeric water discharges
        dplyr::mutate(
          q_cms = trimws(q_cms),
          q_cms = stringr::str_replace_all(q_cms, ",", "."),
          q_cms = stringr::str_replace_all(q_cms, " ", "."),
          q_cms = stringr::str_replace_all(q_cms, "З", "3"),
          q_cms = stringr::str_remove_all(q_cms, "\\)|x")
        ) |>
        dplyr::mutate(
          q_cms = readr::parse_double(q_cms, na = c("", "-", "¾")),
        ) |>
        dplyr::arrange(date) |>
        dplyr::filter(!is.na(date)) |>
        tidyr::complete(
          date = seq.Date(from = min(date), to = max(date), by = "1 day")
        )
    }
  ) |>
  collapse::unlist2d(idcols = FALSE)

# Save to CSV -----------------------------------------------------------
Q_DIR <- "data/hydro"
fs::dir_create(Q_DIR)

anadyr_list <-
  streamflow_data |>
  split(streamflow_data$id) |>
  lapply(dplyr::select, -id) |>
  lapply(dplyr::arrange, date)

list_to_csv(anadyr_list, Q_DIR)
