library(openxlsx)
library(dplyr)
library(lubridate)
library(tidyr)

source("src/utils.R")

# Lookup table -----------------------------------------------------------
# Source: personal communication manually digitised by students
# from yearbooks
streamflow_excel <- snakemake@input[["file"]]

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
Q_DIR <- dirname(snakemake@output[["obs_files"]][1])
fs::dir_create(Q_DIR)

anadyr_list <-
  streamflow_data |>
  split(streamflow_data$id) |>
  lapply(dplyr::select, -id) |>
  lapply(dplyr::arrange, date)

list_to_csv(anadyr_list, Q_DIR)

# Data availability ------------------------------------------------------
library(ggplot2)
source("src/utils_ggplot.R")

ggplot2::theme_set(theme_mw())

anadyr_na <-
  streamflow_data |>
  filter(!is.na(id)) |>
  dplyr::mutate(
    id = factor(
      id,
      levels = lookup$id,
      labels = glue::glue(
        "{lookup$river_en} — {lookup$name_en} ({lookup$id})"
      )
    )
  ) |>
  dplyr::group_by(id) |>
  tidyr::complete(
    date = seq.Date(as.Date("1979-01-01"), max(date))
  ) |>
  dplyr::filter(lubridate::month(date) %in% c(5:10)) |>
  dplyr::mutate(year = lubridate::year(date)) |>
  dplyr::group_by(id, year) |>
  dplyr::reframe(na_rate = 1 - sum(is.na(q_cms)) / dplyr::n())

anadyr_missing_plot <-
  anadyr_na |>
  dplyr::filter(year >= 1979) |>
  ggplot2::ggplot(ggplot2::aes(x = year)) +
  ggplot2::geom_errorbar(
    ggplot2::aes(ymin = 0, ymax = 1, color = "Missing"),
    width = 0,
    lwd = 4,
    key_glyph = draw_key_rect
  ) +
  ggplot2::geom_errorbar(
    ggplot2::aes(ymin = 0, ymax = na_rate, color = "Available"),
    width = 0,
    lwd = 4,
    key_glyph = draw_key_rect
  ) +
  ggplot2::scale_x_continuous(
    name = "",
    breaks = scales::breaks_width(2),
    minor_breaks = scales::breaks_width(1)
  ) +
  ggplot2::scale_y_continuous(
    name = "Daily streamflow data availability",
    limits = c(0, 1),
    expand = ggplot2::expansion(0, 0),
    labels = scales::percent_format(),
    breaks = c(0, 0.5, 1)
  ) +
  ggplot2::scale_color_viridis_d(
    name = "",
    option = "rocket",
    begin = 0.2,
    end = 0.9,
    guide = ggplot2::guide_legend(
      override.aes = list(alpha = 1),
      title.position = "top",
      nrow = 2
    )
  ) +
  ggplot2::facet_wrap(~id, ncol = 1) +
  ggplot2::theme(
    legend.position = "inside",
    legend.position.inside = c(0.8, 0.06),
    legend.key.height = ggplot2::unit(0.7, "lines"),
    legend.key.width = ggplot2::unit(0.7, "lines"),
    legend.key.spacing.y = ggplot2::unit(0.1, "lines")
  )

save_png(
  snakemake@output[["figure"]],
  anadyr_missing_plot,
  w = 13,
  h = 16
)
