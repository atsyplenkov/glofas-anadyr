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
    obs = q_cms,
    .keep = "unused"
  ) |>
  filter(!is.na(obs))

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
    cor = q_cor,
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
  snakemake@output[["scatter"]],
  scatter_plots,
  w = 20,
  h = 11,
  dpi = 300
)

# ADCP measurements -----------------------------------------------------------
#fmt: skip
measured <- tibble::tribble(
  ~date,      ~adcp,
  "25-06-21", NA,       
  "28-06-21", 7415L,    
  "29-06-21", NA,       
  "01-07-21", 6163L,    
  "22-07-22", 1794L,    
  "23-07-22", NA,       
  "24-07-22", NA,       
  "27-07-22", 1152L,    
  "13-06-24", 5270L,    
  "17-06-24", NA,       
  "18-06-24", NA,       
  "22-06-24", 4608L,    
  "15-08-24", 1425L,    
  "20-08-24", NA,       
  "22-08-24", 986L
) |>
  mutate(date = lubridate::as_date(date, format = "%d-%m-%y")) |>
  filter(!is.na(adcp))

# Bind together
cor_meas <-
  measured |>
  left_join(
    filter(cor_data, gauge_id == 1499),
    by = join_by(date)
  ) |>
  left_join(
    filter(raw_data, gauge_id == 1499),
    by = join_by(date)
  )

# Estimate metrics
metrics <- yardstick::metric_set(kge2012, nse, pbias, rmse)

cor_metrics <- metrics(cor_meas, adcp, cor)
raw_metrics <- metrics(cor_meas, adcp, raw)

all_m <-
  bind_rows(
    raw_metrics,
    cor_metrics,
    .id = "type"
  ) |>
  select(-.estimator) |>
  tidyr::pivot_wider(
    names_from = `type`,
    values_from = .estimate,
    values_fn = ~ round(.x, 2)
  ) |>
  rename(Raw = 2, DQM = 3) |>
  mutate(.metric = c("KGE'", "NSE", "pBIAS", "RMSE")) |>
  rename(" " = 1)

# Plot
adcp_glofas <-
  cor_meas |>
  ggplot() +
  geom_abline(slope = 1, color = "grey60", lty = "longdash") +
  geom_smooth(
    aes(x = adcp, y = raw),
    method = "lm",
    color = colorspace::adjust_transparency(mw_red, 0.7),
    se = FALSE,
    show.legend = FALSE
  ) +
  geom_smooth(
    aes(x = adcp, y = cor),
    color = colorspace::adjust_transparency(mw_blue, 0.7),
    method = "lm",
    se = FALSE,
    show.legend = FALSE
  ) +
  geom_point(
    aes(x = adcp, y = raw, fill = "Raw"),
    size = 2.5,
    shape = 21,
    color = "black"
  ) +
  geom_point(
    aes(x = adcp, y = cor, fill = "Bias-corrected"),
    size = 2.5,
    shape = 21,
    color = "black"
  ) +
  ggpp::geom_table_npc(
    label = list(all_m),
    npcx = 0.1,
    npcy = 0.9,
    parse = TRUE,
    table.theme = ttheme_gtminimal,
    family = mw_font,
    inherit.aes = FALSE
  ) +
  scale_fill_manual(
    name = "GloFAS-ERA5",
    values = c("Raw" = mw_red, "Bias-corrected" = mw_blue),
    breaks = c("Raw", "Bias-corrected")
  ) +
  coord_fixed(xlim = c(0, 8000), ylim = c(0, 8000), expand = FALSE) +
  scale_y_continuous(
    breaks = scales::pretty_breaks(),
    labels = scales::number_format(big.mark = ",")
  ) +
  scale_x_continuous(
    breaks = scales::pretty_breaks(),
    labels = scales::number_format(big.mark = ",")
  ) +
  labs(
    x = "Observed Q (ADCP), m³/s",
    y = "Predicted Q, m³/s"
  ) +
  theme(legend.position = "inside", legend.position.inside = c(0.75, 0.1))

save_png(snakemake@output[["adcp"]], plot = adcp_glofas)
