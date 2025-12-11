library(ggplot2)
library(ggpp)

# Fonts
systemfonts::require_font("Ubuntu", dir = "assets")
mw_font <- "Ubuntu"

# Colors
# https://personal.sron.nl/~pault/data/colourschemes.pdf
mw_black <- "#231f20"
mw_blue <- "#4477AA"
mw_cyan <- "#66CCEE"
mw_green <- "#228833"
mw_yellow <- "#CCBB44"
mw_red <- "#EE6677"
mw_purple <- "#AA3377"

# ggplot2 theme ----------------------------------------------------------
theme_mw <- function(
  base_size = 9,
  base_family = mw_font
) {
  ggplot2::theme_bw(base_size, base_family) +
    ggplot2::theme(
      plot.title = ggtext::element_markdown(
        size = ggplot2::rel(1.2),
        face = "bold",
        color = mw_black,
        family = base_family
      ),
      panel.border = ggplot2::element_rect(
        color = mw_black,
        fill = NA,
        linewidth = ggplot2::unit(0.5, "lines")
      ),
      axis.line = ggplot2::element_blank(),
      panel.grid = ggplot2::element_line(
        color = "grey92", #"#d8d9da",
        linewidth = ggplot2::unit(0.25, "lines")
      ),
      panel.grid.minor = ggplot2::element_line(
        color = "grey92",
        linewidth = ggplot2::unit(0.25, "lines")
      ),
      panel.grid.major = ggplot2::element_line(
        color = "grey92",
        linewidth = ggplot2::unit(0.25, "lines")
      ),
      axis.ticks = ggplot2::element_line(
        color = mw_black,
        linewidth = ggplot2::unit(0.25, "lines")
      ),
      # !NB: make length negative for inner ticks
      axis.ticks.length = ggplot2::unit(0.25, "lines"),
      # axis.ticks.length = ggplot2::unit(-0.35, "lines"),
      axis.text = ggplot2::element_text(
        family = base_family,
        color = mw_black,
        size = ggplot2::unit(8, "lines")
      ),
      axis.text.x.bottom = ggplot2::element_text(
        margin = ggplot2::margin(
          t = 0.5,
          r = 0,
          b = 0.5,
          l = 0,
          unit = "lines"
        )
      ),
      axis.text.x.top = ggplot2::element_text(
        margin = ggplot2::margin(
          t = 0.5,
          r = 0,
          b = 0.5,
          l = 0,
          unit = "lines"
        )
      ),
      axis.text.y.left = ggplot2::element_text(
        margin = ggplot2::margin(
          t = 0,
          r = 0.5,
          b = 0,
          l = 0.3,
          unit = "lines"
        )
      ),
      axis.text.y.right = ggplot2::element_text(
        margin = ggplot2::margin(
          t = 0,
          r = 0.5,
          b = 0,
          l = 0.5,
          unit = "lines"
        )
      ),
      axis.title = ggtext::element_markdown(
        family = base_family,
        color = mw_black,
        size = 9,
      ),
      axis.title.x.bottom = ggplot2::element_text(
        margin = ggplot2::margin(
          t = -0.5,
          r = 0,
          b = 0.25,
          l = 0,
          unit = "lines"
        )
      ),
      axis.title.y = ggplot2::element_text(
        margin = ggplot2::margin(
          t = 0,
          r = -0.5,
          b = 0,
          l = 0.5,
          unit = "lines"
        )
      ),
      strip.text = ggplot2::element_text(
        family = base_family,
        color = mw_black,
        size = ggplot2::unit(9, "lines"),
        hjust = 0,
        vjust = 0.3,
        face = "bold.italic"
      ),
      strip.text.x = ggplot2::element_text(
        margin = ggplot2::margin(
          t = 0.15,
          l = 0.15,
          b = 0.15,
          unit = "lines"
        )
      ),
      strip.background = ggplot2::element_blank(),
      legend.key = ggplot2::element_blank(),
      legend.background = ggplot2::element_blank(),
      legend.box.margin = ggplot2::margin(
        t = -0.5,
        unit = "lines"
      ),
      legend.margin = ggplot2::margin(t = 0),
      legend.position = "bottom",
      legend.justification = "left",
      legend.key.height = ggplot2::unit(0.7, "lines"),
      legend.key.width = ggplot2::unit(1, "lines"),
      legend.spacing = ggplot2::unit(0.3, "lines"),
      plot.margin = ggplot2::margin(
        t = 0.5,
        r = 0.5,
        l = -0.25,
        b = 0.25,
        "lines"
      )
    )
}


# Add model metrics on plot ----------------------------------------------
geom_sites_metrics <-
  function(.metrics, .x = 0.85, .y = 0.15, ...) {
    .metrics2 <- .metrics |>
      dplyr::transmute(
        site_name,
        .metric = ifelse(
          grepl("rsq", .metric),
          "R^2",
          toupper(.metric)
        ),
        .estimate = format(.estimate, digits = 2, scientific = FALSE)
      )

    .tables <- split(
      .metrics2[, c(".metric", ".estimate")],
      .metrics2$site_name
    )

    ggpp::geom_table_npc(
      data = dplyr::distinct(.metrics2, site_name),
      label = unname(.tables),
      mapping = ggplot2::aes(group = site_name),
      npcx = .x,
      npcy = .y,
      parse = TRUE,
      table.colnames = FALSE,
      table.theme = ttheme_gtminimal,
      family = mw_font,
      inherit.aes = FALSE,
      ...
    )
  }

# Save as png ------------------------------------------------------------
save_png <-
  function(
    filename,
    plot,
    dpi = 1000,
    w = 16,
    h = 12,
    units = "cm"
  ) {
    ggplot2::ggsave(
      filename = filename,
      plot = plot,
      device = ragg::agg_png,
      dpi = dpi,
      width = w,
      height = h,
      units = units,
      bg = "#ffffff"
    )
  }
