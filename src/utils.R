# Save named list to individual csv -----------------------------------------------
# Following the D.Abramov structure
list_to_csv <-
  function(list, dir) {
    for (nm in names(list)) {
      fn <- file.path(dir, paste0(nm, ".csv"))
      # if (fs::file_exists(fn)) {
      #   stop(sprintf("File %s.csv already exists", nm))
      # }
      write.csv(
        x = list[[nm]],
        file = fn,
        quote = FALSE,
        na = "",
        row.names = FALSE
      )
    }
  }

# Rounding function ------------------------------------------------------
mw_round <- function(x) {
  xa <- abs(x)

  # Calculate the preliminary result.
  # Note: format() returns a character value.
  r <- ifelse(
    xa < 10,
    ifelse(
      xa < 1,
      format(x, digits = 2, scientific = FALSE),
      # round(x, 2),
      round(x, 2)
    ),
    ifelse(xa < 100, round(x, 1), round(x, 0))
  )

  # Initialize the output vector (as character).
  result <- r

  # For values where abs(x) is at least 1, apply additional formatting:
  idx <- xa >= 1
  idx[is.na(idx)] <- FALSE
  if (any(idx)) {
    # Use prettyNum to add thousand separators.
    rf <- prettyNum(as.numeric(r[idx]), big.mark = " ")
    # Replace the normal hyphen with a Unicode minus sign for negatives.
    rf <- ifelse(as.numeric(r[idx]) < 0, gsub("-", "−", rf), rf)
    result[idx] <- rf
  }

  trimws(result)
}
# Mann-Kendall test ------------------------------------------------------
# Modified from
# https://github.com/USGS-R/EGRETextra/blob/fb1b44a1107dbba1af3f179466814f488e8a5627/vignettes/vignetteFlowWeighted.Rmd#L74-L81

mw_kendall <-
  function(.date, .y, .remove = NULL) {
    # Early return if there is insufficient amount of datapoints
    # see help("rkt", "rkt")
    if (any(length(.y) < 4, length(.date) < 4)) {
      return(
        c(
          "slope" = NA_real_,
          "intercept" = NA_real_,
          "tau" = NA_real_,
          "p" = NA_real_,
          "pct" = NA_real_
        )
      )
    }

    # Replace values of interest with NAs
    if (!is.null(.remove)) {
      .y[.remove] <- NA_real_
    }

    # Estimate Mann-Kendall trend
    mk_test <- rkt::rkt(date = .date, y = .y)
    mk_slope <- mk_test$B # Theil-Sen slope
    mk_intercept <- median(.y, na.rm = TRUE) -
      mk_slope * median(.date)

    # Percent change
    # fmt:skip
    mk_pct <- 100 * mk_slope / exp(mean(log(.y), na.rm = TRUE))

    # Return
    c(
      "slope" = mk_slope,
      "intercept" = mk_intercept,
      "tau" = mk_test$tau,
      "p" = mk_test$sl,
      "pct" = mk_pct
    )
  }
