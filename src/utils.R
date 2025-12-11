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
      # format(x, digits = 2, scientific = FALSE),
      round(x, 2),
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
