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
