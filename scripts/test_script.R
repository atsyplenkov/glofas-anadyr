library(wk)

# Check the Snakemake input variables
print(snakemake@input)

# Check the dplyr package
x <-
  read.csv(snakemake@input[["file"]]) |>
  dplyr::select(cyl)
write.csv(x, "out/cyl.csv")

# Check package paths
dplyr_path <- find.package("dplyr")
wk_path <- find.package("wk")
print(dplyr_path)
print(wk_path)

# Save package paths
writeLines(c(dplyr_path, wk_path), "out/paths.txt")
