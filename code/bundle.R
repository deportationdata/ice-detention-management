# Build distribution bundles from the per-dataset files in data/:
#   data/detention-management.xlsx          - one tab per dataset, TOC up front
#   data/detention-management-parquet.zip   - all .parquet files
#   data/detention-management-dta.zip       - all .dta files

library(dplyr)
library(purrr)
library(writexl)

# Excel caps sheet names at 31 chars and forbids \ / ? * [ ]. These are the
# human-readable labels used as tab names and TOC entries; keys are the slugs
# that match data/<slug>.{parquet,xlsx,dta}.
sheet_titles <- c(
  "adp-and-stay-length-by-agency-fy-ytd" = "ADP & Stay by Agency YTD",
  "adp-and-stay-length-by-agency-monthly" = "ADP & Stay by Agency Monthly",
  "atd-by-aor" = "ATD by AOR",
  "atd-court-appearances" = "ATD Court Appearances",
  "atd-population" = "ATD Population",
  "bond-stats" = "Bond Stats",
  "book-ins-by-arresting-agency-fy-ytd" = "Book-ins by Agency YTD",
  "book-ins-by-arresting-agency-monthly" = "Book-ins by Agency Monthly",
  "book-outs-by-reason-fy-ytd" = "Book-outs by Reason YTD",
  "book-outs-by-reason-monthly" = "Book-outs by Reason Monthly",
  "currently-detained-by-criminality" = "Detained by Criminality",
  "currently-detained-by-disposition" = "Detained by Disposition",
  "facilities" = "Facilities",
  "facility-alos" = "Facility ALOS",
  "fear-decision-time" = "Fear Decision Time",
  "fear-decisions-by-facility-type" = "Fear Decisions by Facility Type",
  "iclos-and-detainees" = "ICLOS and Detainees",
  "movements-by-facility-type" = "Movements by Facility Type",
  "removals" = "Removals",
  "segregation" = "Segregation",
  "special-population-actions" = "Special Population Actions",
  "vulnerable-population" = "Vulnerable Population"
)

stopifnot(max(nchar(sheet_titles)) <= 31)

parquet_files <- list.files("data", pattern = "\\.parquet$", full.names = TRUE)
slugs <- tools::file_path_sans_ext(basename(parquet_files))

missing <- setdiff(slugs, names(sheet_titles))
if (length(missing)) {
  stop("Add sheet titles for new datasets: ", paste(missing, collapse = ", "))
}

datasets <-
  set_names(parquet_files, slugs) |>
  map(nanoparquet::read_parquet)

toc <- tibble(
  Sheet = sheet_titles[slugs],
  Dataset = slugs
) |>
  arrange(Sheet)

sheets <- c(
  list(`Table of Contents` = toc),
  set_names(datasets, sheet_titles[slugs])
)

xlsx_path <- "data/detention-management.xlsx"
write_xlsx(sheets, xlsx_path)

# Stage the per-dataset files in a tempdir and zip from there so the archive
# doesn't carry data/ as a wrapper dir on extract.
zip_bundle <- function(zipfile, pattern) {
  files <- list.files("data", pattern = pattern, full.names = TRUE)
  temp_dir <- tempdir()
  staged <- file.path(temp_dir, basename(files))
  file.copy(files, staged, overwrite = TRUE)
  if (file.exists(zipfile)) {
    file.remove(zipfile)
  }
  utils::zip(zipfile = zipfile, files = staged, flags = "-j")
  file.remove(staged)
}

zip_bundle("data/detention-management-parquet.zip", "\\.parquet$")
zip_bundle("data/detention-management-dta.zip", "\\.dta$")

message("Wrote:")
message("  ", xlsx_path)
message("  data/detention-management-parquet.zip")
message("  data/detention-management-dta.zip")
