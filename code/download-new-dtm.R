library(rvest)
library(httr2)
library(xml2)
library(stringr)
library(dplyr)
library(purrr)

url <- "https://www.ice.gov/detain/detention-management"

# 1) Download the HTML reliably (libcurl), then parse it
doc <-
  request(url) |>
  req_timeout(30) |>
  req_retry(max_tries = 3, backoff = ~5) |>
  req_perform() |>
  resp_body_html()

# 2) Extract the XLSX link (and make it absolute if it's relative)
link <-
  doc |>
  html_elements("a") |>
  html_attr("href") |>
  discard(is.na) |>
  url_absolute(url) |>
  keep(\(x) str_detect(x, "\\.xlsx(\\?|$)")) |>
  keep(\(x) str_detect(x, "detentionStats")) |>
  first()

stopifnot(!is.na(link))

# Strip query parameters from filename to avoid polluted filenames
fname <- basename(link) |> str_remove("\\?.*$")
dir.create("spreadsheets", showWarnings = FALSE, recursive = TRUE)

dest <- file.path("spreadsheets", fname)

if (!file.exists(dest)) {
  download.file(link, destfile = dest, mode = "wb", method = "libcurl")
}
