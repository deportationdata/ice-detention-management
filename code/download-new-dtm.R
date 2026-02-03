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
  req_user_agent(
    str_c(
      "Mozilla/5.0 (X11; Linux x86_64)",
      "AppleWebKit/537.36 (KHTML, like Gecko)",
      "Chrome/121.0.0.0 Safari/537.36"
    )
  ) |>
  req_headers(
    "Accept" = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language" = "en-US,en;q=0.9",
    "Cache-Control" = "no-cache",
    "Pragma" = "no-cache"
  ) |>
  req_timeout(1) |>
  req_perform() |>
  resp_body_html()

# 2) Extract the XLSX link (and make it absolute if it’s relative)
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

fname <- basename(link)
dir.create("spreadsheets", showWarnings = FALSE, recursive = TRUE)

dest <- file.path("spreadsheets", fname)

if (!file.exists(dest)) {
  download.file(link, destfile = dest, mode = "wb", method = "libcurl")
}
