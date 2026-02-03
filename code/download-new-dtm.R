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
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.32 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.32"
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
