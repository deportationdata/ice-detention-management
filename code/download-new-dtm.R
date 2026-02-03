library(rvest)
library(tidyverse)

url <- "https://www.ice.gov/detain/detention-management"

link <-
  read_html(url) |>
  html_elements("a[href$='.xlsx']") |>
  html_attr("href") |>
  str_subset("detentionStats") |>
  first()

fname <- basename(link)

if (!file.exists(file.path("spreadsheets", fname))) {
  download.file(
    link,
    destfile = file.path("spreadsheets", fname),
    mode = "wb"
  )
}
