library(rvest)
library(httr2)
library(stringr)
library(purrr)

url <- "https://www.ice.gov/detain/detention-management"
req <- request(url) |> req_timeout(30) |> req_retry(max_tries = 3, backoff = ~5)

link <- req |> req_perform() |> resp_body_html() |>
  html_elements("a") |> html_attr("href") |> discard(is.na) |>
  url_absolute(url) |>
  keep(\(x) str_detect(x, regex("detentionStats.*\\.xlsx(\\?|$)", ignore_case = TRUE))) |>
  pluck(1, .default = NA_character_)

stopifnot(!is.na(link))

# strip query string so saved file isn't named foo.xlsx?bar=baz
fname <- basename(link) |> str_remove("\\?.*$")
dir.create("spreadsheets", showWarnings = FALSE, recursive = TRUE)
dest <- file.path("spreadsheets", fname)

if (!file.exists(dest)) {
  tmp <- tempfile(fileext = ".xlsx")
  request(link) |> req_timeout(120) |> req_retry(max_tries = 3) |>
    req_perform(path = tmp)
  # file.rename() can't move across filesystems (EXDEV) and tempdir() may be
  # on a different device than the repo, so copy then remove
  stopifnot(file.copy(tmp, dest))
  unlink(tmp)
}
