library(tidyr)
library(dplyr)
library(purrr)
library(stringr)
library(readxl)
library(glue)
library(lubridate)

fls <- list.files("spreadsheets", full.names = TRUE)

# ── Helpers ───────────────────────────────────────────────────────────────────

get_sheet <- function(path, pattern) {
  shts <- excel_sheets(path)
  shts[str_detect(shts, pattern)][1]
}

find_table_start <- function(path, sheet, pattern) {
  df <- read_excel(path, sheet = sheet, range = cell_cols("A"), col_names = FALSE)
  which(str_detect(df[[1]], pattern))
}

# ── Pull dates ────────────────────────────────────────────────────────────────

file_pull_dates <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      read_excel(
        .x,
        sheet = get_sheet(.x, "^Facilities"),
        range = "A1:A7",
        col_names = "file_pull_date"
      ) |>
        filter(str_detect(file_pull_date, "IIDS|^Source:"))
    },
    .id = "file"
  ) |>
  mutate(
    fy = str_extract(file, "(?<=FY)\\d{2}"),

    # last 6- or 8-digit run anywhere in the filename
    date_str = str_extract_all(file, "\\d{6,8}") |>
      map_chr(~ dplyr::last(.x, default = NA_character_)),

    # parse:
    # - 8 digits: assume MMDDYYYY
    # - 6 digits: if first two <= 12 => MMDDYY, else => YYMMDD
    date_raw = case_when(
      str_length(date_str) == 8 ~ mdy(date_str),

      str_length(date_str) == 6 ~ {
        mm <- suppressWarnings(as.integer(str_sub(date_str, 1, 2)))
        if_else(
          !is.na(mm) & mm <= 12L,
          mdy(date_str),
          ymd(date_str)
        )
      },

      TRUE ~ as.Date(NA)
    ),

    file_date   = coalesce(date_raw, as.Date(str_c("20", fy, "-12-31"))),
    pull_date   = str_extract(file_pull_date, "\\d{1,2}/\\d{1,2}/\\d{4}") |> mdy(),
    fiscal_year = str_extract(file, "FY\\d{2}")
  ) |>
  select(file, fiscal_year, file_date, pull_date)

# ── Datasets ──────────────────────────────────────────────────────────────────

book_ins_by_arresting_agency <-
  fls |>
  set_names() |>
  map_dfr(
    ~ read_excel(.x, sheet = get_sheet(.x, "^Detention"), range = "I19:V22"),
    .id = "file"
  ) |>
  (\(x) { stopifnot(all(fls %in% x$file)); x })() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file) |>
  pivot_longer(cols = Oct:Sep, names_to = "month", values_to = "n_book_ins") |>
  rename(arresting_agency = Agency) |>
  mutate(
    month_num = match(month, month.abb),
    cy = as.integer(str_remove(fiscal_year, "FY")) +
      if_else(month_num >= 10, -1L, 0L) +
      2000L
  ) |>
  mutate(date = make_date(cy, month_num, 1L), .keep = "unused") |>
  rename(n_book_ins_ytd = Total)

final_release_reasons_col_types <-
  c(
    "text", # Release Reason
    "text", # Criminality
    rep("numeric", 13) # Oct–Sep + Total
  )

book_outs_by_reason <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      sheet <- get_sheet(.x, "^Detention")
      start_row <- find_table_start(
        .x, sheet,
        "ICE Final Book Outs by Release Reason, Month and Criminality"
      )
      if (length(start_row) > 0)
        read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row+1}:O{start_row+26}"),
          col_types = final_release_reasons_col_types
        )
    },
    .id = "file"
  ) |>
  left_join(file_pull_dates, by = "file") |>
  select(-file) |>
  pivot_longer(cols = Oct:Sep, names_to = "month", values_to = "n_book_outs") |>
  mutate(
    month_num = match(month, month.abb),
    cy = as.integer(str_remove(fiscal_year, "FY")) +
      if_else(month_num >= 10, -1L, 0L) +
      2000L
  ) |>
  mutate(date = make_date(cy, month_num, 1L), .keep = "unused") |>
  rename(n_book_outs_ytd = Total) |>
  janitor::clean_names()

# Shared col types for single-text-column monthly tables (text + Oct–Sep + Total)
monthly_col_types <- c("text", rep("numeric", 13))

adp_by_agency_criminality <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      sheet <- get_sheet(.x, "^Detention")
      start_row <- find_table_start(
        .x, sheet,
        "ICE Average Daily Population by Arresting Agency, Month and Criminality"
      )
      if (length(start_row) > 0)
        read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row+1}:N{start_row+13}"),
          col_types = monthly_col_types
        )
    },
    .id = "file"
  ) |>
  left_join(file_pull_dates, by = "file") |>
  select(-file) |>
  pivot_longer(cols = Oct:Sep, names_to = "month", values_to = "adp") |>
  mutate(
    month_num = match(month, month.abb),
    cy = as.integer(str_remove(fiscal_year, "FY")) +
      if_else(month_num >= 10, -1L, 0L) +
      2000L
  ) |>
  mutate(date = make_date(cy, month_num, 1L), .keep = "unused") |>
  rename(adp_fy_ytd = `FY Overall`) |>
  janitor::clean_names()

avg_stay_length_by_agency_criminality <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      sheet <- get_sheet(.x, "^Detention")
      start_row <- find_table_start(
        .x, sheet,
        "ICE Average Length of Stay by Arresting Agency, Month and Criminality"
      )
      if (length(start_row) > 0)
        read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row+1}:N{start_row+13}"),
          col_types = monthly_col_types
        )
    },
    .id = "file"
  ) |>
  left_join(file_pull_dates, by = "file") |>
  select(-file) |>
  pivot_longer(cols = Oct:Sep, names_to = "month", values_to = "avg_stay_length_days") |>
  mutate(
    month_num = match(month, month.abb),
    cy = as.integer(str_remove(fiscal_year, "FY")) +
      if_else(month_num >= 10, -1L, 0L) +
      2000L
  ) |>
  mutate(date = make_date(cy, month_num, 1L), .keep = "unused") |>
  rename(avg_stay_length_days_fy_ytd = `FY Overall`) |>
  janitor::clean_names()

detainees_by_facility <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      sheet <- get_sheet(.x, "^Facilities")
      start_row_df <- read_excel(.x, sheet = sheet, range = cell_cols("A"), col_names = FALSE)
      start_row <- which(!is.na(start_row_df[[1]]) & str_detect(start_row_df[[1]], "Name"))
      end_row <- nrow(start_row_df)
      if (length(start_row) > 0) {
        if (.x == "spreadsheets/FY25_detentionStats07072025.xlsx") start_row <- start_row + 1
        df <- read_excel(.x, sheet = sheet, range = glue("A{start_row}:N{end_row}")) |>
          filter(!is.na(Name))
        # coerce any "FY## ALOS" columns to character for consistent binding across years
        alos_cols <- grep("^FY\\d{2} ALOS$", colnames(df), value = TRUE)
        df[alos_cols] <- lapply(df[alos_cols], as.character)
        df$Zip <- as.character(df$Zip)
        df
      }
    },
    .id = "file"
  ) |>
  janitor::clean_names() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

removals <-
  fls |>
  set_names() |>
  map_dfr(
    ~ read_excel(.x, sheet = get_sheet(.x, "^Detention"), range = "P29", col_names = "removals"),
    .id = "file"
  ) |>
  (\(x) { stopifnot(all(fls %in% x$file)); x })() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# ── Write outputs ─────────────────────────────────────────────────────────────

arrow::write_feather(book_ins_by_arresting_agency, "data/book-ins-by-arresting-agency.feather")
arrow::write_feather(book_outs_by_reason, "data/book-outs-by-reason.feather")
arrow::write_feather(adp_by_agency_criminality, "data/adp-by-agency-criminality.feather")
arrow::write_feather(avg_stay_length_by_agency_criminality, "data/avg-stay-length-by-agency-criminality.feather")
arrow::write_feather(removals, "data/removals.feather")
arrow::write_feather(detainees_by_facility, "data/facilities.feather")
