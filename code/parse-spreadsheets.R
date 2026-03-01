library(tidyr)
library(dplyr)
library(purrr)
library(stringr)
library(readxl)
library(glue)
library(lubridate)

# fls <- list.files(
#   "~/Library/CloudStorage/Box-Box/deportationdata-web-archive/ice/detention_management/",
#   pattern = "FY2\\d{1}\\_detentionStats\\d{8}\\.xlsx$",
#   full.names = TRUE
# )
fls <- list.files(
  "spreadsheets",
  full.names = TRUE
)

file_pull_dates <-
  fls |>
  set_names() |>
  map_df(
    ~ {
      shts <- readxl::excel_sheets(.x)
      readxl::read_excel(
        .x,
        sheet = shts[str_detect(shts, "^Facilities*")][1],
        range = "A1:A7",
        col_names = "file_pull_date"
      ) |>
        filter(str_detect(file_pull_date, "IIDS|^Source:"))
    },
    .id = "file"
  ) |>
  mutate(
    fiscal_year = str_extract(file, "FY2\\d{1}"),
    file_date = str_extract(file, "\\d{8}") |> lubridate::mdy(),
    # extract MM/DD/YYYY from "Data pulled on MM/DD/YYYY"
    pull_date = str_extract(file_pull_date, "\\d{1,2}/\\d{1,2}/\\d{4}") |>
      lubridate::mdy()
    # .keep = "unused"
  )

book_ins_by_arresting_agency <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      shts <- readxl::excel_sheets(.x)
      readxl::read_excel(
        .x,
        sheet = shts[str_detect(shts, "^Detention*")][1],
        range = "I19:V22"
      )
    },
    .id = "file"
  ) |>
  mutate(
    fiscal_year = str_extract(file, "FY2\\d{1}"),
    file_date = str_extract(file, "\\d{8}") |> lubridate::mdy()
  ) |>
  (\(x) {
    # check if all the files are in the data
    stopifnot(all(fls %in% x$file))
    x
  })() |>
  select(-file) |>
  pivot_longer(
    cols = c(Oct:Sep),
    names_to = "month",
    values_to = "n_book_ins"
  ) |>
  rename(arresting_agency = Agency) |>
  mutate(
    month_num = match(month, month.abb),
    cy = as.integer(str_remove(fiscal_year, "FY")) +
      if_else(month_num >= 10, -1L, 0L) +
      2000L
  ) |>
  mutate(
    date = lubridate::make_date(cy, month_num, 1L),
    .keep = "unused"
  ) |>
  rename(n_book_ins_ytd = Total) |>
  left_join(
    file_pull_dates |> select(file_date, pull_date),
    by = "file_date"
  )

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
      shts <- readxl::excel_sheets(.x)
      # find start row of table
      start_row_df <- readxl::read_excel(
        .x,
        sheet = shts[str_detect(shts, "^Detention*")][1],
        range = readxl::cell_cols("A"),
        col_names = FALSE
      )
      start_row <- which(
        str_detect(
          start_row_df[[1]],
          "ICE Final Book Outs by Release Reason, Month and Criminality"
        )
      )
      if (length(start_row) > 0) {
        # note in earlier years it does not have monthly data, so don't collect if so
        readxl::read_excel(
          .x,
          sheet = shts[str_detect(shts, "^Detention*")][1],
          range = glue::glue("A{start_row+1}:O{start_row + 26}"),
          col_types = final_release_reasons_col_types
        )
      }
    },
    .id = "file"
  ) |>
  mutate(
    fiscal_year = str_extract(file, "FY2\\d{1}"),
    file_date = str_extract(file, "\\d{8}") |> lubridate::mdy(),
    .keep = "unused"
  ) |>
  pivot_longer(
    cols = c(Oct:Sep),
    names_to = "month",
    values_to = "n_book_outs"
  ) |>
  mutate(
    month_num = match(month, month.abb),
    cy = as.integer(str_remove(fiscal_year, "FY")) +
      if_else(month_num >= 10, -1L, 0L) +
      2000L
  ) |>
  mutate(
    date = lubridate::make_date(cy, month_num, 1L),
    .keep = "unused"
  ) |>
  rename(n_book_outs_ytd = Total) |>
  janitor::clean_names() |>
  left_join(
    file_pull_dates |> select(file_date, pull_date),
    by = "file_date"
  )

adp_by_agency_criminality_col_types <-
  c(
    "text", # Agency
    rep("numeric", 13) # Oct–Sep + Total
  )

adp_by_agency_criminality <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      shts <- readxl::excel_sheets(.x)
      # find start row of table
      start_row_df <- readxl::read_excel(
        .x,
        sheet = shts[str_detect(shts, "^Detention*")][1],
        range = readxl::cell_cols("A"),
        col_names = FALSE
      )
      start_row <- which(
        str_detect(
          start_row_df[[1]],
          "ICE Average Daily Population by Arresting Agency, Month and Criminality"
        )
      )
      if (length(start_row) > 0) {
        # note in earlier years it does not have monthly data, so don't collect if so
        readxl::read_excel(
          .x,
          sheet = shts[str_detect(shts, "^Detention*")][1],
          range = glue::glue("A{start_row+1}:N{start_row + 13}"),
          col_types = adp_by_agency_criminality_col_types
        )
      }
    },
    .id = "file"
  ) |>
  mutate(
    fiscal_year = str_extract(file, "FY2\\d{1}"),
    file_date = str_extract(file, "\\d{8}") |> lubridate::mdy(),
    .keep = "unused"
  ) |>
  pivot_longer(
    cols = c(Oct:Sep),
    names_to = "month",
    values_to = "adp"
  ) |>
  mutate(
    month_num = match(month, month.abb),
    cy = as.integer(str_remove(fiscal_year, "FY")) +
      if_else(month_num >= 10, -1L, 0L) +
      2000L
  ) |>
  mutate(
    date = lubridate::make_date(cy, month_num, 1L),
    .keep = "unused"
  ) |>
  rename(adp_fy_ytd = `FY Overall`) |>
  janitor::clean_names() |>
  left_join(
    file_pull_dates |> select(file_date, pull_date),
    by = "file_date"
  )

avg_stay_length_by_agency_criminality <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      shts <- readxl::excel_sheets(.x)
      # find start row of table
      start_row_df <- readxl::read_excel(
        .x,
        sheet = shts[str_detect(shts, "^Detention*")][1],
        range = readxl::cell_cols("A"),
        col_names = FALSE
      )
      start_row <- which(
        str_detect(
          start_row_df[[1]],
          "ICE Average Length of Stay by Arresting Agency, Month and Criminality"
        )
      )
      if (length(start_row) > 0) {
        # note in earlier years it does not have monthly data, so don't collect if so
        readxl::read_excel(
          .x,
          sheet = shts[str_detect(shts, "^Detention*")][1],
          range = glue::glue("A{start_row+1}:N{start_row + 13}"),
          col_types = adp_by_agency_criminality_col_types
        )
      }
    },
    .id = "file"
  ) |>
  mutate(
    fiscal_year = str_extract(file, "FY2\\d{1}"),
    file_date = str_extract(file, "\\d{8}") |> lubridate::mdy(),
    .keep = "unused"
  ) |>
  pivot_longer(
    cols = c(Oct:Sep),
    names_to = "month",
    values_to = "avg_stay_length_days"
  ) |>
  mutate(
    month_num = match(month, month.abb),
    cy = as.integer(str_remove(fiscal_year, "FY")) +
      if_else(month_num >= 10, -1L, 0L) +
      2000L
  ) |>
  mutate(
    date = lubridate::make_date(cy, month_num, 1L),
    .keep = "unused"
  ) |>
  rename(avg_stay_length_days_fy_ytd = `FY Overall`) |>
  janitor::clean_names() |>
  left_join(
    file_pull_dates |> select(file_date, pull_date),
    by = "file_date"
  )

detainees_by_facility <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      shts <- readxl::excel_sheets(.x)
      # find start row of table
      start_row_df <- readxl::read_excel(
        .x,
        sheet = shts[str_detect(shts, "^Facilities*")][1],
        range = readxl::cell_cols("A"),
        col_names = FALSE
      )
      start_row <- which(
        !is.na(start_row_df[[1]]) &
          str_detect(
            start_row_df[[1]],
            "Name"
          )
      )
      end_row <- nrow(start_row_df)
      if (length(start_row) > 0) {
        if (.x == fls[92]) {
          start_row <- start_row + 1
        }
        # note in earlier years it does not have monthly data, so don't collect if so
        df <- readxl::read_excel(
          .x,
          sheet = shts[str_detect(shts, "^Facilities*")][1],
          range = glue::glue("A{start_row}:N{end_row}")
        ) |>
          filter(!is.na(Name))
        if ("FY24 ALOS" %in% colnames(df)) {
          df$`FY24 ALOS` <- as.character(df$`FY24 ALOS`)
        }
        if ("FY26 ALOS" %in% colnames(df)) {
          df$`FY26 ALOS` <- as.character(df$`FY26 ALOS`)
        }
        df$Zip <- as.character(df$Zip)
        df
      }
    },
    .id = "file"
  ) |>
  janitor::clean_names() |>
  mutate(
    fiscal_year = str_extract(file, "FY2\\d{1}"),
    file_date = str_extract(file, "\\d{8}") |> lubridate::mdy(),
    .keep = "unused"
  ) |>
  left_join(
    file_pull_dates |> select(file_date, pull_date),
    by = "file_date"
  )

removals <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      shts <- readxl::excel_sheets(.x)
      readxl::read_excel(
        .x,
        sheet = shts[str_detect(shts, "^Detention*")][1],
        range = "P29",
        col_names = "removals"
      )
    },
    .id = "file"
  ) |>
  mutate(
    fiscal_year = str_extract(file, "FY2\\d{1}"),
    file_date = str_extract(file, "\\d{8}") |> lubridate::mdy()
  ) |>
  (\(x) {
    # check if all the files are in the data
    stopifnot(all(fls %in% x$file))
    x
  })() |>
  select(-file) |>
  left_join(file_pull_dates |> select(file_date, pull_date), by = "file_date")

arrow::write_feather(
  book_ins_by_arresting_agency,
  "data/book-ins-by-arresting-agency.feather"
)

arrow::write_feather(
  book_outs_by_reason,
  "data/book-outs-by-reason.feather"
)

arrow::write_feather(
  adp_by_agency_criminality,
  "data/adp-by-agency-criminality.feather"
)

arrow::write_feather(
  avg_stay_length_by_agency_criminality,
  "data/avg-stay-length-by-agency-criminality.feather"
)

arrow::write_feather(
  book_ins_by_arresting_agency,
  "data/book-ins-by-arresting-agency.feather"
)

arrow::write_feather(
  removals,
  "data/removals.feather"
)

arrow::write_feather(
  detainees_by_facility,
  "data/facilities.feather"
)
