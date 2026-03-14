library(tidyr)
library(dplyr)
library(purrr)
library(stringr)
library(readxl)
library(glue)
library(lubridate)

fls <- list.files("spreadsheets", full.names = TRUE, pattern = "^[^~].*\\.xlsx$")

# ── Helpers ───────────────────────────────────────────────────────────────────

get_sheet <- function(path, pattern) {
  shts <- excel_sheets(path)
  shts[str_detect(shts, pattern)][1]
}

find_table_start <- function(path, sheet, pattern) {
  df <- read_excel(
    path,
    sheet = sheet,
    range = cell_cols("A"),
    col_names = FALSE
  )
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

    file_date = coalesce(date_raw, as.Date(str_c("20", fy, "-09-30"))),
    pull_date = str_extract(file_pull_date, "\\d{1,2}/\\d{1,2}/\\d{4}") |>
      mdy(),
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
  (\(x) {
    stopifnot(all(fls %in% x$file))
    x
  })() |>
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
      sheet  <- get_sheet(.x, "^Detention")
      col_a  <- read_excel(.x, sheet = sheet, range = cell_cols("A"), col_names = FALSE)[[1]]
      col_a_safe <- replace(col_a, is.na(col_a), "")

      # Matches both the older "ICE Final Releases" and newer "ICE Final Book Outs" headings.
      # The format with "Month and" in the name has a monthly breakdown (Oct–Sep);
      # the annual-only format ("by Release Reason and Criminality") has a different
      # column structure and is not parsed here.
      start_row <- which(str_detect(
        col_a_safe,
        "ICE Final (Releases|Book Outs) by Release Reason, Month and Criminality"
      ))[1]

      if (!is.na(start_row)) {
        # Stop reading before the next section header
        next_section <- which(
          seq_along(col_a) > start_row &
          str_detect(col_a_safe, "ICE Average Daily Population|ICE Average Length")
        )[1]
        end_row <- if (!is.na(next_section)) next_section - 1L else min(start_row + 60L, length(col_a))

        read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row+1}:O{end_row}"),
          col_types = final_release_reasons_col_types
        ) |>
          # drop fully-blank trailing rows
          filter(!is.na(`Release Reason`) | !is.na(Criminality)) |>
          # early FY24 spreadsheets appended " Total" to some reason names; strip it
          mutate(`Release Reason` = str_remove(`Release Reason`, "\\s+Total$"))
      }
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
        .x,
        sheet,
        "ICE Average Daily Population by Arresting Agency, Month and Criminality"
      )
      if (length(start_row) > 0) {
        read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row+1}:N{start_row+13}"),
          col_types = monthly_col_types
        )
      }
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
        .x,
        sheet,
        "ICE Average Length of Stay by Arresting Agency, Month and Criminality"
      )
      if (length(start_row) > 0) {
        read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row+1}:N{start_row+13}"),
          col_types = monthly_col_types
        )
      }
    },
    .id = "file"
  ) |>
  left_join(file_pull_dates, by = "file") |>
  select(-file) |>
  pivot_longer(
    cols = Oct:Sep,
    names_to = "month",
    values_to = "avg_stay_length_days"
  ) |>
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
      start_row_df <- read_excel(
        .x,
        sheet = sheet,
        range = cell_cols("A"),
        col_names = FALSE
      )
      start_row <- which(
        !is.na(start_row_df[[1]]) & str_detect(start_row_df[[1]], "Name")
      )
      # Footnotes appear at the bottom of the sheet and start with [, (*), or *.
      # Stop reading before the first such row.
      footnote_rows <- which(
        str_detect(replace(start_row_df[[1]], is.na(start_row_df[[1]]), ""),
                   "^\\[|^\\(\\*|^\\*[A-Za-z]")
      )
      footnote_rows <- footnote_rows[footnote_rows > max(start_row, 0)]
      end_row <- if (length(footnote_rows) > 0) footnote_rows[1] - 1 else nrow(start_row_df)
      if (length(start_row) > 0) {
        if (.x == "spreadsheets/FY25_detentionStats07072025.xlsx") {
          start_row <- start_row + 1
        }
        df <- read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row}:N{end_row}")
        ) |>
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
    ~ read_excel(
      .x,
      sheet = get_sheet(.x, "^Detention"),
      range = "P29",
      col_names = "removals"
    ),
    .id = "file"
  ) |>
  (\(x) {
    stopifnot(all(fls %in% x$file))
    x
  })() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# ── Interval ADP ──────────────────────────────────────────────────────────────
#
# Interval ADP isolates the average daily population for the period *between*
# two consecutive snapshot pull dates, rather than the cumulative FY-to-date
# average that ICE reports.
#
# Formula (for two consecutive snapshots at dates D1 and D2):
#   days_i  = as.numeric(pull_date_i - FY_start) + 1   # days elapsed in FY
#   interval_adp = (ADP2 * days2 - ADP1 * days1) / (days2 - days1)
#
# See: https://relevant-research.com/assets/pdf/methodology_writeup.pdf

fy_days_elapsed <- function(pull_date, fiscal_year) {
  fy_year <- as.integer(str_remove(fiscal_year, "FY")) + 2000L
  fy_start <- make_date(fy_year - 1L, 10L, 1L)
  as.numeric(pull_date - fy_start) + 1L
}

apply_interval_adp <- function(adp_col, days_col) {
  days_lag <- lag(days_col)
  adp_lag <- lag(adp_col)
  interval_days <- days_col - days_lag
  ifelse(
    interval_days > 0,
    (adp_col * days_col - adp_lag * days_lag) / interval_days,
    NA_real_
  )
}

# ── Interval ADP: Facilities ───────────────────────────────────────────────────
# total_adp = sum of the four detainee classification levels (A–D)

interval_adp_facilities <-
  detainees_by_facility |>
  mutate(
    total_adp = level_a + level_b + level_c + level_d,
    fy_year = as.integer(str_remove(fiscal_year, "FY")) + 2000L,
    fy_start = make_date(fy_year - 1L, 10L, 1L)
  ) |>
  # drop snapshots whose pull_date predates the fiscal year
  filter(pull_date >= fy_start) |>
  select(
    name,
    aor,
    type_detailed,
    male_female,
    fiscal_year,
    pull_date,
    total_adp,
    level_a,
    level_b,
    level_c,
    level_d
  ) |>
  # one row per facility × fiscal_year × pull_date (take max if dupes)
  group_by(name, aor, type_detailed, male_female, fiscal_year, pull_date) |>
  summarise(
    across(
      c(total_adp, level_a, level_b, level_c, level_d),
      ~ suppressWarnings(max(.x, na.rm = TRUE))
    ),
    .groups = "drop"
  ) |>
  # -Inf arises when all values were NA; restore to NA
  mutate(across(
    c(total_adp, level_a, level_b, level_c, level_d),
    ~ if_else(is.infinite(.x), NA_real_, .x)
  )) |>
  arrange(name, fiscal_year, pull_date) |>
  group_by(name, fiscal_year) |>
  mutate(
    days = fy_days_elapsed(pull_date, fiscal_year),
    date_start = lag(pull_date),
    date_end = pull_date,
    interval_days = days - lag(days),
    interval_adp = apply_interval_adp(total_adp, days),
    interval_adp_level_a = apply_interval_adp(level_a, days),
    interval_adp_level_b = apply_interval_adp(level_b, days),
    interval_adp_level_c = apply_interval_adp(level_c, days),
    interval_adp_level_d = apply_interval_adp(level_d, days)
  ) |>
  filter(!is.na(date_start), interval_days > 0) |>
  ungroup() |>
  select(
    name,
    aor,
    type_detailed,
    male_female,
    fiscal_year,
    date_start,
    date_end,
    interval_days,
    interval_adp,
    interval_adp_level_a,
    interval_adp_level_b,
    interval_adp_level_c,
    interval_adp_level_d
  )

# ── Interval ADP: Agency / Criminality ────────────────────────────────────────
# Uses the cumulative FY-YTD ADP (adp_fy_ytd) per agency row.
#
# The agency column contains both "parent" rows (CBP Average, ICE Average,
# Average) and criminality sub-rows (Convicted Criminal, etc.) that repeat
# across the three parent groups.  Sub-rows are disambiguated by position
# within each snapshot; only snapshots with the standard 12-row structure
# are included.

adp_agency_ytd <-
  adp_by_agency_criminality |>
  distinct(agency, fiscal_year, pull_date, adp_fy_ytd) |>
  # drop snapshots whose pull_date predates the fiscal year
  mutate(
    fy_year = as.integer(str_remove(fiscal_year, "FY")) + 2000L,
    fy_start = make_date(fy_year - 1L, 10L, 1L)
  ) |>
  filter(pull_date >= fy_start) |>
  select(-fy_year, -fy_start) |>
  group_by(fiscal_year, pull_date) |>
  mutate(row_pos = row_number(), n_rows = n()) |>
  ungroup() |>
  # keep only standard-format snapshots (12 agency rows per pull date)
  filter(n_rows == 12, !is.na(adp_fy_ytd)) |>
  # assign stable labels: rows 1/5/9 = parent totals; 2-4/6-8/10-12 = subs
  mutate(
    parent_agency = case_when(
      row_pos %in% 1:4 ~ "CBP",
      row_pos %in% 5:8 ~ "ICE",
      row_pos %in% 9:12 ~ "All"
    )
  )

interval_adp_agency_criminality <-
  adp_agency_ytd |>
  arrange(row_pos, fiscal_year, pull_date) |>
  group_by(row_pos, fiscal_year) |>
  mutate(
    days = fy_days_elapsed(pull_date, fiscal_year),
    date_start = lag(pull_date),
    date_end = pull_date,
    interval_days = days - lag(days),
    interval_adp = apply_interval_adp(adp_fy_ytd, days)
  ) |>
  filter(!is.na(date_start), interval_days > 0) |>
  ungroup() |>
  select(
    parent_agency,
    agency,
    fiscal_year,
    date_start,
    date_end,
    interval_days,
    interval_adp
  )

# ── Write outputs ─────────────────────────────────────────────────────────────

arrow::write_feather(
  book_ins_by_arresting_agency,
  "data/book-ins-by-arresting-agency.feather"
)
arrow::write_feather(book_outs_by_reason, "data/book-outs-by-reason.feather")

arrow::write_feather(
  adp_by_agency_criminality,
  "data/adp-by-agency-criminality.feather"
)
arrow::write_feather(
  avg_stay_length_by_agency_criminality,
  "data/avg-stay-length-by-agency-criminality.feather"
)
arrow::write_feather(removals, "data/removals.feather")
arrow::write_feather(detainees_by_facility, "data/facilities.feather")
arrow::write_feather(
  interval_adp_facilities,
  "data/interval-adp-facilities.feather"
)
arrow::write_feather(
  interval_adp_agency_criminality,
  "data/interval-adp-agency-criminality.feather"
)
