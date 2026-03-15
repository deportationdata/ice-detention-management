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

has_sheet <- function(path, pattern) {
  shts <- excel_sheets(path)
  any(str_detect(shts, pattern))
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

# Read column A of a sheet (cached per extraction to avoid repeated reads)
read_col_a <- function(path, sheet) {
  read_excel(path, sheet = sheet, range = cell_cols("A"), col_names = FALSE)
}

# Safely extract from a file, warning on failure instead of silently dropping
safe_extract <- function(file, label, expr) {
  tryCatch(expr, error = function(e) {
    warning(glue("  [{label}] Failed for {basename(file)}: {e$message}"))
    NULL
  })
}

# Add fiscal year date columns to a monthly pivoted dataset
add_fy_date_cols <- function(df) {
  df |>
    mutate(
      month_num = match(month, month.abb),
      cy = as.integer(str_remove(fiscal_year, "FY")) +
        if_else(month_num >= 10, -1L, 0L) +
        2000L
    ) |>
    mutate(date = make_date(cy, month_num, 1L), .keep = "unused")
}

# Ensure map_dfr result has the 'file' column (empty results won't have it)
ensure_file_col <- function(df, expect_all = FALSE) {
  if (!"file" %in% names(df)) {
    warning(
      "  map_dfr returned 0 rows — no 'file' column. Adding empty column."
    )
    df$file <- character(0)
  }
  if (expect_all) {
    stopifnot(all(fls %in% df$file))
  }
  df
}

# Shared col types for single-text-column monthly tables (text + Oct-Sep + Total)
monthly_col_types <- c("text", rep("numeric", 13))

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
    date_raw = map(date_str, \(ds) {
      if (is.na(ds)) {
        return(as.Date(NA))
      }
      n <- str_length(ds)
      if (n == 8L) {
        return(mdy(ds))
      }
      if (n == 6L) {
        mm <- as.integer(str_sub(ds, 1, 2))
        if (!is.na(mm) && mm <= 12L) {
          return(mdy(ds))
        }
        return(ymd(ds))
      }
      as.Date(NA)
    }) |>
      list_c(),

    file_date = coalesce(date_raw, as.Date(str_c("20", fy, "-12-31"))),
    pull_date = str_extract(file_pull_date, "\\d{1,2}/\\d{1,2}/\\d{4}") |>
      mdy(),
    fiscal_year = str_extract(file, "FY\\d{2}")
  ) |>
  select(file, fiscal_year, file_date, pull_date)

# ── Existing datasets (with bug fixes) ───────────────────────────────────────

# Book-ins by agency: header is in column I (not A), anchored off row 18
# "ICE Currently Detained by Criminality" at A18, "Agency" header at I19
book_ins_by_arresting_agency <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "book_ins", {
        sheet <- get_sheet(.x, "^Detention")
        anchor <- find_table_start(
          .x,
          sheet,
          "Currently Detained by Criminality"
        )
        if (length(anchor) == 0) {
          warning(glue(
            "  [book_ins] Anchor header not found in {basename(.x)}"
          ))
          return(NULL)
        }
        # Agency header is at I(anchor+1), data at I(anchor+2):V(anchor+4)
        read_excel(
          .x,
          sheet = sheet,
          range = glue("I{anchor[1]+1}:V{anchor[1]+4}"),
          col_types = c("text", rep("numeric", 13))
        )
      })
    },
    .id = "file"
  ) |>
  ensure_file_col(expect_all = TRUE) |>
  left_join(file_pull_dates, by = "file") |>
  select(-file) |>
  pivot_longer(cols = Oct:Sep, names_to = "month", values_to = "n_book_ins") |>
  rename(arresting_agency = Agency) |>
  add_fy_date_cols() |>
  rename(n_book_ins_ytd = Total)

final_release_reasons_col_types <-
  c(
    "text", # Release Reason
    "text", # Criminality
    rep("numeric", 13) # Oct-Sep + Total
  )

book_outs_by_reason <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "book_outs", {
        sheet <- get_sheet(.x, "^Detention")
        start_row <- find_table_start(
          .x,
          sheet,
          "ICE Final (Book Outs|Releases) by Release Reason, Month and Criminality"
        )
        if (length(start_row) == 0) {
          # Older files (FY19-FY21) have a different format without monthly columns
          return(NULL)
        }
        read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row+1}:O{start_row+26}"),
          col_types = final_release_reasons_col_types
        )
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file) |>
  pivot_longer(cols = Oct:Sep, names_to = "month", values_to = "n_book_outs") |>
  add_fy_date_cols() |>
  rename(n_book_outs_ytd = Total) |>
  janitor::clean_names()

# Older files (FY19-FY21) use a different format: annual totals by criminality columns
book_outs_by_reason_annual <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "book_outs_annual", {
        sheet <- get_sheet(.x, "^Detention")
        # Only use this for files that DON'T have the monthly format
        monthly_row <- find_table_start(
          .x,
          sheet,
          "ICE Final (Book Outs|Releases) by Release Reason, Month and Criminality"
        )
        if (length(monthly_row) > 0) {
          return(NULL)
        }
        start_row <- find_table_start(
          .x,
          sheet,
          "ICE Final (Book Outs|Releases) by Release Reason"
        )
        if (length(start_row) == 0) {
          return(NULL)
        }
        df <- read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row[1]+1}:E{start_row[1]+9}"),
          col_types = c("text", rep("numeric", 4))
        ) |>
          filter(!is.na(pick(1)[[1]]))
        names(df) <- c(
          "release_reason",
          "convicted_criminal",
          "pending_criminal_charges",
          "other_immigration_violator",
          "total"
        )
        df
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# Unified annual book outs: combine both formats into one long-format annual dataset
book_outs_annual_from_monthly <-
  book_outs_by_reason |>
  filter(month == "Oct") |> # one row per release_reason x criminality x file
  select(
    release_reason,
    criminality,
    n_book_outs_ytd,
    fiscal_year,
    file_date,
    pull_date
  )

book_outs_annual_from_old <-
  book_outs_by_reason_annual |>
  pivot_longer(
    cols = c(
      convicted_criminal,
      pending_criminal_charges,
      other_immigration_violator,
      total
    ),
    names_to = "criminality",
    values_to = "n_book_outs_ytd"
  ) |>
  mutate(
    criminality = recode(
      criminality,
      convicted_criminal = "Convicted Criminal",
      pending_criminal_charges = "Pending Criminal Charges",
      other_immigration_violator = "Other Immigration Violator",
      total = "Total"
    )
  )

book_outs_by_reason_all_years <-
  bind_rows(book_outs_annual_from_monthly, book_outs_annual_from_old)

adp_by_agency_criminality <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "adp", {
        sheet <- get_sheet(.x, "^Detention")
        start_row <- find_table_start(
          .x,
          sheet,
          "ICE Average Daily Population by Arresting Agency, Month and Criminality"
        )
        if (length(start_row) == 0) {
          warning(glue("  [adp] Table header not found in {basename(.x)}"))
          return(NULL)
        }
        read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row+1}:N{start_row+13}"),
          col_types = monthly_col_types
        )
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file) |>
  pivot_longer(cols = Oct:Sep, names_to = "month", values_to = "adp") |>
  add_fy_date_cols() |>
  rename(adp_fy_ytd = `FY Overall`) |>
  janitor::clean_names()

avg_stay_length_by_agency_criminality <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "alos", {
        sheet <- get_sheet(.x, "^Detention")
        start_row <- find_table_start(
          .x,
          sheet,
          "ICE Average Length of Stay by Arresting Agency, Month and Criminality"
        )
        if (length(start_row) == 0) {
          warning(glue("  [alos] Table header not found in {basename(.x)}"))
          return(NULL)
        }
        read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row+1}:N{start_row+13}"),
          col_types = monthly_col_types
        )
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file) |>
  pivot_longer(
    cols = Oct:Sep,
    names_to = "month",
    values_to = "avg_stay_length_days"
  ) |>
  add_fy_date_cols() |>
  rename(avg_stay_length_days_fy_ytd = `FY Overall`) |>
  janitor::clean_names()

detainees_by_facility <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "facilities", {
        sheet <- get_sheet(.x, "^Facilities")
        col_a <- read_col_a(.x, sheet)
        start_row <- which(!is.na(col_a[[1]]) & str_detect(col_a[[1]], "^Name"))
        end_row <- nrow(col_a)
        if (length(start_row) == 0) {
          warning(glue("  [facilities] Header row not found in {basename(.x)}"))
          return(NULL)
        }
        # If the row after "Name" is also a header-like row (e.g. a second header),
        # detect structurally instead of special-casing individual files
        test_row <- read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row + 1}:A{start_row + 1}"),
          col_names = FALSE
        )
        if (!is.na(test_row[[1]]) && str_detect(test_row[[1]], "^Name")) {
          start_row <- start_row + 1
        }
        df <- read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row}:N{end_row}")
        ) |>
          filter(!is.na(Name))
        # coerce varying columns to character for consistent binding across years
        alos_cols <- grep("^FY\\d{2} ALOS$", colnames(df), value = TRUE)
        df[alos_cols] <- lapply(df[alos_cols], as.character)
        df$Zip <- as.character(df$Zip)
        df
      })
    },
    .id = "file"
  ) |>
  janitor::clean_names() |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# Removals: anchored off row 27 "Book-Ins by Facility/Criminality"
# P(anchor+1) = "Total" label, P(anchor+2) = removals count
removals <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "removals", {
        sheet <- get_sheet(.x, "^Detention")
        anchor <- find_table_start(.x, sheet, "Book-Ins by")
        if (length(anchor) == 0) {
          warning(glue(
            "  [removals] Anchor header not found in {basename(.x)}"
          ))
          return(NULL)
        }
        read_excel(
          .x,
          sheet = sheet,
          range = glue("P{anchor[1]+2}:P{anchor[1]+2}"),
          col_names = "removals",
          col_types = "numeric"
        )
      })
    },
    .id = "file"
  ) |>
  ensure_file_col(expect_all = TRUE) |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# ── New datasets: Detention sheet tables ─────────────────────────────────────

# Currently Detained by Processing Disposition and Detention Facility Type
# Always at row 8 in column A; data at rows 9-14 (header + 5 data rows)
currently_detained_by_disposition <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "detained_disposition", {
        sheet <- get_sheet(.x, "^Detention")
        start_row <- find_table_start(
          .x,
          sheet,
          "Currently Detained by Processing Disposition"
        )
        if (length(start_row) == 0) {
          return(NULL)
        }
        df <- read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row[1]+1}:D{start_row[1]+5}"),
          col_types = c("text", "numeric", "numeric", "numeric"),
          na = c("", "-")
        )
        # Standardize: rename first col, drop any unnamed cols (e.g. ...4)
        names(df)[1] <- "disposition"
        df <- df |> select(-starts_with("..."))
        # Ensure consistent 3-col output: disposition + up to 2 numeric cols
        # Rename numeric cols to generic names for consistency across years
        num_cols <- setdiff(names(df), "disposition")
        if (length(num_cols) >= 2) {
          # Has FSC/FRC + Adult + Total (or similar)
          names(df)[2:min(4, ncol(df))] <- c(
            "fsc_frc",
            "adult",
            "total"
          )[seq_along(num_cols)]
        }
        df
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# Average Time from USCIS Fear Decision to ICE Release
# Header at G(anchor), data at G(anchor+1):K(anchor+2)
# Cols: ICE Release Fiscal Year | (gap) | FSC | Adult | Total
fear_decision_time <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "fear_decision_time", {
        sheet <- get_sheet(.x, "^Detention")
        anchor <- find_table_start(
          .x,
          sheet,
          "Currently Detained by Processing Disposition"
        )
        if (length(anchor) == 0) {
          return(NULL)
        }
        df <- read_excel(
          .x,
          sheet = sheet,
          range = glue("G{anchor[1]+1}:K{anchor[1]+2}")
        ) |>
          select(where(~ !all(is.na(.x))))
        if (ncol(df) == 0 || nrow(df) == 0) {
          return(NULL)
        }
        # Standardize columns: first col is release fiscal year label, rest are numeric
        names(df)[1] <- "release_fiscal_year"
        num_cols <- names(df)[-1]
        # Normalize to 3 standard columns: fiscal_year, adult, total
        # Some files have FSC column, some don't
        if (length(num_cols) == 3) {
          names(df)[2:4] <- c("fsc", "adult", "total")
        } else if (length(num_cols) == 2) {
          names(df)[2:3] <- c("adult", "total")
          df$fsc <- NA_real_
        } else if (length(num_cols) == 1) {
          names(df)[2] <- "total"
          df$adult <- NA_real_
          df$fsc <- NA_real_
        }
        df |> select(release_fiscal_year, fsc, adult, total)
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# Aliens with USCIS-Established Fear Decisions by Facility Type
# Header at N(anchor), data: N(anchor+1)=header, N(anchor+2):P(anchor+4)=data
fear_decisions_by_facility_type <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "fear_decisions_facility", {
        sheet <- get_sheet(.x, "^Detention")
        anchor <- find_table_start(
          .x,
          sheet,
          "Currently Detained by Processing Disposition"
        )
        if (length(anchor) == 0) {
          return(NULL)
        }
        df <- read_excel(
          .x,
          sheet = sheet,
          range = glue("N{anchor[1]+1}:P{anchor[1]+4}")
        ) |>
          select(where(~ !all(is.na(.x))))
        if (ncol(df) == 0 || nrow(df) == 0) {
          return(NULL)
        }
        # Standardize column names
        names(df) <- c("facility_type", "total_detained")[seq_len(ncol(df))]
        df$facility_type <- as.character(df$facility_type)
        df |> filter(!is.na(facility_type))
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# Currently Detained by Criminality and Arresting Agency
# Always at row 18 in column A
currently_detained_by_criminality <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "detained_criminality", {
        sheet <- get_sheet(.x, "^Detention")
        start_row <- find_table_start(
          .x,
          sheet,
          "Currently Detained by Criminality"
        )
        if (length(start_row) == 0) {
          return(NULL)
        }
        read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row[1]+1}:F{start_row[1]+4}"),
          col_types = c("text", rep("numeric", 5))
        )
      })
    },
    .id = "file"
  ) |>
  janitor::clean_names() |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# Book-Ins by Facility Type and Criminality
# Always at row 27 in column A
book_ins_by_facility_type <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "book_ins_facility", {
        sheet <- get_sheet(.x, "^Detention")
        start_row <- find_table_start(.x, sheet, "Book-Ins by")
        if (length(start_row) == 0) {
          return(NULL)
        }
        read_excel(
          .x,
          sheet = sheet,
          range = glue("A{start_row[1]+1}:E{start_row[1]+3}"),
          col_types = c("text", rep("numeric", 4))
        )
      })
    },
    .id = "file"
  ) |>
  janitor::clean_names() |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# Book Outs by Facility Type
# Header at H(anchor), data at H(anchor+1):J(anchor+3)
book_outs_by_facility_type <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "book_outs_facility", {
        sheet <- get_sheet(.x, "^Detention")
        anchor <- find_table_start(.x, sheet, "Book-Ins by")
        if (length(anchor) == 0) {
          return(NULL)
        }
        df <- read_excel(
          .x,
          sheet = sheet,
          range = glue("H{anchor[1]+1}:J{anchor[1]+3}")
        ) |>
          select(where(~ !all(is.na(.x))))
        if (ncol(df) == 0 || nrow(df) == 0) {
          return(NULL)
        }
        names(df) <- c("facility_type", "total")[seq_len(ncol(df))]
        df$facility_type <- as.character(df$facility_type)
        df
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# Removals with FAMU/FRC identifier
# At anchor+3 (row 30) in column P
famu_removals <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "famu_removals", {
        sheet <- get_sheet(.x, "^Detention")
        anchor <- find_table_start(.x, sheet, "Book-Ins by")
        if (length(anchor) == 0) {
          return(NULL)
        }
        read_excel(
          .x,
          sheet = sheet,
          range = glue("P{anchor[1]+3}:P{anchor[1]+3}"),
          col_names = "famu_removals",
          col_types = "numeric"
        )
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# ── New datasets: ATD sheet ──────────────────────────────────────────────────

# ATD summary tables: population by technology and by status
atd_population <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "atd_pop", {
        sheet <- get_sheet(.x, "^ATD")
        if (is.na(sheet)) {
          return(NULL)
        }
        col_a <- read_col_a(.x, sheet)

        # Table 1: Population by technology
        tech_row <- which(str_detect(
          col_a[[1]],
          "ATD Active Population Counts|ATD Active Participants"
        ))
        # Table 2: Population by status (FAMU/Single Adult)
        status_row <- which(str_detect(
          col_a[[1]],
          "ATD Active Population by Status"
        ))

        results <- list()

        if (length(tech_row) > 0) {
          # Read down from header until we hit "Total"
          r <- tech_row[1]
          tech_df <- read_excel(
            .x,
            sheet = sheet,
            range = glue("A{r+1}:C{r+8}"),
            col_types = c("text", rep("numeric", 2))
          ) |>
            filter(!is.na(pick(1)[[1]])) |>
            janitor::clean_names() |>
            mutate(table = "technology")
          # Standardize column names across years
          names(tech_df)[1:3] <- c("category", "count", "value")
          results <- c(results, list(tech_df))
        }

        if (length(status_row) > 0) {
          r <- status_row[1]
          status_df <- read_excel(
            .x,
            sheet = sheet,
            range = glue("A{r+1}:C{r+6}"),
            col_types = c("text", rep("numeric", 2))
          ) |>
            filter(!is.na(pick(1)[[1]])) |>
            janitor::clean_names() |>
            mutate(table = "status")
          names(status_df)[1:3] <- c("category", "count", "value")
          results <- c(results, list(status_df))
        }

        if (length(results) == 0) {
          return(NULL)
        }
        bind_rows(results)
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# ATD by AOR and Technology (the detailed breakdown table)
atd_by_aor <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "atd_aor", {
        sheet <- get_sheet(.x, "^ATD")
        if (is.na(sheet)) {
          return(NULL)
        }
        col_a <- read_col_a(.x, sheet)

        aor_row <- which(str_detect(
          col_a[[1]],
          "Active ATD Participants.*by AOR"
        ))
        if (length(aor_row) == 0) {
          return(NULL)
        }

        r <- aor_row[1]
        # Read the full AOR table (header + data rows until end of data)
        end_row <- nrow(col_a)
        df <- read_excel(
          .x,
          sheet = sheet,
          range = glue("A{r+1}:C{end_row}"),
          col_types = c("text", rep("numeric", 2))
        ) |>
          filter(!is.na(pick(1)[[1]]))
        names(df) <- c("aor_technology", "count", "avg_length_in_program")
        df
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# ATD court appearance data
# Layout: header at [r, cc], then [r+1, cc]=Metric, [r+1, cc+1]=Count, [r+1, cc+2]=%
# Data rows: [r+2..r+4, cc]=metric name, [r+2..r+4, cc+1]=count, [r+2..r+4, cc+2]=pct
atd_court_appearances <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "atd_court", {
        sheet <- get_sheet(.x, "^ATD")
        if (is.na(sheet)) {
          return(NULL)
        }
        df <- read_excel(.x, sheet = sheet, col_names = FALSE)

        # Find all rows/cols containing "Court Appearance"
        court_headers <- list()
        for (i in seq_len(nrow(df))) {
          for (j in seq_len(ncol(df))) {
            v <- as.character(df[i, j])
            if (!is.na(v) && str_detect(v, "Court Appearance")) {
              court_headers <- c(court_headers, list(list(row = i, col = j)))
            }
          }
        }
        if (length(court_headers) == 0) {
          return(NULL)
        }

        results <- list()
        for (ch in court_headers) {
          hr <- ch$row
          cc <- ch$col
          label <- as.character(df[hr, cc])
          hearing_type <- case_when(
            str_detect(label, "Total") ~ "total",
            str_detect(label, "Final") ~ "final",
            TRUE ~ "unknown"
          )
          # Data rows are 2-4 rows below header (row hr+1 is the Metric/Count/% header)
          for (mr in (hr + 2):(min(hr + 4, nrow(df)))) {
            metric <- as.character(df[mr, cc])
            count_val <- suppressWarnings(as.numeric(as.character(df[
              mr,
              cc + 1
            ])))
            pct_val <- suppressWarnings(as.numeric(as.character(df[
              mr,
              cc + 2
            ])))
            if (
              !is.na(metric) &&
                metric %in% c("Attended", "Failed to Attend", "Total")
            ) {
              results <- c(
                results,
                list(tibble(
                  hearing_type = hearing_type,
                  metric = metric,
                  count = count_val,
                  pct = pct_val
                ))
              )
            }
          }
        }
        if (length(results) == 0) {
          return(NULL)
        }
        bind_rows(results)
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# ── New datasets: ICLOS and Detainees sheet ──────────────────────────────────

# Since the ICLOS sheet has extremely wide format (72+ columns) with varying
# year ranges across files, we extract the full table per-file into a list-column format
iclos_and_detainees <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "iclos_full", {
        if (!has_sheet(.x, "ICLOS|Detainee")) {
          return(NULL)
        }
        sheet <- get_sheet(.x, "ICLOS|Detainee")
        if (is.na(sheet)) {
          return(NULL)
        }

        # Read full sheet
        df <- read_excel(.x, sheet = sheet, col_names = FALSE)
        if (nrow(df) < 7) {
          return(NULL)
        }

        col_a <- df[[1]]

        # Find ICLOS section (rows with population labels in col A after "Population" header)
        iclos_header <- which(str_detect(col_a, "^Population") & !is.na(col_a))
        if (length(iclos_header) == 0) {
          return(NULL)
        }

        # Find Detainees section
        det_header <- which(str_detect(col_a, "^Detainees$") & !is.na(col_a))

        all_rows <- list()

        # Process the two main sections
        sections <- list()
        if (length(iclos_header) > 0) {
          sections[["iclos"]] <- iclos_header[1]
        }
        if (length(det_header) > 0) {
          # Detainees has its own "Population" header a few rows after "Detainees"
          det_pop <- iclos_header[iclos_header > det_header[1]]
          if (length(det_pop) > 0) {
            sections[["detainees"]] <- det_pop[1]
          }
        }

        for (section_name in names(sections)) {
          pop_row <- sections[[section_name]]
          # Year labels are in the row at pop_row
          # Month labels are in pop_row + 1
          # mid/end labels are in pop_row + 2
          # Data starts at pop_row + 3

          # Find where data ends (next section or end of data)
          data_start <- pop_row + 3
          remaining <- col_a[data_start:length(col_a)]
          # Data rows have population labels; find where they end
          non_na_rows <- which(!is.na(remaining))
          if (length(non_na_rows) == 0) {
            next
          }

          # Find contiguous blocks - stop at a gap > 2 rows or at "Detainees" marker
          for (i in seq_along(non_na_rows)) {
            if (i > 1 && (non_na_rows[i] - non_na_rows[i - 1]) > 2) {
              non_na_rows <- non_na_rows[1:(i - 1)]
              break
            }
            # Also stop if we hit a section header
            label <- remaining[non_na_rows[i]]
            if (
              !is.na(label) && str_detect(label, "^(Detainees|Population)$")
            ) {
              non_na_rows <- non_na_rows[1:(i - 1)]
              break
            }
          }

          if (length(non_na_rows) == 0) {
            next
          }
          data_end <- data_start + max(non_na_rows) - 1

          for (r in seq(data_start, data_end)) {
            pop_label <- col_a[r]
            if (is.na(pop_label)) {
              next
            }
            # Skip "Total" sub-headers and bucket labels like "0-180 Days"
            # that belong to the detainees section
            vals <- suppressWarnings(as.numeric(as.character(df[
              r,
              2:ncol(df)
            ])))
            vals <- vals[!is.na(vals) & vals != 0]
            if (length(vals) == 0) {
              next
            }

            all_rows <- c(
              all_rows,
              list(tibble(
                section = section_name,
                population = pop_label,
                n_observations = length(vals),
                latest_value = dplyr::last(vals)
              ))
            )
          }
        }

        if (length(all_rows) == 0) {
          return(NULL)
        }
        bind_rows(all_rows)
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# ── New datasets: Monthly Bond Statistics ────────────────────────────────────

monthly_bond_stats <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "bond_stats", {
        if (!has_sheet(.x, "Bond")) {
          return(NULL)
        }
        sheet <- get_sheet(.x, "Bond")
        if (is.na(sheet)) {
          return(NULL)
        }

        # Skip title/empty/date rows — data rows are all text + numeric
        df <- read_excel(.x, sheet = sheet, col_names = FALSE, skip = 3)
        if (nrow(df) < 5) {
          return(NULL)
        }
        n_cols <- ncol(df)

        col_a <- df[[1]]
        total_row <- which(str_detect(
          col_a,
          "Total ICE Final (Book Outs|Releases)"
        ))
        bond_row <- which(str_detect(
          col_a,
          "ICE Final (Book Outs|Releases) with Bond"
        ))
        pct_row <- which(str_detect(col_a, "Bond Posted.*%"))
        avg_row <- which(str_detect(col_a, "Average Bond Amount"))
        alos_row <- which(str_detect(col_a, "ALOS"))

        if (length(total_row) == 0) {
          return(NULL)
        }

        # Read date row (row 3) as dates
        dates_df <- read_excel(
          .x,
          sheet = sheet,
          col_names = FALSE,
          range = glue("B3:{LETTERS[n_cols]}3"),
          col_types = "date"
        )
        date_vals <- as.Date(do.call(c, dates_df))

        results <- list()
        metrics <- list(
          list(row = total_row, name = "total_book_outs"),
          list(row = bond_row, name = "bond_book_outs"),
          list(row = pct_row, name = "bond_pct"),
          list(row = avg_row, name = "avg_bond_amount"),
          list(row = alos_row, name = "alos_days")
        )

        for (m in metrics) {
          if (length(m$row) == 0) {
            next
          }
          vals <- unlist(df[m$row[1], 2:n_cols], use.names = FALSE)
          for (j in seq_along(vals)) {
            if (!is.na(vals[j]) && vals[j] != 0 && !is.na(date_vals[j])) {
              results <- c(
                results,
                list(tibble(
                  date = date_vals[j],
                  metric = m$name,
                  value = vals[j]
                ))
              )
            }
          }
        }

        if (length(results) == 0) {
          return(NULL)
        }
        bind_rows(results)
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# ── New datasets: Monthly Segregation ────────────────────────────────────────

monthly_segregation <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "segregation", {
        if (!has_sheet(.x, "Segregation")) {
          return(NULL)
        }
        sheet <- get_sheet(.x, "Segregation")
        if (is.na(sheet)) {
          return(NULL)
        }

        df <- read_excel(.x, sheet = sheet, col_names = FALSE)
        col_a <- as.character(df[[1]])
        col_b <- suppressWarnings(as.numeric(as.character(df[[2]])))

        results <- list()
        current_month <- NA_character_

        for (i in seq_len(nrow(df))) {
          a <- col_a[i]
          b <- col_b[i]
          if (is.na(a)) {
            next
          }

          # Detect month headers (e.g., "November 2025\nThis Segregation...")
          month_match <- str_extract(
            a,
            "^(January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{4}"
          )
          if (!is.na(month_match)) {
            current_month <- month_match
            next
          }

          # Skip header rows and Grand Total
          if (
            str_detect(a, "^(Facilities|Placement Count|Grand Total|U\\.S\\.)")
          ) {
            next
          }
          if (str_detect(a, "Segregation Review")) {
            next
          }

          # Data rows: facility name in A, count in B
          if (!is.na(current_month) && !is.na(b)) {
            results <- c(
              results,
              list(tibble(
                month = current_month,
                facility = a,
                placement_count = b
              ))
            )
          }
        }

        if (length(results) == 0) {
          return(NULL)
        }
        bind_rows(results) |>
          mutate(month_date = myd(paste0(month, " 1")))
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# ── New datasets: Semiannual ─────────────────────────────────────────────────

# Extract all semiannual tables: Armed Forces, US Citizens, Parents of USC,
# and TPS countries (arrests, bookins, removals for each)
semiannual_data <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "semiannual", {
        if (!has_sheet(.x, "Semiannual")) {
          return(NULL)
        }
        sheet <- get_sheet(.x, "Semiannual")
        if (is.na(sheet)) {
          return(NULL)
        }

        df <- read_excel(.x, sheet = sheet, col_names = FALSE)
        col_a <- as.character(df[[1]])

        results <- list()

        # Find simple FY-based tables (2-column: Fiscal Year | Value)
        simple_tables <- list(
          "armed_forces_arrests" = "Armed Forces.*Arrests",
          "armed_forces_bookins" = "Armed Forces.*Bookins",
          "armed_forces_removals" = "Armed Forces.*Removals",
          "us_citizen_arrests" = "United States Citizen Arrests",
          "us_citizen_bookins" = "United States Citizens? Bookins",
          "us_citizen_removals" = "United States Citizens? Removals",
          "parents_usc_arrests" = "Parents of USC? Arrests",
          "parents_usc_bookins" = "Parents of USC? Bookins",
          "parents_usc_removals" = "Parents of USC? Removals"
        )

        for (tbl_name in names(simple_tables)) {
          pattern <- simple_tables[[tbl_name]]
          header_row <- which(str_detect(col_a, pattern))
          if (length(header_row) == 0) {
            next
          }
          hr <- header_row[1]

          # Read data rows (FY + value pairs) below the header
          for (r in (hr + 2):min(hr + 12, nrow(df))) {
            fy <- col_a[r]
            val <- suppressWarnings(as.numeric(as.character(df[r, 2])))
            if (is.na(fy) || !str_detect(fy, "^FY")) {
              break
            }
            results <- c(
              results,
              list(tibble(
                table_name = tbl_name,
                data_fiscal_year = fy,
                country = NA_character_,
                value = val
              ))
            )
          }
        }

        # TPS country tables (multi-column: Country | FY values)
        tps_tables <- list(
          "tps_arrests" = "Temporary Protected Status Countries Arrests",
          "tps_bookins" = "Temporary Protected Status Countries Bookins",
          "tps_removals" = "Temporary Protected Status Countries Removals"
        )

        for (tbl_name in names(tps_tables)) {
          pattern <- tps_tables[[tbl_name]]
          header_row <- which(str_detect(col_a, pattern))
          if (length(header_row) == 0) {
            next
          }
          hr <- header_row[1]

          # Header row has column names: Country | FY years
          fy_header <- as.character(df[hr + 1, ])
          fy_cols <- which(str_detect(fy_header, "^FY\\d{4}$"))
          fy_names <- fy_header[fy_cols]

          # Read data rows
          for (r in (hr + 2):min(hr + 25, nrow(df))) {
            country <- col_a[r]
            if (is.na(country) || str_detect(country, "^$")) {
              break
            }
            for (j in seq_along(fy_cols)) {
              val <- suppressWarnings(as.numeric(as.character(df[
                r,
                fy_cols[j]
              ])))
              if (!is.na(val)) {
                results <- c(
                  results,
                  list(tibble(
                    table_name = tbl_name,
                    data_fiscal_year = fy_names[j],
                    country = country,
                    value = val
                  ))
                )
              }
            }
          }
        }

        if (length(results) == 0) {
          return(NULL)
        }
        bind_rows(results)
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# ── New datasets: Vulnerable & Special Population ────────────────────────────

vulnerable_population <-
  fls |>
  set_names() |>
  map_dfr(
    ~ {
      safe_extract(.x, "vulnerable", {
        if (!has_sheet(.x, "Vulnerable")) {
          return(NULL)
        }
        sheet <- get_sheet(.x, "Vulnerable")
        if (is.na(sheet)) {
          return(NULL)
        }

        df <- read_excel(.x, sheet = sheet, col_names = FALSE)
        col_a <- as.character(df[[1]])

        # Find quarterly data sections
        quarter_rows <- which(str_detect(
          col_a,
          "^Fiscal Year \\(FY\\)\\s+\\d{4} Quarter \\d"
        ))
        if (length(quarter_rows) == 0) {
          return(NULL)
        }

        results <- list()

        for (qr in quarter_rows) {
          # Extract quarter label
          quarter_label <- col_a[qr]
          fy_quarter <- str_extract(quarter_label, "\\d{4} Quarter \\d")

          # Data starts 2 rows below (header row + data rows)
          # Header: Placement Reason | Number of Placements | Avg Consecutive | Avg Cumulative
          for (r in (qr + 2):min(qr + 8, nrow(df))) {
            reason <- col_a[r]
            if (is.na(reason)) {
              break
            }
            if (str_detect(reason, "^\\*|^$")) {
              break
            }

            n_placements <- suppressWarnings(as.numeric(as.character(df[r, 2])))
            avg_consec <- suppressWarnings(as.numeric(as.character(df[r, 3])))
            avg_cumul <- suppressWarnings(as.numeric(as.character(df[r, 4])))

            results <- c(
              results,
              list(tibble(
                fy_quarter = fy_quarter,
                placement_reason = reason,
                n_placements = n_placements,
                avg_consecutive_days = avg_consec,
                avg_cumulative_days = avg_cumul
              ))
            )
          }

          # Try to extract unique detainee count from footnote
          for (fr in (qr + 8):min(qr + 15, nrow(df))) {
            note <- col_a[fr]
            if (!is.na(note) && str_detect(note, "unique detainees")) {
              n_unique <- str_extract(note, "\\d+") |> as.integer()
              if (!is.na(n_unique)) {
                results <- c(
                  results,
                  list(tibble(
                    fy_quarter = fy_quarter,
                    placement_reason = "_unique_detainees",
                    n_placements = n_unique,
                    avg_consecutive_days = NA_real_,
                    avg_cumulative_days = NA_real_
                  ))
                )
              }
              break
            }
          }
        }

        if (length(results) == 0) {
          return(NULL)
        }
        bind_rows(results)
      })
    },
    .id = "file"
  ) |>
  ensure_file_col() |>
  left_join(file_pull_dates, by = "file") |>
  select(-file)

# ── Write outputs ─────────────────────────────────────────────────────────────

dir.create("data", showWarnings = FALSE, recursive = TRUE)

# Original datasets
arrow::write_parquet(
  book_ins_by_arresting_agency,
  "data/book-ins-by-arresting-agency.parquet"
)
arrow::write_parquet(book_outs_by_reason, "data/book-outs-by-reason.parquet")
arrow::write_parquet(
  book_outs_by_reason_annual,
  "data/book-outs-by-reason-annual.parquet"
)
arrow::write_parquet(
  book_outs_by_reason_all_years,
  "data/book-outs-by-reason-all-years.parquet"
)
arrow::write_parquet(
  adp_by_agency_criminality,
  "data/adp-by-agency-criminality.parquet"
)
arrow::write_parquet(
  avg_stay_length_by_agency_criminality,
  "data/avg-stay-length-by-agency-criminality.parquet"
)
arrow::write_parquet(removals, "data/removals.parquet")
arrow::write_parquet(detainees_by_facility, "data/facilities.parquet")

# New Detention sheet datasets
arrow::write_parquet(
  currently_detained_by_disposition,
  "data/currently-detained-by-disposition.parquet"
)
arrow::write_parquet(fear_decision_time, "data/fear-decision-time.parquet")
arrow::write_parquet(
  fear_decisions_by_facility_type,
  "data/fear-decisions-by-facility-type.parquet"
)
arrow::write_parquet(
  currently_detained_by_criminality,
  "data/currently-detained-by-criminality.parquet"
)
arrow::write_parquet(
  book_ins_by_facility_type,
  "data/book-ins-by-facility-type.parquet"
)
arrow::write_parquet(
  book_outs_by_facility_type,
  "data/book-outs-by-facility-type.parquet"
)
arrow::write_parquet(famu_removals, "data/famu-removals.parquet")

# ATD datasets
arrow::write_parquet(atd_population, "data/atd-population.parquet")
arrow::write_parquet(atd_by_aor, "data/atd-by-aor.parquet")
arrow::write_parquet(
  atd_court_appearances,
  "data/atd-court-appearances.parquet"
)

# ICLOS and Detainees
arrow::write_parquet(iclos_and_detainees, "data/iclos-and-detainees.parquet")

# Monthly Bond Statistics
arrow::write_parquet(monthly_bond_stats, "data/monthly-bond-stats.parquet")

# Monthly Segregation
arrow::write_parquet(monthly_segregation, "data/monthly-segregation.parquet")

# Semiannual
arrow::write_parquet(semiannual_data, "data/semiannual.parquet")

# Vulnerable & Special Population
arrow::write_parquet(
  vulnerable_population,
  "data/vulnerable-population.parquet"
)

message("All datasets written successfully.")
