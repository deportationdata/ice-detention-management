library(tidyr)
library(dplyr)
library(purrr)
library(stringr)
library(readxl)
library(glue)
library(lubridate)

fls <- list.files("spreadsheets", pattern = "\\.xlsx$", full.names = TRUE)

# Per-file (file, pattern) pairs to skip — the sheet matching `pattern` in
# `file` contains data from a different fiscal year than the filename claims,
# so reading it would mis-tag rows. Each row in this table tells get_sheet
# to return NA for that combination.
sheet_skiplist <- tibble::tribble(
  ~file,                              ~pattern,
  # Detention sheet here is a stale 06/20/2020 (FY20) snapshot — ATD/Facilities
  # in this file are correctly FY21 and stay.
  "FY21_detentionStats060921.xlsx",   "^Detention",
  # Forward-looking FY24 content sitting inside an FY23-named workbook —
  # Detention/Facilities are correctly FY23 EOFY and stay.
  "FY23_detentionStats.xlsx",         "^ATD",
  "FY23_detentionStats.xlsx",         "ICLOS|Detainee",
  "FY23_detentionStats.xlsx",         "Bond",
  "FY23_detentionStats.xlsx",         "Semiannual",
  "FY23_detentionStats.xlsx",         "Vulnerable"
)

get_sheet <- function(path, pattern) {
  if (any(sheet_skiplist$file == basename(path) & sheet_skiplist$pattern == pattern)) {
    return(NA_character_)
  }
  shts <- excel_sheets(path)
  matches <- shts[str_detect(shts, pattern)]
  if (length(matches) == 0) return(NA_character_)
  # Drop EOFY archive sheets unless the EOFY year matches the filename FY —
  # i.e. keep "Detention EOFY2020" inside FY20-detentionstats.xlsx (canonical
  # source of FY20 data) but drop it inside FY21/FY23/FY26 files where it's a
  # stale duplicate of FY20.
  file_fy <- as.integer(str_extract(basename(path), "(?<=FY)\\d{2}"))
  if (!is.na(file_fy)) file_fy <- file_fy + 2000L
  eofy_year <- as.integer(str_extract(matches, "(?<=EOFY)\\d{2,4}"))
  eofy_year <- if_else(!is.na(eofy_year) & eofy_year < 100L,
                       eofy_year + 2000L, eofy_year)
  is_eofy <- str_detect(matches, "EOFY")
  matches <- matches[!is_eofy | (!is.na(file_fy) & eofy_year == file_fy)]
  if (length(matches) == 0) return(NA_character_)
  # Among the remainder prefer non-EOFY in case of ties.
  non_eofy <- matches[!str_detect(matches, "EOFY")]
  if (length(non_eofy) > 0) non_eofy[1] else matches[1]
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

read_col_a <- function(path, sheet) {
  read_excel(path, sheet = sheet, range = cell_cols("A"), col_names = FALSE)
}

safe_map <- function(fls, fn) {
  result <- fls |>
    set_names() |>
    map_dfr(possibly(fn, otherwise = NULL), .id = "file")
  if (!"file" %in% names(result)) {
    result$file <- character(0)
  }
  result
}

parse_fy <- function(x) {
  digits <- as.integer(str_extract(x, "\\d+"))
  if_else(digits < 100L, digits + 2000L, digits)
}

add_fy_date_cols <- function(df) {
  df |>
    mutate(
      month_num = match(month, month.abb),
      cy = fiscal_year + if_else(month_num >= 10, -1L, 0L)
    ) |>
    mutate(date = make_date(cy, month_num, 1L), .keep = "unused")
}

# text + Oct-Sep + Total
monthly_col_types <- c("text", rep("numeric", 13))

# In the ADP and Avg-Length-of-Stay tables, column A is labeled "Agency" but
# actually contains a mix of arresting-agency totals ("CBP Average", "ICE Average",
# "Average") followed by 2-3 criminality breakdowns under each agency. Lift those
# into separate `agency` and `criminality` columns. FY19 has 2 criminalities,
# FY20+ have 3; this works for both.
lift_agency_criminality <- function(df) {
  # Drop trailing empty padding rows — FY19 has 9 data rows but the 13-row read
  # range pads the rest with NA, which would otherwise inherit "Total" from the
  # last filled agency.
  df <- df[!is.na(df[[1]]), ]
  first <- df[[1]]
  is_agency_row <- str_detect(first, "Average\\s*$")
  agency_for_row <- case_when(
    is_agency_row & str_detect(first, "^CBP") ~ "CBP",
    is_agency_row & str_detect(first, "^ICE") ~ "ICE",
    is_agency_row & str_squish(first) == "Average" ~ "Total",
    TRUE ~ NA_character_
  )
  criminality_for_row <- if_else(is_agency_row, "Total", first)
  df$agency <- agency_for_row
  df <- tidyr::fill(df, agency)
  df$criminality <- criminality_for_row
  df[, c("agency", "criminality", names(df)[2:(ncol(df) - 2)])]
}

# Generic extractor: find sheet + anchor row, read offset range, post-process.
# `range` is a function of the anchor row index.
extract_table <- function(
  path,
  sheet_pattern,
  anchor_pattern,
  range,
  col_types = NULL,
  col_names = TRUE,
  na = "",
  post = identity,
  skip_if = NULL
) {
  sheet <- get_sheet(path, sheet_pattern)
  if (is.na(sheet)) {
    return(NULL)
  }
  if (!is.null(skip_if) && isTRUE(skip_if(path, sheet))) {
    return(NULL)
  }
  anchor <- find_table_start(path, sheet, anchor_pattern)
  if (length(anchor) == 0) {
    return(NULL)
  }
  read_excel(
    path,
    sheet = sheet,
    range = range(anchor[1]),
    col_types = col_types,
    col_names = col_names,
    na = na
  ) |>
    post()
}

# Per-file dates. We collect two sources of "when was this data extracted":
#   1. The Facilities tab "Source: ICE IIDS, MM/DD/YYYY" line (one per file).
#   2. The per-table "EID as of MM/DD/YYYY" date from the Footnotes tab — one
#      per output table family (book-ins, book-outs, ADP, removals, currently
#      detained). ICE maintains these independently, and the Facilities IIDS
#      can be months or years out of sync with the Detention sheet's actual
#      EID. Downstream we prefer the per-table EID and fall back to Facilities
#      IIDS only when absent (ATD/ICLOS/bond/segregation/etc., where there's
#      no equivalent Footnotes entry).
file_pull_dates <-
  fls |>
  set_names() |>
  map_dfr(
    \(.x) {
      sheet <- get_sheet(.x, "^Facilities")
      if (is.na(sheet)) return(NULL)
      fac <- read_excel(
        .x, sheet = sheet, range = "A1:A7", col_names = "src"
      ) |>
        filter(str_detect(src, "IIDS|^Source:")) |>
        slice(1)
      if (nrow(fac) == 0) return(NULL)

      # Per-table EID dates from the Footnotes tab. Pattern matches the row
      # label in column A; date is extracted from column B's narrative.
      eid <- function(pattern) NA_character_
      shts <- excel_sheets(.x)
      if ("Footnotes" %in% shts) {
        foot <- tryCatch(
          read_excel(.x, sheet = "Footnotes", col_names = FALSE),
          error = function(e) NULL
        )
        if (!is.null(foot) && ncol(foot) >= 2) {
          col_a <- replace_na(as.character(foot[[1]]), "")
          col_b <- as.character(foot[[2]])
          eid <- function(pattern) {
            hits <- which(str_detect(col_a, pattern))
            if (length(hits) == 0) return(NA_character_)
            str_extract(
              col_b[hits[1]],
              "(?<=EID as of )\\d{1,2}/\\d{1,2}/\\d{4}"
            )
          }
        }
      }

      tibble(
        src = fac$src,
        eid_bookins  = eid("ICE Initial Book-Ins"),
        eid_bookouts = eid("ICE Final (Book Outs|Releases)"),
        eid_adp      = eid("ICE Average Daily Population"),
        eid_removals = eid("ICE Removals"),
        eid_detained = eid("ICE Currently Detained Population Breakdown")
      )
    },
    .id = "file"
  ) |>
  mutate(
    fy = str_extract(file, "(?<=FY)\\d{2}"),

    # last 6- or 8-digit run anywhere in the filename
    date_str = str_extract_all(file, "\\d{6,8}") |>
      map_chr(~ dplyr::last(.x, default = NA_character_)),

    # 8 digits => MMDDYYYY; 6 digits => MMDDYY if leading mm<=12 else YYMMDD
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
    pull_date_facilities = str_extract(src, "\\d{1,2}/\\d{1,2}/\\d{4}") |> mdy(),
    pull_date_bookins  = mdy(eid_bookins,  quiet = TRUE),
    pull_date_bookouts = mdy(eid_bookouts, quiet = TRUE),
    pull_date_adp      = mdy(eid_adp,      quiet = TRUE),
    pull_date_removals = mdy(eid_removals, quiet = TRUE),
    pull_date_detained = mdy(eid_detained, quiet = TRUE),
    fiscal_year = as.integer(str_c("20", fy))
  ) |>
  select(
    file, fiscal_year, file_date,
    pull_date_facilities, pull_date_bookins, pull_date_bookouts,
    pull_date_adp, pull_date_removals, pull_date_detained
  )

# Resolve `pull_date` for a given table kind: per-table EID if present, else
# fall back to the Facilities tab IIDS date. Pass kind = "facilities" for
# tables anchored on the Facilities tab (ATD, ICLOS, bond, segregation,
# semiannual, vulnerable, facilities itself) — those keep prior behavior.
file_meta_for <- function(kind) {
  kind_col <- paste0("pull_date_", kind)
  file_pull_dates |>
    transmute(
      file,
      fiscal_year,
      file_date,
      pull_date = coalesce(.data[[kind_col]], pull_date_facilities)
    )
}

# "Currently Detained by Criminality" anchor at A; agency table sits at I(anchor+1):V(anchor+4)
book_ins_by_arresting_agency <-
  safe_map(fls, \(.x) {
    extract_table(
      .x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Currently Detained by Criminality",
      range = \(r) glue("I{r+1}:V{r+4}"),
      col_types = c("text", rep("numeric", 13))
    )
  }) |>
  left_join(file_meta_for("bookins"), by = "file") |>
  select(-file) |>
  pivot_longer(cols = Oct:Sep, names_to = "month", values_to = "n_book_ins") |>
  rename(arresting_agency = Agency) |>
  add_fy_date_cols() |>
  rename(n_book_ins_ytd = Total) |>
  relocate(
    arresting_agency,
    month,
    date,
    n_book_ins,
    n_book_ins_ytd,
    fiscal_year,
    file_date,
    pull_date
  )

book_outs_by_reason <-
  safe_map(fls, \(.x) {
    # The number of release reasons grew over time (FY21-23: 6, FY24-25: 12,
    # FY26: 13). Find the next section header to bound the read dynamically
    # rather than hardcoding a row count.
    sheet <- get_sheet(.x, "^Detention")
    if (is.na(sheet)) return(NULL)
    col_a <- read_col_a(.x, sheet)[[1]]
    a <- which(str_detect(
      col_a,
      "ICE Final (Book Outs|Releases) by Release Reason, Month and Criminality"
    ))
    if (length(a) == 0) return(NULL)
    r <- a[1]
    after_r <- which(str_detect(
      col_a,
      "^ICE [A-Z]|^Aliens|^Currently|^Noncitizens|^Individuals"
    ) & seq_along(col_a) > r)
    end_row <- if (length(after_r) > 0) after_r[1] - 1 else length(col_a)
    if (end_row < r + 5) return(NULL)
    read_excel(
      .x, sheet = sheet,
      range = glue("A{r+1}:O{end_row}"),
      col_types = c("text", "text", rep("numeric", 13)),
      col_names = TRUE, na = ""
    ) |>
      filter(!is.na(`Release Reason`) | !is.na(Criminality)) |>
      fill(`Release Reason`)
  }) |>
  left_join(file_meta_for("bookouts"), by = "file") |>
  select(-file) |>
  pivot_longer(cols = Oct:Sep, names_to = "month", values_to = "n_book_outs") |>
  add_fy_date_cols() |>
  rename(n_book_outs_ytd = Total) |>
  janitor::clean_names() |>
  # Clean up source-data inconsistencies in release_reason:
  #   - Some FY24 files (Feb-Apr 2024) have a typo with " Total" appended
  #     (e.g., "Relief Granted by IJ Total"). Strip the trailing " Total".
  #   - FY26 files use inconsistent capitalization ("Bonded out" vs "Bonded
  #     Out", "Order of supervision" vs "Order of Supervision"). Canonicalize.
  mutate(release_reason = release_reason |>
    str_replace(" Total$", "") |>
    str_replace_all("Bonded out", "Bonded Out") |>
    str_replace_all("Order of supervision", "Order of Supervision")
  ) |>
  relocate(
    release_reason,
    criminality,
    month,
    date,
    n_book_outs,
    n_book_outs_ytd,
    fiscal_year,
    file_date,
    pull_date
  )

# Older files (FY19-FY21): annual totals by criminality columns instead of monthly
book_outs_by_reason_annual <-
  safe_map(fls, \(.x) {
    extract_table(
      .x,
      sheet_pattern = "^Detention",
      anchor_pattern = "ICE Final (Book Outs|Releases) by Release Reason",
      range = \(r) glue("A{r+1}:E{r+9}"),
      col_types = c("text", rep("numeric", 4)),
      skip_if = \(p, s) {
        length(find_table_start(
          p,
          s,
          "ICE Final (Book Outs|Releases) by Release Reason, Month and Criminality"
        )) >
          0
      },
      post = \(df) {
        # rename must run per-file so map_dfr can bind on consistent column names
        names(df) <- c(
          "release_reason",
          "convicted_criminal",
          "pending_criminal_charges",
          "other_immigration_violator",
          "total"
        )
        df
      }
    )
  }) |>
  filter(!is.na(release_reason)) |>
  left_join(file_meta_for("bookouts"), by = "file") |>
  select(-file)

book_outs_annual_from_monthly <-
  book_outs_by_reason |>
  filter(month == "Oct") |>
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
  safe_map(fls, \(.x) {
    extract_table(
      .x,
      sheet_pattern = "^Detention",
      anchor_pattern = "ICE Average Daily Population by Arresting Agency, Month and Criminality",
      range = \(r) glue("A{r+1}:N{r+13}"),
      col_types = monthly_col_types,
      post = lift_agency_criminality
    )
  }) |>
  left_join(file_meta_for("adp"), by = "file") |>
  select(-file) |>
  drop_na(agency) |>
  pivot_longer(cols = Oct:Sep, names_to = "month", values_to = "adp") |>
  add_fy_date_cols() |>
  rename(adp_fy_ytd = `FY Overall`) |>
  janitor::clean_names() |>
  relocate(
    agency,
    criminality,
    month,
    date,
    adp,
    adp_fy_ytd,
    fiscal_year,
    file_date,
    pull_date
  )

avg_stay_length_by_agency_criminality <-
  safe_map(fls, \(.x) {
    extract_table(
      .x,
      sheet_pattern = "^Detention",
      anchor_pattern = "ICE Average Length of Stay by Arresting Agency, Month and Criminality",
      range = \(r) glue("A{r+1}:N{r+13}"),
      col_types = monthly_col_types,
      post = lift_agency_criminality
    )
  }) |>
  left_join(file_meta_for("adp"), by = "file") |>
  select(-file) |>
  drop_na(agency) |>
  pivot_longer(
    cols = Oct:Sep,
    names_to = "month",
    values_to = "avg_stay_length_days"
  ) |>
  add_fy_date_cols() |>
  rename(avg_stay_length_days_fy_ytd = `FY Overall`) |>
  janitor::clean_names() |>
  relocate(
    agency,
    criminality,
    month,
    date,
    avg_stay_length_days,
    avg_stay_length_days_fy_ytd,
    fiscal_year,
    file_date,
    pull_date
  )

detainees_by_facility <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "^Facilities")
    if (is.na(sheet)) return(NULL)
    col_a <- read_col_a(.x, sheet)
    start_row <- which(!is.na(col_a[[1]]) & str_detect(col_a[[1]], "^Name"))
    end_row <- nrow(col_a)
    if (length(start_row) == 0) {
      warning(glue("  [facilities] Header row not found in {basename(.x)}"))
      return(NULL)
    }
    sr <- start_row[1]
    # If row sr+1 is a second "Name" header, advance one row
    test_row <- read_excel(
      .x,
      sheet = sheet,
      range = glue("A{sr + 1}:A{sr + 1}"),
      col_names = FALSE
    )
    if (!is.na(test_row[[1]]) && str_detect(test_row[[1]], "^Name")) {
      sr <- sr + 1
    }
    # The Facilities sheet has 27-31 columns spanning Address, AOR, FY ALOS,
    # Level A-D, gender x criminality breakdowns, ICE threat levels, mandatory
    # beds, and inspection metadata. AE covers the widest year (FY19, 31 cols).
    df <- read_excel(
      .x, sheet = sheet, range = glue("A{sr}:AE{end_row}"),
      .name_repair = "minimal"
    )
    # Drop empty trailing columns (years with fewer than 31 cols come back with
    # blank names) before any tidy op, which can't handle "" column names.
    df <- df[, !is.na(colnames(df)) & colnames(df) != ""]
    df <- filter(df, !is.na(Name))
    # Coerce all columns except identity/text columns to character so
    # bind_rows can align across years that differ in column types (e.g., ICE
    # Threat Level was numeric in FY19-23 and text "5%" in FY25+).
    keep_typed <- c("Name", "Address", "City", "State", "AOR", "Type Detailed",
                    "Male/Female")
    coerce_cols <- setdiff(colnames(df), keep_typed)
    df[coerce_cols] <- lapply(df[coerce_cols], as.character)
    alos_cols <- grep("^FY\\d{2} ALOS$", colnames(df), value = TRUE)
    df <- df |>
      pivot_longer(all_of(alos_cols), values_to = "alos") |>
      filter(!is.na(alos)) |>
      select(-name)
    df
  }) |>
  janitor::clean_names() |>
  # Shorten clean_names output that exceeds Stata's 32-char variable name limit.
  rename_with(\(n) recode(n,
    second_to_last_inspection_standard      = "s2l_inspection_standard",
    last_nakamoto_inspection_standard       = "last_nak_inspection_standard",
    last_nakamoto_inspection_rating_final   = "last_nak_inspection_rating",
    second_to_last_nakamoto_inspection_type = "s2l_nak_inspection_type"
  )) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file) |>
  mutate(
    alos = as.numeric(alos),
    male_female = if_else(
      male_female %in% c("Male", "Female", "Female/Male"),
      male_female,
      NA_character_
    ),
    city = str_squish(city),
    state = if_else(str_squish(state) == "", NA_character_, state)
  ) |>
  distinct()

# "Book-Ins by Facility/Criminality" anchor; the cell at P(anchor+2) is the
# fiscal-year-to-date cumulative removals total — one row per snapshot file,
# count is as of pull_date (not file_date).
removals <-
  safe_map(fls, \(.x) {
    extract_table(
      .x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Book-Ins by",
      range = \(r) glue("P{r+2}:P{r+2}"),
      col_names = "n_removals_fy_ytd",
      col_types = "numeric"
    )
  }) |>
  left_join(file_meta_for("removals"), by = "file") |>
  select(-file)

# Disposition table at A(anchor+1):D(anchor+5)
currently_detained_by_disposition <-
  safe_map(fls, \(.x) {
    extract_table(
      .x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Currently Detained by Processing Disposition",
      # 6 rows = 1 header + up to 5 data rows. FY26+ tables include "Other"
      # (row 5) in addition to Total / Expedited / NTA / Reinstatement; older
      # tables have only 4 data rows and the trailing read row comes back NA.
      range = \(r) glue("A{r+1}:D{r+6}"),
      col_types = c("text", "numeric", "numeric", "numeric"),
      na = c("", "-"),
      post = \(df) {
        names(df)[1] <- "disposition"
        df <- df |> select(-starts_with("...")) |> filter(!is.na(disposition))
        # Map header text to canonical names: FRC/FSC -> fsc_frc, Adult ->
        # adult, Total -> total. FY24/FY25 dropped FSC, so the surviving
        # numeric columns are just Adult and Total — content-based renaming
        # ensures values land in the right columns regardless of position.
        canonical <- c(
          "FRC" = "fsc_frc", "FSC" = "fsc_frc",
          "Adult" = "adult", "Total" = "total"
        )
        for (i in seq_along(names(df))[-1]) {
          h <- names(df)[i]
          if (h %in% names(canonical)) names(df)[i] <- canonical[[h]]
        }
        df
      }
    )
  }) |>
  left_join(file_meta_for("detained"), by = "file") |>
  select(-file)

# G(anchor+1):K(anchor+2) — release fiscal year + (gap) + FSC | Adult | Total
fear_decision_time <-
  safe_map(fls, \(.x) {
    extract_table(
      .x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Currently Detained by Processing Disposition",
      range = \(r) glue("G{r+1}:K{r+2}"),
      post = \(df) {
        df <- df |> select(where(~ !all(is.na(.x))))
        if (ncol(df) == 0 || nrow(df) == 0) {
          return(NULL)
        }
        names(df)[1] <- "data_fiscal_year"
        num_cols <- names(df)[-1]
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
        df |>
          mutate(data_fiscal_year = parse_fy(data_fiscal_year)) |>
          select(data_fiscal_year, fsc, adult, total)
      }
    )
  }) |>
  left_join(file_meta_for("detained"), by = "file") |>
  select(-file)

# Fear-decisions-by-facility-type table sits to the right of the disposition
# table. Its label column shifts between M and N depending on whether FSC is
# present in the disposition table — so locate the label column by content
# (cells containing Total/FSC/Adult/FRC) rather than hardcoding it.
fear_decisions_by_facility_type <-
  safe_map(fls, \(.x) {
    extract_table(
      .x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Currently Detained by Processing Disposition",
      range = \(r) glue("M{r+1}:Q{r+4}"),
      col_names = FALSE,
      post = \(df) {
        if (ncol(df) < 2 || nrow(df) == 0) return(NULL)
        # Find the column whose values include facility-type labels.
        label_col <- NA_integer_
        for (i in seq_len(ncol(df))) {
          vals <- as.character(df[[i]])
          if (any(vals %in% c("Total", "FSC", "FRC", "Adult"), na.rm = TRUE)) {
            label_col <- i
            break
          }
        }
        if (is.na(label_col) || label_col >= ncol(df)) return(NULL)
        # Find the first numeric column to the right of the label column.
        count_col <- NA_integer_
        for (i in (label_col + 1):ncol(df)) {
          vals <- suppressWarnings(as.numeric(df[[i]]))
          if (any(!is.na(vals))) {
            count_col <- i
            break
          }
        }
        if (is.na(count_col)) return(NULL)
        tibble::tibble(
          facility_type = as.character(df[[label_col]]),
          total_detained = suppressWarnings(as.numeric(df[[count_col]]))
        ) |>
          # Drop the header row ("Detention Facility Type" / "Type") that
          # ends up in the same column as the labels, and any empty rows.
          filter(
            !is.na(facility_type),
            !facility_type %in% c("Detention Facility Type", "Type")
          )
      }
    )
  }) |>
  left_join(file_meta_for("detained"), by = "file") |>
  select(-file)

currently_detained_by_criminality <-
  safe_map(fls, \(.x) {
    extract_table(
      .x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Currently Detained by Criminality",
      range = \(r) glue("A{r+1}:F{r+4}"),
      col_types = c("text", rep("numeric", 5))
    )
  }) |>
  janitor::clean_names() |>
  left_join(file_meta_for("detained"), by = "file") |>
  select(-file)

# Range r+4 covers all 3 facility types (Total + FSC + Adult) when present;
# r+3 only had room for 2 rows, silently dropping Adult in years with FSC.
# Empty trailing rows are filtered out via filter(!is.na(facility_type)).
book_ins_by_facility_type <-
  safe_map(fls, \(.x) {
    extract_table(
      .x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Book-Ins by",
      range = \(r) glue("A{r+1}:E{r+4}"),
      col_types = c("text", rep("numeric", 4))
    )
  }) |>
  janitor::clean_names() |>
  filter(!is.na(facility_type)) |>
  left_join(file_meta_for("bookins"), by = "file") |>
  select(-file)

# H(anchor+1):J(anchor+4) — see comment on book_ins_by_facility_type for why r+4.
book_outs_by_facility_type <-
  safe_map(fls, \(.x) {
    extract_table(
      .x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Book-Ins by",
      range = \(r) glue("H{r+1}:J{r+4}"),
      post = \(df) {
        df <- df |> select(where(~ !all(is.na(.x))))
        if (ncol(df) == 0 || nrow(df) == 0) {
          return(NULL)
        }
        names(df) <- c("facility_type", "total")[seq_len(ncol(df))]
        df$facility_type <- as.character(df$facility_type)
        filter(df, !is.na(facility_type))
      }
    )
  }) |>
  left_join(file_meta_for("bookouts"), by = "file") |>
  select(-file)

# FAMU/FRC removals at P(anchor+3)
famu_removals <-
  safe_map(fls, \(.x) {
    extract_table(
      .x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Book-Ins by",
      range = \(r) glue("P{r+3}:P{r+3}"),
      col_names = "famu_removals",
      col_types = "numeric"
    )
  }) |>
  left_join(file_meta_for("removals"), by = "file") |>
  select(-file)

atd_population <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "^ATD")
    if (is.na(sheet)) {
      return(NULL)
    }
    col_a <- read_col_a(.x, sheet)

    read_atd_block <- function(label_pattern, n_rows, table_name) {
      r <- which(str_detect(col_a[[1]], label_pattern))
      if (length(r) == 0) {
        return(NULL)
      }
      r <- r[1]
      read_excel(
        .x,
        sheet = sheet,
        range = glue("A{r+1}:C{r+n_rows}"),
        col_types = c("text", rep("numeric", 2))
      ) |>
        setNames(c("category", "count", "value")) |>
        filter(!is.na(category)) |>
        mutate(table = table_name)
    }

    bind_rows(
      read_atd_block(
        "ATD Active Population Counts|ATD Active Participants",
        8,
        "technology"
      ),
      read_atd_block("ATD Active Population by Status", 6, "status")
    )
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file)

atd_by_aor <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "^ATD")
    if (is.na(sheet)) {
      return(NULL)
    }
    col_a <- read_col_a(.x, sheet)
    aor_row <- which(str_detect(col_a[[1]], "Active ATD Participants.*by AOR"))
    if (length(aor_row) == 0) {
      return(NULL)
    }
    r <- aor_row[1]
    end_row <- nrow(col_a)
    read_excel(
      .x,
      sheet = sheet,
      range = glue("A{r+1}:C{end_row}"),
      col_types = c("text", rep("numeric", 2))
    ) |>
      setNames(c("aor_technology", "count", "avg_length_in_program")) |>
      filter(!is.na(aor_technology))
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file) |>
  distinct()

# Court Appearance header at [r,cc]; below: Metric|Count|% header at r+1, data at r+2..r+4
atd_court_appearances <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "^ATD")
    if (is.na(sheet)) {
      return(NULL)
    }
    df <- read_excel(.x, sheet = sheet, col_names = FALSE)
    m <- as.matrix(df)
    storage.mode(m) <- "character"

    mask <- !is.na(m) & str_detect(m, "Court Appearance")
    dim(mask) <- dim(m)
    hits <- which(mask, arr.ind = TRUE)
    if (nrow(hits) == 0) {
      return(NULL)
    }

    map_dfr(seq_len(nrow(hits)), \(i) {
      hr <- hits[i, "row"]
      cc <- hits[i, "col"]
      if (hr + 2 > nrow(m) || cc + 2 > ncol(m)) {
        return(NULL)
      }
      hearing_type <- case_when(
        str_detect(m[hr, cc], "Total") ~ "total",
        str_detect(m[hr, cc], "Final") ~ "final",
        TRUE ~ "unknown"
      )
      rows <- (hr + 2):min(hr + 4, nrow(m))
      tibble(
        hearing_type = hearing_type,
        metric = m[rows, cc],
        count = suppressWarnings(as.numeric(m[rows, cc + 1])),
        pct = suppressWarnings(as.numeric(m[rows, cc + 2]))
      ) |>
        filter(
          !is.na(metric),
          metric %in% c("Attended", "Failed to Attend", "Total")
        )
    })
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file)

# ICLOS sheet has very wide format (72+ cols, varying year ranges); keep
# per-section summaries rather than the full wide table.
iclos_and_detainees <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "ICLOS|Detainee")
    if (is.na(sheet)) {
      return(NULL)
    }
    df <- read_excel(.x, sheet = sheet, col_names = FALSE)
    if (nrow(df) < 7) {
      return(NULL)
    }
    col_a <- df[[1]]

    iclos_header <- which(str_detect(col_a, "^Population") & !is.na(col_a))
    if (length(iclos_header) == 0) {
      return(NULL)
    }
    det_header <- which(str_detect(col_a, "^Detainees$") & !is.na(col_a))

    sections <- list(iclos = iclos_header[1])
    if (length(det_header) > 0) {
      det_pop <- iclos_header[iclos_header > det_header[1]]
      if (length(det_pop) > 0) sections[["detainees"]] <- det_pop[1]
    }

    map_dfr(names(sections), \(section_name) {
      pop_row <- sections[[section_name]]
      data_start <- pop_row + 3
      if (data_start > length(col_a)) {
        return(NULL)
      }
      remaining <- col_a[data_start:length(col_a)]
      non_na_rows <- which(!is.na(remaining))
      if (length(non_na_rows) == 0) {
        return(NULL)
      }

      # Stop block at gap > 2 rows or at next section header
      end_idx <- length(non_na_rows)
      for (i in seq_along(non_na_rows)) {
        gap <- if (i > 1) non_na_rows[i] - non_na_rows[i - 1] else 0L
        label <- remaining[non_na_rows[i]]
        if (
          gap > 2L ||
            (!is.na(label) && str_detect(label, "^(Detainees|Population)$"))
        ) {
          end_idx <- i - 1L
          break
        }
      }
      if (end_idx < 1L) {
        return(NULL)
      }
      data_end <- data_start + max(non_na_rows[seq_len(end_idx)]) - 1L

      map_dfr(seq(data_start, data_end), \(r) {
        pop_label <- col_a[r]
        if (is.na(pop_label)) {
          return(NULL)
        }
        vals <- suppressWarnings(as.numeric(unlist(df[r, 2:ncol(df)])))
        vals <- vals[!is.na(vals) & vals != 0]
        if (length(vals) == 0) {
          return(NULL)
        }
        tibble(
          section = section_name,
          population = pop_label,
          n_observations = length(vals),
          latest_value = dplyr::last(vals)
        )
      })
    })
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file) |>
  distinct()

monthly_bond_stats <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "Bond")
    if (is.na(sheet)) {
      return(NULL)
    }
    # Title/empty/date rows live in 1-3; data rows are text + numeric below
    df <- read_excel(.x, sheet = sheet, col_names = FALSE, skip = 3)
    if (nrow(df) < 5) {
      return(NULL)
    }
    n_cols <- ncol(df)

    col_a <- df[[1]]
    metric_specs <- tribble(
      ~name             , ~pattern                                   ,
      "total_book_outs" , "Total ICE Final (Book Outs|Releases)"     ,
      "bond_book_outs"  , "ICE Final (Book Outs|Releases) with Bond" ,
      "bond_pct"        , "Bond Posted.*%"                           ,
      "avg_bond_amount" , "Average Bond Amount"                      ,
      "alos_days"       , "ALOS"
    ) |>
      mutate(
        row = map_int(pattern, \(p) {
          r <- which(str_detect(col_a, p))
          if (length(r) == 0) NA_integer_ else r[1]
        })
      ) |>
      filter(!is.na(row))

    if (!"total_book_outs" %in% metric_specs$name) {
      return(NULL)
    }

    # Date row is row 3 of the original sheet. readxl returns POSIXct columns;
    # `do.call(c, ...)` preserves that class across concatenation, then as.Date
    # converts cleanly. (Bare unlist would drop the class to seconds-since-epoch.)
    dates <- read_excel(
      .x,
      sheet = sheet,
      col_names = FALSE,
      range = cell_limits(c(3, 2), c(3, n_cols)),
      col_types = "date"
    )
    date_vals <- as.Date(do.call(c, dates))

    pmap_dfr(metric_specs, \(name, pattern, row) {
      vals <- unlist(df[row, -1], use.names = FALSE)
      tibble(date = date_vals, metric = name, value = vals) |>
        filter(!is.na(value), value != 0, !is.na(date))
    })
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file) |>
  mutate(month = month.abb[month(date)]) |>
  relocate(month, date, metric, value, fiscal_year, file_date, pull_date)

monthly_segregation <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "Segregation")
    if (is.na(sheet)) {
      return(NULL)
    }
    df <- read_excel(.x, sheet = sheet, col_names = FALSE)
    col_a <- as.character(df[[1]])
    col_b <- suppressWarnings(as.numeric(as.character(df[[2]])))

    # Month headers like "November 2025\nThis Segregation..."
    month_pattern <- "^(January|February|March|April|May|June|July|August|September|October|November|December)\\s+\\d{4}"
    skip_pattern <- "^(Facilities|Placement Count|Grand Total|U\\.S\\.)|Segregation Review"

    is_month <- !is.na(col_a) & str_detect(col_a, month_pattern)
    is_skip <- !is.na(col_a) & str_detect(col_a, skip_pattern)

    tibble(
      month_raw = if_else(
        is_month,
        str_extract(col_a, month_pattern),
        NA_character_
      ),
      facility = col_a,
      placement_count = col_b,
      drop_row = is_month | is_skip
    ) |>
      fill(month_raw) |>
      filter(
        !drop_row,
        !is.na(month_raw),
        !is.na(facility),
        !is.na(placement_count)
      ) |>
      transmute(
        date = myd(paste0(month_raw, " 1")),
        month = month.abb[month(date)],
        facility = facility,
        placement_count = placement_count
      )
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file) |>
  relocate(
    month,
    date,
    facility,
    placement_count,
    fiscal_year,
    file_date,
    pull_date
  )

# Armed Forces / US Citizens / Parents of USC / TPS country tables, all on the
# same sheet, all keyed off a header row in column A.
semiannual_data <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "Semiannual")
    if (is.na(sheet)) {
      return(NULL)
    }
    df <- read_excel(.x, sheet = sheet, col_names = FALSE)
    col_a <- as.character(df[[1]])

    # 2-column tables: Fiscal Year | Value
    simple_specs <- tribble(
      ~name                   , ~pattern                           ,
      "armed_forces_arrests"  , "Armed Forces.*Arrests"            ,
      "armed_forces_bookins"  , "Armed Forces.*Bookins"            ,
      "armed_forces_removals" , "Armed Forces.*Removals"           ,
      "us_citizen_arrests"    , "United States Citizen Arrests"    ,
      "us_citizen_bookins"    , "United States Citizens? Bookins"  ,
      "us_citizen_removals"   , "United States Citizens? Removals" ,
      "parents_usc_arrests"   , "Parents of USC? Arrests"          ,
      "parents_usc_bookins"   , "Parents of USC? Bookins"          ,
      "parents_usc_removals"  , "Parents of USC? Removals"
    )
    simple_results <- pmap_dfr(simple_specs, \(name, pattern) {
      hr <- which(str_detect(col_a, pattern))
      if (length(hr) == 0) {
        return(NULL)
      }
      hr <- hr[1]
      if (hr + 2 > nrow(df)) {
        return(NULL)
      }
      rows <- (hr + 2):min(hr + 12, nrow(df))
      fy_labels <- col_a[rows]
      keep <- cumall(
        !is.na(fy_labels) & str_detect(replace_na(fy_labels, ""), "^FY")
      )
      rows <- rows[keep]
      if (length(rows) == 0) {
        return(NULL)
      }
      tibble(
        table_name = name,
        data_fiscal_year = parse_fy(col_a[rows]),
        country = NA_character_,
        value = suppressWarnings(as.numeric(unlist(df[rows, 2])))
      )
    })

    # Multi-column tables: Country | FY values
    tps_specs <- tribble(
      ~name          , ~pattern                                        ,
      "tps_arrests"  , "Temporary Protected Status Countries Arrests"  ,
      "tps_bookins"  , "Temporary Protected Status Countries Bookins"  ,
      "tps_removals" , "Temporary Protected Status Countries Removals"
    )
    tps_results <- pmap_dfr(tps_specs, \(name, pattern) {
      hr <- which(str_detect(col_a, pattern))
      if (length(hr) == 0) {
        return(NULL)
      }
      hr <- hr[1]
      if (hr + 2 > nrow(df)) {
        return(NULL)
      }
      fy_header <- as.character(unlist(df[hr + 1, ]))
      fy_cols <- which(str_detect(replace_na(fy_header, ""), "^FY\\d{4}$"))
      if (length(fy_cols) == 0) {
        return(NULL)
      }
      fy_names <- fy_header[fy_cols]

      rows <- (hr + 2):min(hr + 25, nrow(df))
      countries <- col_a[rows]
      keep <- cumall(!is.na(countries) & countries != "")
      rows <- rows[keep]
      if (length(rows) == 0) {
        return(NULL)
      }

      val_chr <- as.matrix(df[rows, fy_cols, drop = FALSE])
      val_num <- suppressWarnings(matrix(
        as.numeric(val_chr),
        nrow = length(rows),
        ncol = length(fy_cols)
      ))
      tibble(
        table_name = name,
        data_fiscal_year = rep(parse_fy(fy_names), each = length(rows)),
        country = rep(col_a[rows], times = length(fy_cols)),
        value = as.vector(val_num)
      ) |>
        filter(!is.na(value))
    })

    bind_rows(simple_results, tps_results)
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file)

vulnerable_population <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "Vulnerable")
    if (is.na(sheet)) {
      return(NULL)
    }
    df <- read_excel(.x, sheet = sheet, col_names = FALSE)
    col_a <- as.character(df[[1]])

    quarter_rows <- which(str_detect(
      col_a,
      "^Fiscal Year \\(FY\\)\\s+\\d{4} Quarter \\d"
    ))
    if (length(quarter_rows) == 0) {
      return(NULL)
    }

    # Header at qr+1: Placement Reason | Number of Placements | Avg Consecutive | Avg Cumulative
    map_dfr(quarter_rows, \(qr) {
      fy_quarter <- str_extract(col_a[qr], "\\d{4} Quarter \\d")
      if (qr + 2 > nrow(df)) {
        return(NULL)
      }
      rows <- (qr + 2):min(qr + 8, nrow(df))
      reasons <- col_a[rows]
      keep <- cumall(
        !is.na(reasons) & !str_detect(replace_na(reasons, ""), "^\\*|^$")
      )
      rows <- rows[keep]
      if (length(rows) == 0) {
        return(NULL)
      }
      tibble(
        fy_quarter = fy_quarter,
        placement_reason = col_a[rows],
        n_placements = suppressWarnings(as.numeric(unlist(df[rows, 2]))),
        avg_consecutive_days = suppressWarnings(as.numeric(unlist(df[
          rows,
          3
        ]))),
        avg_cumulative_days = suppressWarnings(as.numeric(unlist(df[rows, 4])))
      )
    })
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file) |>
  mutate(
    data_fiscal_year = as.integer(str_extract(fy_quarter, "\\d{4}")),
    data_quarter = as.integer(str_extract(fy_quarter, "(?<=Quarter )\\d"))
  ) |>
  select(-fy_quarter) |>
  relocate(
    placement_reason,
    data_fiscal_year,
    data_quarter,
    n_placements,
    avg_consecutive_days,
    avg_cumulative_days,
    fiscal_year,
    file_date,
    pull_date
  )

adp_and_stay_length_by_agency <-
  full_join(
    adp_by_agency_criminality,
    avg_stay_length_by_agency_criminality,
    by = c(
      "agency", "criminality", "month", "date",
      "fiscal_year", "file_date", "pull_date"
    )
  ) |>
  relocate(
    agency,
    criminality,
    month,
    date,
    adp,
    adp_fy_ytd,
    avg_stay_length_days,
    avg_stay_length_days_fy_ytd,
    fiscal_year,
    file_date,
    pull_date
  )

flows_by_facility_type <-
  full_join(
    book_ins_by_facility_type |>
      distinct() |>
      rename(
        n_book_ins_convicted_criminal = convicted_criminal,
        n_book_ins_pending_charges = pending_criminal_charges,
        n_book_ins_other_imm_violator = other_immigration_violator,
        n_book_ins_total = total
      ),
    book_outs_by_facility_type |>
      distinct() |>
      rename(n_book_outs_total = total),
    by = c("facility_type", "fiscal_year", "file_date", "pull_date")
  ) |>
  relocate(
    facility_type,
    n_book_ins_convicted_criminal,
    n_book_ins_pending_charges,
    n_book_ins_other_imm_violator,
    n_book_ins_total,
    n_book_outs_total,
    fiscal_year,
    file_date,
    pull_date
  )

pull_totals <-
  full_join(
    distinct(removals),
    distinct(famu_removals),
    by = c("fiscal_year", "file_date", "pull_date")
  ) |>
  relocate(n_removals_fy_ytd, famu_removals, fiscal_year, file_date, pull_date)

dir.create("data", showWarnings = FALSE, recursive = TRUE)

nanoparquet::write_parquet(
  book_ins_by_arresting_agency,
  "data/book-ins-by-arresting-agency.parquet"
)
nanoparquet::write_parquet(
  book_outs_by_reason,
  "data/book-outs-by-reason-monthly.parquet"
)
nanoparquet::write_parquet(
  book_outs_by_reason_all_years,
  "data/book-outs-by-reason-annual.parquet"
)
nanoparquet::write_parquet(
  adp_and_stay_length_by_agency,
  "data/adp-and-stay-length-by-agency.parquet"
)
nanoparquet::write_parquet(pull_totals, "data/pull-totals.parquet")
nanoparquet::write_parquet(detainees_by_facility, "data/facilities.parquet")
nanoparquet::write_parquet(
  currently_detained_by_disposition,
  "data/currently-detained-by-disposition.parquet"
)
nanoparquet::write_parquet(
  fear_decision_time,
  "data/fear-decision-time.parquet"
)
nanoparquet::write_parquet(
  fear_decisions_by_facility_type,
  "data/fear-decisions-by-facility-type.parquet"
)
nanoparquet::write_parquet(
  currently_detained_by_criminality,
  "data/currently-detained-by-criminality.parquet"
)
nanoparquet::write_parquet(
  flows_by_facility_type,
  "data/flows-by-facility-type.parquet"
)
nanoparquet::write_parquet(atd_population, "data/atd-population.parquet")
nanoparquet::write_parquet(atd_by_aor, "data/atd-by-aor.parquet")
nanoparquet::write_parquet(
  atd_court_appearances,
  "data/atd-court-appearances.parquet"
)
nanoparquet::write_parquet(
  iclos_and_detainees,
  "data/iclos-and-detainees.parquet"
)
nanoparquet::write_parquet(monthly_bond_stats, "data/bond-stats.parquet")
nanoparquet::write_parquet(monthly_segregation, "data/segregation.parquet")
nanoparquet::write_parquet(
  semiannual_data,
  "data/special-population-actions.parquet"
)
nanoparquet::write_parquet(
  vulnerable_population,
  "data/vulnerable-population.parquet"
)

# Mirror every parquet as xlsx and dta for downstream Stata/Excel consumers
for (pq in list.files("data", pattern = "\\.parquet$", full.names = TRUE)) {
  df <- nanoparquet::read_parquet(pq)
  base <- tools::file_path_sans_ext(pq)
  writexl::write_xlsx(df, paste0(base, ".xlsx"))
  haven::write_dta(df, paste0(base, ".dta"))
}
