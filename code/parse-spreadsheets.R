library(tidyr)
library(dplyr)
library(purrr)
library(stringr)
library(readxl)
library(glue)
library(lubridate)

dir.create("data", showWarnings = FALSE, recursive = TRUE)

# Skip "~$*.xlsx" lockfiles Excel leaves when a workbook is open — readxl chokes on them.
fls <- list.files("spreadsheets", pattern = "\\.xlsx$", full.names = TRUE)
fls <- fls[!startsWith(basename(fls), "~$")]

# (file, sheet-pattern) pairs whose sheet contains a different FY than the
# filename claims; get_sheet returns NA for these.
sheet_skiplist <- tibble::tribble(
  ~file,                              ~pattern,
  # Sheet is "Detention FY20 YTD" inside an FY21-named file — verified FY20.
  "FY21_detentionStats060921.xlsx",   "^Detention",
  # One-off "comprehensive" facility list with 1,883 rows (vs ~160 in every
  # neighboring file) — looks like an intergovernmental-agreement dump, not
  # a snapshot of operational facilities. Skip the Facilities tab only.
  "2021-01-07_FY21-detentionstats.xlsx", "^Facilities",
  # Corrupted ATD status block (FAMU ALIP=106 vs ~547 in surrounding files;
  # ECMS Count values look swapped) and missing technology block.
  "FY22_detentionStats07212022.xlsx", "^ATD",
  # ATD AOR/Technology header literally says "as of xx/xx/2022" — ICE
  # published a draft with placeholder values throughout the ATD sheet.
  "FY22_detentionStats09152022.xlsx", "^ATD",
  # SmartLINK counts for Seattle / St Paul / Washington DC are doubled
  # (~2.0x vs neighboring files); other 22 AORs unchanged. ICE published the
  # same broken sheet under two filenames (the "b" one is a content dupe).
  "2023-04-13_FY23_detentionStats04122023.xlsx", "^ATD",
  "FY23_detentionStats04122023b.xlsx",           "^ATD"
)

# Files where Adult/FRC values are swapped in just one table within the
# Detention sheet — used by the per-table extractors' skip_if so the rest
# of the Detention sheet's tables (ADP, Currently Detained, etc.) still parse.
BOOKIN_FACILITY_TABLE_BAD <- c(
  "2021-01-14_FY20-detentionstats.xlsx"
)
BOOKOUT_FACILITY_TABLE_BAD <- c(
  "FY21_detention-stats_0301.xlsx",
  "FY21_detention-stats_0317.xlsx"
)

get_sheet <- function(path, pattern) {
  if (any(sheet_skiplist$file == basename(path) & sheet_skiplist$pattern == pattern)) {
    return(NA_character_)
  }
  matches <- excel_sheets(path) |> keep(\(s) str_detect(s, pattern))
  if (length(matches) == 0) return(NA_character_)
  # Drop EOFY archive sheets unless the EOFY year matches the file's FY — keeps
  # "Detention EOFY2020" inside FY20 files but drops it inside FY21+/FY23+/FY26+.
  file_fy <- as.integer(str_extract(basename(path), "(?<=FY)\\d{2}")) + 2000L
  eofy_year <- as.integer(str_extract(matches, "(?<=EOFY)\\d{2,4}"))
  eofy_year <- if_else(!is.na(eofy_year) & eofy_year < 100L, eofy_year + 2000L, eofy_year)
  is_eofy <- str_detect(matches, "EOFY")
  matches <- matches[!is_eofy | (!is.na(file_fy) & eofy_year == file_fy)]
  if (length(matches) == 0) return(NA_character_)
  non_eofy <- matches[!str_detect(matches, "EOFY")]
  if (length(non_eofy) > 0) non_eofy[1] else matches[1]
}

find_table_start <- function(path, sheet, pattern) {
  read_col_a(path, sheet)[[1]] |> str_detect(pattern) |> which()
}

read_col_a <- function(path, sheet) {
  read_excel(path, sheet = sheet, range = cell_cols("A"), col_names = FALSE)
}

safe_map <- function(fls, fn) {
  out <- fls |> set_names() |> map_dfr(possibly(fn, otherwise = NULL), .id = "file")
  if (!"file" %in% names(out)) out$file <- character(0)
  out
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

# Collapse a long-form table to the latest release per (keys) cell. pull_date
# (the per-table EID) is the primary signal; file_date breaks ties.
collapse_to_latest <- function(df, keys) {
  df |>
    arrange(pull_date, file_date) |>
    group_by(across(all_of(keys))) |>
    slice_tail(n = 1) |>
    ungroup()
}

# text + Oct-Sep + Total
monthly_col_types <- c("text", rep("numeric", 13))

# Column A in the ADP / ALOS tables mixes agency totals ("CBP Average",
# "ICE Average", "Average") with criminality rows under each. Lift to separate
# columns. Works for FY19 (2 criminalities) and FY20+ (3 criminalities).
lift_agency_criminality <- function(df) {
  df |>
    rename(.label = 1) |>
    filter(!is.na(.label)) |>
    mutate(
      agency = case_when(
        str_detect(.label, "^CBP.*Average\\s*$") ~ "CBP",
        str_detect(.label, "^ICE.*Average\\s*$") ~ "ICE",
        str_squish(.label) == "Average" ~ "Total"
      ),
      criminality = if_else(!is.na(agency), "Total", .label)
    ) |>
    fill(agency) |>
    select(agency, criminality, where(is.numeric))
}

# Find sheet + anchor row, read offset range, post-process. `range` is a
# function of the anchor row index.
extract_table <- function(path, sheet_pattern, anchor_pattern, range,
                          col_types = NULL, col_names = TRUE, na = "",
                          post = identity, skip_if = NULL) {
  sheet <- get_sheet(path, sheet_pattern)
  if (is.na(sheet)) return(NULL)
  if (!is.null(skip_if) && isTRUE(skip_if(path, sheet))) return(NULL)
  anchor <- find_table_start(path, sheet, anchor_pattern)
  if (length(anchor) == 0) return(NULL)
  read_excel(path, sheet = sheet, range = range(anchor[1]),
             col_types = col_types, col_names = col_names, na = na) |>
    post()
}

# Per-file dates from two sources:
#   1. Facilities tab "Source: ICE IIDS, MM/DD/YYYY" — one per file.
#   2. Footnotes tab "EID as of MM/DD/YYYY" — one per table family. Patterns
#      anchor at start-of-row so historical "FY2020 ICE …" entries don't leak.
# Downstream we prefer the per-table EID and fall back to Facilities IIDS.

extract_eid <- function(path, pattern) {
  if (!"Footnotes" %in% excel_sheets(path)) return(NA_character_)
  foot <- tryCatch(
    read_excel(path, sheet = "Footnotes", col_names = FALSE),
    error = function(e) NULL
  )
  if (is.null(foot) || ncol(foot) < 2) return(NA_character_)
  col_a <- replace_na(as.character(foot[[1]]), "")
  col_b <- as.character(foot[[2]])
  hits <- which(str_detect(col_a, pattern))
  if (length(hits) == 0) return(NA_character_)
  # Multiple rows may match — historical "FY2020 ICE …" footers can sit
  # alongside the current "FY2023 ICE …" or unprefixed "ICE …" entry. Pick
  # the latest date so stale leaks lose to the current EID.
  dates <- mdy(str_extract(col_b[hits],
                           "(?<=EID as of )\\d{1,2}/\\d{1,2}/\\d{4}"),
               quiet = TRUE)
  dates <- dates[!is.na(dates)]
  if (length(dates) == 0) return(NA_character_)
  format(max(dates), "%m/%d/%Y")
}

# Resolve a publication date from the filename. A leading YYYY-MM-DD prefix
# wins (some files use that convention). Otherwise: 8 digits = MMDDYYYY;
# 6 = MMDDYY (mm<=12) or YYMMDD; 4 = MMDD combined with the FY year.
derive_file_date <- function(file, fy_int) {
  bn <- basename(file)
  yp <- str_extract(bn, "^\\d{4}-\\d{2}-\\d{2}")
  if (!is.na(yp)) return(ymd(yp, quiet = TRUE))
  ds <- file |> str_extract_all("\\d{4,8}") |> map_chr(~ dplyr::last(.x, default = NA_character_))
  if (is.na(ds)) return(as.Date(NA))
  n <- str_length(ds)
  if (n == 8L) return(mdy(ds, quiet = TRUE))
  if (n == 6L) {
    mm <- as.integer(str_sub(ds, 1, 2))
    if (!is.na(mm) && mm <= 12L) return(mdy(ds, quiet = TRUE))
    return(ymd(ds, quiet = TRUE))
  }
  if (n == 4L && !is.na(fy_int)) {
    mm <- as.integer(str_sub(ds, 1, 2))
    dd <- as.integer(str_sub(ds, 3, 4))
    if (is.na(mm) || is.na(dd) || mm > 12L || mm < 1L) return(as.Date(NA))
    cy <- if (mm >= 10L) fy_int - 1L else fy_int
    return(make_date(cy, mm, dd))
  }
  as.Date(NA)
}

file_pull_dates <-
  fls |>
  set_names() |>
  map_dfr(\(.x) {
    # Bypass sheet_skiplist here — the skiplist's intent is to skip data
    # extraction from a bad sheet, not to make the file's metadata invisible.
    fac_sheets <- excel_sheets(.x) |> keep(\(s) str_detect(s, "^Facilities"))
    if (length(fac_sheets) == 0) return(NULL)
    src <- read_excel(.x, sheet = fac_sheets[1], range = "A1:A7", col_names = "src") |>
      filter(str_detect(src, "IIDS|^Source:")) |>
      slice(1) |>
      pull(src)
    if (length(src) == 0) return(NULL)
    tibble(
      src          = src,
      eid_bookins  = extract_eid(.x, "ICE Initial Book-Ins"),
      eid_bookouts = extract_eid(.x, "ICE Final (Book Outs|Releases)"),
      eid_adp      = extract_eid(.x, "ICE Average Daily Population"),
      eid_removals = extract_eid(.x, "ICE Removals"),
      eid_detained = extract_eid(.x, "ICE Currently Detained Population Breakdown")
    )
  }, .id = "file") |>
  mutate(
    fy = str_extract(file, "(?<=FY)\\d{2}"),
    fiscal_year = as.integer(str_c("20", fy)),
    date_raw = map2(file, fiscal_year, derive_file_date) |> list_c(),
    pull_date_facilities = str_extract(src, "\\d{1,2}/\\d{1,2}/\\d{4}") |> mdy(),
    pull_date_bookins    = mdy(eid_bookins,  quiet = TRUE),
    pull_date_bookouts   = mdy(eid_bookouts, quiet = TRUE),
    pull_date_adp        = mdy(eid_adp,      quiet = TRUE),
    pull_date_removals   = mdy(eid_removals, quiet = TRUE),
    pull_date_detained   = mdy(eid_detained, quiet = TRUE),
    # ICE occasionally typos a filename's year (e.g. FY23_detentionStats01192022
    # is FY23 data published Jan 2023, not Jan 2022). Trust the latest of all
    # known dates — filename + every per-table EID — as the canonical file_date.
    file_date = pmax(date_raw, pull_date_facilities,
                     pull_date_bookins, pull_date_bookouts, pull_date_adp,
                     pull_date_removals, pull_date_detained, na.rm = TRUE)
  ) |>
  select(file, fiscal_year, file_date,
         pull_date_facilities, pull_date_bookins, pull_date_bookouts,
         pull_date_adp, pull_date_removals, pull_date_detained)

# Resolve pull_date for a table family: per-table EID if present, else fall
# back to Facilities IIDS. If the resulting pull_date is before the row's
# fiscal year even started (e.g., a stale FY-1 footer on a file whose
# Detention sheet was refreshed but Footnotes weren't), use file_date —
# pmax already pushed it to the most reliable signal in the file.
file_meta_for <- function(kind) {
  file_pull_dates |>
    transmute(
      file, fiscal_year, file_date,
      pull_date = coalesce(.data[[paste0("pull_date_", kind)]], pull_date_facilities),
      pull_date = if_else(
        !is.na(pull_date) & pull_date >= make_date(fiscal_year - 1, 10, 1),
        pull_date, file_date
      )
    )
}

# Anchor "Currently Detained by Criminality"; agency table at I(r+1):V(r+4).
# 12 month columns are stable historical (latest-per-cell); Total is per-release FY YTD.
book_ins_by_arresting_agency_wide <-
  safe_map(fls, \(.x) {
    extract_table(.x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Currently Detained by Criminality",
      range = \(r) glue("I{r+1}:V{r+4}"),
      col_types = c("text", rep("numeric", 13)))
  }) |>
  left_join(file_meta_for("bookins"), by = "file") |>
  select(-file) |>
  rename(arresting_agency = Agency)

book_ins_by_arresting_agency_monthly <-
  book_ins_by_arresting_agency_wide |>
  select(-Total) |>
  pivot_longer(Oct:Sep, names_to = "month", values_to = "n_book_ins") |>
  add_fy_date_cols() |>
  select(arresting_agency, month, date, n_book_ins,
         fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("arresting_agency", "month", "date")) |>
  filter(pull_date >= date)  # drop future months ICE reports as placeholder 0
nanoparquet::write_parquet(book_ins_by_arresting_agency_monthly,
                           "data/book-ins-by-arresting-agency-monthly.parquet")

book_ins_by_arresting_agency_fy_ytd <-
  book_ins_by_arresting_agency_wide |>
  rename(n_book_ins_ytd = Total) |>
  select(arresting_agency, n_book_ins_ytd, fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("arresting_agency", "fiscal_year", "pull_date"))
nanoparquet::write_parquet(book_ins_by_arresting_agency_fy_ytd,
                           "data/book-ins-by-arresting-agency-fy-ytd.parquet")

# Number of release reasons grew over time (FY21-23: 6, FY24-25: 12, FY26: 13).
# Find the next section header to bound the read rather than hardcoding rows.
book_outs_by_reason_wide <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "^Detention")
    if (is.na(sheet)) return(NULL)
    col_a <- read_col_a(.x, sheet)[[1]]
    r <- which(str_detect(col_a,
      "ICE Final (Book Outs|Releases) by Release Reason, Month and Criminality"))[1]
    if (is.na(r)) return(NULL)
    after_r <- which(str_detect(col_a,
      "^ICE [A-Z]|^Aliens|^Currently|^Noncitizens|^Individuals") & seq_along(col_a) > r)
    end_row <- if (length(after_r) > 0) after_r[1] - 1 else length(col_a)
    if (end_row < r + 5) return(NULL)
    read_excel(.x, sheet = sheet, range = glue("A{r+1}:O{end_row}"),
               col_types = c("text", "text", rep("numeric", 13)),
               col_names = TRUE, na = "") |>
      filter(!is.na(`Release Reason`) | !is.na(Criminality)) |>
      fill(`Release Reason`)
  }) |>
  left_join(file_meta_for("bookouts"), by = "file") |>
  select(-file) |>
  rename(release_reason = `Release Reason`, criminality = Criminality) |>
  # Clean up source typos: " Total" suffix in FY24 Feb-Apr; inconsistent
  # casing in FY26 ("Bonded out", "Order of supervision"). The grand-total
  # row at the bottom of the source table has release_reason="Total" with a
  # blank criminality column — coerce to criminality="Total" so it groups
  # cleanly with the other Totals.
  mutate(
    release_reason = release_reason |>
      str_replace(" Total$", "") |>
      str_replace_all("Bonded out", "Bonded Out") |>
      str_replace_all("Order of supervision", "Order of Supervision"),
    criminality = coalesce(criminality, "Total")
  )

book_outs_by_reason_monthly <-
  book_outs_by_reason_wide |>
  select(-Total) |>
  pivot_longer(Oct:Sep, names_to = "month", values_to = "n_book_outs") |>
  add_fy_date_cols() |>
  select(release_reason, criminality, month, date, n_book_outs,
         fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("release_reason", "criminality", "month", "date")) |>
  filter(pull_date >= date)
nanoparquet::write_parquet(book_outs_by_reason_monthly,
                           "data/book-outs-by-reason-monthly.parquet")

# Older files (FY19-FY21) have annual-by-criminality columns instead of monthly.
# Skip when the monthly table is present in the same file.
book_outs_by_reason_annual <-
  safe_map(fls, \(.x) {
    extract_table(.x,
      sheet_pattern = "^Detention",
      anchor_pattern = "ICE Final (Book Outs|Releases) by Release Reason",
      range = \(r) glue("A{r+1}:E{r+9}"),
      col_types = c("text", rep("numeric", 4)),
      skip_if = \(p, s) length(find_table_start(p, s,
        "ICE Final (Book Outs|Releases) by Release Reason, Month and Criminality")) > 0,
      post = \(df) {
        names(df) <- c("release_reason", "convicted_criminal",
                       "pending_criminal_charges", "other_immigration_violator", "total")
        df
      })
  }) |>
  filter(!is.na(release_reason)) |>
  mutate(release_reason = release_reason |>
    str_replace_all("Bonded out", "Bonded Out") |>
    str_replace_all("Order of supervision", "Order of Supervision")) |>
  left_join(file_meta_for("bookouts"), by = "file") |>
  select(-file)

book_outs_by_reason_fy_ytd <-
  bind_rows(
    book_outs_by_reason_wide |>
      rename(n_book_outs_ytd = Total) |>
      select(release_reason, criminality, n_book_outs_ytd,
             fiscal_year, file_date, pull_date),
    book_outs_by_reason_annual |>
      pivot_longer(c(convicted_criminal, pending_criminal_charges,
                     other_immigration_violator, total),
                   names_to = "criminality", values_to = "n_book_outs_ytd") |>
      mutate(criminality = str_to_title(str_replace_all(criminality, "_", " ")))
  ) |>
  select(release_reason, criminality, n_book_outs_ytd,
         fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("release_reason", "criminality", "fiscal_year", "pull_date"))
nanoparquet::write_parquet(book_outs_by_reason_fy_ytd,
                           "data/book-outs-by-reason-fy-ytd.parquet")

# adp / avg_stay_length_days: monthly stable historical (latest per cell).
# adp_fy_ytd / avg_stay_length_days_fy_ytd: per-release FY-to-date snapshot
# (one row per file). Source column header for both: "FY Overall".
adp_wide <-
  safe_map(fls, \(.x) {
    extract_table(.x,
      sheet_pattern = "^Detention",
      anchor_pattern = "ICE Average Daily Population by Arresting Agency, Month and Criminality",
      range = \(r) glue("A{r+1}:N{r+13}"),
      col_types = monthly_col_types,
      post = lift_agency_criminality)
  }) |>
  left_join(file_meta_for("adp"), by = "file") |>
  select(-file) |>
  drop_na(agency) |>
  # Two source files can collapse to the same (file_date, pull_date) after
  # pmax-derives file_date; dedupe to keep the downstream full_join clean.
  collapse_to_latest(c("agency", "criminality", "fiscal_year",
                       "file_date", "pull_date"))

stay_wide <-
  safe_map(fls, \(.x) {
    extract_table(.x,
      sheet_pattern = "^Detention",
      anchor_pattern = "ICE Average Length of Stay by Arresting Agency, Month and Criminality",
      range = \(r) glue("A{r+1}:N{r+13}"),
      col_types = monthly_col_types,
      post = lift_agency_criminality)
  }) |>
  left_join(file_meta_for("adp"), by = "file") |>
  select(-file) |>
  drop_na(agency) |>
  collapse_to_latest(c("agency", "criminality", "fiscal_year",
                       "file_date", "pull_date"))

adp_and_stay_length_by_agency_monthly <-
  full_join(
    adp_wide  |> select(-`FY Overall`) |>
      pivot_longer(Oct:Sep, names_to = "month", values_to = "adp") |>
      add_fy_date_cols(),
    stay_wide |> select(-`FY Overall`) |>
      pivot_longer(Oct:Sep, names_to = "month", values_to = "avg_stay_length_days") |>
      add_fy_date_cols(),
    by = c("agency", "criminality", "month", "date",
           "fiscal_year", "file_date", "pull_date")
  ) |>
  select(agency, criminality, month, date, adp, avg_stay_length_days,
         fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("agency", "criminality", "month", "date")) |>
  filter(pull_date >= date)
nanoparquet::write_parquet(adp_and_stay_length_by_agency_monthly,
                           "data/adp-and-stay-length-by-agency-monthly.parquet")

adp_and_stay_length_by_agency_fy_ytd <-
  full_join(
    adp_wide  |> select(agency, criminality, adp_fy_ytd = `FY Overall`,
                        fiscal_year, file_date, pull_date),
    stay_wide |> select(agency, criminality, avg_stay_length_days_fy_ytd = `FY Overall`,
                        fiscal_year, file_date, pull_date),
    by = c("agency", "criminality", "fiscal_year", "file_date", "pull_date")
  ) |>
  select(agency, criminality, adp_fy_ytd, avg_stay_length_days_fy_ytd,
         fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("agency", "criminality", "fiscal_year", "pull_date"))
nanoparquet::write_parquet(adp_and_stay_length_by_agency_fy_ytd,
                           "data/adp-and-stay-length-by-agency-fy-ytd.parquet")

# Per-release snapshot — bed counts, threat levels, inspection metadata.
# FYxx_alos columns are stable historical and split out into facility_alos.
facilities_wide <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "^Facilities")
    if (is.na(sheet)) return(NULL)
    col_a <- read_col_a(.x, sheet)
    start_row <- which(!is.na(col_a[[1]]) & str_detect(col_a[[1]], "^Name"))
    if (length(start_row) == 0) {
      warning(glue("  [facilities] Header row not found in {basename(.x)}"))
      return(NULL)
    }
    sr <- start_row[1]
    # Some files repeat the "Name" header on the next row; skip it.
    test_row <- read_excel(.x, sheet = sheet, range = glue("A{sr + 1}:A{sr + 1}"),
                           col_names = FALSE)
    if (!is.na(test_row[[1]]) && str_detect(test_row[[1]], "^Name")) sr <- sr + 1
    # AE covers the widest year (FY19, 31 columns); narrower years come back
    # with empty trailing column names which tidy ops can't handle.
    df <- read_excel(.x, sheet = sheet, range = glue("A{sr}:AE{nrow(col_a)}"),
                     .name_repair = "minimal")
    df <- df[, !is.na(colnames(df)) & colnames(df) != ""]
    df <- filter(df, !is.na(Name))
    # ICE Threat Level was numeric in FY19-23 and text "5%" in FY25+; coerce
    # everything except identity/text columns to character so bind_rows aligns.
    keep_typed <- c("Name", "Address", "City", "State", "AOR", "Type Detailed",
                    "Male/Female")
    df[setdiff(colnames(df), keep_typed)] <-
      lapply(df[setdiff(colnames(df), keep_typed)], as.character)
    df
  }) |>
  janitor::clean_names() |>
  # Shorten names that exceed Stata's 32-char variable name limit.
  rename_with(\(n) recode(n,
    second_to_last_inspection_standard      = "s2l_inspection_standard",
    last_nakamoto_inspection_standard       = "last_nak_inspection_standard",
    last_nakamoto_inspection_rating_final   = "last_nak_inspection_rating",
    second_to_last_nakamoto_inspection_type = "s2l_nak_inspection_type"
  )) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file) |>
  mutate(
    male_female = if_else(male_female %in% c("Male", "Female", "Female/Male"),
                          male_female, NA_character_),
    city = str_squish(city),
    state = if_else(str_squish(state) == "", NA_character_, state)
  )

detainees_by_facility <-
  facilities_wide |>
  select(-matches("^fy\\d+_alos$")) |>
  distinct()
nanoparquet::write_parquet(detainees_by_facility, "data/facilities.parquet")

facility_alos <-
  facilities_wide |>
  select(name, fiscal_year, file_date, pull_date, matches("^fy\\d+_alos$")) |>
  pivot_longer(matches("^fy\\d+_alos$"),
               names_to = "alos_fiscal_year", values_to = "alos") |>
  filter(!is.na(alos)) |>
  mutate(alos_fiscal_year = parse_fy(alos_fiscal_year),
         alos = as.numeric(alos)) |>
  collapse_to_latest(c("name", "alos_fiscal_year")) |>
  select(name, alos_fiscal_year, alos, fiscal_year, file_date, pull_date)
nanoparquet::write_parquet(facility_alos, "data/facility-alos.parquet")

# "Book-Ins by Facility/Criminality" anchor; the cell at P(anchor+2) is the
# FY-to-date cumulative removals total. Combined with FAMU removals (built
# below) into the per-release `removals` dataset.
removals_total <-
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

# Disposition table at A(r+1):D(r+6). FY26+ added "Other" as a 5th data row;
# older years have only 4 data rows + a trailing NA. FY24/25 dropped FSC, so
# remap header text → canonical column names regardless of column position.
currently_detained_by_disposition <-
  safe_map(fls, \(.x) {
    extract_table(.x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Currently Detained by Processing Disposition",
      range = \(r) glue("A{r+1}:D{r+6}"),
      col_types = c("text", "numeric", "numeric", "numeric"),
      na = c("", "-"),
      post = \(df) {
        names(df)[1] <- "disposition"
        df <- df |> select(-starts_with("...")) |> filter(!is.na(disposition))
        canonical <- c(FRC = "fsc_frc", FSC = "fsc_frc",
                       Adult = "adult", Total = "total")
        rename_with(df, \(n) coalesce(canonical[n], n), -disposition)
      })
  }) |>
  left_join(file_meta_for("detained"), by = "file") |>
  select(-file) |>
  pivot_longer(any_of(c("fsc_frc", "adult", "total")),
               names_to = "facility_type", values_to = "n_detained",
               values_drop_na = TRUE) |>
  mutate(facility_type = recode(facility_type,
    fsc_frc = "FSC/FRC", adult = "Adult", total = "Total")) |>
  select(disposition, facility_type, n_detained,
         fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("disposition", "facility_type", "pull_date"))
nanoparquet::write_parquet(currently_detained_by_disposition,
                           "data/currently-detained-by-disposition.parquet")

# G(r+1):K(r+2) holds: fiscal year + (gap) + FSC | Adult | Total. FY count
# of populated columns varies (1-3) so name them by position.
fear_decision_time <-
  safe_map(fls, \(.x) {
    extract_table(.x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Currently Detained by Processing Disposition",
      range = \(r) glue("G{r+1}:K{r+2}"),
      post = \(df) {
        df <- df |> select(where(\(x) !all(is.na(x))))
        if (ncol(df) == 0 || nrow(df) == 0) return(NULL)
        # Trailing 1-3 columns are FSC | Adult | Total from the right.
        positional <- tail(c("fsc", "adult", "total"), ncol(df) - 1)
        names(df) <- c("data_fiscal_year", positional)
        for (col in setdiff(c("fsc", "adult", "total"), positional)) df[[col]] <- NA_real_
        df |>
          mutate(data_fiscal_year = parse_fy(data_fiscal_year)) |>
          select(data_fiscal_year, fsc, adult, total)
      })
  }) |>
  left_join(file_meta_for("detained"), by = "file") |>
  select(-file) |>
  pivot_longer(c(fsc, adult, total), names_to = "facility_type",
               values_to = "decision_days", values_drop_na = TRUE) |>
  mutate(facility_type = recode(facility_type,
    fsc = "FSC/FRC", adult = "Adult", total = "Total")) |>
  select(data_fiscal_year, facility_type, decision_days,
         fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("data_fiscal_year", "facility_type")) |>
  distinct()
nanoparquet::write_parquet(fear_decision_time, "data/fear-decision-time.parquet")

# Fear-decisions table sits to the right of the disposition table. The label
# column shifts between M and N depending on whether FSC is present, so we
# locate it by content (cells containing Total/FSC/Adult/FRC).
fear_decisions_by_facility_type <-
  safe_map(fls, \(.x) {
    extract_table(.x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Currently Detained by Processing Disposition",
      range = \(r) glue("M{r+1}:Q{r+4}"),
      col_names = FALSE,
      post = \(df) {
        if (ncol(df) < 2 || nrow(df) == 0) return(NULL)
        label_col <- detect_index(df, \(c)
          any(as.character(c) %in% c("Total", "FSC", "FRC", "Adult"), na.rm = TRUE))
        if (label_col == 0 || label_col >= ncol(df)) return(NULL)
        count_col <- label_col + detect_index(df[(label_col + 1):ncol(df)], \(c)
          any(!is.na(suppressWarnings(as.numeric(c)))))
        if (count_col == label_col) return(NULL)
        tibble(facility_type = as.character(df[[label_col]]),
               total_detained = suppressWarnings(as.numeric(df[[count_col]]))) |>
          filter(!is.na(facility_type),
                 !facility_type %in% c("Detention Facility Type", "Type"))
      })
  }) |>
  left_join(file_meta_for("detained"), by = "file") |>
  select(-file) |>
  collapse_to_latest(c("facility_type", "pull_date"))
nanoparquet::write_parquet(fear_decisions_by_facility_type,
                           "data/fear-decisions-by-facility-type.parquet")

currently_detained_by_criminality <-
  safe_map(fls, \(.x) {
    extract_table(.x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Currently Detained by Criminality",
      # 4 data rows: Total, Convicted Criminal, Pending Criminal Charges,
      # Other Immigration Violator. Earlier code used r+4 (3 rows) and
      # silently dropped Other Immigration Violator.
      range = \(r) glue("A{r+1}:F{r+5}"),
      col_types = c("text", rep("numeric", 5)))
  }) |>
  janitor::clean_names() |>
  filter(!is.na(criminality)) |>
  left_join(file_meta_for("detained"), by = "file") |>
  select(-file) |>
  pivot_longer(c(ice, cbp, total),
               names_to = "arresting_agency", values_to = "n_detained") |>
  mutate(
    share_of_total = case_when(
      arresting_agency == "ice" ~ percent_ice,
      arresting_agency == "cbp" ~ percent_cbp
    ),
    arresting_agency = recode(arresting_agency,
      ice = "ICE", cbp = "CBP", total = "Total")
  ) |>
  select(criminality, arresting_agency, n_detained, share_of_total,
         fiscal_year, file_date, pull_date) |>
  # When ICE re-publishes the same EID under a new filename (sometimes with
  # corrections), keep the latest file_date for that EID.
  collapse_to_latest(c("criminality", "arresting_agency", "pull_date"))
nanoparquet::write_parquet(currently_detained_by_criminality,
                           "data/currently-detained-by-criminality.parquet")

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
      col_types = c("text", rep("numeric", 4)),
      skip_if = \(p, s) basename(p) %in% BOOKIN_FACILITY_TABLE_BAD
    )
  }) |>
  janitor::clean_names() |>
  filter(!is.na(facility_type)) |>
  left_join(file_meta_for("bookins"), by = "file") |>
  select(-file)

# Older files put this table at H:J; FY22+ shifted it to I:L. Read a wider
# range (H:M) and locate the label/value columns by content.
book_outs_by_facility_type <-
  safe_map(fls, \(.x) {
    extract_table(.x,
      sheet_pattern = "^Detention",
      anchor_pattern = "Book-Ins by",
      range = \(r) glue("H{r+1}:M{r+4}"),
      col_names = FALSE,
      skip_if = \(p, s) basename(p) %in% BOOKOUT_FACILITY_TABLE_BAD,
      post = \(df) {
        if (ncol(df) < 2 || nrow(df) == 0) return(NULL)
        label_col <- detect_index(df, \(c)
          any(as.character(c) %in% c("Total", "FSC", "FRC", "Adult"), na.rm = TRUE))
        if (label_col == 0 || label_col >= ncol(df)) return(NULL)
        count_col <- label_col + detect_index(df[(label_col + 1):ncol(df)], \(c)
          any(!is.na(suppressWarnings(as.numeric(c)))))
        if (count_col == label_col) return(NULL)
        tibble(facility_type = as.character(df[[label_col]]),
               total = suppressWarnings(as.numeric(df[[count_col]]))) |>
          filter(!is.na(facility_type))
      })
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

# Two stacked blocks share an A:C shape but report different metrics in
# column C: technology block → daily_tech_cost (USD), status block → alip
# (days). bind_rows leaves the unrelated metric column NA per row.
atd_blocks <- tribble(
  ~label_pattern,                                          ~n_rows, ~value_name,       ~table_name,
  "ATD Active Population Counts|ATD Active Participants",  8L,      "daily_tech_cost", "technology",
  "ATD Active Population by Status",                       6L,      "alip",            "status"
)

atd_population <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "^ATD")
    if (is.na(sheet)) return(NULL)
    col_a <- read_col_a(.x, sheet)
    pmap_dfr(atd_blocks, \(label_pattern, n_rows, value_name, table_name) {
      r <- which(str_detect(col_a[[1]], label_pattern))[1]
      if (is.na(r)) return(NULL)
      read_excel(.x, sheet = sheet, range = glue("A{r+1}:C{r+n_rows}"),
                 col_types = c("text", rep("numeric", 2))) |>
        setNames(c("category", "n_active", value_name)) |>
        # n_active discriminates trailing footnote rows from real data.
        filter(!is.na(category), !is.na(n_active)) |>
        mutate(table = table_name)
    })
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file) |>
  relocate(table, category, n_active, daily_tech_cost, alip,
           fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("table", "category", "pull_date"))
nanoparquet::write_parquet(atd_population, "data/atd-population.parquet")

# Source has Total rollup → AOR header → 4-5 technology rows under each.
# Split into (aor, technology) by matching labels to a known vocabulary.
ATD_TECHNOLOGIES <- c(
  "SmartLINK", "Dual Tech", "Ankle Monitor", "Wristworn",
  "GPS", "Telephonic", "VoiceID", "VeriWatch",
  "TR", "No Tech", "BI Phone"
)

atd_by_aor <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "^ATD")
    if (is.na(sheet)) return(NULL)
    col_a <- read_col_a(.x, sheet)
    r <- which(str_detect(col_a[[1]], "Active ATD Participants.*by AOR"))
    if (length(r) == 0) return(NULL)
    read_excel(.x, sheet = sheet,
               range = glue("A{r[1]+1}:C{nrow(col_a)}"),
               col_types = c("text", rep("numeric", 2))) |>
      setNames(c("label", "n_active", "avg_length_in_program")) |>
      filter(!is.na(label)) |>
      mutate(
        # ICE inconsistently spells some labels — canonicalize.
        label = label |>
          str_replace_all("Veriwatch", "VeriWatch") |>
          str_replace_all("^WashingtonDC$", "Washington DC"),
        is_tech   = label %in% ATD_TECHNOLOGIES,
        is_rollup = label %in% c("Total", "Grand Total"),
        aor = if_else(!is_tech & !is_rollup, label, NA_character_)
      ) |>
      fill(aor, .direction = "down") |>
      mutate(
        aor = if_else(is_rollup, "Total", aor),
        technology = if_else(is_tech, label, NA_character_)
      ) |>
      select(aor, technology, n_active, avg_length_in_program)
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file) |>
  collapse_to_latest(c("aor", "technology", "pull_date"))
nanoparquet::write_parquet(atd_by_aor, "data/atd-by-aor.parquet")

# Court Appearance header at [r,cc]; below: Metric|Count|% header at r+1,
# data at r+2..r+4. Multiple Court Appearance tables can appear per file.
atd_court_appearances <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "^ATD")
    if (is.na(sheet)) return(NULL)
    m <- read_excel(.x, sheet = sheet, col_names = FALSE) |> as.matrix()
    storage.mode(m) <- "character"
    hits <- which(!is.na(m) & str_detect(m, "Court Appearance"), arr.ind = TRUE)
    if (nrow(hits) == 0) return(NULL)
    map_dfr(seq_len(nrow(hits)), \(i) {
      hr <- hits[i, "row"]; cc <- hits[i, "col"]
      if (hr + 2 > nrow(m) || cc + 2 > ncol(m)) return(NULL)
      rows <- (hr + 2):min(hr + 4, nrow(m))
      tibble(
        hearing_type = case_when(
          str_detect(m[hr, cc], "Total") ~ "total",
          str_detect(m[hr, cc], "Final") ~ "final",
          .default = "unknown"
        ),
        metric = m[rows, cc],
        count  = suppressWarnings(as.numeric(m[rows, cc + 1])),
        pct    = suppressWarnings(as.numeric(m[rows, cc + 2]))
      ) |>
        filter(metric %in% c("Attended", "Failed to Attend", "Total"))
    })
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file) |>
  collapse_to_latest(c("hearing_type", "metric", "pull_date"))
nanoparquet::write_parquet(atd_court_appearances, "data/atd-court-appearances.parquet")

# ICLOS / Detainees sheet: wide table with three header rows (sparse year row
# at pop_row, month names at +1, mid/end period markers at +2) spanning ~24
# columns per year. Pivot to long.
#
# Detainees has an extra nested level: each population type is followed by
# 4-5 duration-bucket rows (0-180 Days, 181-365 Days, 366-730 Days, More
# than 730 Days, Total). ICLOS is flat — each row IS a population type.
DETAINEE_BUCKET_PATTERN <- "^\\d+-\\d+ Days$|^More than \\d+ Days$|^Total$"

iclos_and_detainees <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "ICLOS|Detainee")
    if (is.na(sheet)) return(NULL)
    df <- read_excel(.x, sheet = sheet, col_names = FALSE)
    if (nrow(df) < 7) return(NULL)
    col_a <- df[[1]]

    iclos_header <- which(str_detect(col_a, "^Population") & !is.na(col_a))
    if (length(iclos_header) == 0) return(NULL)
    det_header <- which(str_detect(col_a, "^Detainees$") & !is.na(col_a))

    sections <- list(iclos = iclos_header[1])
    if (length(det_header) > 0) {
      det_pop <- iclos_header[iclos_header > det_header[1]]
      if (length(det_pop) > 0) sections[["detainees"]] <- det_pop[1]
    }

    map_dfr(names(sections), \(section_name) {
      pop_row <- sections[[section_name]]
      data_start <- pop_row + 3
      if (data_start > length(col_a)) return(NULL)

      years_raw   <- as.character(unlist(df[pop_row, ]))
      months_raw  <- as.character(unlist(df[pop_row + 1, ]))
      periods_raw <- as.character(unlist(df[pop_row + 2, ]))

      # Forward-fill the sparse year markers across the full header row.
      years <- if_else(!is.na(years_raw) & str_detect(years_raw, "^\\d{4}$"),
                       years_raw, NA_character_) |>
        accumulate(\(prev, cur) coalesce(cur, prev))

      data_cols <- which(months_raw %in% month.name & periods_raw %in% c("mid", "end"))
      if (length(data_cols) == 0) return(NULL)

      # Walk from data_start until a >2-row blank gap or a section header.
      remaining <- col_a[data_start:length(col_a)]
      non_na_idx <- which(!is.na(remaining))
      if (length(non_na_idx) == 0) return(NULL)
      gaps   <- c(0L, diff(non_na_idx))
      labels <- replace_na(remaining[non_na_idx], "")
      break_at <- gaps > 2L | str_detect(labels, "^(Detainees|Population)$")
      end_idx <- coalesce(which(break_at)[1] - 1L, length(non_na_idx))
      if (end_idx < 1L) return(NULL)
      data_end <- data_start + non_na_idx[end_idx] - 1L
      row_indices <- seq(data_start, data_end)
      row_labels <- col_a[row_indices]

      if (section_name == "detainees") {
        is_bucket <- str_detect(replace_na(row_labels, ""), DETAINEE_BUCKET_PATTERN)
        pop_type <- if_else(!is.na(row_labels) & !is_bucket,
                            row_labels, NA_character_) |>
          accumulate(\(prev, cur) coalesce(cur, prev))
        duration_bucket <- if_else(is_bucket, row_labels, NA_character_)
      } else {
        pop_type <- row_labels
        duration_bucket <- rep(NA_character_, length(row_indices))
      }

      map_dfr(seq_along(row_indices), \(i) {
        r <- row_indices[i]
        # Keep only data-bearing rows: ICLOS needs a population_type;
        # detainees needs a duration_bucket (population_type rows are
        # header-only and have no values).
        if (section_name == "detainees" && is.na(duration_bucket[i])) {
          return(NULL)
        }
        if (is.na(pop_type[i])) return(NULL)
        vals <- suppressWarnings(as.numeric(unlist(df[r, data_cols])))
        tibble(
          section         = section_name,
          population_type = pop_type[i],
          duration_bucket = duration_bucket[i],
          year            = suppressWarnings(as.integer(years[data_cols])),
          month           = months_raw[data_cols],
          period          = periods_raw[data_cols],
          value           = vals
        ) |>
          filter(!is.na(value), !is.na(year), !is.na(month))
      })
    })
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file) |>
  mutate(
    date  = make_date(year, match(month, month.name), 1L),
    month = month.abb[match(month, month.name)],
    # Source-data inconsistencies: ICE relabeled "Adult Facility Aliens" to
    # "Adult Facility Individuals" mid-history; the FRC/FSC label varies; and
    # earlier files spelled "Postive" without the second 'i'. Canonicalize
    # so a population_type identifies one series across all releases.
    population_type = population_type |>
      str_replace_all("^Adult Facility Aliens$", "Adult Facility Individuals") |>
      str_replace_all("^FRC Facility Individuals$", "FSC Facility Individuals") |>
      str_replace_all("Postive Fear", "Positive Fear") |>
      str_replace_all(
        "^Post-Determination for FRC ",
        "Post-Determination for FSC "
      )
  ) |>
  select(-year) |>
  relocate(section, population_type, duration_bucket, month, date, period,
           value, fiscal_year, file_date, pull_date) |>
  distinct() |>
  filter(value != 0) |>  # ICE leaves placeholder 0s in some cells; prefer non-zero
  collapse_to_latest(
    c("section", "population_type", "duration_bucket", "date", "period")
  ) |>
  filter(pull_date >= date)
nanoparquet::write_parquet(iclos_and_detainees, "data/iclos-and-detainees.parquet")

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
  # value's unit depends on metric — surface it so consumers don't sum USD next to days.
  mutate(
    month = month.abb[month(date)],
    unit  = case_when(
      metric %in% c("total_book_outs", "bond_book_outs") ~ "count",
      metric == "bond_pct"        ~ "percent",
      metric == "avg_bond_amount" ~ "usd",
      metric == "alos_days"       ~ "days"
    )
  ) |>
  relocate(month, date, metric, unit, value, fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("date", "metric")) |>
  filter(pull_date >= date)
nanoparquet::write_parquet(monthly_bond_stats, "data/bond-stats.parquet")

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
  relocate(month, date, facility, placement_count,
           fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("date", "facility")) |>
  filter(pull_date >= date)
nanoparquet::write_parquet(monthly_segregation, "data/segregation.parquet")

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
      hr <- which(str_detect(col_a, pattern))[1]
      if (is.na(hr) || hr + 2 > nrow(df)) return(NULL)
      rows <- (hr + 2):min(hr + 12, nrow(df))
      keep <- cumall(str_detect(replace_na(col_a[rows], ""), "^FY"))
      rows <- rows[keep]
      if (length(rows) == 0) return(NULL)
      tibble(
        table_name       = name,
        data_fiscal_year = parse_fy(col_a[rows]),
        country          = NA_character_,
        value            = suppressWarnings(as.numeric(unlist(df[rows, 2])))
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
      hr <- which(str_detect(col_a, pattern))[1]
      if (is.na(hr) || hr + 2 > nrow(df)) return(NULL)
      fy_header <- as.character(unlist(df[hr + 1, ]))
      fy_cols <- which(str_detect(replace_na(fy_header, ""), "^FY\\d{4}$"))
      if (length(fy_cols) == 0) return(NULL)
      rows <- (hr + 2):min(hr + 25, nrow(df))
      keep <- cumall(!is.na(col_a[rows]) & col_a[rows] != "")
      rows <- rows[keep]
      if (length(rows) == 0) return(NULL)

      df[rows, fy_cols] |>
        mutate(across(everything(), \(x) suppressWarnings(as.numeric(x)))) |>
        rename_with(\(.) fy_header[fy_cols]) |>
        mutate(country = col_a[rows]) |>
        pivot_longer(-country, names_to = "fy", values_to = "value",
                     values_drop_na = TRUE) |>
        transmute(table_name = name,
                  data_fiscal_year = parse_fy(fy),
                  country, value)
    })

    bind_rows(simple_results, tps_results)
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file) |>
  # Split table_name (e.g. "us_citizen_bookins") into population_group + action.
  mutate(
    population_group = str_remove(table_name, "_(arrests|bookins|removals)$"),
    action           = str_extract(table_name, "(arrests|bookins|removals)$")
  ) |>
  select(-table_name) |>
  relocate(population_group, action, data_fiscal_year, country, value,
           fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("population_group", "action", "data_fiscal_year", "country"))
nanoparquet::write_parquet(semiannual_data, "data/special-population-actions.parquet")

vulnerable_population <-
  safe_map(fls, \(.x) {
    sheet <- get_sheet(.x, "Vulnerable")
    if (is.na(sheet)) return(NULL)
    df <- read_excel(.x, sheet = sheet, col_names = FALSE)
    col_a <- as.character(df[[1]])
    quarter_rows <- which(str_detect(col_a, "^Fiscal Year \\(FY\\)\\s+\\d{4} Quarter \\d"))
    if (length(quarter_rows) == 0) return(NULL)

    # Each block: header at qr+1: Reason | Placements | Avg Consec | Avg Cumul.
    map_dfr(quarter_rows, \(qr) {
      if (qr + 2 > nrow(df)) return(NULL)
      rows <- (qr + 2):min(qr + 8, nrow(df))
      keep <- cumall(!is.na(col_a[rows]) &
                     !str_detect(replace_na(col_a[rows], ""), "^\\*|^$"))
      rows <- rows[keep]
      if (length(rows) == 0) return(NULL)
      tibble(
        fy_quarter           = str_extract(col_a[qr], "\\d{4} Quarter \\d"),
        placement_reason     = col_a[rows],
        n_placements         = suppressWarnings(as.numeric(unlist(df[rows, 2]))),
        avg_consecutive_days = suppressWarnings(as.numeric(unlist(df[rows, 3]))),
        avg_cumulative_days  = suppressWarnings(as.numeric(unlist(df[rows, 4])))
      )
    })
  }) |>
  left_join(file_meta_for("facilities"), by = "file") |>
  select(-file) |>
  mutate(
    data_fiscal_year = as.integer(str_extract(fy_quarter, "\\d{4}")),
    data_quarter     = as.integer(str_extract(fy_quarter, "(?<=Quarter )\\d"))
  ) |>
  select(-fy_quarter) |>
  relocate(placement_reason, data_fiscal_year, data_quarter,
           n_placements, avg_consecutive_days, avg_cumulative_days,
           fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("placement_reason", "data_fiscal_year", "data_quarter"))
nanoparquet::write_parquet(vulnerable_population, "data/vulnerable-population.parquet")

# Book-ins are reported per criminality bucket; book-outs only at Total —
# the source asymmetry surfaces as criminality = "Total" on book-out rows.
flows_by_facility_type <-
  bind_rows(
    book_ins_by_facility_type |>
      distinct() |>
      pivot_longer(c(convicted_criminal, pending_criminal_charges,
                     other_immigration_violator, total),
                   names_to = "criminality", values_to = "n_flows") |>
      mutate(flow = "book-in",
             criminality = str_to_title(str_replace_all(criminality, "_", " "))),
    book_outs_by_facility_type |>
      distinct() |>
      rename(n_flows = total) |>
      mutate(flow = "book-out", criminality = "Total")
  ) |>
  relocate(facility_type, flow, criminality, n_flows,
           fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("facility_type", "flow", "criminality", "pull_date"))
nanoparquet::write_parquet(flows_by_facility_type, "data/flows-by-facility-type.parquet")

removals <-
  full_join(distinct(removals_total), distinct(famu_removals),
            by = c("fiscal_year", "file_date", "pull_date")) |>
  relocate(n_removals_fy_ytd, famu_removals, fiscal_year, file_date, pull_date) |>
  collapse_to_latest(c("fiscal_year", "pull_date"))
nanoparquet::write_parquet(removals, "data/removals.parquet")

# Mirror every parquet as xlsx and dta for downstream Stata/Excel consumers.
for (pq in list.files("data", pattern = "\\.parquet$", full.names = TRUE)) {
  df <- nanoparquet::read_parquet(pq)
  base <- tools::file_path_sans_ext(pq)
  writexl::write_xlsx(df, paste0(base, ".xlsx"))
  haven::write_dta(df, paste0(base, ".dta"))
}
