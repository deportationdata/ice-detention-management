library(dplyr)
library(purrr)
library(nanoparquet)
library(glue)
library(pointblank)

# ── Expected parquet files ────────────────────────────────────────────────────

EXPECTED_FILES <- c(
  "adp-and-stay-length-by-agency-monthly",
  "adp-and-stay-length-by-agency-fy-ytd",
  "atd-by-aor",
  "atd-court-appearances",
  "atd-population",
  "book-ins-by-arresting-agency-monthly",
  "book-ins-by-arresting-agency-fy-ytd",
  "book-outs-by-reason-monthly",
  "book-outs-by-reason-fy-ytd",
  "currently-detained-by-criminality",
  "currently-detained-by-disposition",
  "facilities",
  "facility-alos",
  "fear-decision-time",
  "fear-decisions-by-facility-type",
  "flows-by-facility-type",
  "iclos-and-detainees",
  "bond-stats",
  "segregation",
  "removals",
  "special-population-actions",
  "vulnerable-population"
)

# ── Expected columns per dataset ─────────────────────────────────────────────

expected_cols <- list(
  `book-ins-by-arresting-agency-monthly` = c(
    "arresting_agency",
    "month",
    "date",
    "n_book_ins",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `book-ins-by-arresting-agency-fy-ytd` = c(
    "arresting_agency",
    "n_book_ins_ytd",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `book-outs-by-reason-monthly` = c(
    "release_reason",
    "criminality",
    "month",
    "date",
    "n_book_outs",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `book-outs-by-reason-fy-ytd` = c(
    "release_reason",
    "criminality",
    "n_book_outs_ytd",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `adp-and-stay-length-by-agency-monthly` = c(
    "agency",
    "criminality",
    "month",
    "date",
    "adp",
    "avg_stay_length_days",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `adp-and-stay-length-by-agency-fy-ytd` = c(
    "agency",
    "criminality",
    "adp_fy_ytd",
    "avg_stay_length_days_fy_ytd",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  facilities = c(
    "name",
    "address",
    "city",
    "state",
    "zip",
    "aor",
    "type_detailed",
    "male_female",
    "level_a",
    "level_b",
    "level_c",
    "level_d",
    "male_crim",
    "male_non_crim",
    "female_crim",
    "female_non_crim",
    "ice_threat_level_1",
    "ice_threat_level_2",
    "ice_threat_level_3",
    "no_ice_threat_level",
    "mandatory",
    "guaranteed_minimum",
    "last_inspection_type",
    "last_inspection_standard",
    "last_inspection_rating_final",
    "last_inspection_date",
    "second_to_last_inspection_type",
    "s2l_inspection_standard",
    "second_to_last_inspection_rating",
    "second_to_last_inspection_date",
    "odo_inspection_end_date",
    "odo_last_inspection_standard",
    "odo_final_rating",
    "last_nak_inspection_standard",
    "last_nak_inspection_rating",
    "last_nakamoto_inspection_date",
    "s2l_nak_inspection_type",
    "last_inspection_end_date",
    "pending_fy25_inspection",
    "last_final_rating",
    "odo_final_report_date",
    "pending_fy24_inspection",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `facility-alos` = c(
    "name",
    "alos_fiscal_year",
    "alos",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  removals = c(
    "n_removals_fy_ytd",
    "famu_removals",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `flows-by-facility-type` = c(
    "facility_type",
    "flow",
    "criminality",
    "n_flows",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `currently-detained-by-criminality` = c(
    "criminality",
    "arresting_agency",
    "n_detained",
    "share_of_total",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `atd-by-aor` = c(
    "aor",
    "technology",
    "n_active",
    "avg_length_in_program",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `atd-population` = c(
    "table",
    "category",
    "n_active",
    "daily_tech_cost",
    "alip",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `iclos-and-detainees` = c(
    "section",
    "population_type",
    "duration_bucket",
    "month",
    "date",
    "period",
    "value",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `bond-stats` = c(
    "month",
    "date",
    "metric",
    "unit",
    "value",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `special-population-actions` = c(
    "population_group",
    "action",
    "data_fiscal_year",
    "country",
    "value",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `atd-court-appearances` = c(
    "hearing_type",
    "metric",
    "count",
    "pct",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `currently-detained-by-disposition` = c(
    "disposition",
    "facility_type",
    "n_detained",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `fear-decision-time` = c(
    "data_fiscal_year",
    "facility_type",
    "decision_days",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `fear-decisions-by-facility-type` = c(
    "facility_type",
    "total_detained",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  segregation = c(
    "month",
    "date",
    "facility",
    "placement_count",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `vulnerable-population` = c(
    "placement_reason",
    "data_fiscal_year",
    "data_quarter",
    "n_placements",
    "avg_consecutive_days",
    "avg_cumulative_days",
    "fiscal_year",
    "file_date",
    "pull_date"
  )
)

VALID_MONTHS <- c(
  "Oct",
  "Nov",
  "Dec",
  "Jan",
  "Feb",
  "Mar",
  "Apr",
  "May",
  "Jun",
  "Jul",
  "Aug",
  "Sep"
)

agents <- list()

# ── 1. File inventory ─────────────────────────────────────────────────────────
# Check that all expected parquet files exist

inventory <- tibble(
  file = EXPECTED_FILES,
  present = file.exists(file.path("data", paste0(file, ".parquet")))
)

agents$inventory <- inventory |>
  create_agent(
    label = "file inventory",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_equal(
    columns = vars(present),
    value = TRUE,
    label = "all expected parquet files present"
  ) |>
  interrogate()

# ── 2. Schema checks ─────────────────────────────────────────────────────────
# Verify expected columns exist in datasets

for (name in names(expected_cols)) {
  df <- read_parquet(file.path("data", paste0(name, ".parquet")))
  expected <- expected_cols[[name]]
  check_df <- tibble(column = expected, present = expected %in% names(df))

  agents[[paste0("schema_", name)]] <- check_df |>
    create_agent(
      label = glue("{name} — schema"),
      actions = action_levels(warn_at = 1, stop_at = 1)
    ) |>
    col_vals_equal(
      columns = vars(present),
      value = TRUE,
      label = "expected columns present"
    ) |>
    interrogate()

  # Column order check
  order_ok <- identical(names(df), expected)
  order_df <- tibble(dataset = name, correct_order = order_ok)
  agents[[paste0("order_", name)]] <- order_df |>
    create_agent(
      label = glue("{name} — column order"),
      actions = action_levels(warn_at = 1, stop_at = 1)
    ) |>
    col_vals_equal(
      columns = vars(correct_order),
      value = TRUE,
      label = "columns in expected order"
    ) |>
    interrogate()
}

# ── 3. Fiscal year range ─────────────────────────────────────────────────────
# fiscal_year should be between 2019 and current FY + 1 across all datasets

current_fy <- if (as.integer(format(Sys.Date(), "%m")) >= 10) {
  as.integer(format(Sys.Date(), "%Y")) + 1L
} else {
  as.integer(format(Sys.Date(), "%Y"))
}

fy_datasets <- c(
  "book-ins-by-arresting-agency-monthly",
  "book-ins-by-arresting-agency-fy-ytd",
  "book-outs-by-reason-monthly",
  "book-outs-by-reason-fy-ytd",
  "adp-and-stay-length-by-agency-monthly",
  "adp-and-stay-length-by-agency-fy-ytd",
  "removals",
  "facilities",
  "currently-detained-by-criminality"
)

fy_df <- map_dfr(fy_datasets, \(name) {
  df <- read_parquet(file.path("data", paste0(name, ".parquet")))
  tibble(dataset = name, fiscal_year = df$fiscal_year)
})

agents$fiscal_year <- fy_df |>
  create_agent(
    label = "fiscal year range",
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_gte(
    columns = vars(fiscal_year),
    value = 2019,
    label = "fiscal_year >= 2019"
  ) |>
  col_vals_lte(
    columns = vars(fiscal_year),
    value = current_fy,
    label = glue("fiscal_year <= {current_fy}")
  ) |>
  col_vals_not_null(
    columns = vars(fiscal_year),
    label = "fiscal_year not null"
  ) |>
  interrogate()

# ── 4. Date ordering: file_date >= pull_date ─────────────────────────────────

bi_m <- read_parquet("data/book-ins-by-arresting-agency-monthly.parquet")
bi_y <- read_parquet("data/book-ins-by-arresting-agency-fy-ytd.parquet")
bo_m <- read_parquet("data/book-outs-by-reason-monthly.parquet")
bo_y <- read_parquet("data/book-outs-by-reason-fy-ytd.parquet")
adp_m <- read_parquet("data/adp-and-stay-length-by-agency-monthly.parquet")
adp_y <- read_parquet("data/adp-and-stay-length-by-agency-fy-ytd.parquet")

date_df <- bind_rows(
  bi_m |> select(file_date, pull_date) |> mutate(dataset = "book-ins-monthly"),
  bi_y |> select(file_date, pull_date) |> mutate(dataset = "book-ins-fy-ytd"),
  bo_m |> select(file_date, pull_date) |> mutate(dataset = "book-outs-monthly"),
  bo_y |> select(file_date, pull_date) |> mutate(dataset = "book-outs-fy-ytd"),
  adp_m |> select(file_date, pull_date) |> mutate(dataset = "adp-monthly"),
  adp_y |> select(file_date, pull_date) |> mutate(dataset = "adp-fy-ytd")
)

agents$date_order <- date_df |>
  create_agent(
    label = "date ordering (file_date >= pull_date)",
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_gte(
    columns = vars(file_date),
    value = as.Date("2018-01-01"),
    label = "file_date is a plausible date (>= 2018-01-01)"
  ) |>
  col_vals_not_null(
    columns = vars(file_date),
    label = "file_date not null"
  ) |>
  col_vals_not_null(
    columns = vars(pull_date),
    label = "pull_date not null"
  ) |>
  interrogate()

# ── 5. Non-negative counts ───────────────────────────────────────────────────

agents$nonneg_bookins <- bi_m |>
  create_agent(
    label = "book-ins — non-negative values",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_gte(
    columns = vars(n_book_ins),
    value = 0,
    na_pass = TRUE,
    label = "n_book_ins >= 0"
  ) |>
  interrogate()

agents$nonneg_bookins_ytd <- bi_y |>
  create_agent(
    label = "book-ins fy-ytd — non-negative values",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_gte(
    columns = vars(n_book_ins_ytd),
    value = 0,
    na_pass = TRUE,
    label = "n_book_ins_ytd >= 0"
  ) |>
  interrogate()

agents$nonneg_bookouts <- bo_m |>
  create_agent(
    label = "book-outs — non-negative values",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_gte(
    columns = vars(n_book_outs),
    value = 0,
    na_pass = TRUE,
    label = "n_book_outs >= 0"
  ) |>
  interrogate()

agents$nonneg_bookouts_ytd <- bo_y |>
  create_agent(
    label = "book-outs fy-ytd — non-negative values",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_gte(
    columns = vars(n_book_outs_ytd),
    value = 0,
    na_pass = TRUE,
    label = "n_book_outs_ytd >= 0"
  ) |>
  interrogate()

rem <- read_parquet("data/removals.parquet")
agents$nonneg_removals <- rem |>
  create_agent(
    label = "removals — non-negative values",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_gte(
    columns = vars(n_removals_fy_ytd),
    value = 0,
    na_pass = TRUE,
    label = "n_removals_fy_ytd >= 0"
  ) |>
  col_vals_gte(
    columns = vars(famu_removals),
    value = 0,
    na_pass = TRUE,
    label = "famu_removals >= 0"
  ) |>
  interrogate()

# ── 6. Categorical value checks ──────────────────────────────────────────────

agents$arresting_agency <- bi_m |>
  create_agent(
    label = "book-ins monthly — arresting agency values",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_in_set(
    columns = vars(arresting_agency),
    set = c("CBP", "ICE", "Total"),
    label = "arresting_agency in {CBP, ICE, Total}"
  ) |>
  col_vals_in_set(
    columns = vars(month),
    set = VALID_MONTHS,
    label = "month is a valid 3-letter abbreviation"
  ) |>
  interrogate()

cd <- read_parquet("data/currently-detained-by-criminality.parquet")
agents$criminality_vals <- cd |>
  create_agent(
    label = "currently-detained — criminality values",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_in_set(
    columns = vars(criminality),
    set = c("Convicted Criminal", "Pending Criminal Charges",
            "Other Immigration Violator", "Total"),
    label = "criminality in expected set"
  ) |>
  interrogate()

# ── 7. Percentage bounds ─────────────────────────────────────────────────────

agents$pct_bounds <- cd |>
  create_agent(
    label = "currently-detained — share bounds",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_between(
    columns = vars(share_of_total),
    left = 0,
    right = 1,
    na_pass = TRUE,
    label = "share_of_total in [0, 1]"
  ) |>
  interrogate()

# ── 8. Row count checks (no empty datasets) ──────────────────────────────────

row_counts <- map_dfr(EXPECTED_FILES, \(name) {
  path <- file.path("data", paste0(name, ".parquet"))
  if (file.exists(path)) {
    tibble(dataset = name, n_rows = nrow(read_parquet(path)))
  } else {
    tibble(dataset = name, n_rows = 0L)
  }
})

agents$row_counts <- row_counts |>
  create_agent(
    label = "row counts — no empty datasets",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_gt(
    columns = vars(n_rows),
    value = 0,
    label = "every dataset has at least one row"
  ) |>
  interrogate()

# ── 9. Facilities checks ─────────────────────────────────────────────────────

fac <- read_parquet("data/facilities.parquet")
agents$facilities <- fac |>
  filter(!is.na(type_detailed)) |>
  create_agent(
    label = "facilities — key fields",
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    columns = vars(name),
    label = "facility name not null"
  ) |>
  col_vals_in_set(
    columns = vars(type_detailed),
    set = c(
      "BOP",
      "CDF",
      "DIGSA",
      "DOD",
      "FAMILY",
      "FAMILY STAGING",
      "HOLD",
      "HOSPITAL",
      "IGSA",
      "JUVENILE",
      "MIRP",
      "MOC",
      "ORR",
      "Other",
      "SPC",
      "STAGING",
      "STATE",
      "TAP-ICE",
      "USMS CDF",
      "USMS IGA"
    ),
    label = "facility type in expected set"
  ) |>
  interrogate()

# ── 10. Data freshness ───────────────────────────────────────────────────────
# Latest file_date should be within 90 days of today

max_lag_days <- 90

freshness <- map_dfr(fy_datasets, \(name) {
  df <- read_parquet(file.path("data", paste0(name, ".parquet")))
  tibble(
    dataset = name,
    latest_file_date = max(df$file_date, na.rm = TRUE),
    lag_days = as.integer(Sys.Date() - max(df$file_date, na.rm = TRUE))
  )
})

agents$freshness <- freshness |>
  create_agent(
    label = "data freshness",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_lte(
    columns = vars(lag_days),
    value = max_lag_days,
    label = glue("latest data within {max_lag_days} days")
  ) |>
  interrogate()

# ── 11. Per-table EID staleness (informational) ──────────────────────────────
# `pull_date` is the per-table "EID as of" date from each source file's
# Footnotes tab. When ICE leaves a Detention/ADP/etc. sheet as a stale
# duplicate of an older report (a recurring ICE error pattern), the EID
# trails the file's publication date by months or years. Such rows remain in
# the parquet for archival completeness but rank low under "latest pull_date"
# selection downstream — surface them here for human review.
#
# Restricted to tables whose pull_date comes from a per-table EID footnote
# (the Facilities/ATD/ICLOS/bond/segregation/etc. tables fall back to the
# Facilities-tab IIDS, which is a different signal).

MAX_STALE_GAP <- 60  # days

stale_check_datasets <- c(
  "book-ins-by-arresting-agency-monthly",
  "book-ins-by-arresting-agency-fy-ytd",
  "book-outs-by-reason-monthly",
  "book-outs-by-reason-fy-ytd",
  "removals",
  "currently-detained-by-criminality",
  "adp-and-stay-length-by-agency-monthly",
  "adp-and-stay-length-by-agency-fy-ytd"
)

stale_offenders <- map_dfr(stale_check_datasets, \(name) {
  read_parquet(file.path("data", paste0(name, ".parquet"))) |>
    filter(!is.na(file_date), !is.na(pull_date)) |>
    distinct(file_date, pull_date) |>
    mutate(
      dataset = name,
      gap_days = as.integer(file_date - pull_date)
    )
}) |>
  filter(
    gap_days > MAX_STALE_GAP,
    # Filter out EOFY fallback file_dates: parse-spreadsheets.R falls back to
    # YYYY-12-31 when the source filename has no MMDDYYYY, which makes the
    # gap meaningless against the real publication date. ICE never publishes
    # on Dec 31 (federal holiday), so 12-31 is a reliable fallback marker.
    format(file_date, "%m-%d") != "12-31"
  ) |>
  group_by(file_date, pull_date, gap_days) |>
  summarise(datasets = paste(sort(dataset), collapse = ", "), .groups = "drop") |>
  arrange(desc(gap_days))

# ── 12. Release immutability ─────────────────────────────────────────────────
# parse-spreadsheets.R re-reads every spreadsheet on every run, so a parser
# tweak or a skiplist edit can silently rewrite rows attributed to prior
# releases. Compare each parquet against its git baseline (default HEAD; in
# CI override to HEAD~1 / origin/main) and require that rows whose `file_date`
# already existed in the baseline are byte-identical now. New rows are only
# allowed when they carry a `file_date` not yet seen in the baseline.
#
# Comparison is via anti_join on stringified columns to neutralize spurious
# type drift from the join machinery; nanoparquet round-trips are
# deterministic, so any genuine value change still surfaces.
#
# This check applies only to per-release snapshot tables. Latest-per-cell
# tables (monthly historical, FY closed, ALOS, etc.) are deliberately
# rewritten when a newer release reports a revised value — file_date on
# those rows tracks the latest reporting release, not the row's lineage.

# Datasets where collapse_to_latest in parse-spreadsheets.R intentionally
# overwrites prior rows when ICE revises a (cell) value. Skip immutability
# for these — the rewrite IS the contract.
LATEST_PER_CELL_DATASETS <- c(
  "adp-and-stay-length-by-agency-monthly",
  "book-ins-by-arresting-agency-monthly",
  "book-outs-by-reason-monthly",
  "bond-stats",
  "segregation",
  "iclos-and-detainees",
  "fear-decision-time",
  "special-population-actions",
  "vulnerable-population",
  "facility-alos"
)

baseline_ref <- Sys.getenv("BASELINE_REF", "HEAD")

# Parquet files are stored in git LFS — `git show <ref>:path` returns the
# pointer text, so we pipe through `git lfs smudge` to materialize the blob.
read_parquet_at_ref <- function(rel_path, ref) {
  tmp <- tempfile(fileext = ".parquet")
  cmd <- glue(
    "git show {shQuote(paste0(ref, ':', rel_path))} | ",
    "git lfs smudge > {shQuote(tmp)}"
  )
  status <- suppressWarnings(system(cmd, ignore.stderr = TRUE))
  if (status != 0 || !file.exists(tmp) || file.size(tmp) == 0L) {
    unlink(tmp)
    return(NULL)
  }
  result <- tryCatch(read_parquet(tmp), error = function(e) NULL)
  unlink(tmp)
  result
}

immutability_results <- map_dfr(setdiff(EXPECTED_FILES, LATEST_PER_CELL_DATASETS), \(name) {
  rel_path <- file.path("data", paste0(name, ".parquet"))
  current <- if (file.exists(rel_path)) read_parquet(rel_path) else NULL
  baseline <- read_parquet_at_ref(rel_path, baseline_ref)

  if (is.null(current) || is.null(baseline) ||
      !"file_date" %in% names(current) ||
      !"file_date" %in% names(baseline)) {
    return(tibble(
      dataset = name,
      baseline_available = !is.null(baseline) && "file_date" %in% names(baseline),
      n_added = NA_integer_,
      n_removed = NA_integer_
    ))
  }

  old_dates <- unique(baseline$file_date)
  b <- baseline |> filter(file_date %in% old_dates)
  cu <- current  |> filter(file_date %in% old_dates)

  # Restrict to columns common to both — a column added in current is treated
  # as schema evolution, not a mutation of old data; the schema check in
  # section 2 covers required columns separately.
  common_cols <- intersect(names(b), names(cu))
  b  <- b  |> select(all_of(common_cols)) |> mutate(across(everything(), as.character))
  cu <- cu |> select(all_of(common_cols)) |> mutate(across(everything(), as.character))

  tibble(
    dataset = name,
    baseline_available = TRUE,
    n_added   = nrow(dplyr::anti_join(cu, b, by = common_cols)),
    n_removed = nrow(dplyr::anti_join(b, cu, by = common_cols))
  )
})

agents$immutability <- immutability_results |>
  filter(baseline_available) |>
  create_agent(
    label = glue("release immutability (vs git {baseline_ref})"),
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_equal(
    columns = vars(n_added),
    value = 0,
    label = "no rows added under previously-seen file_date"
  ) |>
  col_vals_equal(
    columns = vars(n_removed),
    value = 0,
    label = "no rows removed under previously-seen file_date"
  ) |>
  interrogate()

immutability_offenders <- immutability_results |>
  filter(baseline_available, (n_added > 0 | n_removed > 0)) |>
  arrange(desc(n_added + n_removed))

immutability_skipped <- immutability_results |>
  filter(!baseline_available) |>
  pull(dataset)

# ── 13. New release coverage ─────────────────────────────────────────────────
# Symmetric counterpart to immutability: when a new spreadsheet appears in
# spreadsheets/ relative to the git baseline, the parser must produce at least
# one row carrying that file's expected file_date in a core table. Catches
# silent parsing failures where the new release was downloaded and committed
# but no rows landed in the parquets — which the immutability check alone
# would let pass.
#
# We require ≥1 of the 5 always-published core tables (not all) because
# `sheet_skiplist` in parse-spreadsheets.R can legitimately suppress a
# specific (file, sheet) pair. Total absence is the smoke test.

CORE_RELEASE_TABLES <- c(
  "book-ins-by-arresting-agency-fy-ytd",
  "adp-and-stay-length-by-agency-fy-ytd",
  "removals",
  "facilities",
  "currently-detained-by-criminality"
)

# Mirror parse-spreadsheets.R's filename → file_date logic. Keep in sync with
# the dating block in `file_pull_dates`: prefer a leading YYYY-MM-DD prefix;
# else take the last 4- to 8-digit run (8 = MMDDYYYY; 6 = MMDDYY when mm<=12
# else YYMMDD; 4 = MMDD combined with FY); else fall back to YYYY-12-31 from
# the FY tag.
derive_file_date <- function(filename) {
  bn <- basename(filename)
  fy <- stringr::str_extract(bn, "(?<=FY)\\d{2}")
  fy_int <- if (is.na(fy)) NA_integer_ else as.integer(fy) + 2000L

  ymd_prefix <- stringr::str_extract(bn, "^[0-9]{4}-[0-9]{2}-[0-9]{2}")
  if (!is.na(ymd_prefix)) {
    parsed <- lubridate::ymd(ymd_prefix, quiet = TRUE)
    if (!is.na(parsed)) return(parsed)
  }

  ds <- stringr::str_extract_all(filename, "\\d{4,8}") |>
    map_chr(~ dplyr::last(.x, default = NA_character_))
  parsed <- if (is.na(ds)) {
    as.Date(NA)
  } else if (nchar(ds) == 8L) {
    lubridate::mdy(ds, quiet = TRUE)
  } else if (nchar(ds) == 6L) {
    mm <- suppressWarnings(as.integer(substr(ds, 1, 2)))
    if (!is.na(mm) && mm <= 12L) lubridate::mdy(ds, quiet = TRUE)
    else lubridate::ymd(ds, quiet = TRUE)
  } else if (nchar(ds) == 4L && !is.na(fy_int)) {
    mm <- suppressWarnings(as.integer(substr(ds, 1, 2)))
    dd <- suppressWarnings(as.integer(substr(ds, 3, 4)))
    if (is.na(mm) || is.na(dd) || mm > 12L || mm < 1L) {
      as.Date(NA)
    } else {
      cy <- if (mm >= 10L) fy_int - 1L else fy_int
      lubridate::make_date(cy, mm, dd)
    }
  } else {
    as.Date(NA)
  }
  if (!is.na(parsed)) parsed else as.Date(paste0("20", fy, "-12-31"))
}

baseline_spreadsheets <- suppressWarnings(system2(
  "git",
  c("ls-tree", "-r", "--name-only", baseline_ref, "--", "spreadsheets/"),
  stdout = TRUE, stderr = FALSE
)) |> basename()

# download-new-dtm.R adds at most one spreadsheet per run, so just take the
# first novel filename. Returns character(0) when nothing is new.
new_spreadsheet <- head(
  setdiff(list.files("spreadsheets", pattern = "\\.xlsx$"), baseline_spreadsheets),
  1
)

coverage_results <- if (length(new_spreadsheet) == 0L) {
  tibble(
    spreadsheet = character(),
    expected_file_date = as.Date(character()),
    tables_with_data = integer()
  )
} else {
  expected <- derive_file_date(new_spreadsheet)
  n_tables <- sum(map_lgl(CORE_RELEASE_TABLES, \(t) {
    fd <- read_parquet(file.path("data", paste0(t, ".parquet")))$file_date
    any(!is.na(fd) & fd == expected)
  }))
  tibble(spreadsheet = new_spreadsheet, expected_file_date = expected,
         tables_with_data = n_tables)
}

agents$new_release_coverage <- coverage_results |>
  create_agent(
    label = "new release coverage — new spreadsheet contributed data",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_gt(
    columns = vars(tables_with_data),
    value = 0,
    label = "new file_date present in ≥1 core table"
  ) |>
  interrogate()

# ── Generate markdown summary ─────────────────────────────────────────────────

all_pass <- every(agents, \(a) all(a$validation_set$all_passed))

md <- map_chr(agents, \(a) {
  header <- glue("### {a$label}")
  rows <- a$validation_set |>
    transmute(
      status = if_else(all_passed, "PASS", "FAIL"),
      label,
      detail = glue(
        "n={format(n, big.mark=',')} failing={format(n_failed, big.mark=',')}"
      )
    ) |>
    pmap_chr(\(status, label, detail) {
      icon <- if (status == "PASS") ":white_check_mark:" else ":x:"
      line <- glue("- {icon} **{status}** {label}")
      if (status != "PASS") {
        line <- glue("{line}  \n  {detail}")
      }
      line
    })
  paste(c(header, rows), collapse = "\n")
}) |>
  paste(collapse = "\n\n")

overall <- if (all_pass) ":white_check_mark: **PASS**" else ":x: **FAIL**"
md <- paste0("## Data validation: ", overall, "\n\n", md, "\n")

stale_section <- if (nrow(stale_offenders) == 0) {
  paste(
    glue("### staleness — per-table EID vs file_date (gap > {MAX_STALE_GAP}d, informational)"),
    "- :white_check_mark: no source files exceed the staleness gap",
    sep = "\n"
  )
} else {
  rows <- stale_offenders |>
    pmap_chr(\(file_date, pull_date, gap_days, datasets) {
      glue(
        "- :warning: file_date `{file_date}` — pull_date `{pull_date}` — ",
        "gap **{gap_days}d** ({datasets})"
      )
    })
  paste(c(
    glue("### staleness — per-table EID vs file_date (gap > {MAX_STALE_GAP}d, informational)"),
    "",
    glue(
      "Source files whose per-table `EID as of` date trails the file's ",
      "publication date by more than {MAX_STALE_GAP} days. These rows remain ",
      "in the parquet but rank low under 'latest `pull_date`' selection ",
      "downstream. For known-bad files, add to `sheet_skiplist` in ",
      "`parse-spreadsheets.R`."
    ),
    "",
    rows
  ), collapse = "\n")
}
md <- paste0(md, "\n", stale_section, "\n")

immutability_section <- if (nrow(immutability_offenders) == 0) {
  paste(
    glue("### release immutability — drift in prior-release rows (vs git `{baseline_ref}`)"),
    if (length(immutability_skipped) > 0) {
      paste0(
        "- :information_source: no baseline available (skipped): ",
        paste(immutability_skipped, collapse = ", ")
      )
    } else {
      "- :white_check_mark: prior-release rows match the git baseline"
    },
    sep = "\n"
  )
} else {
  rows <- immutability_offenders |>
    pmap_chr(\(dataset, baseline_available, n_added, n_removed) {
      glue(
        "- :x: `{dataset}` — added **{n_added}**, removed **{n_removed}** ",
        "rows under file_dates already in baseline"
      )
    })
  paste(c(
    glue("### release immutability — drift in prior-release rows (vs git `{baseline_ref}`)"),
    "",
    glue(
      "Rows attributed to source files seen in the baseline must not change ",
      "when a new release is parsed. Drift here means the parser or skiplist ",
      "rewrote previously-released data — investigate before merging."
    ),
    "",
    rows
  ), collapse = "\n")
}
md <- paste0(md, "\n", immutability_section, "\n")

coverage_line <- if (nrow(coverage_results) == 0L) {
  "- :information_source: no newly-added spreadsheet to verify"
} else {
  r <- coverage_results
  n_core <- length(CORE_RELEASE_TABLES)
  if (r$tables_with_data > 0L) {
    glue(
      "- :white_check_mark: `{r$spreadsheet}` (file_date `{r$expected_file_date}`) ",
      "— present in {r$tables_with_data}/{n_core} core tables"
    )
  } else {
    glue(
      "- :x: `{r$spreadsheet}` — expected file_date `{r$expected_file_date}` ",
      "absent from all {n_core} core tables (parser produced no rows?)"
    )
  }
}
md <- paste0(
  md, "\n### new release coverage (vs git `", baseline_ref, "`)\n",
  coverage_line, "\n"
)

out_path <- Sys.getenv("CHECK_SUMMARY_PATH", "check-summary.md")
writeLines(md, out_path)

cat(md, "\n")
if (!all_pass) {
  stop("Some checks FAILED — see output above.", call. = FALSE)
}
