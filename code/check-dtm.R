library(dplyr)
library(purrr)
library(nanoparquet)
library(glue)
library(pointblank)

# ── Expected parquet files ────────────────────────────────────────────────────

EXPECTED_FILES <- c(
  "adp-and-stay-length-by-agency",
  "atd-by-aor",
  "atd-court-appearances",
  "atd-population",
  "book-ins-by-arresting-agency",
  "book-outs-by-reason-annual",
  "book-outs-by-reason-monthly",
  "currently-detained-by-criminality",
  "currently-detained-by-disposition",
  "facilities",
  "fear-decision-time",
  "fear-decisions-by-facility-type",
  "flows-by-facility-type",
  "iclos-and-detainees",
  "bond-stats",
  "segregation",
  "pull-totals",
  "special-population-actions",
  "vulnerable-population"
)

# ── Expected columns per dataset ─────────────────────────────────────────────

expected_cols <- list(
  `book-ins-by-arresting-agency` = c(
    "arresting_agency",
    "month",
    "date",
    "n_book_ins",
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
    "n_book_outs_ytd",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `adp-and-stay-length-by-agency` = c(
    "agency",
    "criminality",
    "month",
    "date",
    "adp",
    "adp_fy_ytd",
    "avg_stay_length_days",
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
    "alos",
    "odo_inspection_end_date",
    "odo_last_inspection_standard",
    "odo_final_rating",
    "last_nak_inspection_standard",
    "last_nak_inspection_rating",
    "last_nakamoto_inspection_date",
    "s2l_nak_inspection_type",
    "odo_final_report_date",
    "last_inspection_end_date",
    "pending_fy25_inspection",
    "last_final_rating",
    "pending_fy24_inspection",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `pull-totals` = c(
    "n_removals_fy_ytd",
    "famu_removals",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `flows-by-facility-type` = c(
    "facility_type",
    "n_book_ins_convicted_criminal",
    "n_book_ins_pending_charges",
    "n_book_ins_other_imm_violator",
    "n_book_ins_total",
    "n_book_outs_total",
    "fiscal_year",
    "file_date",
    "pull_date"
  ),
  `currently-detained-by-criminality` = c(
    "criminality",
    "ice",
    "percent_ice",
    "cbp",
    "percent_cbp",
    "total",
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
  "book-ins-by-arresting-agency",
  "book-outs-by-reason-monthly",
  "adp-and-stay-length-by-agency",
  "pull-totals",
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

bi <- read_parquet("data/book-ins-by-arresting-agency.parquet")
bo <- read_parquet("data/book-outs-by-reason-monthly.parquet")
adp <- read_parquet("data/adp-and-stay-length-by-agency.parquet")

date_df <- bind_rows(
  bi |> select(file_date, pull_date) |> mutate(dataset = "book-ins"),
  bo |> select(file_date, pull_date) |> mutate(dataset = "book-outs"),
  adp |> select(file_date, pull_date) |> mutate(dataset = "adp")
)

agents$date_order <- date_df |>
  create_agent(
    label = "date ordering (file_date >= pull_date)",
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_gte(
    columns = vars(file_date),
    value = min(date_df$pull_date, na.rm = TRUE),
    label = "file_date is a plausible date"
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

agents$nonneg_bookins <- bi |>
  create_agent(
    label = "book-ins — non-negative values",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_gte(
    columns = vars(n_book_ins_ytd),
    value = 0,
    na_pass = TRUE,
    label = "n_book_ins_ytd >= 0"
  ) |>
  col_vals_gte(
    columns = vars(n_book_ins),
    value = 0,
    na_pass = TRUE,
    label = "n_book_ins >= 0"
  ) |>
  interrogate()

agents$nonneg_bookouts <- bo |>
  create_agent(
    label = "book-outs — non-negative values",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_gte(
    columns = vars(n_book_outs_ytd),
    value = 0,
    na_pass = TRUE,
    label = "n_book_outs_ytd >= 0"
  ) |>
  col_vals_gte(
    columns = vars(n_book_outs),
    value = 0,
    na_pass = TRUE,
    label = "n_book_outs >= 0"
  ) |>
  interrogate()

rem <- read_parquet("data/pull-totals.parquet")
agents$nonneg_removals <- rem |>
  create_agent(
    label = "pull-totals — non-negative values",
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

agents$arresting_agency <- bi |>
  create_agent(
    label = "book-ins — arresting agency values",
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
    set = c("Convicted Criminal", "Pending Criminal Charges", "Total"),
    label = "criminality in expected set"
  ) |>
  interrogate()

# ── 7. Percentage bounds ─────────────────────────────────────────────────────

agents$pct_bounds <- cd |>
  create_agent(
    label = "currently-detained — percent bounds",
    actions = action_levels(warn_at = 1, stop_at = 1)
  ) |>
  col_vals_between(
    columns = vars(percent_ice),
    left = 0,
    right = 1,
    na_pass = TRUE,
    label = "percent_ice in [0, 1]"
  ) |>
  col_vals_between(
    columns = vars(percent_cbp),
    left = 0,
    right = 1,
    na_pass = TRUE,
    label = "percent_cbp in [0, 1]"
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
      "IGSA",
      "JUVENILE",
      "MOC",
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

out_path <- Sys.getenv("CHECK_SUMMARY_PATH", "check-summary.md")
writeLines(md, out_path)

cat(md, "\n")
if (!all_pass) {
  stop("Some checks FAILED — see output above.", call. = FALSE)
}
