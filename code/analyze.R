# Manual-use plotting script. Read a parquet, make ggplots inline. Run blocks
# interactively in RStudio.

library(ggplot2)
library(dplyr)
library(tidyr)
library(stringr)
library(nanoparquet)

theme_set(theme_minimal(base_size = 11) + theme(legend.position = "bottom"))

# ── adp-and-stay-length-by-agency-monthly ────────────────────────────────────
adp_m <- read_parquet("data/adp-and-stay-length-by-agency-monthly.parquet")

ggplot(adp_m, aes(date, adp, color = criminality)) +
  geom_line() +
  facet_wrap(~agency, scales = "free_y") +
  labs(title = "Average daily population", y = "ADP")

ggplot(adp_m, aes(date, avg_stay_length_days, color = criminality)) +
  geom_line() +
  facet_wrap(~agency, scales = "free_y") +
  labs(title = "Average length of stay", y = "Days")

# ── adp-and-stay-length-by-agency-fy-ytd ─────────────────────────────────────
adp_y <- read_parquet("data/adp-and-stay-length-by-agency-fy-ytd.parquet")

ggplot(
  adp_y,
  aes(
    pull_date,
    adp_fy_ytd,
    color = criminality,
    group = interaction(criminality, fiscal_year)
  )
) +
  geom_line() +
  facet_wrap(~agency, scales = "free_y") +
  labs(
    title = "ADP — fiscal year to date (one point per release)",
    y = "ADP FY YTD"
  )

ggplot(
  adp_y,
  aes(
    pull_date,
    avg_stay_length_days_fy_ytd,
    color = criminality,
    group = interaction(criminality, fiscal_year)
  )
) +
  geom_line() +
  facet_wrap(~agency, scales = "free_y") +
  labs(
    title = "Avg length of stay — FY YTD (one point per release)",
    y = "Days"
  )

# ── book-ins-by-arresting-agency-monthly ─────────────────────────────────────
bi_m <- read_parquet("data/book-ins-by-arresting-agency-monthly.parquet")

ggplot(bi_m, aes(date, n_book_ins, color = arresting_agency)) +
  geom_line() +
  labs(title = "Monthly book-ins", y = "Book-ins")

# ── book-ins-by-arresting-agency-fy-ytd ──────────────────────────────────────
bi_y <- read_parquet("data/book-ins-by-arresting-agency-fy-ytd.parquet")

ggplot(
  bi_y,
  aes(
    file_date,
    n_book_ins_ytd,
    color = arresting_agency,
    group = interaction(arresting_agency, fiscal_year)
  )
) +
  geom_line() +
  labs(
    title = "Book-ins — FY to date (one point per release)",
    y = "Book-ins YTD"
  )

# ── book-outs-by-reason-monthly ──────────────────────────────────────────────
bo_m <- read_parquet("data/book-outs-by-reason-monthly.parquet")

ggplot(bo_m, aes(date, n_book_outs, color = criminality)) +
  geom_line() +
  facet_wrap(~release_reason, scales = "free_y") +
  labs(title = "Monthly book-outs", y = "Book-outs")

# ── book-outs-by-reason-fy-ytd ───────────────────────────────────────────────
bo_y <- read_parquet("data/book-outs-by-reason-fy-ytd.parquet")

ggplot(
  bo_y,
  aes(
    file_date,
    n_book_outs_ytd,
    color = criminality,
    group = interaction(criminality, fiscal_year)
  )
) +
  geom_line() +
  facet_wrap(~release_reason, scales = "free_y") +
  labs(title = "Book-outs FY YTD (one point per release)", y = "Book-outs YTD")

# ── currently-detained-by-criminality ────────────────────────────────────────
cd <- read_parquet("data/currently-detained-by-criminality.parquet")

ggplot(cd, aes(file_date, n_detained, color = arresting_agency)) +
  geom_line() +
  facet_wrap(~criminality, scales = "free_y") +
  labs(
    title = "Currently detained by criminality × arresting agency",
    y = "Count"
  )

ggplot(
  filter(cd, arresting_agency != "Total"),
  aes(file_date, share_of_total, color = arresting_agency)
) +
  geom_line() +
  facet_wrap(~criminality) +
  labs(
    title = "Arresting-agency share of currently detained",
    y = "Share of total (0-1)"
  )

# ── currently-detained-by-disposition ────────────────────────────────────────
cdd <- read_parquet("data/currently-detained-by-disposition.parquet")

ggplot(cdd, aes(file_date, n_detained, color = facility_type)) +
  geom_line() +
  facet_wrap(~disposition, scales = "free_y") +
  labs(title = "Currently detained — disposition × facility type", y = "Count")

# ── removals ─────────────────────────────────────────────────────────────────
pt <- read_parquet("data/removals.parquet")

ggplot(pt, aes(file_date, n_removals_fy_ytd, color = factor(fiscal_year))) +
  geom_line() +
  geom_point() +
  labs(title = "Removals — FY to date", y = "Removals", color = "FY")

ggplot(pt, aes(file_date, famu_removals, color = factor(fiscal_year))) +
  geom_line() +
  geom_point() +
  labs(title = "FAMU removals — FY to date", y = "Removals", color = "FY")

# ── movements-by-facility-type ───────────────────────────────────────────────
movements <- read_parquet("data/movements-by-facility-type.parquet")

ggplot(
  filter(movements, movement == "book-in"),
  aes(
    file_date,
    n_movements,
    color = criminality,
    group = interaction(criminality, fiscal_year)
  )
) +
  geom_line() +
  facet_wrap(~facility_type, scales = "free_y") +
  labs(title = "Book-ins by facility type × criminality (FY YTD)", y = "Count")

ggplot(
  filter(movements, movement == "book-out"),
  aes(
    file_date,
    n_movements,
    color = facility_type,
    group = interaction(facility_type, fiscal_year)
  )
) +
  geom_line() +
  labs(title = "Book-outs by facility type (FY YTD, Total only)", y = "Count")

# ── facilities ───────────────────────────────────────────────────────────────
# ICE's facility columns mix counts vs. FY-averages vs. percentages across
# years, so summing across facilities is unreliable. Per-facility lookups
# and ALOS (next block) are more trustworthy.
fac <- read_parquet("data/facilities.parquet") |>
  filter(!is.na(type_detailed))

# Number of operational facilities per type, over time.
fac |>
  count(file_date, type_detailed, name = "n_facilities") |>
  ggplot(aes(file_date, n_facilities, color = type_detailed)) +
  geom_line() +
  labs(title = "Operational facilities per type", y = "Count")

# Top 12 largest facilities (by recent total beds) over time.
fac_beds <- fac |>
  mutate(across(c(level_a, level_b, level_c, level_d), \(x) {
    suppressWarnings(as.numeric(x))
  })) |>
  mutate(
    total_beds = rowSums(
      across(c(level_a, level_b, level_c, level_d)),
      na.rm = TRUE
    )
  )

top12 <- fac_beds |>
  filter(file_date == max(file_date, na.rm = TRUE), total_beds > 0) |>
  slice_max(total_beds, n = 12) |>
  pull(name)

fac_beds |>
  filter(name %in% top12) |>
  ggplot(aes(file_date, total_beds, color = name)) +
  geom_line() +
  labs(
    title = "Top-12 largest facilities — total beds (level A-D) over time",
    y = "Total beds"
  )

# ── facility-alos ────────────────────────────────────────────────────────────
fal <- read_parquet("data/facility-alos.parquet") |>
  left_join(
    fac |>
      arrange(file_date) |>
      group_by(name) |>
      slice_tail(n = 1) |>
      ungroup() |>
      select(name, type_detailed),
    by = "name"
  )

fal |>
  filter(!is.na(type_detailed)) |>
  group_by(type_detailed, alos_fiscal_year) |>
  summarise(mean_alos = mean(alos, na.rm = TRUE), .groups = "drop") |>
  ggplot(aes(alos_fiscal_year, mean_alos, color = type_detailed)) +
  geom_line() +
  geom_point() +
  labs(
    title = "Mean facility ALOS by alos_fiscal_year × type_detailed",
    y = "ALOS days"
  )

# Top 12 facilities by recent ALOS, per-facility lines.
top12_alos <- fal |>
  filter(alos_fiscal_year == max(alos_fiscal_year, na.rm = TRUE), alos > 0) |>
  slice_max(alos, n = 12) |>
  pull(name)

fal |>
  filter(name %in% top12_alos) |>
  ggplot(aes(alos_fiscal_year, alos, color = name)) +
  geom_line() +
  geom_point() +
  labs(
    title = "Top-12 facilities by latest ALOS — per-facility trend",
    y = "ALOS days"
  )

# ── fear-decision-time ───────────────────────────────────────────────────────
fdt <- read_parquet("data/fear-decision-time.parquet")

ggplot(fdt, aes(data_fiscal_year, decision_days, fill = facility_type)) +
  geom_col(position = "dodge") +
  labs(title = "Fear decision time by FY × facility type", y = "Days")

# ── fear-decisions-by-facility-type ──────────────────────────────────────────
fdf <- read_parquet("data/fear-decisions-by-facility-type.parquet")

ggplot(fdf, aes(file_date, total_detained, color = facility_type)) +
  geom_line() +
  labs(
    title = "Fear decisions detained — by facility type",
    y = "Total detained"
  )

# ── bond-stats ───────────────────────────────────────────────────────────────
bs <- read_parquet("data/bond-stats.parquet")

ggplot(bs, aes(date, value)) +
  geom_line() +
  facet_wrap(~metric, scales = "free_y") +
  labs(title = "Monthly bond stats — value by metric")

# ── segregation ──────────────────────────────────────────────────────────────
seg <- read_parquet("data/segregation.parquet")

top_facilities <- seg |>
  group_by(facility) |>
  summarise(total = sum(placement_count, na.rm = TRUE), .groups = "drop") |>
  slice_max(total, n = 12) |>
  pull(facility)

ggplot(filter(seg, facility %in% top_facilities), aes(date, placement_count)) +
  geom_line() +
  facet_wrap(~facility, scales = "free_y") +
  labs(
    title = "Segregation placements — top 12 facilities by total",
    y = "Placements"
  )

# ── atd-population ───────────────────────────────────────────────────────────
atdp <- read_parquet("data/atd-population.parquet")

ggplot(atdp, aes(file_date, n_active, color = category)) +
  geom_line() +
  facet_wrap(~table, scales = "free_y") +
  labs(
    title = "ATD population — active participants",
    y = "Active participants"
  )

ggplot(
  filter(atdp, table == "technology", !is.na(daily_tech_cost)),
  aes(file_date, daily_tech_cost, color = category)
) +
  geom_line() +
  labs(
    title = "ATD population — daily tech cost (USD, technology block)",
    y = "Daily tech cost (USD)"
  )

ggplot(
  filter(atdp, table == "status", !is.na(alip)),
  aes(file_date, alip, color = category)
) +
  geom_line() +
  labs(
    title = "ATD population — average length in program (status block)",
    y = "ALIP (days)"
  )

# ── atd-by-aor ───────────────────────────────────────────────────────────────
atda <- read_parquet("data/atd-by-aor.parquet")

aor_rollup <- filter(atda, is.na(technology), aor != "Total")
aor_detail <- filter(atda, !is.na(technology))

ggplot(aor_rollup, aes(file_date, n_active, color = aor)) +
  geom_line() +
  labs(
    title = "ATD active participants — per-AOR snapshots",
    y = "Active participants"
  )

ggplot(aor_rollup, aes(file_date, avg_length_in_program, color = aor)) +
  geom_line() +
  labs(title = "ATD avg length in program — per-AOR totals", y = "Days")

ggplot(aor_detail, aes(file_date, n_active, color = technology)) +
  geom_line() +
  facet_wrap(~aor, scales = "free_y") +
  labs(
    title = "ATD active participants — by AOR × technology",
    y = "Active participants"
  )

ggplot(aor_detail, aes(file_date, avg_length_in_program, color = technology)) +
  geom_line() +
  facet_wrap(~aor, scales = "free_y") +
  labs(title = "ATD avg length in program — by AOR × technology", y = "Days")

# ── atd-court-appearances ────────────────────────────────────────────────────
atdc <- read_parquet("data/atd-court-appearances.parquet")

ggplot(atdc, aes(file_date, count, color = metric)) +
  geom_line() +
  facet_wrap(~hearing_type) +
  labs(title = "ATD court appearances — count", y = "Count")

ggplot(atdc, aes(file_date, pct, color = metric)) +
  geom_line() +
  facet_wrap(~hearing_type) +
  labs(title = "ATD court appearances — share", y = "Share (0-1)")

# ── iclos-and-detainees ──────────────────────────────────────────────────────
iclos_full <- read_parquet("data/iclos-and-detainees.parquet")

iclos_only <- filter(iclos_full, section == "iclos")
det_only <- filter(iclos_full, section == "detainees")

ggplot(
  iclos_only,
  aes(date, value, color = population_type, linetype = period)
) +
  geom_line() +
  labs(
    title = "ICLOS — average length of stay by population type",
    y = "Days"
  )

ggplot(det_only, aes(date, value, color = duration_bucket, linetype = period)) +
  geom_line() +
  facet_wrap(~population_type, scales = "free_y") +
  labs(title = "Detainees — population by duration bucket", y = "Detainees")

# ── special-population-actions ───────────────────────────────────────────────
spa <- read_parquet("data/special-population-actions.parquet")

spa_rollup <- filter(spa, is.na(country))
spa_tps <- filter(spa, !is.na(country))

ggplot(spa_rollup, aes(data_fiscal_year, value, color = population_group)) +
  geom_line() +
  geom_point() +
  facet_wrap(~action, scales = "free_y") +
  labs(
    title = "Special-population actions — non-country rollups",
    y = "FY total count"
  )

ggplot(spa_tps, aes(data_fiscal_year, value, color = country)) +
  geom_line() +
  facet_wrap(~action, scales = "free_y") +
  labs(
    title = "Special-population actions — TPS by country × action",
    y = "FY total count"
  )

# ── vulnerable-population ────────────────────────────────────────────────────
vp <- read_parquet("data/vulnerable-population.parquet") |>
  mutate(period = data_fiscal_year + (data_quarter - 1) / 4)

ggplot(vp, aes(period, n_placements, color = placement_reason)) +
  geom_line() +
  labs(
    title = "Vulnerable population — placements per FY-quarter",
    x = "Fiscal year (quarter as fraction)",
    y = "Placements"
  )

ggplot(vp, aes(period, avg_consecutive_days, color = placement_reason)) +
  geom_line() +
  labs(
    title = "Vulnerable population — avg consecutive days",
    x = "Fiscal year (quarter as fraction)",
    y = "Days"
  )

ggplot(vp, aes(period, avg_cumulative_days, color = placement_reason)) +
  geom_line() +
  labs(
    title = "Vulnerable population — avg cumulative days",
    x = "Fiscal year (quarter as fraction)",
    y = "Days"
  )
