# =============================================================================
# WeCare ETL • run.R
# -----------------------------------------------------------------------------
# Orchestrates the full pipeline:
#   1) Load secrets (tokens) and preflight checks
#   2) Source step files (bootstrap, utils, fetch, clean, attach, merge, QC)
#   3) Fetch raw REDCap data (baseline + follow-ups)
#   4) Clean baselines (1 row per ID; canonical IDs)
#   5) Attach follow-ups (3m/6m) to baselines + build completion indicators
#   6) Merge Youth & Caregiver (full outer), label/coalesce overlaps, write outputs
#   7) QC summaries + schema overlap reports
#
# Usage:
#   From the repo root:   source("run.R")
#
# Outputs:
#   data/raw/    : raw pulls from REDCap
#   data/out/    : cleaned baselines, plus tables, merged dataset
#   data/checks/ : audits, summaries, overlap maps, unmatched IDs
# =============================================================================


# --- 0) Load secrets ----------------------------------------------------------
# Expect a file tokens.R defining:
#   - SHARED_URL: REDCap base URL (string)
#   - tokens: named vector/list with elements:
#       "youth_baseline", "caregiver_baseline", "youth_followup", "caregiver_followup"

if (!file.exists("tokens.R")) {
  stop("Missing tokens.R. Copy tokens.example.R → tokens.R and fill in credentials.")
}
source("tokens.R")

# Back-compat: allow SHARED_URL but prefer SHARED_URL
if (!exists("SHARED_URL") || !nzchar(SHARED_URL)) {
  if (exists("SHARED_URL") && nzchar(SHARED_URL)) {
    SHARED_URL <- SHARED_URL
    message("ℹ Using SHARED_URL as SHARED_URL for backward compatibility.")
  } else {
    stop("SHARED_URL missing in tokens.R")
  }
}

# Validate token labels
required_labels <- c("youth_baseline","caregiver_baseline","youth_followup","caregiver_followup")
if (!exists("tokens")) stop("`tokens` object not found in tokens.R")
miss <- setdiff(required_labels, names(tokens))
if (length(miss)) stop("`tokens` is missing labels: ", paste(miss, collapse = ", "))


# --- 1) Source step files -----------------------------------------------------
# Prefer new filenames; fall back to old ones for compatibility.

source_first <- function(paths) {
  for (p in paths) if (file.exists(p)) { source(p); return(invisible(p)) }
  stop("None of these files exist: ", paste(paths, collapse = ", "))
}

# Core bootstrap & packages
source_first(c("R/00_bootstrap.R",                "R/00_setup.R"))
ensure_pkgs(c("REDCapR","dplyr","janitor","readr"))  # janitor used in fetch; readr used by write_csv_safe

# Shared utils + pipeline steps
source_first(c("R/05_utils_io.R",                 "R/05_utils.R"))
source_first(c("R/10_fetch_redcap.R",             "R/10_fetch.R"))
source_first(c("R/20_baseline_cleaning.R",        "R/20_clean_baseline.R"))
source_first(c("R/30_followups_attach.R",         "R/30_followups.R"))
source_first(c("R/35_followups_export_audits.R",  "R/35_export_followups.R"))
source_first(c("R/40_qc_checks.R",                "R/40_checks.R"))

# Merge helpers (numbered before merge in the new layout)
source_first(c("R/48_overlap_label_columns.R",    "R/56_overlap_label_columns.R", "R/56_label_overlaps.R"))
source_first(c("R/49_baseline_coalesce_overlaps.R","R/58_baseline_coalesce_overlaps.R","R/58_coalesce_baseline.R"))

# Merge step
source_first(c("R/50_merge_youth_caregiver.R",    "R/50_merge.R"))

# Optional post-merge schema overlap report
source_first(c("R/60_schema_overlap_report.R",    "R/55_schema_overlap_report.R", "R/55_overlap_checks.R"))


# --- 2) Configure follow-up events -------------------------------------------
# If your REDCap event names change, update here.
YOUTH_EVENTS     <- c("3_month_followup_arm_1","6_month_followup_arm_1")
CAREGIVER_EVENTS <- c("3_month_caregiver_arm_1","6_month_caregiver_arm_1")

# Ensure output directories exist
dir.create("data/raw",    recursive = TRUE, showWarnings = FALSE)
dir.create("data/out",    recursive = TRUE, showWarnings = FALSE)
dir.create("data/checks", recursive = TRUE, showWarnings = FALSE)


# --- 3) Fetch raw REDCap tables ----------------------------------------------
message("📥 Pulling REDCap data ...")

youth_base_raw <- pull_redcap("youth_baseline",     tokens["youth_baseline"])
cg_base_raw    <- pull_redcap("caregiver_baseline", tokens["caregiver_baseline"])
youth_fu_raw   <- pull_redcap("youth_followup",     tokens["youth_followup"],     events = YOUTH_EVENTS)
cg_fu_raw      <- pull_redcap("caregiver_followup", tokens["caregiver_followup"], events = CAREGIVER_EVENTS)

# Persist raw pulls for reproducibility/debugging
write_csv_safe(youth_base_raw, "data/raw/youth_baseline.csv")
write_csv_safe(cg_base_raw,    "data/raw/caregiver_baseline.csv")
write_csv_safe(youth_fu_raw,   "data/raw/youth_followup.csv")
write_csv_safe(cg_fu_raw,      "data/raw/caregiver_followup.csv")

# Small per-wave exports/audits (dups, missing IDs, columns, etc.)
export_followup_csvs(youth_fu_raw, cg_fu_raw, outdir = "data/checks/followups")


# --- 4) Clean baselines (one row per ID; canonical keys) ---------------------
# Uses legacy cleaners if present; otherwise uses hardened defaults.
youth_base_clean <- clean_baseline_youth_main(youth_base_raw)   # ensures participant_id & p_participant_id
cg_base_clean    <- clean_baseline_caregiver_main(cg_base_raw)  # ensures caregiver_id  & p_participant_id

write_csv_safe(youth_base_clean, "data/out/youth_baseline_clean.csv")
write_csv_safe(cg_base_clean,    "data/out/caregiver_baseline_clean.csv")

# Quick ID sanity check: p_participant_id vs wecare roots
id_align <- check_id_alignment(youth_base_clean, cg_base_clean, out_path = "data/checks/id_alignment.csv")
if (is.data.frame(id_align) && nrow(id_align)) {
  message("❗ ID alignment issues found. See data/checks/id_alignment.csv")
} else {
  message("✅ ID alignment OK (p_participant_id roots match wecare roots).")
}


# --- 5) Attach follow-ups to baselines + indicators --------------------------
message("🖇️ Attaching follow-ups ...")

youth_plus <- attach_youth_followups(youth_base_clean, youth_fu_raw)  # adds i_youth_3m / i_youth_6m
cg_plus    <- attach_caregiver_followups(cg_base_clean, cg_fu_raw)    # adds i_caregiver_3m / i_caregiver_6m

write_csv_safe(youth_plus, "data/out/youth_baseline_plus.csv")
write_csv_safe(cg_plus,    "data/out/caregiver_baseline_plus.csv")


# --- 5.5) Merge (full outer) -------------------------------------------------
message("📑 Merging Youth & Caregiver follow-ups ...")

merged <- youth_caregiver_full_join(
  youth_plus, cg_plus,
  out_file   = "data/out/dat_merged.csv",
  checks_dir = "data/checks"
)


# --- 6) QC summaries & overlap reports ---------------------------------------
message("🔎 Running checks ...")

# (a) Event-level completion summary (by cohort & wave)
chk1 <- build_followup_completion_summary(youth_fu_raw, cg_fu_raw)

# (b) Attached indicators summary (from i_* columns on the plus tables)
chk2 <- build_attached_indicator_summary(youth_plus, cg_plus)

write_csv_safe(chk1, "data/checks/followup_completion_summary.csv")
write_csv_safe(chk2, "data/checks/attached_indicator_summary.csv")

# (c) Column-overlap (schema) reports across source datasets
write_overlap_checks(
  baseline_youth     = youth_base_clean,
  baseline_caregiver = cg_base_clean,
  youth_followup     = youth_fu_raw,
  caregiver_followup = cg_fu_raw
)

# Console dashboard
print(chk1)
print(chk2)

message("✅ Done.\n  Raw:   data/raw\n  Clean: data/out\n  Checks:data/checks")