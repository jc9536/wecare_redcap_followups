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
#   8) Post-process dat_merged.csv file for appropriate ID names and composite variables
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

# --- 0) Load user config (URL, tokens, output dirs) --------------------------
cfg_file <- Sys.getenv("WECARE_CONFIG_FILE", unset = "config.user.R")
if (!file.exists(cfg_file)) {
  stop(sprintf(
    "Missing %s. Copy config.user.example.R → config.user.R and fill in values.",
    cfg_file
  ))
}
source(cfg_file)

# Validate required objects
if (!exists("SHARED_URL") || !is.character(SHARED_URL) || !nzchar(SHARED_URL)) {
  stop("SHARED_URL missing or empty in config.user.R")
}
required_labels <- c("youth_baseline","caregiver_baseline","youth_followup","caregiver_followup")
if (!exists("tokens")) stop("`tokens` not found in config.user.R")
miss <- setdiff(required_labels, names(tokens))
if (length(miss)) stop("`tokens` is missing labels: ", paste(miss, collapse = ", "))

# Default output dirs if user didn’t set them
if (!exists("RAW_DIR"))    RAW_DIR    <- "data/raw"
if (!exists("OUT_DIR"))    OUT_DIR    <- "data/out"
if (!exists("CHECKS_DIR")) CHECKS_DIR <- "data/checks"

# Ensure output directories exist
dir.create(RAW_DIR,    recursive = TRUE, showWarnings = FALSE)
dir.create(OUT_DIR,    recursive = TRUE, showWarnings = FALSE)
dir.create(CHECKS_DIR, recursive = TRUE, showWarnings = FALSE)

# --- 1) Source step files -----------------------------------

# Core bootstrap & utils
source("R/00_bootstrap.R")
source("R/05_utils_io.R")
ensure_pkgs(c("REDCapR","dplyr","janitor","readr"))

# Pipeline steps
source("R/10_fetch_redcap.R")

# NEW: Coalesce audit (youth) – runs on raw export
source("R/19_youth_coalesce_audit.R")

source("R/20_baseline_cleaning.R")
source("R/30_followups_attach.R")
source("R/35_followups_export_audits.R")
source("R/40_qc_checks.R")
source("R/45_id_alignment_by_name.R")

# Merge helpers & merge
source("R/48_overlap_label_columns.R")
source("R/49_baseline_coalesce_overlaps.R")
source("R/50_merge_youth_caregiver.R")

# Post-merge schema overlap report
source("R/60_schema_overlap_report.R")

# Post-process dat_merged.csv file
source("R/70_postprocess_dat_merged.R")

# NIMH GUID dataset creation
source("R/72_nda_guid_prep.R")
source("R/73_nda_cde_exports.R")


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

# Persist raw pulls
write_csv_safe(youth_base_raw, file.path(RAW_DIR, "youth_baseline.csv"))
write_csv_safe(cg_base_raw,    file.path(RAW_DIR, "caregiver_baseline.csv"))
write_csv_safe(youth_fu_raw,   file.path(RAW_DIR, "youth_followup.csv"))
write_csv_safe(cg_fu_raw,      file.path(RAW_DIR, "caregiver_followup.csv"))


# Per-wave follow-up exports/audits
export_followup_csvs(
  youth_fu_raw, cg_fu_raw,
  outdir = file.path(CHECKS_DIR, "followups")
)

# --- 3.4) Youth baseline coalesce audit (runs on RAW) ------------------------
# Requires helpers you added (coalesce_check_frame / write_all_youth_coalesce_checks)
# and the same coalesce_columns() used by cleaning.

message("🧪 Writing youth baseline coalesce QC files ...")
qc_dir_y <- file.path(CHECKS_DIR, "baseline_coalesce", "youth", format(Sys.Date(), "%Y%m%d"))
write_all_youth_coalesce_checks(youth_base_raw, out_dir = qc_dir_y)

# --- 4) Clean baselines (one row per ID; canonical keys) ---------------------
# Uses legacy cleaners if present; otherwise uses hardened defaults.
youth_base_clean <- clean_baseline_youth_main(youth_base_raw)   # ensures participant_id & p_participant_id
cg_base_clean    <- clean_baseline_caregiver_main(cg_base_raw)  # ensures caregiver_id  & p_participant_id

write_csv_safe(youth_base_clean, file.path(OUT_DIR, "youth_baseline_clean.csv"))
write_csv_safe(cg_base_clean,    file.path(OUT_DIR, "caregiver_baseline_clean.csv"))


# Example object names; replace with yours
# clean_youth_baseline: has wecare_id_y, first_name, last_name
# clean_caregiver_baseline: has wecare_id_cg, p_ps_youth_name
message("❗ Fixing mismatched caregiver IDs by youth name")

# 1) Build crosswalk from your cleaned baselines
xwalk <- build_id_crosswalk_by_name(
  youth_df     = youth_base_clean,
  caregiver_df = cg_base_clean
  # optionally: set explicit names if your columns are unusual
  # youth_pid_col = "p_participant_id",
  # youth_first   = "p_youth_firstname",
  # youth_last    = "p_youth_lastname",
  # cg_pid_col    = "p_participant_id",
  # cg_youthname  = "p_ps_youth_name"
)

# Review files in data/checks/:
# - id_namekey_confident.csv
# - id_namekey_ambiguous.csv
# - id_namekey_all_matches.csv
# - id_namekey_flags.csv   <-- new, with your date-based flags

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

write_csv_safe(youth_plus, file.path(OUT_DIR, "youth_baseline_plus.csv"))
write_csv_safe(cg_plus,    file.path(OUT_DIR, "caregiver_baseline_plus.csv"))


# --- 5.5) Merge (full outer) -------------------------------------------------
message("📑 Merging Youth & Caregiver follow-ups ...")

merged <- youth_caregiver_full_join(
  youth_plus, cg_plus,
  out_file   = file.path(OUT_DIR, "dat_merged.csv"),
  checks_dir = "data/checks"
)


# --- 6) QC summaries & overlap reports ---------------------------------------
message("🔎 Running checks ...")

# (a) Event-level completion summary (by cohort & wave)
chk1 <- build_followup_completion_summary(youth_fu_raw, cg_fu_raw)

# (b) Attached indicators summary (from i_* columns on the plus tables)
chk2 <- build_attached_indicator_summary(youth_plus, cg_plus)

write_csv_safe(chk1, file.path(CHECKS_DIR, "followup_completion_summary.csv"))
write_csv_safe(chk2, file.path(CHECKS_DIR, "attached_indicator_summary.csv"))

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

# --- 8) Post-process dat_merged.csv file ---------------------------------------
message("🛠️ Running post-processing ...")

tryCatch(
  postprocess_dat_merged_in_place(
    out_dir         = OUT_DIR,
    fname           = "dat_merged.csv",
    backup          = FALSE,
    write_cass_sogi = FALSE,
    cass_sogi_fname = "cass_sogi_with_lgbtq_20251124.csv"
  ),
  error = function(e) warning("Post-process failed; original dat_merged.csv left as-is. sourcReason: ", conditionMessage(e))
)

# --- 9) Create NIMH GUID dataset ---------------------------------------
message("📑 Creating NIMH GUID dataset ...")

nda_guid_prep_in_place(
  out_dir = OUT_DIR  # adjust if needed
)

message("📑 Creating NDA CDE datasets ...")

nda_build_cde_exports(
  out_dir       = OUT_DIR,
  guid_filename = "GUIDs_12052025.csv"
)

message("✅ Done.\n  Raw:   data/raw\n  Clean: data/out\n  Checks:data/checks")