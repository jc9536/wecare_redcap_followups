# ------------------------------------------
# WeCare runner: fetch → baseline clean → attach → checks
# Run from repo root: source("run.R")
# ------------------------------------------

# 0) Load secrets
if (!file.exists("tokens.R")) stop("Missing tokens.R. Copy tokens.example.R → tokens.R")
source("tokens.R")
req <- c("youth_baseline","caregiver_baseline","youth_followup","caregiver_followup")
miss <- setdiff(req, names(tokens))
if (length(miss)) stop("tokens missing labels: ", paste(miss, collapse=", "))
if (!exists("SHARED_URI") || !nzchar(SHARED_URI)) stop("SHARED_URI missing in tokens.R")

# 1) Source step files
source("R/00_setup.R"); ensure_pkgs(c("REDCapR","dplyr"))
source("R/10_fetch.R")
source("R/20_clean_baseline.R")
source("R/30_followups.R")
source("R/35_export_followups.R")
source("R/40_checks.R")
source("R/50_merge.R")

# 2) Define follow-up events (edit here if needed)
YOUTH_EVENTS     <- c("3_month_followup_arm_1","6_month_followup_arm_1")
CAREGIVER_EVENTS <- c("3_month_caregiver_arm_1","6_month_caregiver_arm_1")

dir.create("data/raw", recursive = TRUE, showWarnings = FALSE)
dir.create("data/out", recursive = TRUE, showWarnings = FALSE)
dir.create("data/checks", recursive = TRUE, showWarnings = FALSE)

# 3) Fetch raw
message("Pulling REDCap data ...")
youth_base_raw <- pull_redcap("youth_baseline",     tokens["youth_baseline"])
cg_base_raw    <- pull_redcap("caregiver_baseline", tokens["caregiver_baseline"])
youth_fu_raw   <- pull_redcap("youth_followup",     tokens["youth_followup"],     events = YOUTH_EVENTS)
cg_fu_raw      <- pull_redcap("caregiver_followup", tokens["caregiver_followup"], events = CAREGIVER_EVENTS)
write_csv_safe(youth_base_raw, "data/raw/youth_baseline.csv")
write_csv_safe(cg_base_raw,    "data/raw/caregiver_baseline.csv")
write_csv_safe(youth_fu_raw,   "data/raw/youth_followup.csv")
write_csv_safe(cg_fu_raw,      "data/raw/caregiver_followup.csv")

export_followup_csvs(youth_fu_raw, cg_fu_raw, outdir = "data/checks/followups")

# 4) Baseline clean (use legacy if present; else minimal)
youth_base_clean <- clean_baseline_youth_main(youth_base_raw)   # ensures participant_id & 1 row/id
cg_base_clean    <- clean_baseline_caregiver_main(cg_base_raw)  # ensures caregiver_id & 1 row/id
write_csv_safe(youth_base_clean, "data/out/youth_baseline_clean.csv")
write_csv_safe(cg_base_clean,    "data/out/caregiver_baseline_clean.csv")

# After writing baseline clean CSVs
id_align <- check_id_alignment(youth_base_clean, cg_base_clean, out_path = "data/checks/id_alignment.csv")
if (is.data.frame(id_align) && nrow(id_align)) {
  message("⚠️ ID alignment issues found. See data/checks/id_alignment.csv")
} else {
  message("✅ ID alignment OK (p_participant_id roots match wecare roots).")
}

# 5) Attach follow-ups + indicators
message("Attaching follow-ups ...")
youth_plus <- attach_youth_followups(youth_base_clean, youth_fu_raw)  # adds i_youth_3m / i_youth_6m
cg_plus    <- attach_caregiver_followups(cg_base_clean, cg_fu_raw)    # adds i_caregiver_3m / i_caregiver_6m
write_csv_safe(youth_plus, "data/out/youth_baseline_plus.csv")
write_csv_safe(cg_plus,    "data/out/caregiver_baseline_plus.csv")

# 5.5) Merge (FULL OUTER: keep all Youth and all Caregivers)
merged <- youth_caregiver_full_join(youth_plus, cg_plus,
                                    out_file   = "data/out/dat_merged.csv",
                                    checks_dir = "data/checks")

# 6) Checks — including NUM COMPLETED FOLLOW-UPS per wave
message("Running checks ...")
chk1 <- build_followup_completion_summary(youth_fu_raw, cg_fu_raw)           # by event & cohort
chk2 <- build_attached_indicator_summary(youth_plus, cg_plus)                # from i_* columns
write_csv_safe(chk1, "data/checks/followup_completion_summary.csv")
write_csv_safe(chk2, "data/checks/attached_indicator_summary.csv")

# 7) Print quick dashboard to console
print(chk1)
print(chk2)
message("✅ Done. Raw: data/raw; Clean/Attached: data/out; Checks: data/checks")