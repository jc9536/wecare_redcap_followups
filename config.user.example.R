# WeCare ETL • User config (copy this file to `config.user.R` and edit)
# NOTE: Keep `config.user.R` out of git.

# —— REDCap connection ——
SHARED_URL <- "https://redcap.XXX.edu/redcap_vXX.X.XX"   # changes when REDCap updates
tokens <- c(
  youth_baseline     = "<PASTE_TOKEN>",
  caregiver_baseline = "<PASTE_TOKEN>",
  youth_followup     = "<PASTE_TOKEN>",
  caregiver_followup = "<PASTE_TOKEN>"
)

# —— Output locations (users may change these) ——
OUT_DIR    <- "data/out"
RAW_DIR    <- "data/raw"
CHECKS_DIR <- "data/checks"

# (Optional) If you prefer explicit subfolders:
# FOLLOWUPS_DIR <- file.path(CHECKS_DIR, "followups")
# OVERLAPS_DIR  <- file.path(CHECKS_DIR, "overlaps")