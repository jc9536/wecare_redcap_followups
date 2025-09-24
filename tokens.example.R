# Copy this file to tokens.R (do NOT commit tokens.R) and fill in your values.

# One shared REDCap API endpoint for ALL projects:
SHARED_URI <- "https://<your-redcap-host>/api/"  # <-- edit me

# Per-project tokens; names must match the labels used in the pipeline script.
tokens <- c(
  youth_baseline     = "<PASTE_TOKEN>",
  caregiver_baseline = "<PASTE_TOKEN>",
  youth_followup     = "<PASTE_TOKEN>",
  caregiver_followup = "<PASTE_TOKEN>"
)