# =============================================================================
# WeCare ETL • 10_fetch_redcap.R
# -----------------------------------------------------------------------------
# pull_redcap()
#   Read records from a REDCap project using REDCapR::redcap_read.
#   - Keeps ALL columns as character (guess_type = FALSE) to avoid parsing drift.
#   - Returns snake_case names.
#   - Adds a 'project_label' column for traceability.
#
# Requirements:
#   - Global SHARED_URL must be defined elsewhere (e.g., 00_setup / run.R).
#   - REDCapR installed; function uses REDCapR::redcap_read().
#
# Notes:
#   - 'events' is optional; pass character(0) to read all events.
#   - Name cleaning is robust to 0-column returns and missing names.
# =============================================================================

#--- internal: robust name cleaner for a data.frame ---------------------------
.clean_names_df <- function(dat, context = "pull_redcap") {
  if (is.null(dat)) {
    stop(sprintf("❌ %s: REDCap returned NULL data.", context), call. = FALSE)
  }
  if (!is.data.frame(dat)) dat <- as.data.frame(dat)
  
  if (ncol(dat) == 0L) {
    # 0-column frames can happen on empty pulls; leave as-is
    return(dat)
  }
  
  nm <- names(dat)
  if (is.null(nm)) {
    nm <- paste0("v", seq_len(ncol(dat)))
  }
  
  # Prefer janitor::make_clean_names if available; otherwise, simple fallback.
  if (requireNamespace("janitor", quietly = TRUE)) {
    nm <- janitor::make_clean_names(nm)
  } else {
    # fallback: lower, non-alnum -> _, collapse repeats, trim _
    nm <- tolower(gsub("[^A-Za-z0-9]+", "_", nm))
    nm <- gsub("_+", "_", nm)
    nm <- sub("^_", "", sub("_$", "", nm))
  }
  names(dat) <- nm
  dat
}

#--- main: read from REDCap ----------------------------------------------------
#' Read data from REDCap (keeps character types; cleans names; tags project)
#'
#' @param label  Character scalar used to tag rows (e.g., "youth_baseline")
#' @param token  REDCap API token (character)
#' @param events Character vector of event unique names, or character(0) for all
#'
#' @return data.frame with snake_case names and a 'project_label' column
#' @throws error if SHARED_URL is missing or the API call fails
pull_redcap <- function(label, token, events = character(0)) {
  # --- sanity checks ----------------------------------------------------------
  if (!exists("SHARED_URL", inherits = TRUE) || is.null(SHARED_URL) || !nzchar(SHARED_URL)) {
    stop("❌ pull_redcap(): global SHARED_URL is not set; define the REDCap API URL before calling.", call. = FALSE)
  }
  if (length(label) != 1L || !nzchar(label)) {
    stop("❌ pull_redcap(): 'label' must be a non-empty scalar character.", call. = FALSE)
  }
  if (length(token) != 1L || !nzchar(token)) {
    stop(sprintf("❌ pull_redcap('%s'): API token is missing/empty.", label), call. = FALSE)
  }
  
  # --- call REDCap ------------------------------------------------------------
  res <- tryCatch({
    suppressWarnings(suppressMessages(REDCapR::redcap_read(
      redcap_uri            = SHARED_URL,
      token                 = as.character(token),
      events                = if (length(events)) events else NULL,
      raw_or_label          = "raw",
      export_checkbox_label = FALSE,
      guess_type            = FALSE  # keep character columns to avoid parsing surprises
    )))
  }, error = function(e) {
    stop(sprintf("❌ REDCap read failed for '%s': %s", label, conditionMessage(e)), call. = FALSE)
  })
  
  if (!isTRUE(res$success)) {
    msg <- res$raw_text
    if (length(msg) == 0 || is.na(msg)) msg <- "Unknown REDCap error"
    stop(sprintf("❌ REDCap read failed for '%s': %s", label, msg), call. = FALSE)
  }
  
  dat <- res$data
  dat <- .clean_names_df(dat, context = sprintf("pull_redcap('%s')", label))
  
  # Tag the source project for downstream auditing
  dat$project_label <- label
  
  dat
}