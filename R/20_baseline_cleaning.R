# =============================================================================
# WeCare ETL • 20_baseline_cleaning.R
# -----------------------------------------------------------------------------
# Purpose
#   Normalize, deduplicate, and finalize Youth/Caregiver baseline tables.
#   - Derive canonical IDs
#   - Filter to baseline event (if events are present)
#   - Pick the “most complete” row per ID
#   - Guarantee keys required downstream (wecare_id, p_participant_id, roots)
#
# Dependencies (provided by 00_bootstrap.R)
#   is_blank(), normalize_id(), mk_root_id(), compute_ppid(),
#   derive_youth_wecare_id(), derive_caregiver_wecare_id(),
#   derive_caregiver_p_wecare_id
#
# Optional
#   R/legacy_cleaning.R with clean_youth() / clean_caregiver() to override defaults
# =============================================================================

# --- Most-complete picker -----------------------------------------------------
# Pick the most "complete" row per ID:
#   1) Most present (non-missing, non-empty) fields
#   2) More *_complete == 2 flags (REDCap complete)
#   3) More of the 'prefer_cols' present (light tie-break)
#   4) Latest timestamp across 'time_cols'
pick_most_complete_by <- function(df,
                                  by,
                                  prefer_cols = NULL,
                                  time_cols = c(
                                    "redcap_last_update",
                                    "redcap_survey_timestamp",
                                    "last_modified",
                                    "updated_at",
                                    "completion_date"
                                  )) {
  stopifnot(is.data.frame(df))
  if (!all(by %in% names(df))) {
    stop(sprintf("❌ pick_most_complete_by(): 'by' column(s) not found: %s",
                 paste(setdiff(by, names(df)), collapse = ", ")))
  }
  
  # presence: TRUE if not NA and not empty string for character
  is_present_vec <- function(x) {
    if (is.list(x)) {
      return(vapply(x, function(el) !is.null(el) && length(el) > 0, logical(1)))
    }
    if (is.character(x)) return(!is.na(x) & trimws(x) != "")
    !is.na(x)
  }
  
  # per-row count of present values (exclude key columns)
  count_cols <- setdiff(names(df), by)
  if (length(count_cols) == 0L) {
    df$..present_n <- 0L
  } else {
    present_mat <- vapply(df[count_cols], is_present_vec, logical(nrow(df)))
    if (!is.matrix(present_mat)) present_mat <- cbind(present_mat)  # when single column
    df$..present_n <- rowSums(present_mat, na.rm = TRUE)
  }
  
  # bonus for *_complete == 2
  comp_cols <- grep("_complete$", names(df), value = TRUE)
  if (length(comp_cols)) {
    comp_mat <- vapply(df[comp_cols], function(x) as.integer(x == 2), integer(nrow(df)))
    if (!is.matrix(comp_mat)) comp_mat <- cbind(comp_mat)
    df$..comp_score <- rowSums(comp_mat, na.rm = TRUE)
  } else df$..comp_score <- 0L
  
  # light preference weighting
  if (!is.null(prefer_cols)) prefer_cols <- intersect(prefer_cols, names(df))
  if (!is.null(prefer_cols) && length(prefer_cols)) {
    pref_mat <- vapply(df[prefer_cols], is_present_vec, logical(nrow(df)))
    if (!is.matrix(pref_mat)) pref_mat <- cbind(pref_mat)
    df$..prefer_wt <- rowSums(pref_mat, na.rm = TRUE)
  } else df$..prefer_wt <- 0L
  
  # best available timestamp across candidates
  tc <- intersect(time_cols, names(df))
  if (length(tc)) {
    to_time <- function(x) suppressWarnings(as.POSIXct(x, tz = "UTC"))
    times_list <- lapply(df[tc], to_time)
    max_time <- do.call(pmax, c(times_list, list(na.rm = TRUE)))
    max_time[is.infinite(max_time) | is.na(max_time)] <- as.POSIXct(0, origin = "1970-01-01", tz = "UTC")
    df$..time_score <- max_time
  } else {
    df$..time_score <- as.POSIXct(0, origin = "1970-01-01", tz = "UTC")
  }
  
  # order and keep first per id
  out <- df %>%
    dplyr::arrange(
      dplyr::across(dplyr::all_of(by)),
      dplyr::desc(..present_n),
      dplyr::desc(..comp_score),
      dplyr::desc(..prefer_wt),
      dplyr::desc(..time_score)
    ) %>%
    dplyr::distinct(dplyr::across(dplyr::all_of(by)), .keep_all = TRUE) %>%
    dplyr::select(-..present_n, -..comp_score, -..prefer_wt, -..time_score)
  
  out
}


# --- Baseline normalization ---------------------------------------------------
# Keep only baseline rows when events are provided; drop blank IDs; force character.
prep_baseline <- function(df, id_col) {
  # force character to avoid factor/num surprises downstream
  for (j in seq_along(df)) if (!is.character(df[[j]])) df[[j]] <- as.character(df[[j]])
  
  # filter to baseline event if present
  if ("redcap_event_name" %in% names(df)) {
    is_base <- !is.na(df$redcap_event_name) & df$redcap_event_name == "baseline_visit_arm_1"
    if (any(is_base)) df <- df[is_base, , drop = FALSE]
  }
  
  # drop empty IDs
  if (id_col %in% names(df)) {
    df <- df[!is.na(df[[id_col]]) & trimws(df[[id_col]]) != "", , drop = FALSE]
  }
  df
}


# --- Defaults: Youth baseline -------------------------------------------------
# Key: wecare_id  (alias participant_id for compatibility)
default_clean_youth <- function(dat_raw) {
  df <- dat_raw
  
  df$wecare_id <- derive_youth_wecare_id(df)
  if (all(is.na(df$wecare_id) | trimws(df$wecare_id) == "")) {
    stop("❌ Youth baseline missing wecare_id", call. = FALSE)
  }
  
  # convenient aliases/roots
  df$participant_id <- df$wecare_id
  df$wecare_root    <- mk_root_id(df$wecare_id)
  
  # keep original p_participant_id root if present (for checks only)
  if ("p_participant_id" %in% names(df)) {
    df$p_participant_root <- mk_root_id(df$p_participant_id)
  }
  
  df <- prep_baseline(df, "wecare_id")
  pick_most_complete_by(df, "wecare_id")
}


# --- Defaults: Caregiver baseline --------------------------------------------
# Preferred merge key in legacy code was p_wecare_id; we ensure both p_wecare_id
# and wecare_id are available and consistent.
default_clean_caregiver <- function(dat_raw) {
  df <- dat_raw
  
  # robustly derive p_wecare_id, then mirror to wecare_id for consistency
  df$p_wecare_id <- derive_caregiver_p_wecare_id(df)
  if (all(is.na(df$p_wecare_id) | trimws(df$p_wecare_id) == "")) {
    stop("❌ Caregiver baseline missing p_wecare_id", call. = FALSE)
  }
  
  # aliases/roots
  df$caregiver_id  <- df$p_wecare_id
  df$wecare_id     <- df$p_wecare_id          # maintain a plain 'wecare_id' for downstream joins
  df$p_wecare_root <- mk_root_id(df$p_wecare_id)
  
  if ("p_participant_id" %in% names(df)) {
    df$p_participant_root <- mk_root_id(df$p_participant_id)
  }
  
  df <- prep_baseline(df, "wecare_id")
  pick_most_complete_by(df, "wecare_id")
}


# --- Public wrappers (support legacy overrides) -------------------------------
clean_baseline_youth_main <- function(dat_raw) {
  if (file.exists("R/legacy_cleaning.R")) source("R/legacy_cleaning.R")
  
  run_clean <- function() {
    if (exists("clean_youth", mode = "function")) clean_youth(dat_raw) else default_clean_youth(dat_raw)
  }
  
  out <- tryCatch(
    run_clean(),
    error = function(e) {
      # Log more detail
      message("❌ clean_youth failed\n",
              "• Class: ", paste(class(e), collapse = "/"), "\n",
              "• Message: ", conditionMessage(e), "\n",
              "• Call: ", deparse(conditionCall(e), width.cutoff = 200))
      
      # Optional: show an approximate call stack
      calls <- utils::capture.output(print(sys.calls()))
      message("• Calls:\n", paste(calls, collapse = "\n"))
      
      # Rethrow so you see the full error/traceback at the calling site
      stop(e)
    }
  )
  
  finalize_youth_baseline(out)
}

clean_baseline_caregiver_main <- function(dat_raw) {
  if (file.exists("R/legacy_cleaning.R")) source("R/legacy_cleaning.R")  # defines clean_caregiver()
  out <- tryCatch(
    {
      if (exists("clean_caregiver", mode = "function")) clean_caregiver(dat_raw) else default_clean_caregiver(dat_raw)
    },
    error = function(e) { message("⚠️ clean_caregiver failed: ", e$message); dat_raw }
  )
  finalize_caregiver_baseline(out)
}


# --- Finalizers (guarantee keys, one row per ID) -----------------------------

# Youth finalizer: guarantees 'wecare_id', 'p_participant_id' (root), 'ppid_root', 'wecare_root'
finalize_youth_baseline <- function(df) {
  df <- as.data.frame(df, stringsAsFactors = FALSE)
  
  if (!"wecare_id" %in% names(df) || all(is_blank(df$wecare_id))) {
    df$wecare_id <- derive_youth_wecare_id(df)
  }
  if (all(is_blank(df$wecare_id))) {
    stop("❌ Youth baseline: missing wecare_id after normalization.", call. = FALSE)
  }
  
  # caregiver id is not on youth baseline; keep NA placeholder for compute_ppid()
  if (!"wecare_id_cg" %in% names(df)) df$wecare_id_cg <- NA_character_
  
  # canonical participant ID (root). compute_ppid returns ROOT already.
  if (!"p_participant_id" %in% names(df)) df$p_participant_id <- NA_character_
  df$p_participant_id <- compute_ppid(df$wecare_id, df$wecare_id_cg, df$p_participant_id)
  
  # roots for convenience
  df$wecare_root <- mk_root_id(df$wecare_id)
  df$ppid_root   <- df$p_participant_id
  
  # one row per wecare_id
  df <- prep_baseline(df, "wecare_id")
  df <- pick_most_complete_by(df, "wecare_id")
  df
}

# Caregiver finalizer: guarantees 'wecare_id', 'p_participant_id' (root), 'ppid_root', 'wecare_root'
finalize_caregiver_baseline <- function(df) {
  df <- as.data.frame(df, stringsAsFactors = FALSE)
  
  # Ensure caregiver has a usable 'wecare_id'
  if (!"wecare_id" %in% names(df) || all(is_blank(df$wecare_id))) {
    # try to derive from caregiver fields (p_wecare_id et al.)
    df$wecare_id <- derive_caregiver_p_wecare_id(df)
  }
  if (all(is_blank(df$wecare_id))) {
    stop("❌ Caregiver baseline: missing wecare_id after normalization.", call. = FALSE)
  }
  
  # youth id not on caregiver baseline; NA placeholder for compute_ppid()
  if (!"wecare_id_y" %in% names(df)) df$wecare_id_y <- NA_character_
  
  # canonical participant ID (root). compute_ppid returns ROOT already.
  if (!"p_participant_id" %in% names(df)) df$p_participant_id <- NA_character_
  df$p_participant_id <- compute_ppid(df$wecare_id_y, df$wecare_id, df$p_participant_id)
  
  # roots for convenience
  df$wecare_root <- mk_root_id(df$wecare_id)
  df$ppid_root   <- df$p_participant_id
  
  # one row per wecare_id
  df <- prep_baseline(df, "wecare_id")
  df <- pick_most_complete_by(df, "wecare_id")
  df
}

# =============================================================================
# End of 20_clean_baseline.R
# =============================================================================