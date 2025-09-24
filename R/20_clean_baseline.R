# Use your legacy cleaners if available, else minimal, robust defaults.

# normalize to baseline rows if events present; drop blank IDs; force character
prep_baseline <- function(df, id_col) {
  for (j in seq_along(df)) if (!is.character(df[[j]])) df[[j]] <- as.character(df[[j]])
  if ("redcap_event_name" %in% names(df)) {
    is_base <- !is.na(df$redcap_event_name) & df$redcap_event_name == "baseline_visit_arm_1"
    if (any(is_base)) df <- df[is_base, , drop = FALSE]
  }
  if (id_col %in% names(df)) df <- df[!is.na(df[[id_col]]) & df[[id_col]] != "", , drop = FALSE]
  df
}

# YOUTH baseline: key = wecare_id (alias participant_id for compatibility)
default_clean_youth <- function(dat_raw) {
  df <- dat_raw
  df$wecare_id <- derive_youth_wecare_id(df)
  if (all(is.na(df$wecare_id) | df$wecare_id == "")) {
    stop("Youth baseline missing wecare_id", call. = FALSE)
  }
  df$participant_id <- df$wecare_id  # alias
  df$wecare_root    <- mk_root_id(df$wecare_id)
  
  # If a p_participant_id exists, store its root for checks
  if ("p_participant_id" %in% names(df)) {
    df$p_participant_root <- mk_root_id(df$p_participant_id)
  }
  
  df <- prep_baseline(df, "wecare_id")
  pick_most_complete_by(df, "wecare_id")
}

# CAREGIVER baseline: key = p_wecare_id (alias caregiver_id for compatibility)
default_clean_caregiver <- function(dat_raw) {
  df <- dat_raw
  df$p_wecare_id <- derive_caregiver_p_wecare_id(df)
  if (all(is.na(df$p_wecare_id) | df$p_wecare_id == "")) {
    stop("Caregiver baseline missing p_wecare_id", call. = FALSE)
  }
  df$caregiver_id   <- df$p_wecare_id  # alias
  df$p_wecare_root  <- mk_root_id(df$p_wecare_id)
  if ("p_participant_id" %in% names(df)) {
    df$p_participant_root <- mk_root_id(df$p_participant_id)
  }
  
  df <- prep_baseline(df, "p_wecare_id")
  pick_most_complete_by(df, "p_wecare_id")
}

# --- Replace the two main wrappers with these hardened versions ---

clean_baseline_youth_main <- function(dat_raw) {
  if (file.exists("R/legacy_cleaning.R")) source("R/legacy_cleaning.R")
  out <- tryCatch(
    { if (exists("clean_youth")) clean_youth(dat_raw) else default_clean_youth(dat_raw) },
    error = function(e) { message("⚠️ clean_youth failed: ", e$message); dat_raw }
  )
  finalize_youth_baseline(out)
}

clean_baseline_caregiver_main <- function(dat_raw) {
  if (file.exists("R/legacy_cleaning.R")) source("R/legacy_cleaning.R")
  out <- tryCatch(
    { if (exists("clean_caregiver")) clean_caregiver(dat_raw) else default_clean_caregiver(dat_raw) },
    error = function(e) { message("⚠️ clean_caregiver failed: ", e$message); dat_raw }
  )
  finalize_caregiver_baseline(out)
}

# --- Add these two finalizer helpers anywhere after prep_baseline(), pick_most_complete_by() ---

# Ensure Youth baseline has canonical keys and one row per wecare_id
# Youth baseline finalizer: guarantees wecare_id + p_participant_id
# Youth baseline finalizer: guarantees wecare_id + p_participant_id
finalize_youth_baseline <- function(df) {
  df <- as.data.frame(df, stringsAsFactors = FALSE)
  
  if (!"wecare_id" %in% names(df) || all(is_blank(df$wecare_id))) {
    df$wecare_id <- derive_youth_wecare_id(df)
  }
  if (all(is_blank(df$wecare_id))) stop("Youth baseline: missing wecare_id after normalization.", call. = FALSE)
  
  # Caregiver wecare_id may not exist on youth baseline; fill NA for compute_ppid()
  if (!"wecare_id_cg" %in% names(df)) df$wecare_id_cg <- NA_character_
  
  if (!"p_participant_id" %in% names(df)) df$p_participant_id <- NA_character_
  df$p_participant_id <- compute_ppid(df$wecare_id, df$wecare_id_cg, df$p_participant_id)
  
  df$wecare_root <- mk_root_id(df$wecare_id)
  
  df <- prep_baseline(df, "wecare_id")
  df <- pick_most_complete_by(df, "wecare_id")
  df
}

# Caregiver baseline finalizer: guarantees wecare_id + p_participant_id
finalize_caregiver_baseline <- function(df) {
  df <- as.data.frame(df, stringsAsFactors = FALSE)
  
  if (!"wecare_id" %in% names(df) || all(is_blank(df$wecare_id))) {
    df$wecare_id <- derive_caregiver_wecare_id(df)
  }
  if (all(is_blank(df$wecare_id))) stop("Caregiver baseline: missing wecare_id after normalization.", call. = FALSE)
  
  # Youth wecare_id not on caregiver baseline; fill NA for compute_ppid()
  if (!"wecare_id_y" %in% names(df)) df$wecare_id_y <- NA_character_
  
  if (!"p_participant_id" %in% names(df)) df$p_participant_id <- NA_character_
  df$p_participant_id <- compute_ppid(df$wecare_id_y, df$wecare_id, df$p_participant_id)
  
  df$wecare_root <- mk_root_id(df$wecare_id)
  
  df <- prep_baseline(df, "wecare_id")
  df <- pick_most_complete_by(df, "wecare_id")
  df
}