# R/legacy_cleaning.R
# Minimal, robust baseline cleaners for WeCare
# - One row per ID (participant/caregiver)
# - No fragile rowwise/transmute/pull coalescing
# - All columns kept as character; you can add specific transforms later

# ---------- tiny utilities (base R only) ----------

# Given a data.frame and an ID column name, return one row per ID.
# If duplicates exist, keep the row with the greatest count of non-empty cells.
lc_pick_most_complete_by <- function(df, id_col) {
  if (!id_col %in% names(df)) stop(sprintf("ID column '%s' not found", id_col), call. = FALSE)
  cols <- setdiff(names(df), id_col)
  if (!length(cols)) return(df[!duplicated(df[[id_col]]), , drop = FALSE])
  
  # Make a "non-empty" matrix (TRUE where value is non-blank)
  mat <- df[, cols, drop = FALSE]
  for (j in seq_along(mat)) {
    v <- mat[[j]]
    mat[[j]] <- !(is.na(v) | v == "")
  }
  completeness <- rowSums(as.data.frame(mat), na.rm = TRUE)
  
  # Order by completeness (desc) and then keep the first row per ID
  ord <- order(-completeness, seq_len(nrow(df)))
  dfo <- df[ord, , drop = FALSE]
  keep <- !duplicated(dfo[[id_col]])
  dfo[keep, , drop = FALSE]
}

# Normalize to baseline rows if the project is longitudinal.
lc_prepare_baseline <- function(df, id_col) {
  # ensure simple character columns
  for (j in seq_along(df)) if (!is.character(df[[j]])) df[[j]] <- as.character(df[[j]])
  
  # If an events column exists, keep just baseline rows (when present)
  if ("redcap_event_name" %in% names(df)) {
    is_base <- !is.na(df$redcap_event_name) & df$redcap_event_name == "baseline_visit_arm_1"
    if (any(is_base)) df <- df[is_base, , drop = FALSE]
  }
  
  # Drop rows with missing/blank IDs
  if (id_col %in% names(df)) {
    df <- df[!is.na(df[[id_col]]) & df[[id_col]] != "", , drop = FALSE]
  }
  df
}

# ---------- public cleaners the pipeline calls ----------

# Must return a data.frame with a column named 'participant_id'
clean_youth <- function(dat_youth_raw) {
  df <- dat_youth_raw
  
  # Ensure the join key exists; fall back to common fields if needed
  if (!"participant_id" %in% names(df)) {
    if ("wecare_id" %in% names(df)) {
      df$participant_id <- df$wecare_id
    } else if ("youth_wecare_id" %in% names(df)) {
      df$participant_id <- df$youth_wecare_id
    } else {
      stop("clean_youth(): missing 'participant_id' and no fallback id found.", call. = FALSE)
    }
  }
  
  df <- lc_prepare_baseline(df, "participant_id")
  df <- lc_pick_most_complete_by(df, "participant_id")
  df
}

# Must return a data.frame with a column named 'caregiver_id'
clean_caregiver <- function(dat_caregiver_raw) {
  df <- dat_caregiver_raw
  
  # Ensure the join key exists; fall back to common fields if needed
  if (!"caregiver_id" %in% names(df)) {
    if ("p_wecare_id" %in% names(df)) {
      df$caregiver_id <- df$p_wecare_id
    } else if ("caregiver_id_3m" %in% names(df)) {
      df$caregiver_id <- df$caregiver_id_3m
    } else {
      stop("clean_caregiver(): missing 'caregiver_id' and no fallback id found.", call. = FALSE)
    }
  }
  
  df <- lc_prepare_baseline(df, "caregiver_id")
  df <- lc_pick_most_complete_by(df, "caregiver_id")
  df
}