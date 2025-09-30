# =============================================================================
# WeCare ETL • 48_overlap_label_columns.R
# -----------------------------------------------------------------------------
# Purpose
#   Rename only the columns that overlap between youth_plus and cg_plus to the
#   canonical format:
#       {base}_{cohort}_{wave}
#   where:
#       cohort ∈ {"y","cg"}
#       wave   ∈ {"bl","3m","6m"}
#
# Wave detection:
#   - if name contains "_6m" anywhere  → wave = "6m"
#   - else if contains "_3m"           → wave = "3m"
#   - else                             → wave = "bl"   (baseline)
#
# For readability we strip ONLY a *trailing* "_3m"/"_6m" from the base so
# we never duplicate the suffix. Example: "site_id_3m" → base "site_id".
#
# Outputs (side effect)
#   data/checks/overlaps/overlap_renames.csv   (old → new mapping)
# =============================================================================

# --- Helpers ------------------------------------------------------------------

.detect_wave <- function(nm) {
  if (grepl("_6m", nm, fixed = TRUE)) return("6m")
  if (grepl("_3m", nm, fixed = TRUE)) return("3m")
  "bl"
}

.base_trim_trailing_wave <- function(nm) sub("_(3m|6m)$", "", nm)

# Build a mapping for a set of column names belonging to a specific cohort tag
# (cohort_tag is "y" for youth, "cg" for caregiver)
.build_rename_map <- function(cols, cohort_tag) {
  if (!length(cols)) return(setNames(character(0), character(0)))
  wave <- vapply(cols, .detect_wave, character(1))
  base <- vapply(cols, .base_trim_trailing_wave, character(1))
  setNames(paste0(base, "_", cohort_tag, "_", wave), cols)  # names(mapping) = old, values = new
}

# Rename columns in-place using a name→name map (only renames existing columns).
.rename_with_map <- function(df, map) {
  if (!length(map)) return(df)
  old <- intersect(names(map), names(df))
  if (!length(old)) return(df)
  nm <- names(df)
  idx <- match(old, nm)
  nm[idx] <- unname(map[old])
  names(df) <- nm
  df
}

# --- Main ---------------------------------------------------------------------

#' Label overlapping columns between youth_plus and cg_plus.
#'
#' Only columns that exist in BOTH data frames (minus `ignore`) are renamed to
#' the canonical pattern {base}_{cohort}_{wave}, where cohort is "y" or "cg".
#'
#' @param youth_plus data.frame (baseline+FU youth table)
#' @param cg_plus    data.frame (baseline+FU caregiver table)
#' @param ignore     character vector of columns to leave untouched (keys, indicators, etc.)
#' @param map_file   path to write the (source, old, new) rename map CSV
#'
#' @return list(youth = <renamed youth_plus>, caregiver = <renamed cg_plus>)
label_overlaps_for_merge <- function(
    youth_plus, cg_plus,
    ignore = c(
      "p_participant_id", "ppid_root", "wecare_id", "wecare_root",
      "i_youth_3m","i_youth_6m",
      "i_caregiver_3m","i_caregiver_6m"
    ),
    map_file = "data/checks/overlaps/overlap_renames.csv"
) {
  stopifnot(is.data.frame(youth_plus), is.data.frame(cg_plus))
  
  yl <- youth_plus
  cl <- cg_plus
  
  # Determine true overlaps (shared column names), excluding keys/indicators
  overlaps <- setdiff(intersect(names(yl), names(cl)), ignore)
  
  if (!length(overlaps)) {
    message("✅ No overlapping columns to label. Returning inputs unchanged.")
    return(list(youth = yl, caregiver = cl))
  }
  
  # Build maps for the overlapping columns present in each df
  y_map <- .build_rename_map(intersect(overlaps, names(yl)), cohort_tag = "y")
  c_map <- .build_rename_map(intersect(overlaps, names(cl)), cohort_tag = "cg")
  
  # Apply renames
  yl <- .rename_with_map(yl, y_map)
  cl <- .rename_with_map(cl, c_map)
  
  # Safety: ensure join key still present
  if (!"p_participant_id" %in% names(yl))
    stop("❌ label_overlaps_for_merge: youth_plus lost p_participant_id; check ignore list.")
  if (!"p_participant_id" %in% names(cl))
    stop("❌ label_overlaps_for_merge: cg_plus lost p_participant_id; check ignore list.")
  
  # Write rename map (old → new) for both sources
  map_df <- rbind(
    if (length(y_map)) data.frame(source = "youth_plus", old = names(y_map), new = unname(y_map), stringsAsFactors = FALSE),
    if (length(c_map)) data.frame(source = "cg_plus",    old = names(c_map), new = unname(c_map), stringsAsFactors = FALSE)
  )
  if (!nrow(map_df)) {
    map_df <- data.frame(source = character(), old = character(), new = character(), stringsAsFactors = FALSE)
  }
  write_csv_safe(map_df, map_file)
  message("📝 Overlap rename map written: ", map_file)
  
  list(youth = yl, caregiver = cl)
}