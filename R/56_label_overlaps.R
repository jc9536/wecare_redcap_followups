# R/56_label_overlaps.R
# ------------------------------------------------------------------------------
# Rename only the columns that overlap between youth_plus and cg_plus
# to: {base}_{cohort}_{wave}, where cohort in {y,cg}, wave in {bl,3m,6m}.
#
# Wave detection:
#   - contains "_6m" anywhere  -> "6m"
#   - else contains "_3m"      -> "3m"
#   - else                     -> "bl"  (baseline)
#
# For readability we strip ONLY a *trailing* "_3m"/"_6m" from the base
# so we don't duplicate the suffix (e.g., "site_id_3m" -> base "site_id").

# -------- helpers --------------------------------------------------------------

.detect_wave <- function(nm) {
  if (grepl("_6m", nm, fixed = TRUE)) return("6m")
  if (grepl("_3m", nm, fixed = TRUE)) return("3m")
  "bl"
}

.base_trim_trailing_wave <- function(nm) sub("_(3m|6m)$", "", nm)

.build_rename_map <- function(cols, cohort_tag) {
  wave  <- vapply(cols, .detect_wave, character(1))
  base  <- vapply(cols, .base_trim_trailing_wave, character(1))
  # *** requested format: {base}_{cohort}_{wave}
  newnm <- paste0(base, "_", cohort_tag, "_", wave)
  setNames(newnm, cols)
}

.write_csv_safe <- function(df, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  utils::write.csv(df, path, row.names = FALSE, na = "")
}

# -------- main ---------------------------------------------------------------

label_overlaps_for_merge <- function(youth_plus, cg_plus,
                                     ignore = c("p_participant_id", "ppid_root", "wecare_id", "wecare_root",
                                                "i_youth_3m","i_youth_6m",
                                                "i_caregiver_3m","i_caregiver_6m"),
                                     map_file = "data/checks/overlaps/overlap_renames.csv") {
  stopifnot(is.data.frame(youth_plus), is.data.frame(cg_plus))
  yl <- youth_plus
  cl <- cg_plus
  
  # Overlaps excluding keys/indicators
  overlaps <- intersect(names(yl), names(cl))
  overlaps <- setdiff(overlaps, ignore)
  
  if (!length(overlaps)) {
    message("No overlapping columns to label. Returning inputs unchanged.")
    return(list(youth = yl, caregiver = cl))
  }
  
  # Build maps for the columns that actually exist in each df
  y_cols <- intersect(overlaps, names(yl))
  c_cols <- intersect(overlaps, names(cl))
  y_map  <- .build_rename_map(y_cols, cohort_tag = "y")
  c_map  <- .build_rename_map(c_cols, cohort_tag = "cg")
  
  # Apply renames
  if (length(y_map)) {
    idx <- match(names(yl), names(y_map), nomatch = 0L)
    names(yl)[idx > 0] <- unname(y_map[names(yl)[idx > 0]])
  }
  if (length(c_map)) {
    idx <- match(names(cl), names(c_map), nomatch = 0L)
    names(cl)[idx > 0] <- unname(c_map[names(cl)[idx > 0]])
  }
  
  # Safety: ensure join key still present
  if (!"p_participant_id" %in% names(yl))
    stop("label_overlaps_for_merge: youth_plus lost p_participant_id; check ignore list.")
  if (!"p_participant_id" %in% names(cl))
    stop("label_overlaps_for_merge: cg_plus lost p_participant_id; check ignore list.")
  
  # Write map (old → new)
  map_df <- rbind(
    if (length(y_map)) data.frame(source="youth_plus", old=names(y_map), new=unname(y_map), stringsAsFactors=FALSE),
    if (length(c_map)) data.frame(source="cg_plus",    old=names(c_map), new=unname(c_map), stringsAsFactors=FALSE)
  )
  if (nrow(map_df) == 0) {
    .write_csv_safe(data.frame(source=character(), old=character(), new=character()), map_file)
  } else {
    .write_csv_safe(map_df, map_file)
  }
  message("Overlap rename map written: ", map_file)
  
  list(youth = yl, caregiver = cl)
}