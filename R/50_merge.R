# R/50_merge.R
# Full outer merge of Youth & Caregiver on p_participant_id.
# Keeps ALL youths and ALL caregivers (matched when IDs align).
# Suffixes: _y for Youth columns, _cg for Caregiver columns.

# -- helpers ---------------------------------------------------------------

is_blank <- function(x) is.na(x) | x == ""

# rows where id appears more than once
rows_with_dup_id <- function(df, id_col) {
  if (!id_col %in% names(df)) return(df[0, , drop = FALSE])
  tab <- table(df[[id_col]])
  dup_ids <- names(tab)[tab > 1 & nzchar(names(tab))]
  if (!length(dup_ids)) return(df[0, , drop = FALSE])
  df[df[[id_col]] %in% dup_ids, , drop = FALSE]
}

# collapse to one row per p_participant_id (most-complete rule)
coalesce_by_ppid <- function(df) {
  stopifnot("p_participant_id" %in% names(df))
  df <- df[!is_blank(df$p_participant_id), , drop = FALSE]
  pick_most_complete_by(df, "p_participant_id")
}

# remove leftover merge artifacts so suffixing is predictable
sanitize_before_merge <- function(df, side = c("y","cg")) {
  side <- match.arg(side)
  # Drop prior merge artifacts if present
  drop_these <- intersect(c("wecare_id_y","wecare_id_cg","has_youth","has_caregiver"), names(df))
  if (length(drop_these)) df[drop_these] <- NULL
  
  # Normalize key name if someone saved a suffixed version only
  if (!"wecare_id" %in% names(df)) {
    if ("wecare_id_y"  %in% names(df)) names(df)[names(df) == "wecare_id_y"]  <- "wecare_id"
    if ("wecare_id_cg" %in% names(df)) names(df)[names(df) == "wecare_id_cg"] <- "wecare_id"
  }
  df
}

merge_summary_full <- function(merged) {
  data.frame(
    n_rows_merged             = nrow(merged),
    n_with_youth              = sum(!is_blank(merged$wecare_id_y)),
    n_with_caregiver          = sum(!is_blank(merged$wecare_id_cg)),
    n_youth_only              = sum(!is_blank(merged$wecare_id_y)  &  is_blank(merged$wecare_id_cg)),
    n_caregiver_only          = sum( is_blank(merged$wecare_id_y)  & !is_blank(merged$wecare_id_cg)),
    n_matched_both            = sum(!is_blank(merged$wecare_id_y)  & !is_blank(merged$wecare_id_cg)),
    stringsAsFactors = FALSE
  )
}

# -- main ------------------------------------------------------------------

# Full outer join on p_participant_id
# Writes:
#   data/out/dat_merged.csv
#   data/checks/merge_dups_youth.csv
#   data/checks/merge_dups_caregiver.csv
#   data/checks/unmatched_youth.csv
#   data/checks/unmatched_caregiver.csv
#   data/checks/merge_summary.csv
youth_caregiver_full_join <- function(youth_plus, caregiver_plus,
                                      out_file   = "data/out/dat_merged.csv",
                                      checks_dir = "data/checks") {
  dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)
  dir.create(checks_dir,        recursive = TRUE, showWarnings = FALSE)
  
  # Ensure p_participant_id exists (compute from wecare_id if missing)
  if (!"p_participant_id" %in% names(youth_plus)) {
    if (!"wecare_id" %in% names(youth_plus)) stop("Youth table missing p_participant_id and wecare_id.")
    youth_plus$p_participant_id <- mk_root_id(youth_plus$wecare_id)
  }
  if (!"p_participant_id" %in% names(caregiver_plus)) {
    if (!"wecare_id" %in% names(caregiver_plus)) stop("Caregiver table missing p_participant_id and wecare_id.")
    caregiver_plus$p_participant_id <- mk_root_id(caregiver_plus$wecare_id)
  }
  
  # Drop leftover merge artifacts so suffixing won't collide
  youth_plus     <- sanitize_before_merge(youth_plus,     side = "y")
  caregiver_plus <- sanitize_before_merge(caregiver_plus, side = "cg")
  
  # Audit duplicates BEFORE collapsing
  yt_dups <- rows_with_dup_id(youth_plus,     "p_participant_id")
  cg_dups <- rows_with_dup_id(caregiver_plus, "p_participant_id")
  write_csv_safe(yt_dups, file.path(checks_dir, "merge_dups_youth.csv"))
  write_csv_safe(cg_dups, file.path(checks_dir, "merge_dups_caregiver.csv"))
  
  # Collapse to one row per ID
  yt1 <- coalesce_by_ppid(youth_plus)
  cg1 <- coalesce_by_ppid(caregiver_plus)
  
  # Make sure the minimal keys exist for flagging
  if (!"wecare_id" %in% names(yt1)) yt1$wecare_id <- NA_character_
  if (!"wecare_id" %in% names(cg1)) cg1$wecare_id <- NA_character_
  
  # FULL OUTER JOIN
  merged <- merge(
    yt1, cg1,
    by       = "p_participant_id",
    all      = TRUE,                 # full outer
    suffixes = c("_y", "_cg")
  )
  
  # Flags + tidy key ordering
  merged$has_youth     <- as.integer(!is_blank(merged$wecare_id_y))
  merged$has_caregiver <- as.integer(!is_blank(merged$wecare_id_cg))
  
  front <- intersect(c("p_participant_id","wecare_id_y","wecare_id_cg","has_youth","has_caregiver"),
                     names(merged))
  merged <- merged[, c(front, setdiff(names(merged), front)), drop = FALSE]
  
  # Unmatched lists
  unmatched_youth     <- merged[ merged$has_youth == 1 & merged$has_caregiver == 0, 
                                 front, drop = FALSE]
  unmatched_caregiver <- merged[ merged$has_youth == 0 & merged$has_caregiver == 1, 
                                 front, drop = FALSE]
  
  # Outputs
  write_csv_safe(merged,               out_file)
  write_csv_safe(unmatched_youth,      file.path(checks_dir, "unmatched_youth.csv"))
  write_csv_safe(unmatched_caregiver,  file.path(checks_dir, "unmatched_caregiver.csv"))
  write_csv_safe(merge_summary_full(merged), file.path(checks_dir, "merge_summary.csv"))
  
  message("🧩 Full merged Youth ⟷ Caregiver written: ", out_file)
  invisible(merged)
}