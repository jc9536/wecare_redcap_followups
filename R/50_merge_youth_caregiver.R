# =============================================================================
# WeCare ETL • 50_merge_youth_caregiver.R
# -----------------------------------------------------------------------------
# Purpose
#   Full outer merge of Youth & Caregiver “plus” tables on p_participant_id.
#   After merging:
#     - (optional) label overlaps to {base}_{cohort}_{wave} using 56_label_overlaps.R
#     - (optional) coalesce baseline overlaps to a single {base} via 58_coalesce_baseline.R
#     - keep follow-up fields as {base}_{cohort}_{wave} for 3m/6m
#
# Invariants
#   - wecare_id_cg in the merged output reflects the caregiver PPID ROOT
#     (i.e., caregiver ‘wecare_id’ is forced to ppid_root pre-merge)
#
# Inputs
#   youth_plus:     Youth baseline+FU table (from previous steps)
#   caregiver_plus: Caregiver baseline+FU table (from previous steps)
#
# Outputs (side effects)
#   data/out/dat_merged.csv
#   data/checks/merge_summary.csv
#   data/checks/unmatched_youth.csv
#   data/checks/unmatched_caregiver.csv
#   data/checks/overlaps/overlap_renames.csv          (if 56_label_overlaps.R used)
#   data/checks/overlap_value_audit.csv               (if 58_coalesce_baseline.R used)
#   data/checks/overlaps/wecare_sanitization.csv      (sanitization report)
#
# Dependencies expected from earlier files
#   normalize_id(), mk_root_id(), derive_caregiver_wecare_id()
# =============================================================================

# --- Optional overlap/coalesce hooks -----------------------------------------
if (file.exists("R/48_overlap_label_columns.R")) source("R/48_overlap_label_columns.R")
if (file.exists("R/49_baseline_coalesce_overlaps.R")) source("R/49_baseline_coalesce_overlaps.R")

# Return only rows whose id appears more than once (for auditing)
rows_with_dup_id <- function(df, id_col) {
  if (!id_col %in% names(df)) return(df[0, , drop = FALSE])
  tab <- table(df[[id_col]])
  dup_ids <- names(tab)[tab > 1 & nzchar(names(tab))]
  if (!length(dup_ids)) return(df[0, , drop = FALSE])
  df[df[[id_col]] %in% dup_ids, , drop = FALSE]
}

# --- Pre-merge sanitation -----------------------------------------------------

# Ensure a plain 'wecare_id' column exists prior to merge so suffixes are produced,
# and drop any pre-existing side-suffixed id columns that would collide.
sanitize_wecare_before_merge <- function(df, who = c("y","cg")) {
  who <- match.arg(who)
  nm <- names(df)
  
  # Create unsuffixed wecare_id if missing by copying a side-suffixed one
  if (!"wecare_id" %in% nm) {
    pref <- if (who == "y") "wecare_id_y" else "wecare_id_cg"
    if (pref %in% nm) {
      df$wecare_id <- df[[pref]]
    } else if ("wecare_id_y" %in% nm) {
      df$wecare_id <- df[["wecare_id_y"]]
    } else if ("wecare_id_cg" %in% nm) {
      df$wecare_id <- df[["wecare_id_cg"]]
    } else {
      df$wecare_id <- NA_character_
    }
  }
  
  # Drop any side-suffixed wecare_id columns to avoid merge() name clashes
  drops <- intersect(c("wecare_id_y","wecare_id_cg"), names(df))
  if (length(drops)) df[drops] <- NULL
  
  df
}

# Record if we sanitized away side-suffixed id columns
report_wecare_sanitization <- function(before, after, label,
                                       out = "data/checks/overlaps/wecare_sanitization.csv") {
  removed <- setdiff(names(before), names(after))
  removed <- removed[removed %in% c("wecare_id_y","wecare_id_cg")]
  if (!length(removed)) return(invisible(NULL))
  dir.create(dirname(out), recursive = TRUE, showWarnings = FALSE)
  utils::write.table(
    data.frame(when = label, removed = removed),
    out,
    sep = ",",
    row.names = FALSE,
    col.names = !file.exists(out),
    append    = file.exists(out),
    quote     = TRUE
  )
  invisible(NULL)
}

# Summarize merged coverage for quick QA
merge_summary_full <- function(merged) {
  yi <- if ("wecare_id_y"  %in% names(merged)) "wecare_id_y"  else NULL
  ci <- if ("wecare_id_cg" %in% names(merged)) "wecare_id_cg" else NULL
  data.frame(
    n_rows_merged    = nrow(merged),
    n_with_youth     = if (!is.null(yi)) sum(!is_blank(merged[[yi]])) else NA_integer_,
    n_with_caregiver = if (!is.null(ci)) sum(!is_blank(merged[[ci]])) else NA_integer_,
    n_youth_only     = if (!is.null(yi) && !is.null(ci)) sum(!is_blank(merged[[yi]]) &  is_blank(merged[[ci]])) else NA_integer_,
    n_caregiver_only = if (!is.null(yi) && !is.null(ci)) sum( is_blank(merged[[yi]]) & !is_blank(merged[[ci]])) else NA_integer_,
    n_matched_both   = if (!is.null(yi) && !is.null(ci)) sum(!is_blank(merged[[yi]]) & !is_blank(merged[[ci]])) else NA_integer_,
    stringsAsFactors = FALSE
  )
}

# --- Main ---------------------------------------------------------------------

#' Full outer merge of Youth & Caregiver on p_participant_id (with audits).
#'
#' @param youth_plus     Youth baseline+FU table
#' @param caregiver_plus Caregiver baseline+FU table
#' @param out_file       Output CSV path for merged data
#' @param checks_dir     Directory for QA artifacts
#' @param overlaps_csv_for_coalesce Path used by overlap coalescer (if present)
#' @return invisibly returns the merged data.frame
youth_caregiver_full_join <- function(
    youth_plus, caregiver_plus,
    out_file   = "data/out/dat_merged.csv",
    checks_dir = "data/checks",
    overlaps_csv_for_coalesce = "data/checks/overlaps/overlapping_2plus.csv"
) {
  dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)
  dir.create(checks_dir,        recursive = TRUE, showWarnings = FALSE)
  
  # Join key preparation: if p_participant_id missing, derive from ROOT(wecare_id)
  if (!"p_participant_id" %in% names(youth_plus)) {
    if ("wecare_id" %in% names(youth_plus) && exists("normalize_id") && exists("mk_root_id")) {
      youth_plus$p_participant_id <- mk_root_id(normalize_id(youth_plus$wecare_id))
    } else stop("Youth table missing p_participant_id and wecare_id.")
  }
  if (!"p_participant_id" %in% names(caregiver_plus)) {
    if ("wecare_id" %in% names(caregiver_plus) && exists("normalize_id") && exists("mk_root_id")) {
      caregiver_plus$p_participant_id <- mk_root_id(normalize_id(caregiver_plus$wecare_id))
    } else stop("Caregiver table missing p_participant_id and wecare_id.")
  }
  
  # Audit duplicate keys BEFORE any collapsing (visibility)
  yt_dups <- rows_with_dup_id(youth_plus,     "p_participant_id")
  cg_dups <- rows_with_dup_id(caregiver_plus, "p_participant_id")
  write_csv_safe(yt_dups, file.path(checks_dir, "merge_dups_youth.csv"))
  write_csv_safe(cg_dups, file.path(checks_dir, "merge_dups_caregiver.csv"))
  
  # Remove stray .x/.y/_x/_y artifacts that may exist from upstream merges
  yl <- collapse_suffix_pairs(youth_plus,     prefer = "x")
  cl <- collapse_suffix_pairs(caregiver_plus, prefer = "x")
  
  # (Optional) Label overlaps so FU columns become {base}_{cohort}_{wave}
  if (exists("label_overlaps_for_merge")) {
    ren <- label_overlaps_for_merge(
      youth_plus = yl,
      cg_plus    = cl,
      map_file   = file.path(checks_dir, "overlaps/overlap_renames.csv")
    )
    yl <- ren$youth
    cl <- ren$caregiver
  }
  
  # --- Critical: caregiver merge id must be PPID ROOT (ppid_root) -------------
  if (!"ppid_root" %in% names(cl)) {
    stop("50_merge.R: caregiver table 'cl' lacks 'ppid_root'. Create it earlier.", call. = FALSE)
  }
  # Keep original (audit)
  if (!"wecare_id_raw_cg" %in% names(cl)) {
    cl$wecare_id_raw_cg <- cl$wecare_id %||% NA_character_
  }
  # Force caregiver-side unsuffixed id to PPID ROOT so suffixing yields wecare_id_cg == ppid_root
  cl$wecare_id <- cl$ppid_root
  
  # If youth lacks ppid_root, mirror its wecare_id (not strictly required here)
  if (!"ppid_root" %in% names(yl) && "wecare_id" %in% names(yl)) {
    yl$ppid_root <- yl$wecare_id
  }
  
  # Ensure unsuffixed 'wecare_id' exists and drop side-suffixed variants pre-merge
  yl_before <- yl; cl_before <- cl
  yl <- sanitize_wecare_before_merge(yl, who = "y")
  cl <- sanitize_wecare_before_merge(cl, who = "cg")
  report_wecare_sanitization(yl_before, yl, "youth_premerge")
  report_wecare_sanitization(cl_before, cl, "caregiver_premerge")
  
  # Final safety: if caregiver wecare_id still blank, try to derive one
  if ("wecare_id" %in% names(cl) && all(is_blank(cl$wecare_id)) && exists("derive_caregiver_wecare_id")) {
    cl$wecare_id <- derive_caregiver_wecare_id(cl)
  }
  
  # Add empty columns if completely absent so merge produces suffixes
  if (!"wecare_id" %in% names(yl)) yl$wecare_id <- NA_character_
  if (!"wecare_id" %in% names(cl)) cl$wecare_id <- NA_character_
  
  # --- FULL OUTER JOIN on p_participant_id -----------------------------------
  stopifnot("p_participant_id" %in% names(yl), "p_participant_id" %in% names(cl))
  merged <- merge(yl, cl, by = "p_participant_id", all = TRUE, suffixes = c("_y", "_cg"))
  
  # Presence flags
  merged$has_youth     <- if ("wecare_id_y"  %in% names(merged)) as.integer(!is_blank(merged$wecare_id_y))  else 0L
  merged$has_caregiver <- if ("wecare_id_cg" %in% names(merged)) as.integer(!is_blank(merged$wecare_id_cg)) else 0L
  
  # Column order: keys up front
  front <- intersect(c("p_participant_id","wecare_id_y","wecare_id_cg","has_youth","has_caregiver"), names(merged))
  merged <- merged[, c(front, setdiff(names(merged), front)), drop = FALSE]
  
  # (Optional) Coalesce baseline overlaps down to a single {base}
  if (exists("coalesce_baseline_overlaps")) {
    merged <- coalesce_baseline_overlaps(
      dat           = merged,
      overlaps_csv  = overlaps_csv_for_coalesce,
      conflicts_out = "data/checks/overlap_value_audit.csv",
      drop_sources  = TRUE,
      ignore_bases  = c("p_participant_id","wecare_id","wecare_root")
    )
  }
  
  # Unmatched lists (post-coalesce—keys unchanged)
  yi <- if ("wecare_id_y"  %in% names(merged)) "wecare_id_y"  else NULL
  ci <- if ("wecare_id_cg" %in% names(merged)) "wecare_id_cg" else NULL
  
  unmatched_youth <- if (!is.null(yi) && !is.null(ci)) {
    merged[ !is_blank(merged[[yi]]) &  is_blank(merged[[ci]]),
            front, drop = FALSE ]
  } else merged[0, , drop = FALSE]
  
  unmatched_caregiver <- if (!is.null(yi) && !is.null(ci)) {
    merged[  is_blank(merged[[yi]]) & !is_blank(merged[[ci]]),
             front, drop = FALSE ]
  } else merged[0, , drop = FALSE]
  
  # Backfill site_id and family_id from p_participant_id
  merged <- merged |> derive_site_id()
  merged <- merged |> derive_family_id()
  
  # Sanitize for STATA imports 
  merged <- sanitize_for_csv(merged, keep_breaks = FALSE)
  
  # Writes
  write_csv_safe(merged,                       out_file)
  write_csv_safe(unmatched_youth,             file.path(checks_dir, "unmatched_youth.csv"))
  write_csv_safe(unmatched_caregiver,         file.path(checks_dir, "unmatched_caregiver.csv"))
  write_csv_safe(merge_summary_full(merged),  file.path(checks_dir, "merge_summary.csv"))
  
  message("✅ Youth ⟷ Caregiver merged + baseline coalesced: ", out_file)
  invisible(merged)
}