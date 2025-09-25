# R/50_merge.R
# -------------------------------------------------------------------
# Full outer merge of Youth & Caregiver on p_participant_id.
# After merging:
#   - label overlaps to {base}_{cohort}_{wave} (if R/56_label_overlaps.R exists)
#   - coalesce baseline overlaps to a single {base} (R/58_coalesce_baseline.R)
#   - keep follow-up fields as {base}_{cohort}_{wave} for 3m/6m
# -------------------------------------------------------------------

if (file.exists("R/56_label_overlaps.R")) source("R/56_label_overlaps.R")
if (file.exists("R/58_coalesce_baseline.R")) source("R/58_coalesce_baseline.R")

# --- ensure clean wecare_id before final merge --------------------------------
sanitize_wecare_before_merge <- function(df, who = c("y","cg")) {
  who <- match.arg(who)
  nm <- names(df)
  
  # If the unsuffixed wecare_id is missing, try to reconstruct it from a side-tagged one
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
  
  # Drop any preexisting side-suffixed wecare_id columns to avoid name clashes in merge()
  drops <- intersect(c("wecare_id_y","wecare_id_cg"), names(df))
  if (length(drops)) df[drops] <- NULL
  
  df
}

# optional: write a quick report of any columns we sanitized away
report_wecare_sanitization <- function(before, after, label, out = "data/checks/overlaps/wecare_sanitization.csv") {
  removed <- setdiff(names(before), names(after))
  removed <- removed[removed %in% c("wecare_id_y","wecare_id_cg")]
  if (!length(removed)) return(invisible(NULL))
  dir.create(dirname(out), recursive = TRUE, showWarnings = FALSE)
  utils::write.table(data.frame(when = label, removed = removed), out,
                     sep = ",", row.names = FALSE, col.names = !file.exists(out), append = file.exists(out), quote = TRUE)
  invisible(NULL)
}

write_csv_safe <- function(df, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  utils::write.csv(df, path, row.names = FALSE, na = "")
}
is_blank <- function(x) is.na(x) | x == ""

rows_with_dup_id <- function(df, id_col) {
  if (!id_col %in% names(df)) return(df[0, , drop = FALSE])
  tab <- table(df[[id_col]])
  dup_ids <- names(tab)[tab > 1 & nzchar(names(tab))]
  if (!length(dup_ids)) return(df[0, , drop = FALSE])
  df[df[[id_col]] %in% dup_ids, , drop = FALSE]
}

# light suffix collapser in case earlier joins created .x/.y
collapse_suffix_pairs <- function(df, prefer = c("x","y")) {
  prefer <- match.arg(prefer)
  nm <- names(df)
  pair_idx <- grepl("(\\.|_)[xy]$", nm)
  if (!any(pair_idx)) return(df)
  base_from <- function(n) sub("(\\.|_)x$|(\\.|_)y$", "", n)
  bases <- unique(base_from(nm[pair_idx]))
  .is_blank <- function(x) is.na(x) | x == ""
  for (b in bases) {
    x_dot <- paste0(b, ".x"); y_dot <- paste0(b, ".y")
    x_ul  <- paste0(b, "_x"); y_ul  <- paste0(b, "_y")
    x_nm <- if (x_dot %in% nm) x_dot else if (x_ul %in% nm) x_ul else NA_character_
    y_nm <- if (y_dot %in% nm) y_dot else if (y_ul %in% nm) y_ul else NA_character_
    has_x <- !is.na(x_nm) && x_nm %in% nm
    has_y <- !is.na(y_nm) && y_nm %in% nm
    if (!has_x && !has_y) next
    winner <- if (has_x && has_y) {
      vx <- df[[x_nm]]; vy <- df[[y_nm]]
      if (prefer == "x") { vx[.is_blank(vx)] <- vy[.is_blank(vx)]; vx }
      else               { vy[.is_blank(vy)] <- vx[.is_blank(vy)]; vy }
    } else if (has_x) df[[x_nm]] else df[[y_nm]]
    df[[b]] <- winner
    drop <- intersect(c(x_nm, y_nm), names(df))
    df[drop] <- NULL
    nm <- names(df)
  }
  df
}

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

# ---------------------- main ---------------------------------------------------

youth_caregiver_full_join <- function(youth_plus, caregiver_plus,
                                      out_file   = "data/out/dat_merged.csv",
                                      checks_dir = "data/checks",
                                      overlaps_csv_for_coalesce = "data/checks/overlaps/overlapping_2plus.csv") {
  dir.create(dirname(out_file), recursive = TRUE, showWarnings = FALSE)
  dir.create(checks_dir,        recursive = TRUE, showWarnings = FALSE)
  
  # Join key; compute if missing
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
  
  # Audit duplicates BEFORE collapsing (for visibility)
  yt_dups <- rows_with_dup_id(youth_plus,     "p_participant_id")
  cg_dups <- rows_with_dup_id(caregiver_plus, "p_participant_id")
  write_csv_safe(yt_dups, file.path(checks_dir, "merge_dups_youth.csv"))
  write_csv_safe(cg_dups, file.path(checks_dir, "merge_dups_caregiver.csv"))
  
  # (Optionally you can call the smart coalescer here; assuming attachers already resolved within-wave dupes.)
  
  # Remove any inner .x/.y artifacts that came from earlier merges
  yl <- collapse_suffix_pairs(youth_plus, prefer = "x")
  cl <- collapse_suffix_pairs(caregiver_plus, prefer = "x")
  
  # Label overlaps BETWEEN youth and caregiver so FU columns are {base}_{cohort}_{wave}
  if (exists("label_overlaps_for_merge")) {
    ren <- label_overlaps_for_merge(
      youth_plus = yl,
      cg_plus    = cl,
      map_file   = file.path(checks_dir, "overlaps/overlap_renames.csv")
    )
    yl <- ren$youth
    cl <- ren$caregiver
  }
  
  # Keep wecare_id unsuffixed pre-merge so we get wecare_id_y / wecare_id_cg for flags
  # Also ensure we don't already carry wecare_id_y/wecare_id_cg columns from upstream.
  yl_before <- yl; cl_before <- cl
  yl <- sanitize_wecare_before_merge(yl, who = "y")
  cl <- sanitize_wecare_before_merge(cl, who = "cg")
  report_wecare_sanitization(yl_before, yl, "youth_premerge")
  report_wecare_sanitization(cl_before, cl, "caregiver_premerge")
  
  # As a safety net, if still missing, add empty wecare_id so merge suffixes can be produced
  if (!"wecare_id" %in% names(yl)) yl$wecare_id <- NA_character_
  if (!"wecare_id" %in% names(cl)) cl$wecare_id <- NA_character_
  
  # FULL OUTER join
  stopifnot("p_participant_id" %in% names(yl), "p_participant_id" %in% names(cl))
  merged <- merge(yl, cl, by = "p_participant_id", all = TRUE, suffixes = c("_y", "_cg"))
  
  # Flags + order
  merged$has_youth     <- if ("wecare_id_y"  %in% names(merged)) as.integer(!is_blank(merged$wecare_id_y))  else 0L
  merged$has_caregiver <- if ("wecare_id_cg" %in% names(merged)) as.integer(!is_blank(merged$wecare_id_cg)) else 0L
  front <- intersect(c("p_participant_id","wecare_id_y","wecare_id_cg","has_youth","has_caregiver"), names(merged))
  merged <- merged[, c(front, setdiff(names(merged), front)), drop = FALSE]
  
  # ✅ NEW: Coalesce BASELINE overlaps to a single {base}; keep FU as {base}_{cohort}_{wave}
  if (exists("coalesce_baseline_overlaps")) {
    merged <- coalesce_baseline_overlaps(
      dat           = merged,
      overlaps_csv  = "data/checks/overlaps/overlapping_2plus.csv",
      conflicts_out = "data/checks/overlap_value_audit.csv",
      drop_sources  = TRUE,
      ignore_bases  = c("p_participant_id","wecare_id","wecare_root")
    )
  }
  
  # Unmatched lists (after coalescing—keys unchanged)
  yi <- if ("wecare_id_y"  %in% names(merged)) "wecare_id_y"  else NULL
  ci <- if ("wecare_id_cg" %in% names(merged)) "wecare_id_cg" else NULL
  unmatched_youth     <- if (!is.null(yi) && !is.null(ci)) merged[ merged[[yi]] != "" & !is.na(merged[[yi]]) &
                                                                     (merged[[ci]] == "" | is.na(merged[[ci]])),
                                                                   front, drop = FALSE] else merged[0,,drop=FALSE]
  unmatched_caregiver <- if (!is.null(yi) && !is.null(ci)) merged[ (merged[[yi]] == "" | is.na(merged[[yi]])) &
                                                                     merged[[ci]] != "" & !is.na(merged[[ci]]),
                                                                   front, drop = FALSE] else merged[0,,drop=FALSE]
  
  # Writes
  write_csv_safe(merged,                    out_file)
  write_csv_safe(unmatched_youth,          file.path(checks_dir, "unmatched_youth.csv"))
  write_csv_safe(unmatched_caregiver,      file.path(checks_dir, "unmatched_caregiver.csv"))
  write_csv_safe(merge_summary_full(merged), file.path(checks_dir, "merge_summary.csv"))
  
  message("🧩 Youth ⟷ Caregiver merged + baseline coalesced: ", out_file)
  invisible(merged)
}