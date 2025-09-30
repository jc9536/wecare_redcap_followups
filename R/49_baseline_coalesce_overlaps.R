# =============================================================================
# WeCare ETL • 49_baseline_coalesce_overlaps.R
# -----------------------------------------------------------------------------
# Purpose
#   Refine baseline overlaps in the merged Youth–Caregiver table.
#
# Rules implemented:
#   • Keep *_y and *_cg for these *baseline-only* fields, and AUDIT differences:
#       - eligibility_screen_complete
#       - harlem_subject_information_and_informed_consent_fo_complete
#       - project_label
#
#   • For family_id, site_id, over_18:
#       - Create canonical UNSUFFIXED column from **baseline youth** value.
#       - Audit when any other source (baseline_cg or any FU) has a different
#         non-blank value.
#       - Drop all source columns for these bases from the final output.
#
#   • Normalize *_y_bl / *_cg_bl → *_y / *_cg before applying the rules.
#   • Follow-up fields remain {base}_{y|cg}_{3m|6m}.
#
# Outputs (side effect)
#   - data/checks/overlap_value_refined_audit.csv  (or `conflicts_out` path)
# =============================================================================

# --- helpers ------------------------------------------------------------------

# Normalize baseline suffixes on column names:
#   *_y_bl → *_y
#   *_cg_bl → *_cg
.rename_baseline_suffixes <- function(dat) {
  nm <- names(dat)
  
  # Youth baseline cols
  y_bl <- nm[grepl("_y_bl$", nm)]
  if (length(y_bl)) {
    y_new <- sub("_y_bl$", "_y", y_bl)
    for (i in seq_along(y_bl)) {
      old <- y_bl[i]; new <- y_new[i]
      if (new %in% names(dat)) {
        # Keep the canonical name; drop the old if duplicate exists
        dat[[old]] <- NULL
      } else {
        names(dat)[names(dat) == old] <- new
      }
    }
  }
  
  # Caregiver baseline cols
  nm <- names(dat)
  cg_bl <- nm[grepl("_cg_bl$", nm)]
  if (length(cg_bl)) {
    cg_new <- sub("_cg_bl$", "_cg", cg_bl)
    for (i in seq_along(cg_bl)) {
      old <- cg_bl[i]; new <- cg_new[i]
      if (new %in% names(dat)) {
        dat[[old]] <- NULL
      } else {
        names(dat)[names(dat) == old] <- new
      }
    }
  }
  dat
}

# Gather all sources for a given base into a list of vectors (missing → NA)
.get_sources <- function(dat, base) {
  list(
    bl_y  = if (paste0(base, "_y")     %in% names(dat)) dat[[paste0(base, "_y")]]     else rep(NA, nrow(dat)),
    bl_cg = if (paste0(base, "_cg")    %in% names(dat)) dat[[paste0(base, "_cg")]]    else rep(NA, nrow(dat)),
    y_3m  = if (paste0(base, "_y_3m")  %in% names(dat)) dat[[paste0(base, "_y_3m")]]  else rep(NA, nrow(dat)),
    y_6m  = if (paste0(base, "_y_6m")  %in% names(dat)) dat[[paste0(base, "_y_6m")]]  else rep(NA, nrow(dat)),
    cg_3m = if (paste0(base, "_cg_3m") %in% names(dat)) dat[[paste0(base, "_cg_3m")]] else rep(NA, nrow(dat)),
    cg_6m = if (paste0(base, "_cg_6m") %in% names(dat)) dat[[paste0(base, "_cg_6m")]] else rep(NA, nrow(dat))
  )
}

# (A) Baseline-only rule: keep *_y and *_cg; audit differing non-blank pairs.
.audit_baseline_both_only <- function(dat, base) {
  src <- .get_sources(dat, base)
  v_y  <- as.character(src$bl_y);  v_y[is_blank(v_y)]  <- NA
  v_cg <- as.character(src$bl_cg); v_cg[is_blank(v_cg)] <- NA
  both <- !is.na(v_y) & !is.na(v_cg)
  diff <- both & (v_y != v_cg)
  if (!any(diff, na.rm = TRUE)) return(NULL)
  
  ids <- if ("p_participant_id" %in% names(dat)) dat$p_participant_id else seq_len(nrow(dat))
  data.frame(
    p_participant_id = ids[diff],
    base_column      = base,
    rule             = "baseline_both_only_diff",
    value_bl_y       = v_y[diff],
    value_bl_cg      = v_cg[diff],
    value_y_3m       = NA_character_,
    value_y_6m       = NA_character_,
    value_cg_3m      = NA_character_,
    value_cg_6m      = NA_character_,
    stringsAsFactors = FALSE
  )
}

# (B) Canonical-from-youth rule:
#     - Create unsuffixed {base} from baseline youth value.
#     - Audit when any other non-blank source differs.
#     - Optionally drop all source columns for this base.
.apply_canonical_from_bl_y <- function(dat, base, drop_sources = TRUE) {
  src  <- .get_sources(dat, base)
  v_y   <- as.character(src$bl_y);   v_y[is_blank(v_y)]   <- NA
  v_cg  <- as.character(src$bl_cg);  v_cg[is_blank(v_cg)] <- NA
  v_y3  <- as.character(src$y_3m);   v_y3[is_blank(v_y3)] <- NA
  v_y6  <- as.character(src$y_6m);   v_y6[is_blank(v_y6)] <- NA
  v_cg3 <- as.character(src$cg_3m);  v_cg3[is_blank(v_cg3)] <- NA
  v_cg6 <- as.character(src$cg_6m);  v_cg6[is_blank(v_cg6)] <- NA
  
  # Canonical unsuffixed column == baseline youth value
  dat[[base]] <- v_y
  
  # flag differences vs any other non-blank value
  any_diff <- (!is.na(v_cg)  & v_cg  != v_y) |
    (!is.na(v_y3)  & v_y3  != v_y) |
    (!is.na(v_y6)  & v_y6  != v_y) |
    (!is.na(v_cg3) & v_cg3 != v_y) |
    (!is.na(v_cg6) & v_cg6 != v_y)
  
  audit <- NULL
  if (any(any_diff, na.rm = TRUE)) {
    ids <- if ("p_participant_id" %in% names(dat)) dat$p_participant_id else seq_len(nrow(dat))
    audit <- data.frame(
      p_participant_id = ids[any_diff],
      base_column      = base,
      rule             = "canonical_from_bl_y_diff",
      value_bl_y       = v_y[any_diff],
      value_bl_cg      = v_cg[any_diff],
      value_y_3m       = v_y3[any_diff],
      value_y_6m       = v_y6[any_diff],
      value_cg_3m      = v_cg3[any_diff],
      value_cg_6m      = v_cg6[any_diff],
      stringsAsFactors = FALSE
    )
  }
  
  # Drop source columns for this base if requested
  if (isTRUE(drop_sources)) {
    drops <- c(
      paste0(base, "_y"), paste0(base, "_cg"),
      paste0(base, "_y_3m"), paste0(base, "_y_6m"),
      paste0(base, "_cg_3m"), paste0(base, "_cg_6m")
    )
    drops <- intersect(drops, names(dat))
    if (length(drops)) dat[drops] <- NULL
  }
  
  list(dat = dat, audit = audit)
}

# --- main ---------------------------------------------------------------------

#' Refine baseline overlaps and emit value audits.
#'
#' @param dat           merged Youth–Caregiver table (data.frame)
#' @param overlaps_csv  (unused; kept for compatibility) path to overlaps list
#' @param conflicts_out path to write the audit CSV (default: "data/checks/overlap_value_audit.csv")
#' @param audit_out     DEPRECATED alias for `conflicts_out` (if provided, takes precedence)
#' @param drop_sources  logical; drop source columns for canonicalized bases (default TRUE)
#' @param ignore_bases  character; base names to ignore entirely (default none relevant here)
#' @return data.frame with refined baseline columns
coalesce_baseline_overlaps <- function(
    dat,
    overlaps_csv  = "data/checks/overlaps/overlapping_2plus.csv",
    conflicts_out = "data/checks/overlap_value_audit.csv",
    audit_out     = NULL,       # legacy name; if set, overrides conflicts_out
    drop_sources  = TRUE,
    ignore_bases  = c("p_participant_id","wecare_id","wecare_root")
) {
  stopifnot(is.data.frame(dat))
  
  # Choose output path (support legacy param name)
  audit_path <- if (!is.null(audit_out)) audit_out else conflicts_out
  dir.create(dirname(audit_path), recursive = TRUE, showWarnings = FALSE)
  
  # 1) Normalize baseline suffixes so we operate on *_y / *_cg
  dat <- .rename_baseline_suffixes(dat)
  
  # 2) Variable sets per the rules
  baseline_both_only_vars <- c(
    "eligibility_screen_complete",
    "harlem_subject_information_and_informed_consent_fo_complete",
    "project_label"
  )
  canonical_from_bl_y_vars <- c("family_id", "site_id", "over_18")
  
  # Honor ignore list defensively (not strictly needed for these sets)
  baseline_both_only_vars <- setdiff(baseline_both_only_vars, ignore_bases)
  canonical_from_bl_y_vars <- setdiff(canonical_from_bl_y_vars, ignore_bases)
  
  audits <- list()
  
  # (A) Keep *_y/*_cg; audit diffs
  for (b in baseline_both_only_vars) {
    if (paste0(b, "_y") %in% names(dat) || paste0(b, "_cg") %in% names(dat)) {
      blk <- .audit_baseline_both_only(dat, b)
      if (!is.null(blk)) audits[[length(audits) + 1L]] <- blk
    }
  }
  
  # (B) Canonical from BL youth; drop sources entirely
  for (b in canonical_from_bl_y_vars) {
    res <- .apply_canonical_from_bl_y(dat, b, drop_sources = drop_sources)
    dat <- res$dat
    if (!is.null(res$audit)) audits[[length(audits) + 1L]] <- res$audit
  }
  
  # 3) Write audit CSV (even if empty, write an empty file)
  if (length(audits)) {
    out <- do.call(rbind, audits)
    write_csv_safe(out, audit_path)
  } else {
    write_csv_safe(data.frame(), audit_path)
  }
  
  dat
}