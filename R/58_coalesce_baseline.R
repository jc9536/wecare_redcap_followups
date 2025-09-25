# R/58_coalesce_baseline.R
# -------------------------------------------------------------------
# Refined overlap rules:
#   • Keep *_y and *_cg for:
#       eligibility_screen_complete,
#       harlem_subject_information_and_informed_consent_fo_complete,
#       project_label
#     -> audit rows where *_y != *_cg (ignoring blanks).
#
#   • For family_id, site_id, over_18:
#       - Create canonical unsuffixed column from baseline youth value.
#       - Audit rows where ANY other source has a non-blank DIFFERENT value.
#       - DROP all source columns for these bases (no extra endings in final file).
#
#   • Normalize *_y_bl/*_cg_bl -> *_y/*_cg first.
#   • Follow-up fields for other bases remain {base}_{y|cg}_{3m|6m}.
#   • Audit file: data/checks/overlap_value_refined_audit.csv
# -------------------------------------------------------------------

.is_blank <- function(x) is.na(x) | x == ""

# Normalize baseline suffixes: *_y_bl → *_y ; *_cg_bl → *_cg
.rename_baseline_suffixes <- function(dat) {
  nm <- names(dat)
  y_bl <- nm[grepl("_y_bl$", nm)]
  if (length(y_bl)) {
    y_new <- sub("_y_bl$", "_y", y_bl)
    for (i in seq_along(y_bl)) {
      old <- y_bl[i]; new <- y_new[i]
      if (new %in% names(dat)) dat[[old]] <- NULL else names(dat)[names(dat) == old] <- new
    }
  }
  nm <- names(dat)
  cg_bl <- nm[grepl("_cg_bl$", nm)]
  if (length(cg_bl)) {
    cg_new <- sub("_cg_bl$", "_cg", cg_bl)
    for (i in seq_along(cg_bl)) {
      old <- cg_bl[i]; new <- cg_new[i]
      if (new %in% names(dat)) dat[[old]] <- NULL else names(dat)[names(dat) == old] <- new
    }
  }
  dat
}

# Pull vectors for a base
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

# (A) Baseline-only splits: keep *_y/*_cg, audit diffs
.audit_baseline_both_only <- function(dat, base) {
  src <- .get_sources(dat, base)
  v_y  <- as.character(src$bl_y);  v_y[.is_blank(v_y)]  <- NA
  v_cg <- as.character(src$bl_cg); v_cg[.is_blank(v_cg)] <- NA
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

# (B) Canonical from baseline youth, with option to drop source columns
.apply_canonical_from_bl_y <- function(dat, base, drop_sources = TRUE) {
  src <- .get_sources(dat, base)
  v_y   <- as.character(src$bl_y);   v_y[.is_blank(v_y)]   <- NA
  v_cg  <- as.character(src$bl_cg);  v_cg[.is_blank(v_cg)] <- NA
  v_y3  <- as.character(src$y_3m);   v_y3[.is_blank(v_y3)] <- NA
  v_y6  <- as.character(src$y_6m);   v_y6[.is_blank(v_y6)] <- NA
  v_cg3 <- as.character(src$cg_3m);  v_cg3[.is_blank(v_cg3)] <- NA
  v_cg6 <- as.character(src$cg_6m);  v_cg6[.is_blank(v_cg6)] <- NA
  
  # Canonical unsuffixed column
  dat[[base]] <- v_y
  
  # Differences vs any other non-blank value
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
  
  # Drop all source columns for this base if requested
  if (isTRUE(drop_sources)) {
    drops <- c(paste0(base, "_y"), paste0(base, "_cg"),
               paste0(base, "_y_3m"), paste0(base, "_y_6m"),
               paste0(base, "_cg_3m"), paste0(base, "_cg_6m"))
    drops <- intersect(drops, names(dat))
    if (length(drops)) dat[drops] <- NULL
  }
  
  list(dat = dat, audit = audit)
}

# ----------------------------- main -------------------------------------------

coalesce_baseline_overlaps <- function(
    dat,
    overlaps_csv  = "data/checks/overlaps/overlapping_2plus.csv",  # not strictly needed now
    audit_out     = "data/checks/overlap_value_refined_audit.csv",
    ...
) {
  stopifnot(is.data.frame(dat))
  dir.create(dirname(audit_out), recursive = TRUE, showWarnings = FALSE)
  
  # 1) Normalize baseline suffixes so we operate on *_y/*_cg
  dat <- .rename_baseline_suffixes(dat)
  
  # 2) Sets per your rules
  baseline_both_only_vars <- c(
    "eligibility_screen_complete",
    "harlem_subject_information_and_informed_consent_fo_complete",
    "project_label"
  )
  canonical_from_bl_y_vars <- c("family_id", "site_id", "over_18")
  
  audits <- list(); k <- 0L
  
  # (A) Keep *_y/*_cg, audit diffs
  for (b in baseline_both_only_vars) {
    if (paste0(b, "_y") %in% names(dat) || paste0(b, "_cg") %in% names(dat)) {
      blk <- .audit_baseline_both_only(dat, b)
      if (!is.null(blk)) { k <- k + 1L; audits[[k]] <- blk }
    }
  }
  
  # (B) Canonical from BL youth, drop sources entirely
  for (b in canonical_from_bl_y_vars) {
    res <- .apply_canonical_from_bl_y(dat, b, drop_sources = TRUE)
    dat <- res$dat
    if (!is.null(res$audit)) { k <- k + 1L; audits[[k]] <- res$audit }
  }
  
  # 3) Write audit CSV
  if (length(audits)) {
    utils::write.csv(do.call(rbind, audits), audit_out, row.names = FALSE, na = "")
  } else {
    utils::write.csv(data.frame(), audit_out, row.names = FALSE, na = "")
  }
  
  dat
}