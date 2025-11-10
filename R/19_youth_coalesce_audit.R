# R/19_youth_coalesce_audit.R
# Functions to write per-field coalesce QC CSVs for YOUTH baseline RAW export.
# This audit INCLUDES icf18 in icf_nih_share matching.
#
# Usage in run.r (after raw pulls):
#   source("R/19_youth_coalesce_audit.R")
#   qc_dir_y <- file.path(CHECKS_DIR, "baseline_coalesce", "youth", format(Sys.Date(), "%Y%m%d"))
#   write_all_youth_coalesce_checks(youth_base_raw, out_dir = qc_dir_y)

# --------------------------- Utilities ---------------------------------------

`%||%` <- function(a, b) if (is.null(a)) b else a

.require_pkgs_or_stop <- function(pkgs) {
  miss <- pkgs[!vapply(pkgs, requireNamespace, quietly = TRUE, FUN.VALUE = logical(1))]
  if (length(miss)) {
    stop("Missing required packages: ", paste(miss, collapse = ", "),
         ". Please install them and re-run.")
  }
}

# Fallback: if the project hasn't defined coalesce_columns(), define a compatible one.
if (!exists("coalesce_columns", mode = "function")) {
  coalesce_columns <- function(data,
                               regex = "ps[0-9]{2}_",
                               keyword,
                               treat_zero_as_missing = TRUE) {
    pat  <- paste0(regex, ".*", keyword, "$")   # allow site prefixes; perl=TRUE downstream
    cols <- grep(pat, names(data), value = TRUE, perl = TRUE)
    n <- nrow(data)
    if (!length(cols)) return(rep(NA_character_, n))
    
    vals <- data[, cols, drop = FALSE]
    vals[] <- lapply(vals, function(x) {
      if (is.data.frame(x)) x <- if (ncol(x) == 1L) x[[1]] else x[[1]]
      if (inherits(x, "haven_labelled")) {
        if (requireNamespace("haven", quietly = TRUE)) x <- haven::zap_labels(x) else x <- as.vector(x)
      }
      if (is.factor(x)) x <- as.character(x)
      if (isTRUE(treat_zero_as_missing)) {
        if (is.numeric(x))        x[x == 0] <- NA_real_
        else if (is.character(x)) x[x %in% c("0", "")] <- NA_character_
      }
      x
    })
    dplyr::coalesce(!!!as.list(vals))
  }
}

# Normalize a vector exactly like coalesce_columns() will see it
.normalize_for_coalesce <- function(x, treat_zero_as_missing = TRUE) {
  if (is.data.frame(x)) x <- if (ncol(x) == 1L) x[[1]] else x[[1]]
  if (inherits(x, "haven_labelled")) {
    if (requireNamespace("haven", quietly = TRUE)) x <- haven::zap_labels(x) else x <- as.vector(x)
  }
  if (is.factor(x)) x <- as.character(x)
  if (isTRUE(treat_zero_as_missing)) {
    if (is.numeric(x))        x[x == 0] <- NA_real_
    else if (is.character(x)) x[x %in% c("0", "")] <- NA_character_
  }
  x
}

# Build a wide QC frame with:
#   - ID columns (best-effort)
#   - each matched source column (normalized)
#   - <out_name> (true coalesced value)
#   - chosen_source, n_nonmissing, n_unique, conflict (values disagree)
coalesce_check_frame <- function(data,
                                 regex,
                                 keyword,
                                 out_name,
                                 id_cols_guess = c("record_id","p_participant_id","participant_id","site_id"),
                                 treat_zero_as_missing = TRUE) {
  .require_pkgs_or_stop(c("dplyr","tibble"))
  pat  <- paste0(regex, ".*", keyword, "$")
  src_cols <- grep(pat, names(data), value = TRUE, perl = TRUE)
  
  if (!length(src_cols)) {
    warning(sprintf("No columns matched for %s (pattern: %s + %s). Skipping.", out_name, regex, keyword))
    return(NULL)
  }
  
  src <- data[, src_cols, drop = FALSE]
  src_norm <- as.data.frame(lapply(src, .normalize_for_coalesce,
                                   treat_zero_as_missing = treat_zero_as_missing),
                            stringsAsFactors = FALSE)
  
  out_vec <- coalesce_columns(data, regex = regex, keyword = keyword,
                              treat_zero_as_missing = treat_zero_as_missing)
  
  src_chr <- as.data.frame(lapply(src_norm, as.character), stringsAsFactors = FALSE)
  
  first_nonmiss <- function(v) {
    idx <- which(!is.na(v) & v != "")
    if (length(idx)) idx[1] else NA_integer_
  }
  
  chosen_idx    <- apply(src_chr, 1, first_nonmiss)
  chosen_source <- ifelse(is.na(chosen_idx), NA_character_, names(src_chr)[chosen_idx])
  
  n_nonmissing <- apply(src_chr, 1, function(v) sum(!is.na(v) & v != ""))
  n_unique     <- apply(src_chr, 1, function(v) length(unique(v[!is.na(v) & v != ""])))
  conflict     <- n_unique > 1
  
  have_ids <- intersect(id_cols_guess, names(data))
  out <- dplyr::bind_cols(
    data[, have_ids, drop = FALSE],
    src_norm,
    tibble::tibble(
      !!out_name := out_vec,
      chosen_source = chosen_source,
      n_nonmissing  = n_nonmissing,
      n_unique      = n_unique,
      conflict      = conflict
    )
  )
  tibble::as_tibble(out)
}

# Write a single field's QC CSV
write_coalesce_check <- function(data, regex, keyword, out_name,
                                 out_dir,
                                 treat_zero_as_missing = TRUE) {
  .require_pkgs_or_stop(c("readr"))
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  fr <- coalesce_check_frame(data, regex, keyword, out_name,
                             treat_zero_as_missing = treat_zero_as_missing)
  if (is.null(fr)) return(invisible(NULL))
  fn <- file.path(out_dir, paste0(out_name, "_coalesce_check.csv"))
  readr::write_csv(fr, fn, na = "")
  message("Wrote: ", fn)
  invisible(fn)
}

# --------------------------- Main entry --------------------------------------

# Writes one CSV per coalesced field + an index summary.
# dat_youth_raw : raw youth baseline data.frame
# out_dir       : where to write CSVs; if NULL, defaults to data/checks/baseline_coalesce/youth/<date>
# NOTE: For NIH share, this INCLUDES icf18 (regex: icf[0-9]{2}y?_ + nih_?share).
write_all_youth_coalesce_checks <- function(dat_youth_raw, out_dir = NULL) {
  .require_pkgs_or_stop(c("readr","tibble","purrr"))
  
  if (missing(dat_youth_raw) || !is.data.frame(dat_youth_raw) || !nrow(dat_youth_raw)) {
    stop("write_all_youth_coalesce_checks(): 'dat_youth_raw' must be a non-empty data.frame.")
  }
  
  if (is.null(out_dir)) {
    checks_root <- if (exists("CHECKS_DIR", inherits = TRUE)) get("CHECKS_DIR", inherits = TRUE) else "data/checks"
    out_dir <- file.path(checks_root, "baseline_coalesce", "youth", format(Sys.Date(), "%Y%m%d"))
  }
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  message("🧪 Youth baseline coalesce audit → ", out_dir)
  
  specs <- tibble::tribble(
    ~out_name,                         ~regex,                      ~keyword,                                   ~treat_zero_as_missing,
    "ps_hear_more",                    "ps[0-9]{2}y_",              "hear_more(?:_[A-Za-z]{2,3})?",             FALSE,  # keep zeros
    "ps_willing_to_contact",           "ps[0-9]{2}y_",              "willing_to_contact",                       TRUE,
    "ps_decline_reason",               "ps[0-9]{2}y_",              "decline_reason",                           TRUE,
    "ps_youth_name",                   "ps[0-9]{2}y_",              "youth_name",                               TRUE,
    "ps_signature",                    "ps[0-9]{2}y_",              "signature",                                TRUE,
    "ps_date",                         "ps[0-9]{2}y_",              "date",                                     TRUE,
    "contact_form_youth_complete",     ".*prescreening_contact_form.*", "complete",                              TRUE,
    
    "icf_name_1",                      "icf[0-9]{2}y?_",            "name_1",                                   TRUE,
    "icf_name_2",                      "icf[0-9]{2}y?_",            "name_2",                                   TRUE,
    # NIH share (INCLUDES 18; allow 'nih_share' or 'nihshare', allow optional 'y')
    "icf_nih_share",                   "icf[0-9]{2}y?_",            "nih_?share",                               TRUE,
    "icf_name_3",                      "icf[0-9]{2}y?_",            "name_3",                                   TRUE,
    "icf_nih_first_name",              "icf[0-9]{2}y?_",            "first_name",                               TRUE,
    "icf_nih_middle_name",             "icf[0-9]{2}y?_",            "middle_name",                              TRUE,
    "icf_nih_last_name",               "icf[0-9]{2}y?_",            "last_name",                                TRUE,
    "icf_nih_dob",                     "icf[0-9]{2}y?_",            "dob",                                      TRUE,
    "icf_nih_sex",                     "icf[0-9]{2}y?_",            "sex",                                      TRUE,
    "icf_nih_city",                    "icf[0-9]{2}y?_",            "city",                                     TRUE,
    
    "informed_consent_form_youth_complete", ".*subject_information_and_informed.*", "complete",                TRUE
  )
  
  # Write one CSV per field
  purrr::pwalk(
    specs,
    ~write_coalesce_check(
      data    = dat_youth_raw,
      regex   = ..2,
      keyword = ..3,
      out_name= ..1,
      out_dir = out_dir,
      treat_zero_as_missing = ..4
    )
  )
  
  # Index summary across all fields
  idx <- purrr::pmap_dfr(
    as.list(specs),
    function(out_name, regex, keyword, treat_zero_as_missing) {
      srcs <- grep(paste0(regex, ".*", keyword, "$"), names(dat_youth_raw), value = TRUE, perl = TRUE)
      fr <- try(
        coalesce_check_frame(dat_youth_raw, regex, keyword, out_name,
                             treat_zero_as_missing = treat_zero_as_missing),
        silent = TRUE
      )
      if (inherits(fr, "try-error") || is.null(fr)) {
        return(tibble::tibble(
          field        = out_name,
          rows         = 0L,
          conflicts    = NA_integer_,
          any_missing  = NA_integer_,
          sources      = paste(srcs, collapse = ";")
        ))
      }
      tibble::tibble(
        field        = out_name,
        rows         = nrow(fr),
        conflicts    = sum(fr$conflict %||% FALSE, na.rm = TRUE),
        any_missing  = sum(is.na(fr[[out_name]]) | fr[[out_name]] == "", na.rm = TRUE),
        sources      = paste(srcs, collapse = ";")
      )
    }
  )
  
  readr::write_csv(idx, file.path(out_dir, "_index_summary.csv"), na = "")
  message("✅ Youth coalesce audit complete: ", nrow(specs), " files + index written.")
  invisible(out_dir)
}