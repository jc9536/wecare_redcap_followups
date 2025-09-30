# =============================================================================
# WeCare ETL • 00_bootstrap.R
# -----------------------------------------------------------------------------
# Purpose:
#   Common utilities used across the pipeline: package bootstrap, filesystem
#   helpers, ID normalization/derivation, and name/suffix helpers for merges.
#
# Notes:
#   - Function names are unchanged (drop-in compatible).
#   - ID helpers normalize to UPPERCASE and trim whitespace for stable joins.
#   - The “root” ID removes trailing "-Y-S" or "-C-S" (case-insensitive).
# =============================================================================

# --- Package bootstrap --------------------------------------------------------

#' Ensure packages are installed and loaded
#' @param pkgs character vector of package names
#' @return (invisible) list of loaded namespaces
ensure_pkgs <- function(pkgs) {
  pkgs <- unique(pkgs)
  to_install <- pkgs[!pkgs %in% rownames(installed.packages())]
  if (length(to_install)) {
    message("Installing packages: ", paste(to_install, collapse = ", "))
    install.packages(to_install, quiet = TRUE)
  }
  invisible(lapply(pkgs, require, character.only = TRUE))
}

# --- Safe filesystem helpers --------------------------------------------------

#' Create directory if it doesn't exist (recursively)
#' @param path directory path
#' @return the same path (invisibly)
ensure_dir <- function(path) {
  dir.create(path, recursive = TRUE, showWarnings = FALSE)
  path
}

# --- Small utilities ----------------------------------------------------------

#' Test for blank values (NA or empty string after trim)
#' @param x vector
#' @return logical vector
is_blank <- function(x) {
  if (is.factor(x)) x <- as.character(x)
  x <- trimws(as.character(x))
  is.na(x) | x == ""
}

#' Normalize IDs: trim, set empty to NA, force UPPERCASE
#' @param x vector
#' @return normalized character vector
normalize_id <- function(x) {
  x <- trimws(as.character(x))
  x[nchar(x) == 0] <- NA_character_
  toupper(x)
}

#' Compute "root" WeCare ID by removing trailing "-Y-S" or "-C-S"
#' e.g., "H-F0123-Y-S" -> "H-F0123"; "K-F0001-C-S" -> "K-F0001"
#' @param x vector of IDs
#' @return character vector of root IDs
mk_root_id <- function(x) {
  x <- normalize_id(x)
  sub("-(Y|C)-S$", "", x, ignore.case = TRUE)
}

# --- Cohort ID derivation -----------------------------------------------------

#' Derive YOUTH wecare_id from available columns (ordered preference)
#' @param df data.frame with potential ID columns
#' @return character vector of derived IDs (normalized)
derive_youth_wecare_id <- function(df) {
  cands <- c("wecare_id", "youth_wecare_id", "youth_wecare_id_3m",
             "youth_wecare_id_6m", "participant_id")
  out <- rep(NA_character_, nrow(df))
  for (nm in cands) {
    if (!nm %in% names(df)) next
    v <- trimws(as.character(df[[nm]]))
    v[!nzchar(v)] <- NA
    out <- ifelse(is_blank(out) & !is.na(v), v, out)
  }
  normalize_id(out)
}

#' Derive CAREGIVER (baseline) wecare_id from available columns (ordered)
#' More permissive to handle export header drift across waves.
#' @param df data.frame with potential ID columns
#' @return character vector of derived IDs (normalized)
derive_caregiver_wecare_id <- function(df) {
  cands <- c(
    "wecare_id",
    "caregiver_id", "caregiver_id_3m", "caregiver_id_6m",
    "p_wecare_id",
    # common fallbacks seen in exports
    "participant_id", "p_participant_id"
  )
  out <- rep(NA_character_, nrow(df))
  for (nm in cands) {
    if (!nm %in% names(df)) next
    v <- trimws(as.character(df[[nm]]))
    v[!nzchar(v)] <- NA
    out <- ifelse(is_blank(out) & !is.na(v), v, out)
  }
  normalize_id(out)
}

#' Derive CAREGIVER follow-up join ID (prioritize 3m ID; fallback sensibly)
#' @param df data.frame with potential FU ID columns
#' @return character vector of derived IDs (normalized)
derive_caregiver_fu_wecare_id <- function(df) {
  if ("caregiver_id_3m" %in% names(df)) {
    v <- trimws(as.character(df$caregiver_id_3m)); v[!nzchar(v)] <- NA
    return(normalize_id(v))
  }
  if ("caregiver_id" %in% names(df)) {
    v <- trimws(as.character(df$caregiver_id)); v[!nzchar(v)] <- NA
    return(normalize_id(v))
  }
  if ("wecare_id" %in% names(df))  return(normalize_id(df$wecare_id))
  if ("p_wecare_id" %in% names(df)) return(normalize_id(df$p_wecare_id))
  rep(NA_character_, nrow(df))
}

# --- Helper: derive caregiver p_wecare_id with sensible fallbacks -------------
# Some exports name the caregiver baseline ID "p_wecare_id"; others drift.
# This helper tries p_wecare_id first, then falls back to known caregiver ID fields.
derive_caregiver_p_wecare_id <- function(df) {
  # Priority 1: the explicit field
  if ("p_wecare_id" %in% names(df)) {
    v <- trimws(as.character(df$p_wecare_id)); v[!nzchar(v)] <- NA
    return(normalize_id(v))
  }
  # Priority 2: reuse the general caregiver derivation
  normalize_id(derive_caregiver_wecare_id(df))
}

#' Compute canonical cross-cohort participant ID (p_participant_id)
#' Preference order:
#'   1) ROOT(YOUTH wecare_id)
#'   2) ROOT(CAREGIVER wecare_id)
#'   3) ROOT(existing p_participant_id)
#' @param wecare_y vector (youth IDs)
#' @param wecare_cg vector (caregiver IDs)
#' @param p_participant_id optional vector to fall back on
#' @return character vector of canonical root IDs
compute_ppid <- function(wecare_y, wecare_cg, p_participant_id = NULL) {
  wroot  <- mk_root_id(wecare_y)
  cgroot <- mk_root_id(wecare_cg)
  pproot <- mk_root_id(p_participant_id)
  out <- ifelse(!is_blank(wroot), wroot,
                ifelse(!is_blank(cgroot), cgroot,
                       ifelse(!is_blank(pproot), pproot, NA_character_)))
  out
}

# --- Merge/column helpers -----------------------------------------------------

#' Append a wave suffix ("_3m" or "_6m") to non-ID columns to avoid collisions
#' @param df data.frame
#' @param wave "3m" or "6m"
#' @param exclude columns that must never be suffixed
#' @return data.frame with renamed columns
force_wave_suffix <- function(
    df,
    wave = c("3m","6m"),
    exclude = c("p_participant_id","ppid_root","wecare_id","caregiver_id_3m")
) {
  wave <- match.arg(wave)
  nm <- names(df)
  keep <- !nm %in% exclude
  need <- keep & !grepl("_(3m|6m)$", nm)
  nm[need] <- paste0(nm[need], "_", wave)
  names(df) <- nm
  df
}

#' Keep only the join key + wave-suffixed columns
#' (drops unsuffixed FU fields; useful after force_wave_suffix)
#' @param df data.frame
#' @param key join key to keep
#' @return pruned data.frame
prune_to_wave <- function(df, key) {
  stopifnot(key %in% names(df))
  wave_cols <- grep("_(3m|6m)$", names(df), value = TRUE)
  df[, c(key, wave_cols), drop = FALSE]
}

#' Collapse .x/.y (or _x/_y) pairs into a single column
#' Prefers one side (x or y) and fills blanks from the other.
#' @param df data.frame resulting from a join
#' @param prefer which side to prefer first: "x" or "y"
#' @return data.frame with pairs collapsed
collapse_suffix_pairs <- function(df, prefer = c("x","y")) {
  prefer <- match.arg(prefer)
  nm <- names(df)
  pair_idx <- grepl("(\\.|_)[xy]$", nm)
  if (!any(pair_idx)) return(df)
  
  base_from <- function(n) sub("(\\.|_)x$|(\\.|_)y$", "", n)
  bases <- unique(base_from(nm[pair_idx]))
  
  for (b in bases) {
    x_dot <- paste0(b, ".x"); y_dot <- paste0(b, ".y")
    x_ul  <- paste0(b, "_x"); y_ul  <- paste0(b, "_y")
    x_nm <- if (x_dot %in% nm) x_dot else if (x_ul %in% nm) x_ul else NA_character_
    y_nm <- if (y_dot %in% nm) y_dot else if (y_ul %in% nm) y_ul else NA_character_
    
    has_x <- !is.na(x_nm) && x_nm %in% nm
    has_y <- !is.na(y_nm) && y_nm %in% nm
    if (!has_x && !has_y) next
    
    if (has_x && has_y) {
      vx <- df[[x_nm]]; vy <- df[[y_nm]]
      win <- if (prefer == "x") {
        vx[is_blank(vx)] <- vy[is_blank(vx)]
        vx
      } else {
        vy[is_blank(vy)] <- vx[is_blank(vy)]
        vy
      }
    } else {
      win <- if (has_x) df[[x_nm]] else df[[y_nm]]
    }
    df[[b]] <- win
    
    drop <- intersect(c(x_nm, y_nm), names(df))
    df[drop] <- NULL
    nm <- names(df) # refresh
  }
  df
}

#' Write a quick report of columns still ending with .x/.y or _x/_y
#' @param df data.frame
#' @param label short label for output filename
#' @param out_dir directory to write the report into
write_suffix_report <- function(df, label, out_dir = "data/checks/suffixes") {
  nm <- names(df)
  bad <- nm[grepl("(\\.|_)[xy]$", nm)]
  ensure_dir(out_dir)
  utils::write.csv(
    data.frame(column = bad, stringsAsFactors = FALSE),
    file = file.path(out_dir, paste0(label, "_suffix_pairs.csv")),
    row.names = FALSE, na = ""
  )
}

# =============================================================================
# End of 00_bootstrap.R
# =============================================================================