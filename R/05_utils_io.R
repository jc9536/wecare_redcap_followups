# =============================================================================
# WeCare ETL • 05_utils_io.R
# -----------------------------------------------------------------------------
# Purpose
#   Pipeline-wide utilities shared across modules:
#   - CSV writer
#   - lenient completion checker
#   - relaxed date parser
#   - row "any data?" helper
#   - small infix + aliases for local/private helpers used elsewhere
# =============================================================================

# Infix "or" (first non-NULL)
`%||%` <- function(x, y) if (!is.null(x)) x else y

# Returns a single vector of length nrow(data).
# Matches columns by paste0("^", regex, ".*", keyword, "$").
# Flattens df/list columns, unlabels, normalizes, and coalesces.
coalesce_columns <- function(data,
                             regex = "ps[0-9]{2}_",  # use "ps[0-9]{2}y_" for youth
                             keyword,
                             treat_zero_as_missing = TRUE) {
  # Allow site prefixes like "harlem_" / "kings_" before the pattern
  # e.g., match "harlem_ps12_hear_more", "kings_ps08y_hear_more"
  pat  <- paste0(regex, ".*", keyword, "$")     # <-- no ^ anchor at start
  cols <- grep(pat, names(data), value = TRUE, perl = TRUE)
  
  n <- nrow(data)
  if (length(cols) == 0L) return(rep(NA_character_, n))
  
  vals <- data[, cols, drop = FALSE]
  
  vals[] <- lapply(vals, function(x) {
    # unwrap df-columns if any slipped in
    if (is.data.frame(x)) x <- if (ncol(x) == 1L) x[[1]] else x[[1]]
    if (inherits(x, "haven_labelled")) {
      if (requireNamespace("haven", quietly = TRUE)) x <- haven::zap_labels(x) else x <- as.vector(x)
    }
    if (is.factor(x)) x <- as.character(x)
    
    if (isTRUE(treat_zero_as_missing)) {
      if (is.numeric(x))       x[x == 0] <- NA_real_
      else if (is.character(x)) x[x %in% c("0", "")] <- NA_character_
    }
    x
  })
  
  dplyr::coalesce(!!!as.list(vals))
}

# Safe CSV writer (readr if present; falls back to utils)
write_csv_safe <- function(df, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  if (requireNamespace("readr", quietly = TRUE)) {
    readr::write_csv(df, path, na = "")
  } else {
    utils::write.csv(df, path, row.names = FALSE, na = "")
  }
}

# Completion checker (treats common encodings as "complete")
is_complete2 <- function(x) {
  x <- tolower(trimws(as.character(x)))
  x %in% c("2", "complete", "completed")
}

# Relaxed date parser: YYYY-MM-DD; MM/DD/YYYY; timestamps → Date
parse_date_relaxed <- function(x) {
  x <- trimws(as.character(x)); x[x == ""] <- NA_character_
  d <- suppressWarnings(as.Date(x))                              # 2001-02-03
  miss <- is.na(d)
  if (any(miss)) d[miss] <- suppressWarnings(as.Date(x[miss], format = "%m/%d/%Y"))
  miss <- is.na(d)
  if (any(miss)) {
    p <- suppressWarnings(as.POSIXct(x[miss], tz = "UTC"))       # 2001-02-03 04:05:06
    d[miss] <- as.Date(p)
  }
  d
}

# Row-level "any data?" excluding id/event/label columns
row_any_data <- function(df, id_col) {
  drop_cols <- intersect(c(id_col, "redcap_event_name", "project_label"), names(df))
  data_cols <- setdiff(names(df), drop_cols)
  if (!length(data_cols)) return(rep(FALSE, nrow(df)))
  apply(df[, data_cols, drop = FALSE], 1, function(row) any(row != "" & !is.na(row)))
}

# --- Private aliases used by some modules (so they no longer need local copies)
# Dot-prefixed versions keep old call-sites working without re-declaring them.

# alias for parse_date used in 30_followups
.parse_date <- function(x) parse_date_relaxed(x)

# alias for completion used in 30/40
.is_complete2 <- function(x) as.integer(is_complete2(x))

# alias for csv writer used in 30/35/55/58
.write_csv_safe <- function(df, path) write_csv_safe(df, path)

# alias for row-any-data used in 40
.row_any_data <- function(df, id_col) row_any_data(df, id_col)

# soft alias for blank checker (actual implementation is in 00_bootstrap.R)
if (!exists(".is_blank", mode = "function") && exists("is_blank", mode = "function")) {
  .is_blank <- function(x) is_blank(x)
}