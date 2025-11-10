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

add_coalesced_column <- function(data,
                                 regex  = "ps[0-9]{2}_",
                                 keyword,
                                 out    = paste0("coalesced_", keyword),
                                 treat_zero_as_missing = TRUE) {
  pat  <- paste0(regex, ".*", keyword, "$")
  cols <- grep(pat, names(data), value = TRUE, perl = TRUE)
  
  if (!length(cols)) {
    data[[out]] <- NA
    return(data)
  }
  
  vals <- data[cols]
  
  # prep only for the calculation; originals in `data` untouched
  prep <- lapply(vals, function(x) {
    y <- x
    if (is.data.frame(y)) y <- if (ncol(y) == 1L) y[[1]] else y[[1]]
    if (inherits(y, "haven_labelled") && requireNamespace("haven", quietly = TRUE)) y <- haven::zap_labels(y)
    if (is.factor(y)) y <- as.character(y)
    if (isTRUE(treat_zero_as_missing)) {
      if (is.numeric(y))        y[y == 0] <- NA_real_
      else if (is.character(y)) y[y %in% c("0", "")] <- NA_character_
    }
    y
  })
  
  data[[out]] <- dplyr::coalesce(!!!prep)
  data
}

# Safe CSV writer (readr if present; falls back to utils)
# - Flattens embedded newlines in character fields so one record == one CSV row
# - Uses CRLF line endings and double-quote escaping (Stata-friendly)
# - Leaves NA as "" (empty)
write_csv_safe <- function(df, path, na = "", keep_breaks = FALSE) {
  stopifnot(is.data.frame(df))
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  # sanitize: remove/encode hard returns and normalize to UTF-8
  repl <- if (isTRUE(keep_breaks)) " \\n " else " "
  df2  <- df
  for (nm in names(df2)) {
    if (is.character(df2[[nm]])) {
      # replace any embedded CR/LF with a placeholder (or space)
      df2[[nm]] <- gsub("\r\n|\n|\r", repl, df2[[nm]], perl = TRUE)
      # normalize encoding to UTF-8
      df2[[nm]] <- enc2utf8(df2[[nm]])
    }
  }
  
  if (requireNamespace("readr", quietly = TRUE)) {
    readr::write_csv(
      df2, path,
      na = na,
      quote_escape = "double",  # escape internal quotes as ""
      eol = "\r\n"              # Windows-style line endings
    )
  } else {
    # base R path: write.table lets us control eol + quote method
    utils::write.table(
      df2, file = path,
      sep = ",", row.names = FALSE, col.names = TRUE,
      na = na, qmethod = "double", fileEncoding = "UTF-8",
      eol = "\r\n"
    )
  }
}

# ---- CSV text sanitizer ------------------------------------------------------
sanitize_for_csv <- function(df, keep_breaks = FALSE) {
  stopifnot(is.data.frame(df))
  repl <- if (isTRUE(keep_breaks)) " \\n " else " "
  df |>
    dplyr::mutate(across(
      where(is.character),
      ~ .x |>
        # escape any internal double-quotes for CSV
        stringr::str_replace_all('"', '""') |>
        # flatten embedded newlines so CSV stays one row per record
        stringr::str_replace_all("\\r\\n|\\n|\\r", repl)
    ))
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

# ---- site_id backfill -> H / K only -----------------------------------------
derive_site_id <- function(df) {
  stopifnot(is.data.frame(df))
  has <- function(nm) nm %in% names(df)
  
  # Normalize any hint to a single-letter site code
  norm_code <- function(x) {
    x <- tolower(trimws(as.character(x)))
    dplyr::case_when(
      is.na(x) | x == ""                         ~ NA_character_,
      x %in% c("h", "harlem", "harlem hospital",
               "harlem hosp")                    ~ "H",
      x %in% c("k", "kings", "kings county")     ~ "K",
      grepl("^h-", x)                            ~ "H",
      grepl("^k-", x)                            ~ "K",
      TRUE                                       ~ NA_character_
    )
  }
  
  # Safely pull candidate sources, converting blanks ("") to NA
  as_chr_or_na <- function(v) dplyr::na_if(as.character(v), "")
  
  df |>
    dplyr::mutate(
      site_id = {
        # Existing site_id
        s_existing <- if (has("site_id")) as_chr_or_na(site_id) else NA_character_
        
        # REDCap DAG
        s_dag <- if (has("redcap_data_access_group"))
          as_chr_or_na(redcap_data_access_group) else NA_character_
        
        # Site flags that might be character or logical
        s_harlem <- if (has("site_harlem")) {
          if (is.logical(site_harlem)) ifelse(site_harlem, "H", NA_character_) else as_chr_or_na(site_harlem)
        } else NA_character_
        
        s_kings <- if (has("site_kings")) {
          if (is.logical(site_kings)) ifelse(site_kings, "K", NA_character_) else as_chr_or_na(site_kings)
        } else NA_character_
        
        # ID-based inference (prefixes H-/K-)
        s_ppid   <- if (has("p_participant_id"))  as_chr_or_na(p_participant_id) else NA_character_
        s_wecare_y <- if (has("wecare_id_y"))           as_chr_or_na(wecare_id_y)         else NA_character_
        s_wecare_cg <- if (has("wecare_id_cg"))         as_chr_or_na(wecare_id_cg)         else NA_character_
        
        # First coalesce to one raw hint, then normalize strictly to H/K
        raw_hint <- dplyr::coalesce(s_existing, s_dag, s_harlem, s_kings,
                                    s_ppid, s_wecare_y, s_wecare_cg)
        norm_code(raw_hint)
      }
    )
}

# ---- family_id backfill (from p_participant_id) ------------------------------
# Extracts trailing digits and converts to integer (drops leading zeros)
derive_family_id <- function(df, prefer_existing = TRUE, fallback_from_other_ids = TRUE) {
  stopifnot(is.data.frame(df))
  
  get_family_num <- function(x) {
    if (is.null(x)) return(rep(NA_integer_, length = nrow(df)))
    digs <- stringr::str_extract(as.character(x), "(\\d+)\\s*$")
    suppressWarnings(as.integer(digs))  # drops leading zeros by design
  }
  
  has_col <- function(nm) nm %in% names(df)
  
  fam_from_ppid  <- if (has_col("p_participant_id")) get_family_num(df$p_participant_id) else rep(NA_integer_, nrow(df))
  fam_from_wecare<- if (fallback_from_other_ids && has_col("wecare_id_y")) get_family_num(df$wecare_id_y) else rep(NA_integer_, nrow(df))
  fam_from_record<- if (fallback_from_other_ids && has_col("wecare_id_cg")) get_family_num(df$record_id_cg) else rep(NA_integer_, nrow(df))
  
  family_id_calc <- dplyr::coalesce(fam_from_ppid, fam_from_wecare, fam_from_record)
  
  # --- FIX: harmonize types before coalescing
  if (prefer_existing && has_col("family_id")) {
    # Safely coerce existing to integer (keeps NA where non-numeric/blank)
    existing_family_int <- suppressWarnings(as.integer(df$family_id))
    df$family_id <- dplyr::coalesce(existing_family_int, family_id_calc)
  } else {
    df$family_id <- family_id_calc
  }
  
  df
}

# Row-level "any data?" excluding id/event/label columns
row_any_data <- function(df, id_col) {
  drop_cols <- intersect(c(id_col, "redcap_event_name", "project_label"), names(df))
  data_cols <- setdiff(names(df), drop_cols)
  if (!length(data_cols)) return(rep(FALSE, nrow(df)))
  apply(df[, data_cols, drop = FALSE], 1, function(row) any(row != "" & !is.na(row)))
}

# Helper for 0/1 reading
to01 <- function(x) {
  ch <- trimws(tolower(as.character(x)))
  out <- dplyr::case_when(
    ch %in% c("1","yes","y","true","t") ~ 1L,
    ch %in% c("0","no","n","false","f") ~ 0L,
    TRUE ~ NA_integer_
  )
  if (all(is.na(out))) {
    suppressWarnings(out <- as.integer(as.character(x)))
    out[!out %in% c(0L,1L)] <- NA_integer_
  }
  out
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

# ---- Helpers to detect binary conflicts across duplicate columns ----

# Normalize mixed encodings to 0/1/NA (character "yes"/"no", TRUE/FALSE, 1/0, etc.)
to_bin01 <- function(x) {
  if (is.logical(x)) return(ifelse(x, 1, 0))
  if (is.numeric(x)) return(ifelse(is.na(x), NA_real_, ifelse(x == 1, 1, ifelse(x == 0, 0, NA_real_))))
  if (is.character(x)) {
    xc <- stringr::str_trim(stringr::str_to_lower(x))
    xc <- dplyr::na_if(xc, "")
    return(dplyr::case_when(
      xc %in% c("1", "y", "yes", "true", "t")  ~ 1,
      xc %in% c("0", "n", "no", "false", "f") ~ 0,
      TRUE ~ NA_real_
    ))
  }
  # anything else (factor/POSIX/etc.)
  xch <- as.character(x)
  return(to_bin01(xch))
}

# Find columns that match a prescreen stem and the "hear_more" keyword.
# Example: stem_regex = "ps[0-9]{2}y_" (youth) or "ps[0-9]{2}_" (caregiver)
matching_hear_more_cols <- function(df, stem_regex) {
  patt <- paste0("^(harlem_|kings_)?", stem_regex, ".*hear_more$")
  names(df)[grepl(patt, names(df))]
}

# Given a data.frame and a set of "hear_more" source columns, return a logical vector
# TRUE when the row has at least one 0 and at least one 1 across those columns (ignoring NA).
flag_binary_conflict <- function(df, cols) {
  if (length(cols) == 0) return(rep(FALSE, nrow(df)))
  mm <- lapply(df[cols], to_bin01)
  mm <- as.data.frame(mm, stringsAsFactors = FALSE)
  if (ncol(mm) == 0) return(rep(FALSE, nrow(df)))
  has0 <- apply(mm, 1, function(v) any(v == 0, na.rm = TRUE))
  has1 <- apply(mm, 1, function(v) any(v == 1, na.rm = TRUE))
  has0 & has1
}

# Convenience to pick a participant id column for reports
pick_participant_id <- function(df) {
  cand <- c("p_participant_id","wecare_id_y","wecare_id_cg","participant_id","youth_id","caregiver_id")
  hit <- cand[cand %in% names(df)]
  if (length(hit)) hit[[1]] else NULL
}