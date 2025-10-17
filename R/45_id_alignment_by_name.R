# R/45_id_alignment_by_name.R
# Read-only ID alignment checks:
# - Build youth↔caregiver crosswalk via normalized names
# - DO NOT mutate any IDs
# - Emit audits + flags for mismatches using date logic
# - Include caregiver decline reason in outputs and messages
#
# Outputs (to data/checks/):
#   - id_namekey_confident.csv
#   - id_namekey_ambiguous.csv
#   - id_namekey_all_matches.csv       (now includes cg_decline_reason)
#   - id_namekey_flags.csv             (now includes cg_decline_reason)

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(tidyr)
  library(readr)
  library(rlang)
})

`%||%` <- function(x, y) if (is.null(x)) y else x

# -------- Helpers --------
normalize_name <- function(x) {
  if (!requireNamespace("stringi", quietly = TRUE)) {
    stop("Please install.packages('stringi') for name normalization (stringi).")
  }
  x |>
    str_trim() |>
    str_to_lower() |>
    stringi::stri_trans_general("Latin-ASCII") |>
    str_replace_all("[^a-z]", "") |>
    na_if("")
}

mk_youth_key <- function(first, last) {
  f <- normalize_name(first)
  l <- normalize_name(last)
  out <- paste0(coalesce(f, ""), coalesce(l, ""))
  ifelse(out == "", NA_character_, out)
}
mk_cg_key <- function(full) normalize_name(full)

# parse a variety of date/datetime inputs and return Date (drop time)
to_date_safe <- function(x) {
  if (inherits(x, "Date"))   return(x)
  if (inherits(x, "POSIXt")) return(as.Date(x))
  if (is.numeric(x))         return(as.Date(x, origin = "1899-12-30")) # Excel serials
  
  x_chr <- as.character(x)
  dt <- suppressWarnings(lubridate::ymd(x_chr, quiet = TRUE))
  miss <- is.na(dt)
  if (any(miss)) {
    dt2 <- suppressWarnings(lubridate::ymd_hm(x_chr[miss], quiet = TRUE))
    dt[miss] <- dt2
    miss <- is.na(dt)
  }
  if (any(miss)) {
    dt3 <- suppressWarnings(lubridate::ymd_hms(x_chr[miss], quiet = TRUE))
    dt[miss] <- dt3
  }
  as.Date(dt)
}

# pick the first available column name from candidates
pick_col <- function(df, candidates, label) {
  hit <- candidates[candidates %in% names(df)]
  if (length(hit) == 0) {
    stop(sprintf("Could not find a column for '%s'. Tried: %s",
                 label, paste(candidates, collapse = ", ")), call. = FALSE)
  }
  hit[[1]]
}

# -------- Main builder (read-only) --------
build_id_crosswalk_by_name <- function(
    youth_df,
    caregiver_df,
    out_dir = "data/checks",
    # Optional explicit names; otherwise auto-detect
    youth_pid_col = NULL,             # e.g., "p_participant_id"
    cg_pid_col    = NULL,             # e.g., "p_participant_id"
    youth_first   = NULL,
    youth_last    = NULL,
    cg_youthname  = NULL,
    youth_date    = NULL,             # e.g., "date_baseline"
    cg_date       = NULL,             # e.g., "p_date_baseline"
    cg_decline    = NULL              # e.g., "p_ps_decline_reason"
) {
  # ---- Resolve columns ----
  y_pid <- youth_pid_col %||% pick_col(
    youth_df, c("p_participant_id","wecare_id_y","wecare_id","youth_id","y_id"),
    "youth participant id"
  )
  y_fn  <- youth_first %||% pick_col(
    youth_df, c("first_name","y_first_name","p_youth_firstname","p_youth_first_name",
                "firstname","given_name"),
    "youth first name"
  )
  y_ln  <- youth_last %||% pick_col(
    youth_df, c("last_name","y_last_name","p_youth_lastname","p_youth_last_name",
                "lastname","family_name","surname"),
    "youth last name"
  )
  y_dt  <- youth_date %||% pick_col(
    youth_df, c("date_baseline","baseline_date","p_date_baseline_y","date_enroll"),
    "youth baseline date"
  )
  
  c_pid <- cg_pid_col %||% pick_col(
    caregiver_df, c("p_participant_id","wecare_id_cg","caregiver_id","cg_id"),
    "caregiver participant id"
  )
  c_nm  <- cg_youthname %||% pick_col(
    caregiver_df, c("p_ps_youth_name","ps_youth_name","youth_name","child_name","student_name"),
    "caregiver's youth name field"
  )
  c_dt  <- cg_date %||% pick_col(
    caregiver_df, c("p_date_baseline","caregiver_baseline_date","date_baseline_cg"),
    "caregiver baseline date"
  )
  c_dr  <- cg_decline %||% pick_col(
    caregiver_df, c("p_ps_decline_reason","ps_decline_reason","decline_reason"),
    "caregiver decline reason"
  )
  
  # ---- Build normalized keyframes (include dates + decline reason) ----
  y_keys <- youth_df |>
    transmute(
      name_key = mk_youth_key(.data[[y_fn]], .data[[y_ln]]),
      y_pid    = .data[[y_pid]],
      y_date   = to_date_safe(.data[[y_dt]])
    ) |>
    filter(!is.na(name_key)) |>
    distinct()
  
  c_keys <- caregiver_df |>
    transmute(
      name_key          = mk_cg_key(.data[[c_nm]]),
      cg_pid            = .data[[c_pid]],
      cg_date           = to_date_safe(.data[[c_dt]]),
      cg_decline_reason = .data[[c_dr]]                 # raw text carried through
    ) |>
    filter(!is.na(name_key)) |>
    distinct()
  
  # ---- Join & summarize ----
  matches <- y_keys |>
    inner_join(c_keys, by = "name_key", multiple = "all") |>
    relocate(name_key, .before = 1) |>
    mutate(ids_equal = y_pid == cg_pid)
  
  key_card <- matches |>
    summarize(
      n_y = n_distinct(y_pid),
      n_c = n_distinct(cg_pid),
      cg_date_variety = n_distinct(cg_date, na.rm = TRUE),
      .by = name_key
    )
  
  matches2 <- matches |>
    inner_join(key_card, by = "name_key")
  
  # ---- Confidence buckets (unchanged) ----
  confident <- matches2 |>
    filter(n_y == 1, n_c == 1) |>
    mutate(confidence = dplyr::case_when(
      ids_equal ~ "already_aligned",
      TRUE      ~ "name_key_one_to_one_mismatch"
    )) |>
    distinct()
  
  ambiguous <- matches2 |>
    filter(n_y > 1 | n_c > 1) |>
    arrange(name_key)
  
  # Build a short decline note to append to messages
  decline_note <- function(reason) {
    ifelse(!is.na(reason) & reason != "",
           paste0(" [Decline noted: ", reason, "]"),
           "")
  }
  
  # ---- Flag logic for ids_equal == FALSE (with decline reason context) ----
  flags <- matches2 |>
    filter(!ids_equal) |>
    mutate(
      base_msg = dplyr::case_when(
        # Case A: caregiver side has two distinct IDs for this name
        n_c == 2 & cg_date_variety >= 2 ~
          "Likely fine to keep, participant was approached twice",
        n_c == 2 & cg_date_variety == 1 ~
          if_else(!is.na(cg_date) & !is.na(y_date) & cg_date == y_date,
                  "caregiver ID date matches youth ID date, please check and edit in REDCAP",
                  "Likely fine to keep, participant was approached twice"),
        
        # Case B: caregiver side has exactly one ID for this name
        n_c == 1 & !is.na(y_date) & !is.na(cg_date) & y_date != cg_date ~
          "Likely fine to keep, participant was likely approached twice",
        n_c == 1 & !is.na(y_date) & !is.na(cg_date) & y_date == cg_date ~
          "caregiver ID date matches youth ID date, please check and edit in REDCAP",
        
        # Fallbacks when dates missing
        n_c == 1 & (is.na(y_date) | is.na(cg_date)) ~
          "Dates missing to evaluate; please verify in REDCap",
        n_c == 2 & cg_date_variety == 0 ~
          "Caregiver dates missing to evaluate; please verify in REDCap",
        
        TRUE ~ NA_character_
      ),
      flag_message = paste0(base_msg, decline_note(cg_decline_reason))
    ) |>
    select(name_key, y_pid, y_date, cg_pid, cg_date,
           n_y, n_c, cg_date_variety, ids_equal,
           cg_decline_reason, flag_message) |>
    distinct()
  
  # ---- Write outputs ----
  dir.create(out_dir, showWarnings = FALSE, recursive = TRUE)
  suppressWarnings(write_csv(confident, file.path(out_dir, "id_namekey_confident.csv")))
  suppressWarnings(write_csv(ambiguous, file.path(out_dir, "id_namekey_ambiguous.csv")))
  # include cg_decline_reason in the comprehensive table for context
  suppressWarnings(write_csv(matches2,  file.path(out_dir, "id_namekey_all_matches.csv")))
  suppressWarnings(write_csv(flags,     file.path(out_dir, "id_namekey_flags.csv")))
  
  # return everything for interactive review
  list(
    confident   = confident,
    ambiguous   = ambiguous,
    all_matches = matches2,
    flags       = flags,
    .resolved = list(
      youth_pid_col = y_pid,
      cg_pid_col    = c_pid,
      youth_first   = y_fn,
      youth_last    = y_ln,
      cg_youthname  = c_nm,
      youth_date    = y_dt,
      cg_date       = c_dt,
      cg_decline    = c_dr
    )
  )
}