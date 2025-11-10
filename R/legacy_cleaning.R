# R/legacy_cleaning.R
# Minimal, robust baseline cleaners for WeCare
# - One row per ID (participant/caregiver)
# - All columns kept as character; specific transforms can be added later
# - Coalescing preserves 0 ("No") for hear_more
# - Conflict flags + QC exports for hear_more sources

# ---------- tiny utilities (base R only; tidyverse via namespaced calls) ----------

# Given a data.frame and an ID column name, return one row per ID.
# If duplicates exist, keep the row with the greatest count of non-empty cells.
lc_pick_most_complete_by <- function(df, id_col) {
  if (!id_col %in% names(df)) stop(sprintf("ID column '%s' not found", id_col), call. = FALSE)
  cols <- setdiff(names(df), id_col)
  if (!length(cols)) return(df[!duplicated(df[[id_col]]), , drop = FALSE])
  
  # Make a "non-empty" matrix (TRUE where value is non-blank)
  mat <- df[, cols, drop = FALSE]
  for (j in seq_along(mat)) {
    v <- mat[[j]]
    mat[[j]] <- !(is.na(v) | v == "")
  }
  completeness <- rowSums(as.data.frame(mat), na.rm = TRUE)
  
  # Order by completeness (desc) and then keep the first row per ID
  ord <- order(-completeness, seq_len(nrow(df)))
  dfo <- df[ord, , drop = FALSE]
  keep <- !duplicated(dfo[[id_col]])
  dfo[keep, , drop = FALSE]
}

# Normalize to baseline rows if the project is longitudinal.
lc_prepare_baseline <- function(df, id_col) {
  # ensure simple character columns
  for (j in seq_along(df)) if (!is.character(df[[j]])) df[[j]] <- as.character(df[[j]])
  
  # If an events column exists, keep just baseline rows (when present)
  if ("redcap_event_name" %in% names(df)) {
    is_base <- !is.na(df$redcap_event_name) & df$redcap_event_name == "baseline_visit_arm_1"
    if (any(is_base)) df <- df[is_base, , drop = FALSE]
  }
  
  # Drop rows with missing/blank IDs
  if (id_col %in% names(df)) {
    df <- df[!is.na(df[[id_col]]) & df[[id_col]] != "", , drop = FALSE]
  }
  df
}

# ---------- conflict helpers for binary 0/1 fields (used for hear_more) ----------

to_bin01 <- function(x) {
  if (is.logical(x)) return(ifelse(x, 1, 0))
  if (is.numeric(x)) return(ifelse(is.na(x), NA_real_, ifelse(x == 1, 1, ifelse(x == 0, 0, NA_real_))))
  # characters, factors, others → coerce then map
  xc <- as.character(x)
  xc <- stringr::str_trim(stringr::str_to_lower(xc))
  xc[xc == ""] <- NA_character_
  dplyr::case_when(
    xc %in% c("1", "y", "yes", "true", "t")  ~ 1,
    xc %in% c("0", "n", "no", "false", "f") ~ 0,
    TRUE ~ NA_real_
  )
}

# Find columns that match a prescreen stem and the "hear_more" keyword,
# allowing optional language suffixes like _fre, _spa, _eng.
# Example stems: "ps[0-9]{2}y_" (youth) or "ps[0-9]{2}_" (caregiver)
matching_hear_more_cols <- function(df, stem_regex) {
  patt <- paste0("^(harlem_|kings_)?", stem_regex, ".*hear_more(?:_[A-Za-z]{2,3})?$")
  names(df)[grepl(patt, names(df))]
}

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

# ---------- QC writers (named functions for use on RHS of base pipe) ----------

write_youth_hearmore_conflicts <- function(tmp) {
  src_cols <- matching_hear_more_cols(tmp, "ps[0-9]{2}y_")
  has_conf <- "ps_hear_more_conflict" %in% names(tmp) &&
    any(tmp$ps_hear_more_conflict, na.rm = TRUE)
  if (length(src_cols) > 0 && has_conf) {
    idcol <- pick_participant_id(tmp)
    out <- tmp |>
      dplyr::filter(ps_hear_more_conflict) |>
      dplyr::select(dplyr::all_of(c(idcol)), ps_hear_more, ps_hear_more_conflict,
                    dplyr::all_of(src_cols))
    dir.create(file.path("data","checks"), showWarnings = FALSE, recursive = TRUE)
    readr::write_csv(out, file.path("data","checks","hear_more_conflicts_youth.csv"))
  }
  tmp
}

write_caregiver_hearmore_conflicts <- function(tmp) {
  src_cols <- matching_hear_more_cols(tmp, "ps[0-9]{2}_")
  has_conf <- "p_ps_hear_more_conflict" %in% names(tmp) &&
    any(tmp$p_ps_hear_more_conflict, na.rm = TRUE)
  if (length(src_cols) > 0 && has_conf) {
    idcol <- pick_participant_id(tmp)
    out <- tmp |>
      dplyr::filter(p_ps_hear_more_conflict) |>
      dplyr::select(dplyr::all_of(c(idcol)), p_ps_hear_more, p_ps_hear_more_conflict,
                    dplyr::all_of(src_cols))
    dir.create(file.path("data","checks"), showWarnings = FALSE, recursive = TRUE)
    readr::write_csv(out, file.path("data","checks","hear_more_conflicts_caregiver.csv"))
  }
  tmp
}

# ---------- public cleaners the pipeline calls ----------

clean_youth <- function(dat_youth_raw) {
  
  dat_youth_cleaned <- dat_youth_raw |>
    janitor::clean_names() |>
    dplyr::filter(!is.na(site_id)) |>
    dplyr::ungroup()
  
  # coalesce pre-ICF variables
  dat_youth_cleaned <- dat_youth_cleaned |>
    dplyr::mutate(
      # Preserve 0 ("No") during coalescing
      ps_hear_more          = coalesce_columns(dplyr::cur_data_all(), "ps[0-9]{2}y_", "hear_more(?:_[A-Za-z]{2,3})?",
                                      treat_zero_as_missing = FALSE),
      ps_willing_to_contact = coalesce_columns(dplyr::cur_data_all(), "ps[0-9]{2}y_", "willing_to_contact"),
      ps_decline_reason     = coalesce_columns(dplyr::cur_data_all(), "ps[0-9]{2}y_", "decline_reason"),
      ps_youth_name         = coalesce_columns(dplyr::cur_data_all(), "ps[0-9]{2}y_", "youth_name"),
      ps_signature          = coalesce_columns(dplyr::cur_data_all(), "ps[0-9]{2}y_", "signature"),
      ps_date               = coalesce_columns(dplyr::cur_data_all(), "ps[0-9]{2}y_", "date"),
      # Conflict flag computed while source columns still exist
      ps_hear_more_conflict = {
        src_cols <- matching_hear_more_cols(dplyr::cur_data_all(), "ps[0-9]{2}y_")
        flag_binary_conflict(dplyr::cur_data_all(), src_cols)
      },
      .after = initial_questions_complete
    ) |>
    # Write youth conflicts (before source columns are dropped)
    write_youth_hearmore_conflicts() |>
    # remove the original un-coalesced variables
    dplyr::select(-dplyr::matches("^(harlem|kings)_ps\\d{2}y.*")) |>
    # coalesce prescreening contact form completion indicators
    dplyr::mutate(
      contact_form_youth_complete =
        coalesce_columns(dplyr::cur_data_all(), ".*prescreening_contact_form.*", "complete"),
      .before = first_name
    ) |>
    # remove the original un-coalesced variables
    dplyr::select(-dplyr::matches("prescreening_contact_form"))
  
  # coalesce ICF variables
  dat_youth_cleaned <- dat_youth_cleaned |>
    dplyr::mutate(
      icf_name_1          = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}y_", "name_1"),
      icf_name_2          = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}y_", "name_2"),
      icf_nih_share       = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}y_", "nih_share"),
      icf_name_3          = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}y_", "name_3"),
      icf_nih_first_name  = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}y_", "first_name"),
      icf_nih_middle_name = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}y_", "middle_name"),
      icf_nih_last_name   = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}y_", "last_name"),
      icf_nih_dob         = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}y_", "dob"),
      icf_nih_sex         = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}y_", "sex"),
      icf_nih_city        = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}y_", "city"),
      .after = eligibility_survey_complete
    ) |>
    dplyr::select(-dplyr::matches("^(harlem|kings)_icf\\d{2}y.*")) |>
    dplyr::mutate(
      informed_consent_form_youth_complete =
        coalesce_columns(dplyr::cur_data_all(), ".*subject_information_and_informed.*", "complete"),
      .before = sw_pause_id
    ) |>
    dplyr::select(-dplyr::matches("subject_information_and_informed")) |>
    tibble::as_tibble()
  
  dat_youth_cleaned
}

# Function to clean caregiver data (drop-in)
clean_caregiver <- function(dat_caregiver_raw){
  
  # auto-clean variable names and ungroup to avoid grouped mutate issues
  dat_caregiver_cleaned <- dat_caregiver_raw |>
    janitor::clean_names() |>
    dplyr::ungroup()
  
  # --- coalesce pre-ICF variables ---
  dat_caregiver_cleaned <- dat_caregiver_cleaned |>
    # coalesce the form language variables across sites
    dplyr::mutate(
      p_screen_language = dplyr::coalesce(p_screen_language_harlem, p_screen_language_kings),
      .after = p_screen_language_kings
    ) |>
    # remove the original un-coalesced variables
    dplyr::select(-dplyr::any_of(c("p_screen_language_harlem", "p_screen_language_kings"))) |>
    
    # prescreen contact form fields
    dplyr::mutate(
      # Preserve 0 ("No") during coalescing
      p_ps_hear_more          = coalesce_columns(dplyr::cur_data_all(), "ps[0-9]{2}_", "hear_more(?:_[A-Za-z]{2,3})?",
                                        treat_zero_as_missing = FALSE),
      p_ps_willing_to_contact = coalesce_columns(dplyr::cur_data_all(), "ps[0-9]{2}_", "willing_to_contact"),
      p_ps_decline_reason     = coalesce_columns(dplyr::cur_data_all(), "ps[0-9]{2}_", "decline_reason"),
      p_ps_caregiver_name     = coalesce_columns(dplyr::cur_data_all(), "ps[0-9]{2}_", "caregiver_name"),
      p_ps_youth_name         = coalesce_columns(dplyr::cur_data_all(), "ps[0-9]{2}_", "youth_name"),
      p_ps_signature          = coalesce_columns(dplyr::cur_data_all(), "ps[0-9]{2}_", "signature"),
      p_ps_date               = coalesce_columns(dplyr::cur_data_all(), "ps[0-9]{2}_", "date"),
      # Conflict flag computed while source columns still exist
      p_ps_hear_more_conflict = {
        src_cols <- matching_hear_more_cols(dplyr::cur_data_all(), "ps[0-9]{2}_")
        flag_binary_conflict(dplyr::cur_data_all(), src_cols)
      },
      .after = initial_questions_complete
    ) |>
    # Write caregiver conflicts (before source columns are dropped)
    write_caregiver_hearmore_conflicts() |>
    # remove originals
    dplyr::select(-dplyr::matches("^(harlem|kings)_ps\\d{2}.*")) |>
    
    # prescreening contact form completion indicator
    dplyr::mutate(
      contact_form_parent_complete =
        coalesce_columns(dplyr::cur_data_all(), ".*prescreening_contact_form.*", "complete"),
      .before = p_first_name
    ) |>
    dplyr::select(-dplyr::matches("prescreening_contact_form"))
  
  # --- rename ICF18 participant name fields so suffixes align ---
  dat_caregiver_cleaned <- dat_caregiver_cleaned |>
    dplyr::rename(
      harlem_icf18_name_5     = harlem_icf18_name_3,
      harlem_icf18_name_4     = harlem_icf18_name_2,
      kings_icf18_name_5      = kings_icf18_name_3,
      kings_icf18_name_4      = kings_icf18_name_2,
      
      harlem_icf18_name_5_spa = harlem_icf18_name_3_spa,
      harlem_icf18_name_4_spa = harlem_icf18_name_2_spa,
      kings_icf18_name_5_spa  = kings_icf18_name_3_spa,
      kings_icf18_name_4_spa  = kings_icf18_name_2_spa,
      
      harlem_icf18_name_5_fre = harlem_icf18_name_3_fre,
      harlem_icf18_name_4_fre = harlem_icf18_name_2_fre,
      kings_icf18_name_5_hai  = kings_icf18_name_3_hai,
      kings_icf18_name_4_hai  = kings_icf18_name_2_hai
    )
  
  # --- coalesce ICF variables ---
  dat_caregiver_cleaned <- dat_caregiver_cleaned |>
    dplyr::mutate(
      p_icf_name_1          = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}_", "name_1"),
      p_icf_name_2          = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}_", "name_2"),
      p_icf_name_3          = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}_", "name_3"),
      p_icf_name_4          = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}_", "name_4"),
      p_icf_nih_share       = coalesce_columns(dplyr::cur_data_all(),
                                               "icf(0[1-9]|1[0-7]|[2-9][0-9])_",  # excludes 18
                                               "nih_share"),
      p_icf_name_5          = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}_", "name_5"),
      p_icf_nih_first_name  = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}_", "first_name"),
      p_icf_nih_middle_name = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}_", "middle_name"),
      p_icf_nih_last_name   = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}_", "last_name"),
      p_icf_nih_dob         = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}_", "dob"),
      p_icf_nih_sex         = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}_", "sex"),
      p_icf_nih_city        = coalesce_columns(dplyr::cur_data_all(), "icf[0-9]{2}_", "city"),
      .after = eligibility_screen_complete
    ) |>
    # remove originals
    dplyr::select(-dplyr::matches("^(harlem|kings)_icf\\d{2}.*")) |>
    # ICF completion indicator
    dplyr::mutate(
      informed_consent_form_parent_complete =
        coalesce_columns(dplyr::cur_data_all(), ".*subject_information_and_informed.*", "complete"),
      .before = p_sw_pause_id
    ) |>
    dplyr::select(-dplyr::matches("subject_information_and_informed")) |>
    
    # rename completion indicators with p_ prefix
    dplyr::rename_with(~ paste0("p_", .x), dplyr::ends_with("_complete") | dplyr::starts_with("gf_")) |>
    dplyr::rename(
      p_wecare_id = wecare_id,
      p_over_18   = over_18,
      p_over_12   = over_12
    ) |>
    tibble::as_tibble()
  
  dat_caregiver_cleaned
}