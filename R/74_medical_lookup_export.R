# =============================================================================
# WeCare ETL • 74_medical_lookup_export.R
# -----------------------------------------------------------------------------
# Purpose
#   Build:
#   1) a CSV of NEW youth participants to send for medical data pull
#   2) a master CSV of all youth participants ever included in a lookup
#
# Required output columns:
#   - Participant ID
#   - Medical Record Number:
#   - EMPI
#   - Date of birth
#   - Date of enrollment
#   - First Name
#   - Last Name
#   - What is your sex assigned at birth?
#
# Source mapping from dat_merged.csv:
#   Participant ID                     <= record_id
#   Medical Record Number:             <= mrn_b
#   EMPI                               <= empi_b
#   Date of birth                      <= screen_dob
#   Date of enrollment                 <= screen_doe
#   First Name                         <= first_name
#   Last Name                          <= last_name
#   What is your sex assigned at birth?<= screen_sex (1 = Male, 2 = Female)
#
# Rules
#   - Remove rows with any missing required field:
#       record_id, dob, doe, first_name, last_name, sex
#   - Include only rows where treatment is not missing / not blank
#   - Check "newness" using participant ID from the old lookup
#     (old file may use either participant_id or record_id once cleaned)
# =============================================================================

build_medical_lookup_exports <- function(
    dat_merged_path,
    old_lookup_path,
    new_out_path = file.path("data/out/MRNLookup", paste0("WeCareYouth-MRNLookup_NEW_", Sys.Date(), ".csv")),
    master_out_path = file.path("data/out/MRNLookup", paste0("WeCareYouth-MRNLookup_MASTER_", Sys.Date(), ".csv")),
    excluded_out_path = file.path("data/checks", paste0("medical_lookup_excluded_", Sys.Date(), ".csv")),
    backfill_mrn = TRUE
) {
  stopifnot(file.exists(dat_merged_path))
  stopifnot(file.exists(old_lookup_path))
  
  if (!requireNamespace("readr", quietly = TRUE)) stop("Package 'readr' is required.")
  if (!requireNamespace("dplyr", quietly = TRUE)) stop("Package 'dplyr' is required.")
  if (!requireNamespace("janitor", quietly = TRUE)) stop("Package 'janitor' is required.")
  if (!requireNamespace("stringr", quietly = TRUE)) stop("Package 'stringr' is required.")
  
  `%>%` <- dplyr::`%>%`
  
  required_export_cols <- c(
    "Participant ID",
    "Medical Record Number:",
    "EMPI",
    "Date of birth",
    "Date of enrollment",
    "First Name",
    "Last Name",
    "What is your sex assigned at birth?"
  )
  
  dir.create(dirname(new_out_path), recursive = TRUE, showWarnings = FALSE)
  dir.create(dirname(master_out_path), recursive = TRUE, showWarnings = FALSE)
  dir.create(dirname(excluded_out_path), recursive = TRUE, showWarnings = FALSE)
  
  # --- helper: standardize old lookup into export shape -----------------------
  standardize_old_lookup <- function(df) {
    nm <- names(df)
    
    id_col <- dplyr::case_when(
      "participant_id" %in% nm ~ "participant_id",
      "record_id" %in% nm ~ "record_id",
      "participant_id_" %in% nm ~ "participant_id_",
      TRUE ~ NA_character_
    )
    
    mrn_col <- dplyr::case_when(
      "medical_record_number" %in% nm ~ "medical_record_number",
      "medical_record_number_" %in% nm ~ "medical_record_number_",
      "mrn_b" %in% nm ~ "mrn_b",
      TRUE ~ NA_character_
    )
    
    empi_col <- dplyr::case_when(
      "empi" %in% nm ~ "empi",
      "empi_b" %in% nm ~ "empi_b",
      TRUE ~ NA_character_
    )
    
    dob_col <- dplyr::case_when(
      "date_of_birth" %in% nm ~ "date_of_birth",
      TRUE ~ NA_character_
    )
    
    doe_col <- dplyr::case_when(
      "date_of_enrollment" %in% nm ~ "date_of_enrollment",
      TRUE ~ NA_character_
    )
    
    fname_col <- dplyr::case_when(
      "first_name" %in% nm ~ "first_name",
      TRUE ~ NA_character_
    )
    
    lname_col <- dplyr::case_when(
      "last_name" %in% nm ~ "last_name",
      TRUE ~ NA_character_
    )
    
    sex_col <- dplyr::case_when(
      "what_is_your_sex_assigned_at_birth" %in% nm ~ "what_is_your_sex_assigned_at_birth",
      TRUE ~ NA_character_
    )
    
    if (is.na(id_col)) {
      stop(
        "Could not find participant identifier column in old lookup file. ",
        "Available columns are: ", paste(nm, collapse = ", ")
      )
    }
    
    out <- df %>%
      dplyr::transmute(
        `Participant ID` = as.character(.data[[id_col]]),
        `Medical Record Number:` = if (!is.na(mrn_col)) as.character(.data[[mrn_col]]) else "",
        `EMPI` = if (!is.na(empi_col)) as.character(.data[[empi_col]]) else "",
        `Date of birth` = if (!is.na(dob_col)) parse_date_relaxed(.data[[dob_col]]) else as.Date(NA),
        `Date of enrollment` = if (!is.na(doe_col)) parse_date_relaxed(.data[[doe_col]]) else as.Date(NA),
        `First Name` = if (!is.na(fname_col)) as.character(.data[[fname_col]]) else NA_character_,
        `Last Name` = if (!is.na(lname_col)) as.character(.data[[lname_col]]) else NA_character_,
        `What is your sex assigned at birth?` = if (!is.na(sex_col)) as.character(.data[[sex_col]]) else NA_character_
      ) %>%
      dplyr::mutate(
        `Participant ID` = stringr::str_trim(`Participant ID`),
        `Medical Record Number:` = stringr::str_trim(as.character(`Medical Record Number:`)),
        `EMPI` = stringr::str_trim(as.character(`EMPI`)),
        `First Name` = stringr::str_trim(`First Name`),
        `Last Name` = stringr::str_trim(`Last Name`)
      ) %>%
      dplyr::filter(!is.na(`Participant ID`), `Participant ID` != "") %>%
      dplyr::distinct(`Participant ID`, .keep_all = TRUE)
    
    out
  }
  
  # --- read current merged data -----------------------------------------------
  dat <- readr::read_csv(dat_merged_path, show_col_types = FALSE) %>%
    janitor::clean_names()
  
  needed_current <- c(
    "record_id",
    "mrn_b",
    "empi_b",
    "screen_dob",
    "screen_doe",
    "first_name",
    "last_name",
    "screen_sex",
    "treatment"
  )
  
  missing_current <- setdiff(needed_current, names(dat))
  if (length(missing_current)) {
    stop(
      "dat_merged.csv is missing required columns: ",
      paste(missing_current, collapse = ", ")
    )
  }
  
  # --- build eligibility table ------------------------------------------------
  current_candidates <- dat %>%
    dplyr::transmute(
      record_id = as.character(record_id),
      treatment = as.character(treatment),
      `Participant ID` = as.character(record_id),
      `Medical Record Number:` = as.character(mrn_b),
      `EMPI` = as.character(empi_b),
      `Date of birth` = parse_date_relaxed(screen_dob),
      `Date of enrollment` = parse_date_relaxed(screen_doe),
      `First Name` = as.character(first_name),
      `Last Name` = as.character(last_name),
      `What is your sex assigned at birth?` = dplyr::case_when(
        as.character(screen_sex) == "1" ~ "Male",
        as.character(screen_sex) == "2" ~ "Female",
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::mutate(
      record_id = stringr::str_trim(record_id),
      treatment = stringr::str_trim(treatment),
      `Participant ID` = stringr::str_trim(`Participant ID`),
      `Medical Record Number:` = stringr::str_trim(as.character(`Medical Record Number:`)),
      `EMPI` = stringr::str_trim(as.character(`EMPI`)),
      `First Name` = stringr::str_trim(`First Name`),
      `Last Name` = stringr::str_trim(`Last Name`),
      exclude_missing_id = is.na(`Participant ID`) | `Participant ID` == "",
      exclude_missing_dob = is.na(`Date of birth`),
      exclude_missing_doe = is.na(`Date of enrollment`),
      exclude_missing_fname = is.na(`First Name`) | `First Name` == "",
      exclude_missing_lname = is.na(`Last Name`) | `Last Name` == "",
      exclude_missing_sex = is.na(`What is your sex assigned at birth?`) |
        `What is your sex assigned at birth?` == "",
      exclude_missing_treatment = is.na(treatment) | treatment == "",
      include_in_lookup =
        !exclude_missing_id &
        !exclude_missing_dob &
        !exclude_missing_doe &
        !exclude_missing_fname &
        !exclude_missing_lname &
        !exclude_missing_sex &
        !exclude_missing_treatment
    ) %>%
    dplyr::distinct(`Participant ID`, .keep_all = TRUE)
  
  # --- excluded QC file -------------------------------------------------------
  excluded_rows <- current_candidates %>%
    dplyr::filter(!include_in_lookup) %>%
    dplyr::mutate(
      exclusion_reason = dplyr::case_when(
        exclude_missing_id ~ "Missing Participant ID",
        exclude_missing_dob ~ "Missing Date of birth",
        exclude_missing_doe ~ "Missing Date of enrollment",
        exclude_missing_fname ~ "Missing First Name",
        exclude_missing_lname ~ "Missing Last Name",
        exclude_missing_sex ~ "Missing Sex",
        exclude_missing_treatment ~ "Missing treatment",
        TRUE ~ "Other"
      )
    ) %>%
    dplyr::select(
      record_id,
      treatment,
      `Participant ID`,
      `Medical Record Number:`,
      `EMPI`,
      `Date of birth`,
      `Date of enrollment`,
      `First Name`,
      `Last Name`,
      `What is your sex assigned at birth?`,
      exclusion_reason,
      dplyr::starts_with("exclude_")
    )
  
  write_csv_safe(excluded_rows, excluded_out_path)
  
  # --- included current file --------------------------------------------------
  current_out <- current_candidates %>%
    dplyr::filter(include_in_lookup) %>%
    dplyr::select(dplyr::all_of(required_export_cols))
  
  # --- read and standardize old lookup ----------------------------------------
  old_lookup_raw <- readr::read_csv(old_lookup_path, show_col_types = FALSE) %>%
    janitor::clean_names()
  
  old_lookup_std <- standardize_old_lookup(old_lookup_raw)
  
  old_ids <- old_lookup_std %>%
    dplyr::distinct(`Participant ID`)
  
  # --- keep only new participants ---------------------------------------------
  new_out <- current_out %>%
    dplyr::anti_join(old_ids, by = "Participant ID") %>%
    dplyr::select(dplyr::all_of(required_export_cols))
  
  # --- build master list -------------------------------------------------------
  if (isTRUE(backfill_mrn)) {
    master_out <- current_out %>%
      dplyr::full_join(
        old_lookup_std,
        by = "Participant ID",
        suffix = c(".current", ".old")
      ) %>%
      dplyr::transmute(
        `Participant ID`,
        `Medical Record Number:` = dplyr::coalesce(
          dplyr::na_if(`Medical Record Number:.current`, ""),
          dplyr::na_if(`Medical Record Number:.old`, "")
        ),
        `EMPI` = dplyr::coalesce(
          dplyr::na_if(`EMPI.current`, ""),
          dplyr::na_if(`EMPI.old`, "")
        ),
        `Date of birth` = dplyr::coalesce(`Date of birth.current`, `Date of birth.old`),
        `Date of enrollment` = dplyr::coalesce(`Date of enrollment.current`, `Date of enrollment.old`),
        `First Name` = dplyr::coalesce(`First Name.current`, `First Name.old`),
        `Last Name` = dplyr::coalesce(`Last Name.current`, `Last Name.old`),
        `What is your sex assigned at birth?` = dplyr::coalesce(
          `What is your sex assigned at birth?.current`,
          `What is your sex assigned at birth?.old`
        )
      ) %>%
      dplyr::filter(!is.na(`Participant ID`), `Participant ID` != "") %>%
      dplyr::arrange(`Participant ID`)
  } else {
    master_out <- dplyr::bind_rows(current_out, old_lookup_std) %>%
      dplyr::filter(!is.na(`Participant ID`), `Participant ID` != "") %>%
      dplyr::distinct(`Participant ID`, .keep_all = TRUE) %>%
      dplyr::arrange(`Participant ID`) %>%
      dplyr::select(dplyr::all_of(required_export_cols))
  }
  
  # --- write outputs -----------------------------------------------------------
  write_csv_safe(new_out, new_out_path)
  write_csv_safe(master_out, master_out_path)
  
  message("✅ Wrote new medical lookup export: ", new_out_path)
  message("   New participants: ", nrow(new_out))
  message("✅ Wrote master medical lookup export: ", master_out_path)
  message("   Master participants: ", nrow(master_out))
  message("✅ Wrote excluded medical lookup QC file: ", excluded_out_path)
  message("   Excluded current rows: ", nrow(excluded_rows))
  
  invisible(list(
    new = new_out,
    master = master_out,
    excluded = excluded_rows
  ))
}