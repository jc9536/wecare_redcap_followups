# 72_nda_guid_prep.R
suppressPackageStartupMessages({
  library(readr); library(dplyr); library(stringr); library(lubridate)
})

truthy <- function(x){
  x <- tolower(trimws(as.character(x)))
  x %in% c("1","y","yes","true","t","checked","consented","agree","agreed","completed")
}

cap_only_first <- function(x){
  x <- as.character(x)
  ifelse(is.na(x)|x=="", x, sub("^\\s*(.)(.*)$", "\\U\\1\\L\\2", x, perl=TRUE))
}

coalesce_chr <- function(...){
  args <- lapply(list(...), \(v) replace(v, is.na(v)|v=="", NA))
  out <- args[[1]]
  for(i in seq_along(args)[-1]) out <- ifelse(is.na(out), args[[i]], out)
  out
}

as_date_quick <- function(x){
  if (inherits(x,"Date")) return(x)
  x <- as.character(x)
  d <- suppressWarnings(ymd(x, quiet=TRUE))
  i <- is.na(d); if(any(i)) d[i] <- suppressWarnings(mdy(x[i], quiet=TRUE))
  i <- is.na(d); if(any(i)) d[i] <- suppressWarnings(dmy(x[i], quiet=TRUE))
  as.Date(d)
}

to_code12 <- function(txt){
  txt <- tolower(trimws(as.character(txt)))
  ifelse(grepl("^m|male|boy", txt), 1L,
         ifelse(grepl("^f|female|girl", txt), 2L, NA_integer_))
}

# remove all punctuation, collapse whitespace, trim
clean_punct <- function(x){
  x <- as.character(x)
  x <- gsub("[[:punct:]]+", "", x)
  x <- gsub("\\s+", " ", x)
  trimws(x)
}

nda_guid_prep_in_place <- function(out_dir,
                                   in_fname        = "dat_merged.csv",
                                   out_full_fname  = "nda_guid_candidates.csv",
                                   out_guid_fname  = "guid_sample_upload.csv",
                                   id_map_fname    = "nda_guid_id_map.csv",
                                   missing_cob_fname = "guid_missing_cob.csv"){
  stopifnot(dir.exists(out_dir))
  
  # input stays at out_dir root
  pin <- file.path(out_dir, in_fname)
  if(!file.exists(pin)) stop("File not found: ", pin)
  
  # all OUTPUTS go into out_dir/NDA
  nda_dir <- file.path(out_dir, "NDA")
  if (!dir.exists(nda_dir)) dir.create(nda_dir, recursive = TRUE)
  
  pout_full        <- file.path(nda_dir, out_full_fname)
  pout_guid        <- file.path(nda_dir, out_guid_fname)
  pmap             <- file.path(nda_dir, id_map_fname)
  pout_missing_cob <- file.path(nda_dir, missing_cob_fname)
  
  df <- readr::read_csv(pin, col_types = cols(.default = col_character()))
  
  # extra variables you want carried into final CSV
  extra_vars <- c(
    "screen_age",
    "screen_doe",
    "screen_sex",
    "follow_visitstatus_3m",
    "follow_age_3m",
    "follow_visitdate_3m",
    "cde1_1_3m",
    "cde1_2_3m",
    "cde1_3_3m",
    "cde1_4_3m",
    "cdeh_1_3m",
    "cdeh_2_3m",
    "follow_visitstatus_6m",
    "followup_age_6m",
    "follow_visitdate_6m",
    "cdeocd1_5_6m",
    "cdeocd1_4_6m",
    "cdeocd1_1_6m",
    "cdeocd1_2_6m",
    "cdeocd1_3_6m",
    "cde_2_b",
    "cde_7_b",
    "cdef_1_b",
    "cde_1_b",
    "cdef_7_b",
    "cdef_2_b",
    "cdef_6_b",
    "cde_3_b",
    "cdef_3_b",
    "cde_6_b",
    "cde_4_b",
    "cdehr_2_6m",
    "cdef_4_b",
    "cde_5_b",
    "cdef_5_b",
    "cde_8_b",
    "cdef_8_b",
    "cde2_2_3m",
    "cde2_7_3m",
    "cde2_1_3m",
    "cde2_3_3m",
    "cde2_6_3m",
    "cde2_4_3m",
    "cded_1_3m",
    "cde2_5_3m",
    "cde2_8_3m",
    "cde2_2_6m",
    "cde2_7_6m",
    "cde2_1_6m",
    "cde2_3_6m",
    "cde2_6_6m",
    "cdehr_1_6m",
    "cdeocd2_1_6m",
    "cde2_4_6m",
    "cdehr_4_6m",
    "cdeocd2_3_6m",
    "cdehr_3_6m",
    "cde2_5_6m",
    "cdeocd2_2_6m",
    "cde2_8_6m"
  )
  
  # core variables needed for logic
  need_core <- c(
    "record_id","site_id","merged_date",
    "icf_nih_dob","p_icf_nih_dob","screen_dob",
    "icf_nih_sex","p_icf_nih_sex","screen_sex",
    "icf_nih_first_name","icf_nih_middle_name","icf_nih_last_name",
    "youth_firstname","youth_lastname","first_name","last_name",
    "p_youth_firstname","p_youth_lastname",
    "icf_nih_city","p_icf_nih_city",
    "icf_nih_share","p_icf_nih_share","over_18"
  )
  
  need <- c(need_core, extra_vars)
  miss <- setdiff(need, names(df))
  if(length(miss)) stop("Missing: ", paste(miss, collapse=", "))
  
  # ----- consent flags (0/1) -----
  y_ok <- as.integer(truthy(df$icf_nih_share)); y_ok[is.na(y_ok)] <- 0L
  p_ok <- as.integer(truthy(df$p_icf_nih_share)); p_ok[is.na(p_ok)] <- 0L
  
  # ----- over_18: compute if blank (DOB vs merged_date) -----
  over18 <- suppressWarnings(as.integer(df$over_18))
  dob_for_age   <- as_date_quick(coalesce_chr(df$icf_nih_dob, df$p_icf_nih_dob, df$screen_dob))
  merged_asdate <- as_date_quick(df$merged_date)
  need_age <- is.na(over18) | !(over18 %in% c(0L,1L))
  if(any(need_age)){
    age_yrs <- floor(as.numeric(difftime(merged_asdate, dob_for_age, units="days"))/365.25)
    over18[need_age] <- ifelse(!is.na(age_yrs[need_age]) & age_yrs[need_age] >= 18, 1L, 0L)
  }
  
  # ----- merged consent (vectorized & NA-safe) -----
  # Rule: if over_18 == 1 -> use youth only; else (0 or NA) -> youth OR caregiver
  nda_any_int <- ifelse(!is.na(over18) & over18==1L, y_ok, pmax(y_ok, p_ok))
  nda_any_int[is.na(nda_any_int)] <- 0L
  
  # ----- coalesce PII (for rows we’ll keep) -----
  icf_fn <- cap_only_first(coalesce_chr(df$icf_nih_first_name, df$youth_firstname, df$first_name, df$p_youth_firstname))
  icf_mn <- cap_only_first(coalesce_chr(
    df$icf_nih_middle_name,
    if("p_icf_nih_middle_name" %in% names(df)) df$p_icf_nih_middle_name else NA
  ))
  icf_ln <- cap_only_first(coalesce_chr(df$icf_nih_last_name, df$youth_lastname, df$last_name, df$p_youth_lastname))
  icf_dob <- as_date_quick(coalesce_chr(df$icf_nih_dob, df$p_icf_nih_dob, df$screen_dob))
  
  sex_y <- to_code12(df$icf_nih_sex)
  sex_s <- suppressWarnings(as.integer(df$screen_sex)); sex_s[!(sex_s %in% c(1L,2L))] <- NA_integer_
  sex_p <- to_code12(df$p_icf_nih_sex)
  icf_sex <- dplyr::coalesce(sex_y, sex_s, sex_p)  # 1/2/NA -> later blank if NA
  
  icf_city <- coalesce_chr(df$icf_nih_city, df$p_icf_nih_city)
  
  # write merged values back (so original names are preserved)
  df$icf_nih_first_name  <- ifelse(nda_any_int==1L, icf_fn, df$icf_nih_first_name)
  df$icf_nih_middle_name <- ifelse(nda_any_int==1L, icf_mn, df$icf_nih_middle_name)
  df$icf_nih_last_name   <- ifelse(nda_any_int==1L, icf_ln, df$icf_nih_last_name)
  df$icf_nih_dob         <- ifelse(nda_any_int==1L,
                                   ifelse(is.na(icf_dob), NA, format(icf_dob,"%Y-%m-%d")),
                                   df$icf_nih_dob)
  df$icf_nih_sex         <- ifelse(nda_any_int==1L, as.character(icf_sex), df$icf_nih_sex)
  df$icf_nih_city        <- ifelse(nda_any_int==1L, icf_city, df$icf_nih_city)
  
  # flags as 0/1 strings
  df$icf_nih_share   <- as.character(y_ok)
  df$p_icf_nih_share <- as.character(p_ok)
  df$over_18         <- as.character(over18)
  df$nda_share_any   <- as.character(nda_any_int)
  
  # keep ONLY consenting
  out <- df %>% filter(nda_share_any == "1")
  
  # sex must be 1/2/blank
  out$icf_nih_sex[!(out$icf_nih_sex %in% c("1","2"))] <- ""
  
  # ---------- STABLE NUMERIC IDs VIA ID MAP ----------
  if (file.exists(pmap)) {
    id_map <- readr::read_csv(pmap, show_col_types = FALSE)
    if (!all(c("record_id","ID") %in% names(id_map))) {
      stop("Existing ID map does not have expected columns: record_id, ID")
    }
    id_map$ID <- as.integer(id_map$ID)
  } else {
    id_map <- tibble::tibble(record_id = character(), ID = integer())
  }
  
  out <- out %>%
    left_join(id_map, by = "record_id")
  
  current_max <- if (nrow(id_map)) max(id_map$ID, na.rm = TRUE) else 0L
  need_new    <- is.na(out$ID)
  if (any(need_new)) {
    n_new <- sum(need_new)
    out$ID[need_new] <- seq.int(from = current_max + 1L, length.out = n_new)
  }
  
  # update ID map
  new_entries <- out %>%
    distinct(record_id, ID) %>%
    anti_join(id_map, by = "record_id")
  
  id_map_updated <- bind_rows(id_map, new_entries) %>%
    arrange(ID)
  
  readr::write_csv(id_map_updated, pmap, na = "")
  
  # ---------- NDA GUID TEMPLATE COLUMNS WITH DOB VALIDATION ----------
  # For GUID creation, MOB/DOB/YOB should come from screen_dob only.
  # We still enforce a reasonable date range: 1900-01-01 to 2025-12-31.
  
  dob_screen <- as_date_quick(out$screen_dob)
  
  valid_range <- function(d){
    !is.na(d) & d >= as.Date("1900-01-01") & d <= as.Date("2025-12-31")
  }
  
  # Use screen_dob as the sole source for DOB
  dob_final <- dob_screen
  dob_final[!valid_range(dob_final)] <- as.Date(NA)  # anything out of range -> NA
  
  out$FIRSTNAME  <- out$icf_nih_first_name
  out$MIDDLENAME <- out$icf_nih_middle_name
  out$LASTNAME   <- out$icf_nih_last_name
  
  # MOB, DOB, YOB strictly from screen_dob (dob_final)
  out$MOB <- lubridate::month(dob_final)
  out$DOB <- lubridate::day(dob_final)
  out$YOB <- lubridate::year(dob_final)
  
  # Using icf_nih_city as City of Birth proxy
  out$COB <- out$icf_nih_city
  
  out$SEX <- dplyr::case_when(
    out$icf_nih_sex == "1" ~ "M",
    out$icf_nih_sex == "2" ~ "F",
    TRUE ~ ""
  )
  
  out$SUBJECTHASMIDDLENAME <- ifelse(
    !is.na(out$MIDDLENAME) & out$MIDDLENAME != "",
    "YES","NO"
  )
  
  # clean punctuation on name + COB fields
  out$FIRSTNAME  <- clean_punct(out$FIRSTNAME)
  out$MIDDLENAME <- clean_punct(out$MIDDLENAME)
  out$LASTNAME   <- clean_punct(out$LASTNAME)
  out$COB        <- clean_punct(out$COB)
  
  # internal GUID column (not used by GUID tool)
  out$GUID <- ""
  
  # ---------- SPLIT BY CITY OF BIRTH (COB) ----------
  cob_valid <- !is.na(out$COB) & out$COB != ""
  out_valid       <- out[cob_valid, , drop = FALSE]
  out_missing_cob <- out[!cob_valid, , drop = FALSE]
  
  # ---------- FULL INTERNAL FILE (PII + CDEs) ----------
  final_cols <- c(
    "ID","FIRSTNAME","MIDDLENAME","LASTNAME",
    "MOB","DOB","YOB","COB","SEX","SUBJECTHASMIDDLENAME",
    "GUID",
    "record_id","site_id","nda_share_any","icf_nih_share","p_icf_nih_share","over_18",
    "icf_nih_first_name","icf_nih_middle_name","icf_nih_last_name",
    "icf_nih_dob","icf_nih_sex","icf_nih_city",
    extra_vars
  )
  miss_out <- setdiff(final_cols, names(out_valid))
  if(length(miss_out)) stop("Missing output columns: ", paste(miss_out, collapse=", "))
  
  out_full <- out_valid[, final_cols, drop=FALSE]
  
  # all character NAs -> blank for full file (numeric IDs left alone)
  out_full <- out_full %>%
    mutate(across(where(is.character), ~ ifelse(is.na(.x), "", .x)))
  
  # ---------- GUID-ONLY FILE FOR NDA GUID TOOL ----------
  guid_only <- out_full %>%
    transmute(
      ID,
      FIRSTNAME,
      MIDDLENAME,
      LASTNAME,
      MOB,
      DOB,
      YOB,
      COB,
      SEX,
      SUBJECTHASMIDDLENAME
    )
  
  # ---------- MISSING COB FILE ----------
  missing_cob_export <- out_missing_cob %>%
    transmute(
      record_id,
      site_id,
      ID,
      FIRSTNAME,
      MIDDLENAME,
      LASTNAME,
      MOB,
      DOB,
      YOB,
      COB,
      SEX,
      SUBJECTHASMIDDLENAME
    )
  
  # ---------- SUMMARY ----------
  over18_num_all  <- suppressWarnings(as.integer(out$over_18))
  over18_num_keep <- suppressWarnings(as.integer(out_full$over_18))
  
  cat("\n=== NDA/GUID Prep Summary ===\n")
  print(tibble::tibble(
    consenting_any           = nrow(out),           # all consenting
    with_cob                 = nrow(out_full),      # kept for GUID
    missing_cob              = nrow(out_missing_cob),
    consenting_y_over18_all  = sum(over18_num_all == 1L, na.rm=TRUE),
    consenting_y_over18_kept = sum(over18_num_keep == 1L, na.rm=TRUE)
  ))
  
  # ---------- WRITE ALL FILES (in NDA/) ----------
  readr::write_csv(out_full,           pout_full,        na = "")
  readr::write_csv(guid_only,          pout_guid,        na = "")
  readr::write_csv(missing_cob_export, pout_missing_cob, na = "")
  
  message("Wrote full NDA prep file:   ", pout_full)
  message("Wrote GUID-only template:   ", pout_guid)
  message("Wrote missing-COB listing:  ", pout_missing_cob)
  message("Updated ID map:             ", pmap)
  
  invisible(list(
    full        = out_full,
    guid        = guid_only,
    missing_cob = missing_cob_export,
    id_map      = id_map_updated
  ))
}