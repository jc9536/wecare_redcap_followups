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
  out <- args[[1]]; for(i in seq_along(args)[-1]) out <- ifelse(is.na(out), args[[i]], out); out
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

nda_guid_prep_in_place <- function(out_dir,
                                   in_fname="dat_merged.csv",
                                   out_fname="nda_guid_candidates.csv"){
  stopifnot(dir.exists(out_dir))
  pin  <- file.path(out_dir, in_fname)
  pout <- file.path(out_dir, out_fname)
  if(!file.exists(pin)) stop("File not found: ", pin)
  
  df <- readr::read_csv(pin, col_types = cols(.default = col_character()))
  
  need <- c("record_id","site_id","merged_date",
            "icf_nih_dob","p_icf_nih_dob","screen_dob",
            "icf_nih_sex","p_icf_nih_sex","screen_sex",
            "icf_nih_first_name","icf_nih_middle_name","icf_nih_last_name",
            "youth_firstname","youth_lastname","first_name","last_name",
            "p_youth_firstname","p_youth_lastname",
            "icf_nih_city","p_icf_nih_city",
            "icf_nih_share","p_icf_nih_share","over_18")
  miss <- setdiff(need, names(df)); if(length(miss)) stop("Missing: ", paste(miss, collapse=", "))
  
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
  icf_mn <- cap_only_first(coalesce_chr(df$icf_nih_middle_name,
                                        if("p_icf_nih_middle_name"%in%names(df)) df$p_icf_nih_middle_name else NA))
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
  df$icf_nih_dob         <- ifelse(nda_any_int==1L, ifelse(is.na(icf_dob), NA, format(icf_dob,"%Y-%m-%d")), df$icf_nih_dob)
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
  
  # order columns exactly as requested
  final_cols <- c(
    "record_id","site_id","nda_share_any","icf_nih_share","p_icf_nih_share","over_18",
    "icf_nih_first_name","icf_nih_middle_name","icf_nih_last_name",
    "icf_nih_dob","icf_nih_sex","icf_nih_city"
  )
  miss_out <- setdiff(final_cols, names(out)); if(length(miss_out)) stop("Missing output columns: ", paste(miss_out, collapse=", "))
  out <- out[, final_cols, drop=FALSE]
  
  # all NAs -> blank
  out <- out %>% mutate(across(where(is.character), ~ ifelse(is.na(.x), "", .x)))
  
  # short summary only
  over18_num <- suppressWarnings(as.integer(out$over_18))
  cat("\n=== NDA/GUID Prep Summary ===\n")
  print(tibble::tibble(
    total_rows              = nrow(out),
    consenting_any          = nrow(out),
    consenting_y_over18     = sum(over18_num == 1L, na.rm=TRUE),
    consenting_under18_orNA = sum(is.na(over18_num) | over18_num == 0L, na.rm=TRUE)
  ))
  
  readr::write_csv(out, pout, na = "")
  message("Wrote: ", pout)
  invisible(out)
}