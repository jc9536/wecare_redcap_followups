# 73_nda_cde_exports.R
suppressPackageStartupMessages({
  library(readr)
  library(dplyr)
  library(stringr)
  library(lubridate)
})

# ---------- Smart date helper from pipeline ----------
if (!exists("to_date_smart")) {
  # Parse mixed date representations to Date (drops time).
  # Handles: YYYYMMDD, Excel serials, Unix epoch (s/ms), Unix *days*, SAS/Stata daily serials.
  to_date_smart <- function(x,
                            tz_epoch = "UTC",
                            tz_local = "America/New_York",
                            min_year = 2010,
                            max_year = 2100) {
    x_chr <- as.character(x)
    n <- length(x_chr)
    out <- rep(as.Date(NA), n)
    
    is_digits <- grepl("^\\d+$", x_chr)
    idx_num   <- which(is_digits)
    
    clip_years <- function(d) {
      bad <- !is.na(d) & (as.integer(format(d, "%Y")) < min_year |
                            as.integer(format(d, "%Y")) > max_year)
      d[bad] <- as.Date(NA)
      d
    }
    
    if (length(idx_num)) {
      num <- suppressWarnings(as.numeric(x_chr[idx_num]))
      nchar_num <- nchar(x_chr[idx_num])
      
      is_yyyymmdd <- nchar_num == 8 & grepl("^(19|20)\\d{6}$", x_chr[idx_num])
      is_excel    <- !is_yyyymmdd & !is.na(num) & num >= 25569 & num <= 60000
      is_epoch_ms <- !is.na(num) & num > 1e12
      is_epoch_s  <- !is.na(num) & num > 1e8 & num <= 1e12
      is_unix_days <- !is_yyyymmdd & !is_excel & !is_epoch_ms & !is_epoch_s &
        !is.na(num) & num >= 15000 & num <= 40000
      is_sas_stata <- !is_yyyymmdd & !is_excel & !is_epoch_ms & !is_epoch_s &
        !is_unix_days & !is.na(num) & num >= -3650 & num <= 32000
      
      if (any(is_yyyymmdd)) {
        y <- as.integer(substr(x_chr[idx_num][is_yyyymmdd], 1, 4))
        m <- as.integer(substr(x_chr[idx_num][is_yyyymmdd], 5, 6))
        d <- as.integer(substr(x_chr[idx_num][is_yyyymmdd], 7, 8))
        d_ymd <- suppressWarnings(as.Date(sprintf("%04d-%02d-%02d", y, m, d)))
        out[idx_num[is_yyyymmdd]] <- clip_years(d_ymd)
      }
      
      if (any(is_excel)) {
        d_xl <- as.Date(num[is_excel], origin = "1899-12-30")
        out[idx_num[is_excel]] <- clip_years(d_xl)
      }
      
      if (any(is_unix_days)) {
        d_ud <- as.Date(num[is_unix_days], origin = "1970-01-01")
        out[idx_num[is_unix_days]] <- clip_years(d_ud)
      }
      
      if (any(is_sas_stata)) {
        d_ss <- as.Date(num[is_sas_stata], origin = "1960-01-01")
        out[idx_num[is_sas_stata]] <- clip_years(d_ss)
      }
      
      if (any(is_epoch_ms)) {
        s <- num[is_epoch_ms] / 1000
        posix <- as.POSIXct(s, origin = "1970-01-01", tz = tz_epoch)
        out[idx_num[is_epoch_ms]] <- clip_years(as.Date(as.POSIXct(
          format(posix, tz = tz_local, usetz = TRUE),
          tz = tz_local
        )))
      }
      
      if (any(is_epoch_s)) {
        s <- num[is_epoch_s]
        posix <- as.POSIXct(s, origin = "1970-01-01", tz = tz_epoch)
        out[idx_num[is_epoch_s]] <- clip_years(as.Date(as.POSIXct(
          format(posix, tz = tz_local, usetz = TRUE),
          tz = tz_local
        )))
      }
    }
    
    # Non-numeric strings
    idx_str <- which(!is_digits)
    if (length(idx_str)) {
      x_s <- x_chr[idx_str]
      d1 <- suppressWarnings(lubridate::ymd(x_s, quiet = TRUE))
      miss <- is.na(d1)
      if (any(miss)) { d2 <- suppressWarnings(lubridate::ymd_hm (x_s[miss], quiet = TRUE)); d1[miss] <- d2; miss <- is.na(d1) }
      if (any(miss)) { d3 <- suppressWarnings(lubridate::ymd_hms(x_s[miss], quiet = TRUE)); d1[miss] <- d3; miss <- is.na(d1) }
      if (any(miss)) { d4 <- suppressWarnings(lubridate::mdy    (x_s[miss], quiet = TRUE)); d1[miss] <- d4; miss <- is.na(d1) }
      if (any(miss)) { d5 <- suppressWarnings(lubridate::mdy_hm (x_s[miss], quiet = TRUE)); d1[miss] <- d5; miss <- is.na(d1) }
      if (any(miss)) { d6 <- suppressWarnings(lubridate::mdy_hms(x_s[miss], quiet = TRUE)); d1[miss] <- d6 }
      out[idx_str] <- clip_years(as.Date(d1))
    }
    
    out
  }
}

# ---------- Age helper (months, 0-1260, rounding rule) ----------
age_in_months <- function(dob, visit_date) {
  # dob may already be a Date (from build_dob_from_components)
  dob_date <- if (inherits(dob, "Date")) {
    dob
  } else {
    # Allow birth years back to 1900
    to_date_smart(dob, min_year = 1900, max_year = 2100)
  }
  
  # visit_date is expected to be in the study period (2010+)
  visit <- if (inherits(visit_date, "Date")) {
    visit_date
  } else {
    to_date_smart(visit_date, min_year = 2010, max_year = 2100)
  }
  
  out <- rep(NA_integer_, length(dob_date))
  ok  <- !is.na(dob_date) & !is.na(visit)
  
  if (any(ok)) {
    days   <- as.numeric(visit[ok] - dob_date[ok])
    # Convert to years -> months and round to nearest month
    months <- round(days / 365.25 * 12)
    
    # Negative ages are invalid
    months[months < 0]    <- NA_integer_
    # Clip extreme ages to 1260 months (105 years)
    months[months > 1260] <- 1260L
    
    out[ok] <- months
  }
  
  out
}

# Build DOB date from MOB/DOB/YOB if present
build_dob_from_components <- function(df) {
  if (!all(c("MOB","DOB","YOB") %in% names(df))) {
    return(rep(NA, nrow(df)))
  }
  y <- suppressWarnings(as.integer(df$YOB))
  m <- suppressWarnings(as.integer(df$MOB))
  d <- suppressWarnings(as.integer(df$DOB))
  ok <- !is.na(y) & !is.na(m) & !is.na(d) &
    y >= 1900 & y <= 2025 &
    m >= 1 & m <= 12 &
    d >= 1 & d <= 31
  dob_str <- rep(NA_character_, length(y))
  dob_str[ok] <- sprintf("%04d-%02d-%02d", y[ok], m[ok], d[ok])
  as.Date(dob_str)
}

# -----------------------------
# Required columns per structure
# -----------------------------

rcads_required <- c(
  "subjectkey",
  "src_subject_id",
  "interview_age",
  "interview_date",
  "sex",
  "rcads_y01",
  "rcads_y02",
  "rcads_y03",
  "rcads_y04",
  "rcads_y05",
  "rcads_y06",
  "rcads_y07",
  "rcads_y08",
  "rcads_y09",
  "rcads_y10",
  "rcads_y11",
  "rcads_y12",
  "rcads_y13",
  "rcads_y14",
  "rcads_y15",
  "rcads_y16",
  "rcads_y17",
  "rcads_y18",
  "rcads_y19",
  "rcads_y20",
  "rcads_y21",
  "rcads_y22",
  "rcads_y23",
  "rcads_y24",
  "rcads_y25",
  "rcads_y26"
)

dsm_required <- c(
  "subjectkey",
  "src_subject_id",
  "interview_age",
  "interview_date",
  "sex",
  "dsm_cross_ch1",
  "dsm_cross_ch2",
  "dsm_cross_ch3",
  "dsm_cross_ch4",
  "dsm_cross_ch5",
  "dsm_cross_ch6",
  "dsm_cross_ch7",
  "dsm_cross_ch8",
  "dsm_cross_ch9",
  "dsm_cross_ch10",
  "dsm_cross_ch11",
  "dsm_cross_ch12",
  "dsm_cross_ch13",
  "dsm_cross_ch14",
  "dsm_cross_ch15",
  "dsm_cross_ch16",
  "dsm_cross_ch17",
  "dsm_cross_ch18",
  "dsm_cross_ch19",
  "dsm_cross_ch20",
  "dsm_cross_ch21",
  "dsm_cross_ch22",
  "dsm_cross_ch23",
  "dsm_cross_ch24",
  "dsm_cross_ch25",
  "dsm_cross_ch25a",
  "dsm_cross_ch26"
)

# ------------------------------------
# Hard-coded mapping: RCADS (baseline)
# ------------------------------------

rcads_map_baseline <- c(
  "src_subject_id" = "record_id",
  "interview_age"  = "screen_age",
  "interview_date" = "screen_doe",
  "sex"            = "SEX",
  "rcads_y01"      = "cde_2_b",
  "rcads_y02"      = "cde_7_b",
  "rcads_y03"      = "cdef_1_b",
  "rcads_y04"      = "cde_1_b",
  "rcads_y05"      = "cdef_7_b",
  "rcads_y06"      = "cdef_2_b",
  "rcads_y07"      = "cdef_6_b",
  "rcads_y08"      = "cde_3_b",
  "rcads_y09"      = "cdef_3_b",
  "rcads_y10"      = "cde_6_b",
  "rcads_y11"      = NA_character_,
  "rcads_y12"      = NA_character_,
  "rcads_y13"      = "cde_4_b",
  "rcads_y14"      = "cdehr_2_6m",
  "rcads_y15"      = NA_character_,
  "rcads_y16"      = NA_character_,
  "rcads_y17"      = NA_character_,
  "rcads_y18"      = NA_character_,
  "rcads_y19"      = NA_character_,
  "rcads_y20"      = "cdef_4_b",
  "rcads_y21"      = "cde_5_b",
  "rcads_y22"      = "cdef_5_b",
  "rcads_y23"      = NA_character_,
  "rcads_y24"      = "cde_8_b",
  "rcads_y25"      = "cdef_8_b",
  "rcads_y26"      = NA_character_   # overridden to 1
)

# ---------------------------------
# Hard-coded mapping: RCADS (3m)
# ---------------------------------

rcads_map_3m <- c(
  "src_subject_id" = "record_id",
  "interview_age"  = "follow_age_3m",
  "interview_date" = "follow_visitdate_3m",
  "sex"            = "SEX",
  "rcads_y01"      = "cde2_2_3m",
  "rcads_y02"      = "cde2_7_3m",
  "rcads_y03"      = NA_character_,
  "rcads_y04"      = "cde2_1_3m",
  "rcads_y05"      = NA_character_,
  "rcads_y06"      = NA_character_,
  "rcads_y07"      = NA_character_,
  "rcads_y08"      = "cde2_3_3m",
  "rcads_y09"      = NA_character_,
  "rcads_y10"      = "cde2_6_3m",
  "rcads_y11"      = NA_character_,
  "rcads_y12"      = NA_character_,
  "rcads_y13"      = "cde2_4_3m",
  "rcads_y14"      = NA_character_,
  "rcads_y15"      = NA_character_,
  "rcads_y16"      = NA_character_,
  "rcads_y17"      = NA_character_,
  "rcads_y18"      = "cded_1_3m",
  "rcads_y19"      = NA_character_,
  "rcads_y20"      = NA_character_,
  "rcads_y21"      = "cde2_5_3m",
  "rcads_y22"      = NA_character_,
  "rcads_y23"      = NA_character_,
  "rcads_y24"      = "cde2_8_3m",
  "rcads_y25"      = NA_character_,
  "rcads_y26"      = NA_character_   # overridden to 1
)

# ---------------------------------
# Hard-coded mapping: RCADS (6m)
# ---------------------------------

rcads_map_6m <- c(
  "src_subject_id" = "record_id",
  "interview_age"  = "followup_age_6m",
  "interview_date" = "follow_visitdate_6m",
  "sex"            = "SEX",
  "rcads_y01"      = "cde2_2_6m",
  "rcads_y02"      = "cde2_7_6m",
  "rcads_y03"      = NA_character_,
  "rcads_y04"      = "cde2_1_6m",
  "rcads_y05"      = NA_character_,
  "rcads_y06"      = NA_character_,
  "rcads_y07"      = NA_character_,
  "rcads_y08"      = "cde2_3_6m",
  "rcads_y09"      = NA_character_,
  "rcads_y10"      = "cde2_6_6m",
  "rcads_y11"      = "cdehr_1_6m",
  "rcads_y12"      = "cdeocd2_1_6m",
  "rcads_y13"      = "cde2_4_6m",
  "rcads_y14"      = NA_character_,
  "rcads_y15"      = "cdehr_4_6m",
  "rcads_y16"      = NA_character_,
  "rcads_y17"      = "cdeocd2_3_6m",
  "rcads_y18"      = NA_character_,
  "rcads_y19"      = "cdehr_3_6m",
  "rcads_y20"      = NA_character_,
  "rcads_y21"      = "cde2_5_6m",
  "rcads_y22"      = NA_character_,
  "rcads_y23"      = "cdeocd2_2_6m",
  "rcads_y24"      = "cde2_8_6m",
  "rcads_y25"      = NA_character_,
  "rcads_y26"      = NA_character_   # overridden to 1
)

# -----------------------------------
# Hard-coded mapping: DSM (baseline)
# -----------------------------------

dsm_map_baseline <- c(
  "src_subject_id"  = "record_id",
  "interview_age"   = "screen_age",
  "interview_date"  = "screen_doe",
  "sex"             = "SEX",
  "dsm_cross_ch1"   = NA_character_,
  "dsm_cross_ch2"   = NA_character_,
  "dsm_cross_ch3"   = NA_character_,
  "dsm_cross_ch4"   = NA_character_,
  "dsm_cross_ch5"   = NA_character_,
  "dsm_cross_ch6"   = NA_character_,
  "dsm_cross_ch7"   = NA_character_,
  "dsm_cross_ch8"   = NA_character_,
  "dsm_cross_ch9"   = NA_character_,
  "dsm_cross_ch10"  = NA_character_,
  "dsm_cross_ch11"  = NA_character_,
  "dsm_cross_ch12"  = NA_character_,
  "dsm_cross_ch13"  = NA_character_,
  "dsm_cross_ch14"  = NA_character_,
  "dsm_cross_ch15"  = NA_character_,
  "dsm_cross_ch16"  = NA_character_,
  "dsm_cross_ch17"  = NA_character_,
  "dsm_cross_ch18"  = NA_character_,
  "dsm_cross_ch19"  = NA_character_,
  "dsm_cross_ch20"  = NA_character_,
  "dsm_cross_ch21"  = NA_character_,
  "dsm_cross_ch22"  = NA_character_,
  "dsm_cross_ch23"  = NA_character_,
  "dsm_cross_ch24"  = NA_character_,
  "dsm_cross_ch25"  = NA_character_,
  "dsm_cross_ch25a" = NA_character_,
  "dsm_cross_ch26"  = NA_character_
)

# --------------------------------
# Hard-coded mapping: DSM (3m)
# --------------------------------

dsm_map_3m <- c(
  "src_subject_id"  = "record_id",
  "interview_age"   = "follow_age_3m",
  "interview_date"  = "follow_visitdate_3m",
  "sex"             = "SEX",
  "dsm_cross_ch1"   = "cde1_1_3m",
  "dsm_cross_ch2"   = "cde1_2_3m",
  "dsm_cross_ch3"   = "cde1_3_3m",
  "dsm_cross_ch4"   = "cde1_4_3m",
  "dsm_cross_ch5"   = NA_character_,
  "dsm_cross_ch6"   = NA_character_,
  "dsm_cross_ch7"   = NA_character_,
  "dsm_cross_ch8"   = NA_character_,
  "dsm_cross_ch9"   = NA_character_,
  "dsm_cross_ch10"  = NA_character_,
  "dsm_cross_ch11"  = NA_character_,
  "dsm_cross_ch12"  = NA_character_,
  "dsm_cross_ch13"  = NA_character_,
  "dsm_cross_ch14"  = "cdeh_1_3m",
  "dsm_cross_ch15"  = "cdeh_2_3m",
  "dsm_cross_ch16"  = NA_character_,
  "dsm_cross_ch17"  = NA_character_,
  "dsm_cross_ch18"  = NA_character_,
  "dsm_cross_ch19"  = NA_character_,
  "dsm_cross_ch20"  = NA_character_,
  "dsm_cross_ch21"  = NA_character_,
  "dsm_cross_ch22"  = NA_character_,
  "dsm_cross_ch23"  = NA_character_,
  "dsm_cross_ch24"  = NA_character_,
  "dsm_cross_ch25"  = NA_character_,
  "dsm_cross_ch25a" = NA_character_,
  "dsm_cross_ch26"  = NA_character_
)

# --------------------------------
# Hard-coded mapping: DSM (6m)
# --------------------------------

dsm_map_6m <- c(
  "src_subject_id"  = "record_id",
  "interview_age"   = "followup_age_6m",
  "interview_date"  = "follow_visitdate_6m",
  "sex"             = "SEX",
  "dsm_cross_ch1"   = NA_character_,
  "dsm_cross_ch2"   = NA_character_,
  "dsm_cross_ch3"   = NA_character_,
  "dsm_cross_ch4"   = NA_character_,
  "dsm_cross_ch5"   = NA_character_,
  "dsm_cross_ch6"   = NA_character_,
  "dsm_cross_ch7"   = NA_character_,
  "dsm_cross_ch8"   = NA_character_,
  "dsm_cross_ch9"   = "cdeocd1_5_6m",
  "dsm_cross_ch10"  = NA_character_,
  "dsm_cross_ch11"  = NA_character_,
  "dsm_cross_ch12"  = NA_character_,
  "dsm_cross_ch13"  = NA_character_,
  "dsm_cross_ch14"  = NA_character_,
  "dsm_cross_ch15"  = NA_character_,
  "dsm_cross_ch16"  = "cdeocd1_4_6m",
  "dsm_cross_ch17"  = "cdeocd1_1_6m",
  "dsm_cross_ch18"  = "cdeocd1_2_6m",
  "dsm_cross_ch19"  = "cdeocd1_3_6m",
  "dsm_cross_ch20"  = NA_character_,
  "dsm_cross_ch21"  = NA_character_,
  "dsm_cross_ch22"  = NA_character_,
  "dsm_cross_ch23"  = NA_character_,
  "dsm_cross_ch24"  = NA_character_,
  "dsm_cross_ch25"  = NA_character_,
  "dsm_cross_ch25a" = NA_character_,
  "dsm_cross_ch26"  = NA_character_
)

# -------------------------------
# Helper: write NDA-style CSV
# -------------------------------

write_nda_csv <- function(df, path, short_name) {
  m <- regexec("^(.*?)(\\d+)$", short_name)
  parts <- regmatches(short_name, m)[[1]]
  if (length(parts) == 3) {
    struct_name <- parts[2]
    version     <- parts[3]
  } else {
    struct_name <- short_name
    version     <- ""
  }
  
  df2 <- df
  df2[] <- lapply(df2, as.character)
  
  con <- file(path, open = "w", encoding = "UTF-8")
  on.exit(close(con), add = TRUE)
  
  k <- ncol(df2)
  header1_fields <- c(struct_name, version, rep("", max(0, k - 2)))
  header1 <- paste(header1_fields, collapse = ",")
  writeLines(header1, con)
  
  writeLines(paste(names(df2), collapse = ","), con)
  
  utils::write.table(
    df2,
    file      = con,
    sep       = ",",
    row.names = FALSE,
    col.names = FALSE,
    quote     = TRUE,
    append    = TRUE
  )
}

# ---------------------------------------------
# Build one NDA structure for one wave
# ---------------------------------------------

build_nda_structure_wave <- function(df_guid,
                                     short_name,
                                     wave,
                                     out_dir,
                                     required_elems) {
  
  # ---------- 0) Choose the correct mapping for this structure + wave ----------
  map_vec <- switch(
    short_name,
    "cde_rcadsyouth01" = switch(
      wave,
      "baseline" = rcads_map_baseline,
      "3m"       = rcads_map_3m,
      "6m"       = rcads_map_6m,
      stop("Unknown wave for rcads: ", wave)
    ),
    "cde_dsm5crossyouth01" = switch(
      wave,
      "baseline" = dsm_map_baseline,
      "3m"       = dsm_map_3m,
      "6m"       = dsm_map_6m,
      stop("Unknown wave for dsm: ", wave)
    ),
    stop("Unknown short_name: ", short_name)
  )
  
  # ---------- 1) Wave-specific filtering (using integer == 1) ----------
  df_wave <- df_guid
  if (wave == "3m") {
    if (!"follow_visitstatus_3m" %in% names(df_guid)) {
      stop("Column 'follow_visitstatus_3m' not found in df_guid.")
    }
    stat3 <- suppressWarnings(as.integer(df_guid$follow_visitstatus_3m))
    df_wave <- df_guid[!is.na(stat3) & stat3 == 1L, , drop = FALSE]
  } else if (wave == "6m") {
    if (!"follow_visitstatus_6m" %in% names(df_guid)) {
      stop("Column 'follow_visitstatus_6m' not found in df_guid.")
    }
    stat6 <- suppressWarnings(as.integer(df_guid$follow_visitstatus_6m))
    df_wave <- df_guid[!is.na(stat6) & stat6 == 1L, , drop = FALSE]
  }
  
  n <- nrow(df_wave)
  
  # If no rows for this wave, write empty data with correct headers
  if (n == 0) {
    empty_df <- tibble::as_tibble(
      stats::setNames(
        replicate(length(required_elems), character(0), simplify = FALSE),
        required_elems
      )
    )
    out_fname <- file.path(out_dir, paste0(short_name, "_", wave, ".csv"))
    write_nda_csv(empty_df, out_fname, short_name)
    message("Wrote ", out_fname, " (no rows for wave '", wave, "')")
    return(invisible(empty_df))
  }
  
  if (!"GUID" %in% names(df_wave)) {
    stop("df_guid/df_wave must contain a 'GUID' column.")
  }
  
  is_rcads <- (short_name == "cde_rcadsyouth01")
  is_dsm   <- (short_name == "cde_dsm5crossyouth01")
  
  # ---------- 2) Build base tibble from mappings ----------
  out_list <- vector("list", length(required_elems))
  
  for (i in seq_along(required_elems)) {
    el <- required_elems[i]
    
    if (el == "subjectkey") {
      # Always GUID
      col <- df_wave$GUID
      
    } else {
      internal <- if (el %in% names(map_vec)) map_vec[[el]] else NA_character_
      
      if (!is.na(internal) && internal %in% names(df_wave)) {
        col <- df_wave[[internal]]
        
      } else if (is_rcads && el == "rcads_y26") {
        # rcads_y26 should always be 1
        col <- rep("1", n)
        
      } else if (is_dsm && el == "dsm_cross_ch26") {
        # dsm_cross_ch26 should always be 1
        col <- rep("1", n)
        
      } else if (is_rcads && grepl("^rcads_y\\d+$", el)) {
        # Other unmapped RCADS items -> NA here (will become -9 later)
        col <- rep(NA_character_, n)
        
      } else if (el == "src_subject_id" && "record_id" %in% names(df_wave)) {
        col <- df_wave$record_id
        
      } else {
        col <- rep(NA_character_, n)
      }
    }
    
    out_list[[i]] <- col
  }
  
  out_df <- tibble::as_tibble(stats::setNames(out_list, required_elems))
  
  # ---------- 3) Parse interview_date FIRST (per wave) ----------
  internal_date <- unname(map_vec["interview_date"])
  visit_raw <- if (!is.na(internal_date) && internal_date %in% names(df_wave)) {
    df_wave[[internal_date]]
  } else {
    out_df$interview_date
  }
  
  # Use to_date_smart to handle datetime, numeric serials, etc.
  visit_date <- to_date_smart(visit_raw)
  
  if ("interview_date" %in% names(out_df)) {
    out_df$interview_date <- ifelse(
      is.na(visit_date),
      NA_character_,
      format(visit_date, "%m/%d/%Y")
    )
  }
  
  # ---------- 4) Build DOB from MOB / DOB / YOB ----------
  dob_date <- build_dob_from_components(df_wave)
  
  # ---------- 5) Compute interview_age in MONTHS from DOB + interview_date ----------
  if ("interview_age" %in% names(out_df)) {
    age_vec <- age_in_months(dob_date, visit_date)
    
    # Where DOB or interview_date are invalid, age_in_months returns NA.
    age_vec[is.na(age_vec)] <- -9L
    
    # Clamp to 0–1260 but keep -9 as missing
    age_vec[age_vec > 1260L] <- 1260L
    age_vec[age_vec < -9L]   <- -9L
    
    out_df$interview_age <- as.character(age_vec)
  }
  
  # ---------- 5b) Recode DSM responses to NDA scale ----------
  if (is_dsm) {
    dsm_cols <- grep("^dsm_cross_ch", names(out_df), value = TRUE)
    for (cn in dsm_cols) {
      v <- suppressWarnings(as.integer(out_df[[cn]]))
      # Recode:
      # 1 -> 1
      # 2 or 3 -> 2
      # 4 -> 3
      # 5 -> 4
      # anything else / missing -> NA (will become -9)
      v_new <- dplyr::case_when(
        v == 1              ~ 1L,
        v %in% c(2L, 3L)    ~ 2L,
        v == 4L             ~ 3L,
        v == 5L             ~ 4L,
        TRUE                ~ NA_integer_
      )
      out_df[[cn]] <- as.character(v_new)
    }
  }
  
  # ---------- 6) Global missing → -9 (except subjectkey) ----------
  out_df[] <- lapply(out_df, as.character)
  for (cn in names(out_df)) {
    if (cn == "subjectkey") next
    v <- out_df[[cn]]
    v[is.na(v) | v == ""] <- "-9"
    out_df[[cn]] <- v
  }
  
  # ---------- 7) Write NDA-style CSV ----------
  out_fname <- file.path(out_dir, paste0(short_name, "_", wave, ".csv"))
  write_nda_csv(out_df, out_fname, short_name)
  message("Wrote ", out_fname)
  
  invisible(out_df)
}

# ---------------------------------------------------
# Main entry: build all CDE exports for NDA
# ---------------------------------------------------

nda_build_cde_exports <- function(out_dir,
                                  guid_filename,           # e.g. "GUIDS_11272025.csv"
                                  candidates_fname = "nda_guid_candidates.csv") {
  
  nda_dir <- file.path(out_dir, "NDA")
  if (!dir.exists(nda_dir)) stop("NDA directory not found: ", nda_dir)
  
  cand <- readr::read_csv(
    file.path(nda_dir, candidates_fname),
    col_types = cols(.default = col_character(), ID = col_integer())
  )
  
  cand <- cand %>% select(-any_of("GUID"))
  
  guid <- readr::read_csv(
    file.path(nda_dir, guid_filename),
    show_col_types = FALSE
  ) %>%
    rename(ID = RECORD_ID) %>%
    mutate(ID = as.integer(ID))
  
  df_guid <- cand %>%
    inner_join(guid, by = "ID")
  
  for (w in c("baseline", "3m", "6m")) {
    build_nda_structure_wave(
      df_guid        = df_guid,
      short_name     = "cde_rcadsyouth01",
      wave           = w,
      out_dir        = nda_dir,
      required_elems = rcads_required
    )
    
    build_nda_structure_wave(
      df_guid        = df_guid,
      short_name     = "cde_dsm5crossyouth01",
      wave           = w,
      out_dir        = nda_dir,
      required_elems = dsm_required
    )
  }
  
  invisible(df_guid)
}