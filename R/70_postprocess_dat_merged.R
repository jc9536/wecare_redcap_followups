# 70_postprocess_dat_merged.R
suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(stringr)
})

# ------------------------- Helpers --------------------------
# Safe left-to-right coalesce for strings; treats "" as missing
coalesce_chr <- function(...) {
  args <- list(...)
  args <- lapply(args, \(v) replace(v, is.na(v) | v == "", NA))
  out <- args[[1]]
  for (i in seq_along(args)[-1]) out <- ifelse(is.na(out), args[[i]], out)
  out
}

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
      out[idx_num[is_epoch_ms]] <- clip_years(as.Date(as.POSIXct(format(posix, tz = tz_local, usetz = TRUE), tz = tz_local)))
    }
    
    if (any(is_epoch_s)) {
      s <- num[is_epoch_s]
      posix <- as.POSIXct(s, origin = "1970-01-01", tz = tz_epoch)
      out[idx_num[is_epoch_s]] <- clip_years(as.Date(as.POSIXct(format(posix, tz = tz_local, usetz = TRUE), tz = tz_local)))
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

# Safe fetcher: returns an NA-character vector if the column doesn't exist
get_col_chr <- function(df, nm) if (nm %in% names(df)) as.character(df[[nm]]) else rep(NA_character_, nrow(df))

# -------------------- Main transformer ---------------------
postprocess_dat_merged <- function(df) {
  df <- df %>%
    # 1) Remove legacy columns that clash
    select(-dplyr::any_of(c("ppid_root_y", "participant_id"))) %>%
    
    # 2) record_id from p_participant_id / p_participant_ID
    { if ("p_participant_ID" %in% names(.))      rename(., record_id = p_participant_ID)
      else if ("p_participant_id" %in% names(.)) rename(., record_id = p_participant_id)
      else . } %>%
    
    # 3) participant_id from wecare_id_y
    { if ("wecare_id_y" %in% names(.)) rename(., participant_id = wecare_id_y) else . } %>%
    
    # 4) p_participant_id from wecare_id_cg, enforce "-C-S" on non-empty
    {
      if ("wecare_id_cg" %in% names(.)) {
        mutate(.,
               p_participant_id = str_trim(.data$wecare_id_cg),
               p_participant_id = dplyr::if_else(
                 is.na(p_participant_id) | p_participant_id == "",
                 p_participant_id,
                 dplyr::if_else(
                   str_detect(p_participant_id, "-C-S$"),
                   p_participant_id,
                   paste0(p_participant_id, "-C-S")
                 )
               )
        ) %>% select(-wecare_id_cg)
      } else .
    } %>%
    
    # 5) family_id rename (common variants)
    {
      out <- .
      if ("Family Id" %in% names(out))       out <- rename(out, family_id = `Family Id`)
      else if ("FamilyID" %in% names(out))   out <- rename(out, family_id = FamilyID)
      else if ("familyId" %in% names(out))   out <- rename(out, family_id = familyId)
      out
    } %>%
    
    # 6) Guarantee the 4 key columns exist (as blanks if absent)
    {
      need <- c("record_id", "participant_id", "p_participant_id", "family_id")
      miss <- setdiff(need, names(.))
      if (length(miss)) for (nm in miss) .[[nm]] <- ""
      .
    } %>%
    
    # 7) Put the 4 keys first in the requested order
    relocate(record_id, participant_id, p_participant_id, family_id, .before = 1)
  
  # 8) Build merged_date_chr from multiple sources, drop rows with blank source
  merged_chr <- coalesce_chr(
    get_col_chr(df, "ps_date"),
    get_col_chr(df, "p_ps_date"),
    get_col_chr(df, "screen_doe"),
    get_col_chr(df, "date_baseline"),
    get_col_chr(df, "p_date_baseline")
  )
  merged_chr <- str_trim(merged_chr)
  
  drop_idx <- which(is.na(merged_chr) | merged_chr == "")
  if (length(drop_idx)) {
    rid <- if ("record_id" %in% names(df)) df$record_id else sprintf("[row %d]", seq_len(nrow(df)))
    message("Dropping ", length(drop_idx), " rows with empty merged_date source. record_id: ",
            paste(rid[drop_idx], collapse = ", "))
    df <- df[-drop_idx, , drop = FALSE]
    merged_chr <- merged_chr[-drop_idx]
  } else {
    message("No rows dropped for merged_date (all have a non-blank source).")
  }
  
  # 9) Parse to Date, then store as character "YYYY-MM-DD" (blank if NA)
  parsed <- to_date_smart(merged_chr)
  merged_date_chr <- ifelse(is.na(parsed), "", format(parsed, "%Y-%m-%d"))
  df$merged_date <- merged_date_chr
  
  # 10) Place merged_date after family_id; ensure blanks for all remaining NA in character cols
  if (all(c("family_id", "merged_date") %in% names(df))) {
    df <- relocate(df, merged_date, .after = family_id)
  }
  df <- df %>% mutate(across(where(is.character), ~ ifelse(is.na(.x), "", .x)))
  
  df
}

# ------------------ Read → transform → write ----------------
postprocess_dat_merged_in_place <- function(out_dir, fname = "dat_merged.csv", backup = FALSE) {
  stopifnot(dir.exists(out_dir))
  path <- file.path(out_dir, fname)
  if (!file.exists(path)) stop("File not found: ", path)
  
  # Force character types on read so 0/1 stay "0"/"1"
  dat_in  <- readr::read_csv(path, col_types = readr::cols(.default = readr::col_character()))
  dat_out <- postprocess_dat_merged(dat_in)
  
  if (isTRUE(backup)) {
    bkp <- file.path(out_dir, paste0("dat_merged_backup_", format(Sys.time(), "%Y%m%d-%H%M%S"), ".csv"))
    readr::write_csv(dat_in, bkp)
  }
  
  readr::write_csv(dat_out, path)
  message(sprintf(
    "Post-process OK → %s | Rows: %s, Cols: %s | First cols: %s",
    basename(path), nrow(dat_out), ncol(dat_out),
    paste(colnames(dat_out)[1:4], collapse = ", ")
  ))
  invisible(dat_out)
}