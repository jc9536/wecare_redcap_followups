# =============================================================================
# WeCare ETL • 30_followups_attach.R
# -----------------------------------------------------------------------------
# Purpose
#   Attach 3m/6m follow-up (FU) data to baseline tables (Youth & Caregiver).
#   - Always baseline-LEFT join (no FU-only rows introduced)
#   - Coalesce duplicate FU rows per participant by **most recent visit date**
#   - Keep values if only one row provides them; otherwise prefer the latest
#   - Emit conflict reports when multiple distinct non-empty values exist
#   - Write join diagnostics for unmatched FU IDs
#
# Outputs (side-effects)
#   data/checks/coalesce/
#     - youth_3m_conflicts.csv, youth_6m_conflicts.csv
#     - youth_3m_unmatched_fu_ids.csv, youth_6m_unmatched_fu_ids.csv
#     - caregiver_3m_conflicts.csv, caregiver_6m_conflicts.csv
#     - caregiver_3m_unmatched_fu_ids.csv, caregiver_6m_unmatched_fu_ids.csv
#
# Requirements (from 00_bootstrap.R)
#   is_blank(), normalize_id(), mk_root_id()
#
# Notes
#   - Youth joins by ROOT of participant_id/wecare_id (ppid_root).
#   - Caregiver joins by ROOT of caregiver_id_3m (ppid_root).
#   - Completion flags use is_complete2() if available (fallback provided here).
# =============================================================================


# --- Local fallbacks (kept lightweight & harmless if globals exist) -----------

# must be base R (no readr)
.parse_date <- function(x) {
  x <- trimws(as.character(x)); x[x == ""] <- NA_character_
  d <- suppressWarnings(as.Date(x))
  miss <- is.na(d)
  if (any(miss)) d[miss] <- suppressWarnings(as.Date(x[miss], format = "%m/%d/%Y"))
  miss <- is.na(d)
  if (any(miss)) {
    p <- suppressWarnings(as.POSIXct(x[miss], tz = "UTC"))
    d[miss] <- as.Date(p)
  }
  d
}

# Keep only *_<suffix> + id + completion (+ optional extras), avoiding event columns
.pick_cols <- function(df, suffix, id_keep, complete_col, extra_keep = character(0)) {
  keep <- unique(c(
    id_keep,
    grep(paste0("_", suffix, "$"), names(df), value = TRUE),
    complete_col,
    "initial_questions_complete",  # legacy name
    extra_keep
  ))
  df[, intersect(keep, names(df)), drop = FALSE]
}

# Coalesce duplicate FU rows by id, preferring most-recent date
# Returns list(data=coalesced_df, conflicts=conflict_df)
.coalesce_by_id_date <- function(df, id_col, date_col, wave_label = "") {
  if (!id_col %in% names(df)) {
    return(list(
      data = df,
      conflicts = data.frame(
        wave=character(), id=character(), variable=character(),
        chosen_value=character(), chosen_date=character(),
        other_values=character(), n_source_rows=integer(),
        stringsAsFactors = FALSE
      )
    ))
  }
  
  # Keep blank-id rows separate (won't match baseline anyway)
  ok <- !is_blank(df[[id_col]])
  df_ok    <- df[ok, , drop = FALSE]
  df_blank <- df[!ok, , drop = FALSE]
  
  # Ensure date col exists; pre-parse to Date
  if (!date_col %in% names(df_ok)) df_ok[[date_col]] <- NA_character_
  df_ok$.__coalesce_date__ <- .parse_date(df_ok[[date_col]])
  
  # Split by ID and collapse within each group
  sp <- split(df_ok, df_ok[[id_col]], drop = TRUE)
  conflicts <- list()
  
  collapse_one <- function(d) {
    out <- d[1, , drop = FALSE]; out[,] <- NA
    out[[id_col]] <- d[[id_col]][1]
    out[[date_col]] <- NA
    
    cols <- setdiff(names(d), c(id_col, ".__coalesce_date__"))
    for (nm in cols) {
      v <- as.character(d[[nm]])
      nonempty <- which(!is_blank(v))
      if (!length(nonempty)) {
        out[[nm]] <- NA
        next
      }
      if (length(nonempty) == 1L) {
        out[[nm]] <- v[nonempty]
        next
      }
      # multiple non-empty values → choose value from most recent date
      dd <- d$.__coalesce_date__
      dd_sel <- dd[nonempty]
      dd_num <- as.numeric(dd_sel); dd_num[is.na(dd_num)] <- -Inf
      pick_rel <- which.max(dd_num)     # index within 'nonempty'
      pick <- nonempty[pick_rel]
      chosen <- v[pick]; chosen_dt <- dd[pick]
      
      # Log conflict if truly distinct values exist
      distinct_vals <- unique(v[nonempty])
      distinct_vals <- distinct_vals[!is.na(distinct_vals) & nzchar(distinct_vals)]
      if (length(unique(distinct_vals)) > 1L) {
        conflicts[[length(conflicts) + 1L]] <<- data.frame(
          wave = wave_label,
          id   = d[[id_col]][1],
          variable     = nm,
          chosen_value = chosen %||% NA_character_,
          chosen_date  = ifelse(is.na(chosen_dt), NA, format(chosen_dt, "%Y-%m-%d")),
          other_values = paste(setdiff(distinct_vals, chosen), collapse = " | "),
          n_source_rows = nrow(d),
          stringsAsFactors = FALSE
        )
      }
      out[[nm]] <- chosen
    }
    
    # Representative date = max across group (for reference)
    mx <- suppressWarnings(max(d$.__coalesce_date__, na.rm = TRUE))
    if (is.finite(mx)) out[[date_col]] <- format(as.Date(mx, origin = "1970-01-01"), "%Y-%m-%d")
    out
  }
  
  agg <- if (length(sp)) do.call(rbind, lapply(sp, collapse_one)) else df_ok[0, , drop = FALSE]
  agg$.__coalesce_date__ <- NULL
  
  # Reattach blank-id rows
  res <- rbind(agg, df_blank); rownames(res) <- NULL
  
  conflicts_df <- if (length(conflicts)) do.call(rbind, conflicts) else data.frame(
    wave=character(), id=character(), variable=character(),
    chosen_value=character(), chosen_date=character(),
    other_values=character(), n_source_rows=integer(),
    stringsAsFactors = FALSE
  )
  
  list(data = res, conflicts = conflicts_df)
}


# =============================================================================
# Youth: attach 3m/6m follow-ups to baseline (LEFT join by ppid_root)
# =============================================================================
attach_youth_followups <- function(baseline_youth, youth_fu) {
  checks_dir <- "data/checks/coalesce"
  if (exists("ensure_dir", mode = "function")) ensure_dir(checks_dir) else dir.create(checks_dir, TRUE, FALSE)
  
  evt3 <- "3_month_followup_arm_1"
  evt6 <- "6_month_followup_arm_1"
  
  y3_all <- youth_fu[youth_fu$redcap_event_name == evt3, , drop = FALSE]
  y6_all <- youth_fu[youth_fu$redcap_event_name == evt6, , drop = FALSE]
  
  # Use participant_id for youth; fallback to wecare_id if missing
  y3_key <- if ("participant_id" %in% names(y3_all)) "participant_id" else "wecare_id"
  y6_key <- if ("participant_id" %in% names(y6_all)) "participant_id" else "wecare_id"
  
  # Keep only wave columns + id + completion (+ visit_date_<wave>)
  y3 <- .pick_cols(y3_all, "3m", y3_key, "initial_questions_3m_complete", extra_keep = "visit_date_3m")
  y6 <- .pick_cols(y6_all, "6m", y6_key, "initial_questions_6m_complete", extra_keep = "visit_date_6m")
  
  # Normalize join key and build ROOT id for join
  y3[[y3_key]] <- normalize_id(y3[[y3_key]])
  y6[[y6_key]] <- normalize_id(y6[[y6_key]])
  names(y3)[names(y3) == y3_key] <- "fu_id"
  names(y6)[names(y6) == y6_key] <- "fu_id"
  y3$ppid_root <- mk_root_id(y3$fu_id)
  y6$ppid_root <- mk_root_id(y6$fu_id)
  
  # Ensure visit_date columns exist
  if (!"visit_date_3m" %in% names(y3)) y3$visit_date_3m <- NA_character_
  if (!"visit_date_6m" %in% names(y6)) y6$visit_date_6m <- NA_character_
  
  # Coalesce duplicate rows per ID using most-recent visit date
  res3 <- .coalesce_by_id_date(
    y3[, setdiff(names(y3), "fu_id"), drop = FALSE],
    id_col   = "ppid_root",
    date_col = "visit_date_3m",
    wave_label = "3m"
  )
  res6 <- .coalesce_by_id_date(
    y6[, setdiff(names(y6), "fu_id"), drop = FALSE],
    id_col   = "ppid_root",
    date_col = "visit_date_6m",
    wave_label = "6m"
  )
  y3c <- res3$data; y6c <- res6$data
  
  # Write conflict CSVs
  write_csv_safe(res3$conflicts, file.path(checks_dir, "youth_3m_conflicts.csv"))
  write_csv_safe(res6$conflicts, file.path(checks_dir, "youth_6m_conflicts.csv"))
  
  # Completion indicators (robust to legacy names)
  comp3_col <- intersect(c("initial_questions_3m_complete", "initial_questions_complete"), names(y3c))[1]
  comp6_col <- intersect(c("initial_questions_6m_complete", "initial_questions_complete"), names(y6c))[1]
  y3c$i_youth_3m <- if (!is.null(comp3_col)) is_complete2(y3c[[comp3_col]]) else 0L
  y6c$i_youth_6m <- if (!is.null(comp6_col)) is_complete2(y6c[[comp6_col]]) else 0L
  
  # Baseline keys: ensure ROOT present and normalized
  base <- baseline_youth
  if (!"wecare_id" %in% names(base)) base$wecare_id <- NA_character_
  base$wecare_id <- normalize_id(base$wecare_id)
  base$ppid_root <- mk_root_id(base$wecare_id)
  
  # Join diagnostics for unmatched FU roots
  jd <- function(fu_df, wave) {
    in_fu   <- unique(fu_df$ppid_root[!is.na(fu_df$ppid_root) & nzchar(fu_df$ppid_root)])
    in_base <- unique(base$ppid_root[!is.na(base$ppid_root) & nzchar(base$ppid_root)])
    unmatched <- setdiff(in_fu, in_base)
    if (!length(unmatched)) {
      return(data.frame(wave = character(0), ppid_root = character(0), stringsAsFactors = FALSE))
    }
    data.frame(wave = rep(wave, length(unmatched)), ppid_root = sort(unmatched), stringsAsFactors = FALSE)
  }
  write_csv_safe(jd(y3c, "3m"), file.path(checks_dir, "youth_3m_unmatched_fu_ids.csv"))
  write_csv_safe(jd(y6c, "6m"), file.path(checks_dir, "youth_6m_unmatched_fu_ids.csv"))
  
  # Baseline-LEFT merges by ROOT id (stable across suffix changes)
  out <- merge(base, y3c, by = "ppid_root", all.x = TRUE)
  out <- merge(out,  y6c, by = "ppid_root", all.x = TRUE)
  
  # Keep keys first for readability
  front <- intersect(c("ppid_root", "wecare_id"), names(out))
  out[, c(front, setdiff(names(out), front)), drop = FALSE]
}


# =============================================================================
# Caregiver: attach 3m/6m follow-ups to baseline (LEFT join by ppid_root)
# =============================================================================
attach_caregiver_followups <- function(baseline_cg, cg_fu) {
  checks_dir <- "data/checks/coalesce"
  if (exists("ensure_dir", mode = "function")) ensure_dir(checks_dir) else dir.create(checks_dir, TRUE, FALSE)
  
  evt3 <- "3_month_caregiver_arm_1"
  evt6 <- "6_month_caregiver_arm_1"
  
  c3_all <- cg_fu[cg_fu$redcap_event_name == evt3, , drop = FALSE]
  c6_all <- cg_fu[cg_fu$redcap_event_name == evt6, , drop = FALSE]
  
  # Always prefer caregiver_id_3m as the FU key (for BOTH 3m and 6m)
  c3_key <- if ("caregiver_id_3m" %in% names(c3_all)) "caregiver_id_3m"
  c6_key <- if ("caregiver_id_3m" %in% names(c6_all)) "caregiver_id_3m"
  
  # --- 3m slice: keep *_3m/_6m, completion, and p_date_3m -------------------
  c3 <- c3_all[, intersect(unique(c(
    c3_key,
    grep("_(3m|6m)", names(c3_all), value = TRUE),
    "initial_questions_complete",   # caregiver completion (raw name)
    "p_date_3m"
  )), names(c3_all)), drop = FALSE]
  
  names(c3)[names(c3) == c3_key] <- "fu_id"
  c3$fu_id     <- normalize_id(c3$fu_id)
  c3$ppid_root <- mk_root_id(c3$fu_id)
  
  if (!"p_date_3m" %in% names(c3)) c3$p_date_3m <- NA_character_
  
  res3 <- .coalesce_by_id_date(
    c3[, setdiff(names(c3), "fu_id"), drop = FALSE],
    id_col   = "ppid_root",
    date_col = "p_date_3m",
    wave_label = "3m"
  )
  c3c <- res3$data
  write_csv_safe(res3$conflicts, file.path(checks_dir, "caregiver_3m_conflicts.csv"))
  
  # --- 6m slice: mirror 3m, then rename *_3m → *_6m (except keys/helpers) ---
  c6 <- c6_all[, intersect(unique(c(
    c6_key,
    grep("_(3m|6m)", names(c6_all), value = TRUE),
    "initial_questions_complete",
    "p_date_3m"  # will become p_date_6m
  )), names(c6_all)), drop = FALSE]
  
  names(c6)[names(c6) == c6_key] <- "fu_id"
  c6$fu_id     <- normalize_id(c6$fu_id)
  c6$ppid_root <- mk_root_id(c6$fu_id)
  
  nm <- names(c6)
  protect <- c("fu_id", "ppid_root", "initial_questions_complete")
  idx <- which(!nm %in% protect)
  nm[idx] <- gsub("_3m", "_6m", nm[idx], fixed = TRUE)
  names(c6) <- nm
  
  if (!"p_date_6m" %in% names(c6)) c6$p_date_6m <- NA_character_
  
  res6 <- .coalesce_by_id_date(
    c6[, setdiff(names(c6), "fu_id"), drop = FALSE],
    id_col   = "ppid_root",
    date_col = "p_date_6m",
    wave_label = "6m"
  )
  c6c <- res6$data
  write_csv_safe(res6$conflicts, file.path(checks_dir, "caregiver_6m_conflicts.csv"))
  
  # --- Completion indicators --------------------------------------------------
  c3c$i_caregiver_3m <- if ("initial_questions_complete" %in% names(c3c))
    is_complete2(c3c$initial_questions_complete) else 0L
  c6c$i_caregiver_6m <- if ("initial_questions_complete" %in% names(c6c))
    is_complete2(c6c$initial_questions_complete) else 0L
  
  # --- Baseline (normalize + ROOT) -------------------------------------------
  base <- baseline_cg
  
  # Re-derive caregiver wecare_id if missing; then derive join ROOT
  base$wecare_id <- if ("wecare_id" %in% names(base)) base$wecare_id else NA_character_
  need <- is_blank(base$wecare_id)
  if (any(need) && exists("derive_caregiver_wecare_id", mode = "function")) {
    base$wecare_id[need] <- derive_caregiver_wecare_id(base[need, , drop = FALSE])
  }
  base$wecare_id <- normalize_id(base$wecare_id)
  base$ppid_root <- mk_root_id(base$wecare_id)
  
  # --- Join diagnostics for unmatched FU roots --------------------------------
  jd <- function(fu_df, wave) {
    in_fu   <- unique(fu_df$ppid_root[!is.na(fu_df$ppid_root) & nzchar(fu_df$ppid_root)])
    in_base <- unique(base$ppid_root[!is.na(base$ppid_root) & nzchar(base$ppid_root)])
    unmatched <- setdiff(in_fu, in_base)
    if (!length(unmatched)) {
      return(data.frame(wave = character(0), ppid_root = character(0), stringsAsFactors = FALSE))
    }
    data.frame(wave = rep(wave, length(unmatched)), ppid_root = sort(unmatched), stringsAsFactors = FALSE)
  }
  write_csv_safe(jd(c3c, "3m"), file.path(checks_dir, "caregiver_3m_unmatched_fu_ids.csv"))
  write_csv_safe(jd(c6c, "6m"), file.path(checks_dir, "caregiver_6m_unmatched_fu_ids.csv"))
  
  # --- Baseline-LEFT merges by ROOT ------------------------------------------
  out <- merge(base, c3c, by = "ppid_root", all.x = TRUE)
  out <- merge(out,  c6c, by = "ppid_root", all.x = TRUE)
  
  # Keep keys first
  front <- intersect(c("ppid_root", "wecare_id"), names(out))
  out[, c(front, setdiff(names(out), front)), drop = FALSE]
}