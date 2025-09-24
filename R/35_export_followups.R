# R/35_export_followups.R
# Export per-wave follow-up CSVs + quick audits, using initial_questions_*_complete.

# helper: write only if there are rows
write_if_rows <- function(df, path) {
  if (!is.data.frame(df) || nrow(df) == 0) return(invisible(FALSE))
  write_csv_safe(df, path)
  TRUE
}

# treat "2" or common "complete" strings as completed
is_complete2 <- function(x) {
  x <- trimws(as.character(x))
  x %in% c("2","complete","Complete","COMPLETED","Completed")
}

# row flag: any non-empty data excluding id/event/label
row_any_data <- function(df, id_col) {
  drop_cols <- intersect(c(id_col, "redcap_event_name", "project_label"), names(df))
  data_cols <- setdiff(names(df), drop_cols)
  if (!length(data_cols)) return(rep(FALSE, nrow(df)))
  apply(df[, data_cols, drop = FALSE], 1, function(row) any(row != "" & !is.na(row)))
}

# Export one wave
export_wave <- function(df, cohort, id_col, event_name, wave_label, outdir,
                        completion_candidates = character(0)) {
  sub <- df[df$redcap_event_name == event_name, , drop = FALSE]
  if (nrow(sub)) for (j in seq_along(sub)) if (!is.character(sub[[j]])) sub[[j]] <- as.character(sub[[j]])
  
  base_dir <- file.path(outdir, tolower(cohort))
  dir.create(base_dir, showWarnings = FALSE, recursive = TRUE)
  
  f_raw   <- file.path(base_dir, paste0(wave_label, "_raw.csv"))
  f_audit <- file.path(base_dir, paste0(wave_label, "_audit.csv"))
  f_dups  <- file.path(base_dir, paste0(wave_label, "_dups.csv"))
  f_miss  <- file.path(base_dir, paste0(wave_label, "_missing_id.csv"))
  f_empty <- file.path(base_dir, paste0(wave_label, "_empty_rows.csv"))
  f_cols  <- file.path(base_dir, paste0(wave_label, "_columns.csv"))
  
  # 1) raw slice
  write_if_rows(sub, f_raw)
  
  # 2) columns list (schema)
  if (nrow(sub)) write_csv_safe(data.frame(column = names(sub), stringsAsFactors = FALSE), f_cols)
  
  # 3) missing id
  write_if_rows(sub[is.na(sub[[id_col]]) | sub[[id_col]] == "", , drop = FALSE], f_miss)
  
  # 4) duplicates by id
  if (nrow(sub) && id_col %in% names(sub)) {
    tb <- table(sub[[id_col]])
    dup_ids <- names(tb)[tb > 1 & nzchar(names(tb))]
    dups <- if (length(dup_ids)) sub[sub[[id_col]] %in% dup_ids, , drop = FALSE] else sub[0, ]
    if (nrow(dups)) dups <- dups[order(dups[[id_col]]), , drop = FALSE]
    write_if_rows(dups, f_dups)
  }
  
  # 5) empty rows (no data aside from id/event)
  if (nrow(sub)) {
    any_data <- row_any_data(sub, id_col)
    empty_rows <- sub[!any_data, , drop = FALSE]
    write_if_rows(empty_rows, f_empty)
    
    # 6) per-row completion using initial_questions_*_complete
    comp_cols <- intersect(completion_candidates, names(sub))
    row_complete <- rep(FALSE, nrow(sub))
    if (length(comp_cols)) {
      comp_mat <- sapply(sub[, comp_cols, drop = FALSE], is_complete2)
      if (is.null(dim(comp_mat))) comp_mat <- cbind(comp_mat)
      row_complete <- apply(comp_mat, 1, any)
    }
    
    # Build audit_row without tidy-eval/data.table:
    audit_row <- data.frame(
      cohort       = rep(cohort, nrow(sub)),
      wave         = rep(wave_label, nrow(sub)),
      event        = rep(event_name, nrow(sub)),
      any_data_row = as.integer(any_data),
      complete_row = as.integer(row_complete),
      stringsAsFactors = FALSE
    )
    # add the id column by name
    audit_row[[id_col]] <- sub[[id_col]]
    # reorder columns nicely
    audit_row <- audit_row[, c("cohort","wave","event", id_col, "any_data_row","complete_row")]
    
    # collapse to one row per id
    split_list <- split(audit_row, audit_row[[id_col]])
    audit_id <- do.call(
      rbind,
      lapply(split_list, function(d) {
        if (!nrow(d)) return(NULL)
        out <- data.frame(
          cohort       = d$cohort[1],
          wave         = d$wave[1],
          event        = d$event[1],
          id           = d[[id_col]][1],
          n_rows_id    = nrow(d),
          any_data_id  = as.integer(any(d$any_data_row == 1, na.rm = TRUE)),
          complete_id  = as.integer(any(d$complete_row == 1,  na.rm = TRUE)),
          stringsAsFactors = FALSE
        )
        out
      })
    )
    if (is.null(audit_id)) {
      audit_id <- data.frame(cohort=character(), wave=character(), event=character(),
                             id=character(), n_rows_id=integer(),
                             any_data_id=integer(), complete_id=integer(),
                             stringsAsFactors = FALSE)
    } else if (nrow(audit_id)) {
      names(audit_id)[names(audit_id) == "id"] <- id_col
      audit_id <- audit_id[order(audit_id[[id_col]]), , drop = FALSE]
    }
    write_csv_safe(audit_id, f_audit)
  }
  
  invisible(list(raw = f_raw, audit = f_audit, dups = f_dups, missing = f_miss, empty = f_empty, columns = f_cols))
}

# Public: export all waves
export_followup_csvs <- function(youth_fu_raw, cg_fu_raw, outdir = "data/checks/followups") {
  dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
  
  # Youth: derive wecare_id for audits
  yf <- youth_fu_raw
  yf$wecare_id <- derive_youth_wecare_id(yf)
  
  # Caregiver FU: join ID comes from caregiver_id_3m → expose as wecare_id for audits/joins
  cf <- cg_fu_raw
  cf$wecare_id <- derive_caregiver_fu_wecare_id(cf)
  
  # Youth waves
  export_wave(yf, "Youth", "wecare_id",
              "3_month_followup_arm_1", "3m", outdir,
              completion_candidates = c("initial_questions_3m_complete"))
  export_wave(yf, "Youth", "wecare_id",
              "6_month_followup_arm_1", "6m", outdir,
              completion_candidates = c("initial_questions_6m_complete"))
  
  # Caregiver waves (raw column names preserved; audits use wecare_id derived above)
  export_wave(cf, "Caregiver", "wecare_id",
              "3_month_caregiver_arm_1", "3m", outdir,
              completion_candidates = c("initial_questions_3m_complete","initial_questions_complete"))
  export_wave(cf, "Caregiver", "wecare_id",
              "6_month_caregiver_arm_1", "6m", outdir,
              completion_candidates = c("initial_questions_6m_complete","initial_questions_complete"))
  
  message("📦 Per-wave follow-up CSVs written to: ", outdir)
}