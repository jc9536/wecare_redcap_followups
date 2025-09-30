# =============================================================================
# WeCare ETL • 35_followups_export_audits.R
# -----------------------------------------------------------------------------
# Purpose
#   Export per-wave follow-up CSV slices (raw + audits) for Youth/Caregiver.
#   - Writes raw event slices, column schema, missing-ID rows, duplicate IDs,
#     empty-data rows, and per-ID audit summaries (any data? completed? n rows).
#   - Completion is based on initial_questions_*_complete fields.
#
# Side-effects (by cohort subfolder under `outdir`)
#   <outdir>/<youth|caregiver>/
#     - 3m_raw.csv / 6m_raw.csv
#     - 3m_columns.csv / 6m_columns.csv
#     - 3m_missing_id.csv / 6m_missing_id.csv
#     - 3m_dups.csv / 6m_dups.csv
#     - 3m_empty_rows.csv / 6m_empty_rows.csv
#     - 3m_audit.csv / 6m_audit.csv
#
# Requirements (from 00_bootstrap.R)
#   derive_youth_wecare_id(), derive_caregiver_fu_wecare_id()
#
# Notes
#   - All values are treated as character for predictable CSV output.
#   - Completion logic accepts common “complete” encodings (2, Complete, etc.).
# =============================================================================

# --- Helpers -----------------------------------------------------------------

# write only if there are rows
write_if_rows <- function(df, path) {
  if (!is.data.frame(df) || !nrow(df)) return(invisible(FALSE))
  write_csv_safe(df, path)
  TRUE
}

# row-level "any data?" excluding id/event/label
row_any_data <- function(df, id_col) {
  drop_cols <- intersect(c(id_col, "redcap_event_name", "project_label"), names(df))
  data_cols <- setdiff(names(df), drop_cols)
  if (!length(data_cols)) return(rep(FALSE, nrow(df)))
  apply(df[, data_cols, drop = FALSE], 1, function(row) any(row != "" & !is.na(row)))
}

# --- Core: export one wave slice ---------------------------------------------

#' Export a single event/wave slice and its audits.
#' @param df          full FU data.frame (character-friendly)
#' @param cohort      "Youth" or "Caregiver" (used for folder names)
#' @param id_col      join/audit ID column name (e.g., "wecare_id")
#' @param event_name  REDCap event unique name to filter on
#' @param wave_label  "3m" or "6m" (used in filenames)
#' @param outdir      base output directory (e.g., "data/checks/followups")
#' @param completion_candidates character vector of completion field names to check
export_wave <- function(df, cohort, id_col, event_name, wave_label, outdir,
                        completion_candidates = character(0)) {
  
  # Slice to event, coerce to character for stable CSVs
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
  
  # 2) column schema
  if (nrow(sub)) write_csv_safe(data.frame(column = names(sub), stringsAsFactors = FALSE), f_cols)
  
  # 3) missing id
  if (nrow(sub) && id_col %in% names(sub)) {
    miss <- is.na(sub[[id_col]]) | trimws(sub[[id_col]]) == ""
    write_if_rows(sub[miss, , drop = FALSE], f_miss)
  } else {
    write_if_rows(sub[0, , drop = FALSE], f_miss)
  }
  
  # 4) duplicates by id
  if (nrow(sub) && id_col %in% names(sub)) {
    tb <- table(sub[[id_col]])
    dup_ids <- names(tb)[tb > 1 & nzchar(names(tb))]
    dups <- if (length(dup_ids)) sub[sub[[id_col]] %in% dup_ids, , drop = FALSE] else sub[0, ]
    if (nrow(dups)) dups <- dups[order(dups[[id_col]]), , drop = FALSE]
    write_if_rows(dups, f_dups)
  }
  
  # 5) empty-data rows (no data aside from id/event/label)
  if (nrow(sub) && id_col %in% names(sub)) {
    any_data   <- row_any_data(sub, id_col)
    empty_rows <- sub[!any_data, , drop = FALSE]
    write_if_rows(empty_rows, f_empty)
    
    # 6) per-row completion (any candidate col indicates completion)
    comp_cols <- intersect(completion_candidates, names(sub))
    row_complete <- rep(FALSE, nrow(sub))
    if (length(comp_cols)) {
      comp_mat <- sapply(sub[, comp_cols, drop = FALSE], is_complete2)
      if (is.null(dim(comp_mat))) comp_mat <- cbind(comp_mat)
      row_complete <- apply(comp_mat, 1, any, na.rm = TRUE)
    }
    
    # Build per-row audit (no tidy-eval)
    audit_row <- data.frame(
      cohort       = rep(cohort, nrow(sub)),
      wave         = rep(wave_label, nrow(sub)),
      event        = rep(event_name, nrow(sub)),
      any_data_row = as.integer(any_data),
      complete_row = as.integer(row_complete),
      stringsAsFactors = FALSE
    )
    audit_row[[id_col]] <- sub[[id_col]]
    audit_row <- audit_row[, c("cohort","wave","event", id_col, "any_data_row","complete_row")]
    
    # Collapse to one row per id
    split_list <- split(audit_row, audit_row[[id_col]])
    audit_id <- do.call(
      rbind,
      lapply(split_list, function(d) {
        if (!nrow(d)) return(NULL)
        data.frame(
          cohort       = d$cohort[1],
          wave         = d$wave[1],
          event        = d$event[1],
          id           = d[[id_col]][1],
          n_rows_id    = nrow(d),
          any_data_id  = as.integer(any(d$any_data_row == 1, na.rm = TRUE)),
          complete_id  = as.integer(any(d$complete_row == 1,  na.rm = TRUE)),
          stringsAsFactors = FALSE
        )
      })
    )
    if (is.null(audit_id)) {
      audit_id <- data.frame(
        cohort=character(), wave=character(), event=character(),
        id=character(), n_rows_id=integer(), any_data_id=integer(), complete_id=integer(),
        stringsAsFactors = FALSE
      )
    } else if (nrow(audit_id)) {
      names(audit_id)[names(audit_id) == "id"] <- id_col
      audit_id <- audit_id[order(audit_id[[id_col]]), , drop = FALSE]
    }
    write_csv_safe(audit_id, f_audit)
  }
  
  invisible(list(
    raw = f_raw, audit = f_audit, dups = f_dups, missing = f_miss, empty = f_empty, columns = f_cols
  ))
}

# --- Public: export all waves -------------------------------------------------

#' Export Youth & Caregiver follow-up audits (all waves) to `outdir`.
#' @param youth_fu_raw raw Youth FU dataframe
#' @param cg_fu_raw    raw Caregiver FU dataframe
#' @param outdir       output directory root (default: "data/checks/followups")
export_followup_csvs <- function(youth_fu_raw, cg_fu_raw, outdir = "data/checks/followups") {
  dir.create(outdir, showWarnings = FALSE, recursive = TRUE)
  
  # Youth: derive wecare_id for audits (participant id proxy)
  yf <- youth_fu_raw
  yf$wecare_id <- derive_youth_wecare_id(yf)
  
  # Caregiver FU: join/audit ID comes from caregiver_id_3m (fallbacks allowed)
  cf <- cg_fu_raw
  cf$wecare_id <- derive_caregiver_fu_wecare_id(cf)
  
  # Youth waves
  export_wave(
    yf, "Youth", "wecare_id",
    "3_month_followup_arm_1", "3m", outdir,
    completion_candidates = c("initial_questions_3m_complete")
  )
  export_wave(
    yf, "Youth", "wecare_id",
    "6_month_followup_arm_1", "6m", outdir,
    completion_candidates = c("initial_questions_6m_complete")
  )
  
  # Caregiver waves (raw columns preserved; audits use derived wecare_id)
  export_wave(
    cf, "Caregiver", "wecare_id",
    "3_month_caregiver_arm_1", "3m", outdir,
    completion_candidates = c("initial_questions_3m_complete", "initial_questions_complete")
  )
  export_wave(
    cf, "Caregiver", "wecare_id",
    "6_month_caregiver_arm_1", "6m", outdir,
    completion_candidates = c("initial_questions_6m_complete", "initial_questions_complete")
  )
  
  message("📦 Per-wave follow-up CSVs written to: ", outdir)
}