# =============================================================================
# WeCare ETL • 40_qc_checks.R
# -----------------------------------------------------------------------------
# Purpose
#   Build quick QA summaries for follow-up (FU) completeness and baseline ID
#   alignment. Designed to consume *raw* FU pulls and *cleaned* baselines.
#
# Exposed functions
#   - completion_counts_for_event(df, id_col, event_name, wave_label, comp_candidates)
#   - build_followup_completion_summary(youth_fu_raw, cg_fu_raw)
#   - build_attached_indicator_summary(youth_plus, cg_plus)
#   - check_id_alignment(youth_base, cg_base, out_path = "data/checks/id_alignment.csv")
#
# Dependencies expected from 00_bootstrap.R
#   - mk_root_id(), is_blank(), derive_youth_wecare_id(), derive_caregiver_fu_wecare_id()
#
# Notes
#   - Completion is counted at the *participant* level per event: any row marked
#     complete for an ID counts that ID as complete for that event.
#   - We treat common encodings of “complete” as completed: 2 / "complete" / "completed".
# =============================================================================

# --- core: completion within a specific event --------------------------------

#' Count completion within a specific REDCap event.
#'
#' @param df              Follow-up raw dataframe (youth or caregiver)
#' @param id_col          Column name that carries the participant id for counting
#' @param event_name      REDCap event unique name (e.g. "3_month_followup_arm_1")
#' @param wave_label      "3m" or "6m" (for reporting only)
#' @param comp_candidates Character vector of completion column candidates
#'
#' @return data.frame with columns:
#'   cohort(NA), event, wave, n_rows, n_participants, n_with_any_data, n_complete
completion_counts_for_event <- function(df, id_col, event_name, wave_label, comp_candidates) {
  if (!id_col %in% names(df)) {
    # id column missing → empty result for this event
    return(data.frame(
      cohort = NA_character_, event = event_name, wave = wave_label,
      n_rows = 0L, n_participants = 0L, n_with_any_data = 0L, n_complete = 0L,
      stringsAsFactors = FALSE
    ))
  }
  
  sub <- df[df$redcap_event_name == event_name, , drop = FALSE]
  if (!nrow(sub)) {
    return(data.frame(
      cohort = NA_character_, event = event_name, wave = wave_label,
      n_rows = 0L, n_participants = 0L, n_with_any_data = 0L, n_complete = 0L,
      stringsAsFactors = FALSE
    ))
  }
  
  # keep as character to avoid type drift
  for (j in seq_along(sub)) if (!is.character(sub[[j]])) sub[[j]] <- as.character(sub[[j]])
  
  n_rows <- nrow(sub)
  ids <- sub[[id_col]]
  n_participants <- length(unique(ids))
  
  # any-data rows
  n_with_any <- sum(row_any_data(sub, id_col))
  
  # completion columns present in this slice
  comp_cols <- intersect(comp_candidates, names(sub))
  if (!length(comp_cols)) {
    n_complete <- 0L
  } else {
    comp_mat <- sapply(sub[, comp_cols, drop = FALSE], is_complete2)
    if (is.null(dim(comp_mat))) comp_mat <- cbind(comp_mat)
    row_complete <- apply(comp_mat, 1, function(z) any(z == 1L, na.rm = TRUE))
    n_complete <- length(unique(ids[row_complete]))
  }
  
  data.frame(
    cohort = NA_character_,
    event  = event_name,
    wave   = wave_label,
    n_rows = n_rows,
    n_participants  = n_participants,
    n_with_any_data = n_with_any,
    n_complete      = n_complete,
    stringsAsFactors = FALSE
  )
}

# --- public: FU completion summary across Youth & Caregiver -------------------

#' Build per-event completion summary for Youth and Caregiver follow-ups.
#'
#' @param youth_fu_raw Raw youth FU dataframe
#' @param cg_fu_raw    Raw caregiver FU dataframe
#' @return data.frame with:
#'   cohort, event, wave, n_rows, n_participants, n_with_any_data,
#'   n_complete, pct_complete_of_participants
build_followup_completion_summary <- function(youth_fu_raw, cg_fu_raw) {
  # make local copies and expose a consistent id for counting
  yf <- youth_fu_raw; yf$wecare_id <- derive_youth_wecare_id(yf)
  cf <- cg_fu_raw;    cf$wecare_id <- derive_caregiver_fu_wecare_id(cf)
  
  y3 <- completion_counts_for_event(
    yf, "wecare_id", "3_month_followup_arm_1", "3m",
    comp_candidates = c("initial_questions_3m_complete")
  )
  y6 <- completion_counts_for_event(
    yf, "wecare_id", "6_month_followup_arm_1", "6m",
    comp_candidates = c("initial_questions_6m_complete")
  )
  c3 <- completion_counts_for_event(
    cf, "wecare_id", "3_month_caregiver_arm_1", "3m",
    comp_candidates = c("initial_questions_3m_complete", "initial_questions_complete")
  )
  c6 <- completion_counts_for_event(
    cf, "wecare_id", "6_month_caregiver_arm_1", "6m",
    comp_candidates = c("initial_questions_6m_complete", "initial_questions_complete")
  )
  
  y3$cohort <- "Youth"; y6$cohort <- "Youth"
  c3$cohort <- "Caregiver"; c6$cohort <- "Caregiver"
  
  out <- rbind(y3, y6, c3, c6)
  out$pct_complete_of_participants <- ifelse(
    out$n_participants > 0,
    round(100 * out$n_complete / out$n_participants, 1),
    0
  )
  
  out[, c("cohort","event","wave","n_rows","n_participants","n_with_any_data","n_complete","pct_complete_of_participants")]
}

# --- public: indicator summary for attached (baseline+FU) tables --------------

#' Summarize completion indicators on *attached* tables.
#' Assumes the following columns are 1/0 completion indicators:
#'   Youth:     i_youth_3m, i_youth_6m
#'   Caregiver: i_caregiver_3m, i_caregiver_6m
#'
#' @param youth_plus Baseline+FU youth table
#' @param cg_plus    Baseline+FU caregiver table
#' @return data.frame with cohort, wave, n_indicator_1, n_total, pct_indicator_1
build_attached_indicator_summary <- function(youth_plus, cg_plus) {
  yi <- data.frame(
    cohort = "Youth",
    wave   = c("3m","6m"),
    n_indicator_1 = c(
      sum(ifelse(is.na(youth_plus$i_youth_3m), 0, youth_plus$i_youth_3m) == 1),
      sum(ifelse(is.na(youth_plus$i_youth_6m), 0, youth_plus$i_youth_6m) == 1)
    ),
    n_total = nrow(youth_plus),
    stringsAsFactors = FALSE
  )
  yi$pct_indicator_1 <- round(100 * yi$n_indicator_1 / pmax(1L, yi$n_total), 1)
  
  ci <- data.frame(
    cohort = "Caregiver",
    wave   = c("3m","6m"),
    n_indicator_1 = c(
      sum(ifelse(is.na(cg_plus$i_caregiver_3m), 0, cg_plus$i_caregiver_3m) == 1),
      sum(ifelse(is.na(cg_plus$i_caregiver_6m), 0, cg_plus$i_caregiver_6m) == 1)
    ),
    n_total = nrow(cg_plus),
    stringsAsFactors = FALSE
  )
  ci$pct_indicator_1 <- round(100 * ci$n_indicator_1 / pmax(1L, ci$n_total), 1)
  
  rbind(yi, ci)
}

# --- public: baseline ID alignment check -------------------------------------

#' Check that p_participant_id (canonical) equals the root of the baseline key.
#' Writes a CSV listing violations (one row per offending record).
#'
#' @param youth_base Cleaned youth baseline
#' @param cg_base    Cleaned caregiver baseline
#' @param out_path   Output CSV path (default "data/checks/id_alignment.csv")
#' @return invisibly returns the violations dataframe
check_id_alignment <- function(youth_base, cg_base, out_path = "data/checks/id_alignment.csv") {
  issues <- list()
  
  if (is.data.frame(youth_base) && nrow(youth_base)) {
    y <- youth_base
    y$ppid_root <- mk_root_id(y$p_participant_id)
    y$wroot     <- mk_root_id(y$wecare_id)
    
    bad_y <- y[!is_blank(y$wroot) & (y$ppid_root != y$wroot),
               c("p_participant_id","wecare_id"), drop = FALSE]
    if (nrow(bad_y)) {
      bad_y$cohort <- "Youth"
      bad_y$violates <- "ppid != wecare_root"
      issues[["y"]] <- bad_y
    }
  }
  
  if (is.data.frame(cg_base) && nrow(cg_base)) {
    c <- cg_base
    c$ppid_root <- mk_root_id(c$p_participant_id)
    c$wroot     <- mk_root_id(c$wecare_id)
    
    bad_c <- c[!is_blank(c$wroot) & (c$ppid_root != c$wroot),
               c("p_participant_id","wecare_id"), drop = FALSE]
    if (nrow(bad_c)) {
      bad_c$cohort <- "Caregiver"
      bad_c$violates <- "ppid != wecare_root"
      issues[["c"]] <- bad_c
    }
  }
  
  out <- if (length(issues)) {
    do.call(rbind, lapply(issues, function(d) {
      d[, c("cohort","violates", setdiff(names(d), c("cohort","violates")))]
    }))
  } else {
    data.frame(
      cohort=character(), violates=character(),
      p_participant_id=character(), wecare_id=character(),
      stringsAsFactors = FALSE
    )
  }
  
  write_csv_safe(out, out_path)
  invisible(out)
}