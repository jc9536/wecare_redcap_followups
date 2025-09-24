# Helper: count completion within a specific event
# - df: follow-up raw (youth_fu_raw or cg_fu_raw)
# - id_col: "participant_id" or "caregiver_id"
# - event_name: e.g., "3_month_followup_arm_1"
# - wave_label: "3m" or "6m" (for reporting only)
# - suffix_hint: "3m"/"6m"/NULL. If set, prefer *_complete cols containing that hint;
#   if none found, fall back to ANY *_complete cols.
# Internal: count completion for a given event, driven by specific completion columns
completion_counts_for_event <- function(df, id_col, event_name, wave_label, comp_candidates) {
  stopifnot(id_col %in% names(df))
  sub <- df[df$redcap_event_name == event_name, , drop = FALSE]
  if (!nrow(sub)) {
    return(data.frame(
      cohort = NA_character_, event = event_name, wave = wave_label,
      n_rows = 0L, n_participants = 0L, n_with_any_data = 0L, n_complete = 0L,
      stringsAsFactors = FALSE
    ))
  }
  
  # rows and unique IDs present
  n_rows <- nrow(sub)
  ids <- sub[[id_col]]
  n_participants <- length(unique(ids))
  
  # any-data rows (handy context)
  drop_cols <- intersect(c(id_col, "redcap_event_name", "project_label"), names(sub))
  data_cols <- setdiff(names(sub), drop_cols)
  has_any <- if (length(data_cols)) {
    apply(sub[, data_cols, drop = FALSE], 1, function(row) any(row != "" & !is.na(row)))
  } else rep(FALSE, nrow(sub))
  n_with_any <- sum(has_any)
  
  # completion: look for the first available completion column from the candidates
  comp_cols <- intersect(comp_candidates, names(sub))
  if (length(comp_cols) == 0L) {
    n_complete <- 0L
  } else {
    # If multiple comp cols exist, consider a row complete if ANY of them says complete
    comp_mat <- sapply(sub[, comp_cols, drop = FALSE], function(x) {
      x <- trimws(as.character(x))
      x %in% c("2","complete","Complete","COMPLETED","Completed")
    })
    if (is.null(dim(comp_mat))) comp_mat <- cbind(comp_mat)
    row_complete <- apply(comp_mat, 1, any)
    # Count *participants* who have at least one completed row
    n_complete <- length(unique(ids[row_complete]))
  }
  
  data.frame(
    cohort = NA_character_, event = event_name, wave = wave_label,
    n_rows = n_rows, n_participants = n_participants,
    n_with_any_data = n_with_any, n_complete = n_complete,
    stringsAsFactors = FALSE
  )
}


# Public: build completion summary across Youth & Caregiver follow-ups
build_followup_completion_summary <- function(youth_fu_raw, cg_fu_raw) {
  yf <- youth_fu_raw; yf$wecare_id <- derive_youth_wecare_id(yf)
  cf <- cg_fu_raw;    cf$wecare_id <- derive_caregiver_fu_wecare_id(cf)
  
  y3 <- completion_counts_for_event(yf, "wecare_id", "3_month_followup_arm_1", "3m",
                                    comp_candidates = c("initial_questions_3m_complete"))
  y6 <- completion_counts_for_event(yf, "wecare_id", "6_month_followup_arm_1", "6m",
                                    comp_candidates = c("initial_questions_6m_complete"))
  c3 <- completion_counts_for_event(cf, "wecare_id", "3_month_caregiver_arm_1", "3m",
                                    comp_candidates = c("initial_questions_3m_complete","initial_questions_complete"))
  c6 <- completion_counts_for_event(cf, "wecare_id", "6_month_caregiver_arm_1", "6m",
                                    comp_candidates = c("initial_questions_6m_complete","initial_questions_complete"))
  
  y3$cohort <- "Youth"; y6$cohort <- "Youth"
  c3$cohort <- "Caregiver"; c6$cohort <- "Caregiver"
  
  out <- rbind(y3, y6, c3, c6)
  out$pct_complete_of_participants <- ifelse(out$n_participants > 0, round(100 * out$n_complete / out$n_participants, 1), 0)
  out[, c("cohort","event","wave","n_rows","n_participants","n_with_any_data","n_complete","pct_complete_of_participants")]
}

# Indicator summary: these i_* columns now mean "completed" (not just any data)
build_attached_indicator_summary <- function(youth_plus, cg_plus) {
  yi <- data.frame(
    cohort = "Youth",
    wave   = c("3m","6m"),
    n_indicator_1 = c(sum(ifelse(is.na(youth_plus$i_youth_3m), 0, youth_plus$i_youth_3m) == 1),
                      sum(ifelse(is.na(youth_plus$i_youth_6m), 0, youth_plus$i_youth_6m) == 1)),
    n_total = nrow(youth_plus),
    stringsAsFactors = FALSE
  )
  yi$pct_indicator_1 <- round(100 * yi$n_indicator_1 / yi$n_total, 1)
  
  ci <- data.frame(
    cohort = "Caregiver",
    wave   = c("3m","6m"),
    n_indicator_1 = c(sum(ifelse(is.na(cg_plus$i_caregiver_3m), 0, cg_plus$i_caregiver_3m) == 1),
                      sum(ifelse(is.na(cg_plus$i_caregiver_6m), 0, cg_plus$i_caregiver_6m) == 1)),
    n_total = nrow(cg_plus),
    stringsAsFactors = FALSE
  )
  ci$pct_indicator_1 <- round(100 * ci$n_indicator_1 / ci$n_total, 1)
  
  rbind(yi, ci)
}

# Ensure p_participant_id equals the root of the relevant keys present
check_id_alignment <- function(youth_base, cg_base, out_path = "data/checks/id_alignment.csv") {
  issues <- list()
  
  if (nrow(youth_base)) {
    y <- youth_base
    y$ppid_root <- mk_root_id(y$p_participant_id)
    y$wroot     <- mk_root_id(y$wecare_id)
    
    bad_y <- y[!is_blank(y$wroot) & (y$ppid_root != y$wroot),
               c("p_participant_id","wecare_id"), drop = FALSE]
    if (nrow(bad_y)) { bad_y$cohort <- "Youth"; bad_y$violates <- "ppid != wecare_root"; issues[["y"]] <- bad_y }
  }
  
  if (nrow(cg_base)) {
    c <- cg_base
    c$ppid_root <- mk_root_id(c$p_participant_id)
    c$wroot     <- mk_root_id(c$wecare_id)
    
    bad_c <- c[!is_blank(c$wroot) & (c$ppid_root != c$wroot),
               c("p_participant_id","wecare_id"), drop = FALSE]
    if (nrow(bad_c)) { bad_c$cohort <- "Caregiver"; bad_c$violates <- "ppid != wecare_root"; issues[["c"]] <- bad_c }
  }
  
  out <- if (length(issues)) {
    do.call(rbind, lapply(issues, function(d) d[, c("cohort","violates", setdiff(names(d), c("cohort","violates")))]))
  } else {
    data.frame(cohort=character(), violates=character(), p_participant_id=character(),
               wecare_id=character(), stringsAsFactors = FALSE)
  }
  
  write_csv_safe(out, out_path)
  invisible(out)
}