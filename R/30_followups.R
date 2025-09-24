# Youth: join by wecare_id (derived in both baseline and follow-ups)
attach_youth_followups <- function(baseline_youth, youth_follow) {
  stopifnot("wecare_id" %in% names(baseline_youth))
  youth_follow$wecare_id <- derive_youth_wecare_id(youth_follow)
  
  evt3 <- "3_month_followup_arm_1"; evt6 <- "6_month_followup_arm_1"
  
  y3_all <- youth_follow[youth_follow$redcap_event_name == evt3, , drop = FALSE]
  y3     <- y3_all[, intersect(c("wecare_id",
                                 grep("_3m$", names(y3_all), value = TRUE),
                                 "initial_questions_3m_complete"), names(y3_all)), drop = FALSE]
  y3     <- pick_most_complete_by(y3, "wecare_id")
  y3     <- add_indicator(y3,
                          complete_col  = "initial_questions_3m_complete",
                          new_name      = "i_youth_3m",
                          fallback_cols = setdiff(names(y3), c("wecare_id","initial_questions_3m_complete")))
  
  y6_all <- youth_follow[youth_follow$redcap_event_name == evt6, , drop = FALSE]
  y6     <- y6_all[, intersect(c("wecare_id",
                                 grep("_6m$", names(y6_all), value = TRUE),
                                 "initial_questions_6m_complete"), names(y6_all)), drop = FALSE]
  y6     <- pick_most_complete_by(y6, "wecare_id")
  y6     <- add_indicator(y6,
                          complete_col  = "initial_questions_6m_complete",
                          new_name      = "i_youth_6m",
                          fallback_cols = setdiff(names(y6), c("wecare_id","initial_questions_6m_complete")))
  
  y_wide <- merge(y3, y6, by = "wecare_id", all = TRUE)
  merge(baseline_youth, y_wide, by = "wecare_id", all.x = TRUE)
}

# Caregiver: join by p_wecare_id (derived in both baseline and follow-ups)
# Caregiver follow-ups: join by baseline wecare_id; FU uses caregiver_id_3m as the join value
attach_caregiver_followups <- function(baseline_cg, cg_follow) {
  stopifnot("wecare_id" %in% names(baseline_cg))
  
  # Build join key on the follow-up side from caregiver_id_3m
  cg_follow$wecare_id <- derive_caregiver_fu_wecare_id(cg_follow)
  
  evt3 <- "3_month_caregiver_arm_1"
  evt6 <- "6_month_caregiver_arm_1"
  drop_3m_ids <- c("caregiver_id_3m","family_id_3m","site_id_3m")
  
  # --- 3m slice ---
  c3_all <- cg_follow[cg_follow$redcap_event_name == evt3, , drop = FALSE]
  c3_keep <- c("wecare_id",
               setdiff(grep("_3m$", names(c3_all), value = TRUE),
                       intersect(names(c3_all), drop_3m_ids)),
               "initial_questions_complete", "initial_questions_3m_complete")
  c3 <- c3_all[, intersect(unique(c3_keep), names(c3_all)), drop = FALSE]
  if ("initial_questions_complete" %in% names(c3) && !"initial_questions_3m_complete" %in% names(c3)) {
    c3$initial_questions_3m_complete <- c3$initial_questions_complete
    c3$initial_questions_complete <- NULL
  }
  c3 <- pick_most_complete_by(c3, "wecare_id")
  c3 <- add_indicator(
    c3,
    complete_col  = "initial_questions_3m_complete",
    new_name      = "i_caregiver_3m",
    fallback_cols = setdiff(names(c3), c("wecare_id","initial_questions_3m_complete"))
  )
  
  # --- 6m slice ---
  c6_all <- cg_follow[cg_follow$redcap_event_name == evt6, , drop = FALSE]
  c6_keep <- c("wecare_id", grep("_3m$", names(c6_all), value = TRUE),
               "initial_questions_complete", "initial_questions_6m_complete")
  c6 <- c6_all[, intersect(unique(c6_keep), names(c6_all)), drop = FALSE]
  # rename *_3m -> *_6m and completion name
  names(c6) <- sub("_3m$", "_6m", names(c6))
  if ("initial_questions_complete" %in% names(c6) && !"initial_questions_6m_complete" %in% names(c6)) {
    names(c6)[names(c6) == "initial_questions_complete"] <- "initial_questions_6m_complete"
  }
  c6 <- pick_most_complete_by(c6, "wecare_id")
  c6 <- add_indicator(
    c6,
    complete_col  = "initial_questions_6m_complete",
    new_name      = "i_caregiver_6m",
    fallback_cols = setdiff(names(c6), c("wecare_id","initial_questions_6m_complete"))
  )
  
  c_wide  <- merge(c3, c6, by = "wecare_id", all = TRUE)
  merge(baseline_cg, c_wide, by = "wecare_id", all.x = TRUE)
}