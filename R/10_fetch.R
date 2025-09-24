pull_redcap <- function(label, token, events = character(0)) {
  res <- suppressMessages(REDCapR::redcap_read(
    redcap_uri            = SHARED_URI,
    token                 = as.character(token),
    events                = if (length(events)) events else NULL,
    raw_or_label          = "raw",
    export_checkbox_label = FALSE,
    guess_type            = FALSE   # keep *character* columns to avoid parsing surprises
  ))
  if (!isTRUE(res$success)) {
    msg <- res$raw_text; if (length(msg) == 0 || is.na(msg)) msg <- "Unknown REDCap error"
    stop(sprintf("REDCap read failed for '%s': %s", label, msg), call. = FALSE)
  }
  dat <- res$data
  names(dat) <- clean_names(names(dat))
  dat$project_label <- label
  dat
}