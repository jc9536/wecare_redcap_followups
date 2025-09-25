# R/55_overlap_checks.R
# --------------------------------------------------------------------------------
# Write overlap reports for column names across source datasets (before any fixes).
#
# Outputs to: data/checks/overlaps/
#   - columns_presence_matrix.csv    : wide matrix; which dataset(s) contain each column
#   - overlapping_2plus.csv          : columns appearing in >= 2 datasets (with which ones)
#   - pairwise/overlap_<A>__<B>.csv  : per-pair overlapping columns
#
# Usage (from run.R or console):
#   source("R/55_overlap_checks.R")
#   write_overlap_checks(
#     baseline_youth    = youth_base_clean,
#     baseline_caregiver= cg_base_clean,
#     youth_followup    = youth_fu_raw,
#     caregiver_followup= cg_fu_raw
#   )
#
# If your objects have different names, pass those instead.

.write_csv_safe <- function(df, path) {
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  utils::write.csv(df, path, row.names = FALSE, na = "")
}

# Make a clean, named list of data frames (dropping NULLs)
.make_df_list <- function(baseline_youth, baseline_caregiver, youth_followup, caregiver_followup) {
  lst <- list(
    baseline_youth     = baseline_youth,
    baseline_caregiver = baseline_caregiver,
    youth_followup     = youth_followup,
    caregiver_followup = caregiver_followup
  )
  # Drop any NULLs with a warning
  keep <- vapply(lst, function(x) !is.null(x) && is.data.frame(x), logical(1))
  if (!all(keep)) {
    warning("These datasets were missing or not data.frames and were skipped: ",
            paste(names(lst)[!keep], collapse = ", "))
  }
  lst[keep]
}

write_overlap_checks <- function(baseline_youth, baseline_caregiver, youth_followup, caregiver_followup,
                                 out_dir = "data/checks/overlaps") {
  dfs <- .make_df_list(baseline_youth, baseline_caregiver, youth_followup, caregiver_followup)
  if (length(dfs) < 2L) stop("Need at least two datasets to compare.")
  
  # 1) Presence matrix: which dataset(s) contain each column
  name_sets <- lapply(dfs, names)
  all_cols  <- sort(unique(unlist(name_sets)))
  presence  <- sapply(name_sets, function(nm) all_cols %in% nm)
  presence_df <- data.frame(column = all_cols, presence, check.names = FALSE)
  .write_csv_safe(presence_df, file.path(out_dir, "columns_presence_matrix.csv"))
  
  # 2) Columns appearing in >= 2 datasets, with list of datasets holding them
  present_list <- apply(presence, 1, function(row) names(dfs)[which(row)])
  n_present    <- vapply(present_list, length, integer(1))
  multi_idx    <- which(n_present >= 2L)
  overlapping_2plus <- data.frame(
    column    = all_cols[multi_idx],
    n_datasets= n_present[multi_idx],
    datasets  = vapply(present_list[multi_idx], function(x) paste(x, collapse = " | "), character(1)),
    stringsAsFactors = FALSE
  )
  .write_csv_safe(overlapping_2plus, file.path(out_dir, "overlapping_2plus.csv"))
  
  # 3) Pairwise overlaps (one CSV per pair)
  pair_dir <- file.path(out_dir, "pairwise")
  dir.create(pair_dir, recursive = TRUE, showWarnings = FALSE)
  pairs <- t(utils::combn(names(dfs), 2))
  if (nrow(pairs) > 0) {
    apply(pairs, 1, function(p) {
      a <- p[[1]]; b <- p[[2]]
      ov <- intersect(names(dfs[[a]]), names(dfs[[b]]))
      df <- data.frame(column = sort(ov), stringsAsFactors = FALSE)
      .write_csv_safe(df, file.path(pair_dir, sprintf("overlap_%s__%s.csv", a, b)))
      invisible(NULL)
    })
  }
  
  # 4) Quick on-screen summary (counts only)
  cat("\n=== Column Overlap Summary ===\n")
  cat(sprintf("- Total unique columns across all: %d\n", length(all_cols)))
  for (nm in names(dfs)) {
    cat(sprintf("  · %-20s : %4d columns\n", nm, length(names(dfs[[nm]]))))
  }
  cat(sprintf("- Columns present in >= 2 datasets: %d\n", nrow(overlapping_2plus)))
  cat(sprintf("- Pairwise reports written to: %s\n", pair_dir))
  cat(sprintf("- Presence matrix written to   : %s\n", file.path(out_dir, "columns_presence_matrix.csv")))
  cat(sprintf("- Overlapping 2+ written to    : %s\n\n", file.path(out_dir, "overlapping_2plus.csv")))
}