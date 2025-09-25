# ---- ID normalization helpers (update these) ----

is_blank <- function(x) is.na(x) | x == ""

# Strip trailing -Y-S or -C-S (case-insensitive) to get the "root" ID
mk_root_id <- function(x) {
  x <- trimws(as.character(x))
  sub("-(Y|C)-S$", "", x, ignore.case = TRUE)
}

# Youth preferred ID (baseline & FU)
derive_youth_wecare_id <- function(df) {
  cands <- c("wecare_id","youth_wecare_id","youth_wecare_id_3m","youth_wecare_id_6m","participant_id")
  out <- rep(NA_character_, nrow(df))
  for (nm in cands) if (nm %in% names(df)) {
    v <- as.character(df[[nm]]); v[!nzchar(v)] <- NA
    out <- ifelse(is_blank(out) & !is.na(v), v, out)
  }
  out
}

# Caregiver **baseline** preferred ID
derive_caregiver_wecare_id <- function(df) {
  cands <- c("wecare_id","caregiver_id","caregiver_id_3m","caregiver_id_6m","p_wecare_id")
  out <- rep(NA_character_, nrow(df))
  for (nm in cands) if (nm %in% names(df)) {
    v <- as.character(df[[nm]]); v[!nzchar(v)] <- NA
    out <- ifelse(is_blank(out) & !is.na(v), v, out)
  }
  out
}

# Caregiver **follow-up** join ID = caregiver_id_3m (fallbacks if missing)
derive_caregiver_fu_wecare_id <- function(df) {
  if ("caregiver_id_3m" %in% names(df)) {
    v <- as.character(df$caregiver_id_3m); v[!nzchar(v)] <- NA; return(v)
  }
  if ("caregiver_id" %in% names(df)) {
    v <- as.character(df$caregiver_id); v[!nzchar(v)] <- NA; return(v)
  }
  if ("wecare_id" %in% names(df)) return(as.character(df$wecare_id))
  if ("p_wecare_id" %in% names(df)) return(as.character(df$p_wecare_id))
  rep(NA_character_, nrow(df))
}

# Canonical cross-cohort ID: p_participant_id
# default to YOUTH wecare_id root; if NA, use CAREGIVER (baseline) wecare_id root;
# if both NA, fallback to existing p_participant_id root.
compute_ppid <- function(wecare_y, wecare_cg, p_participant_id = NULL) {
  wroot  <- mk_root_id(wecare_y)
  cgroot <- mk_root_id(wecare_cg)
  pproot <- mk_root_id(p_participant_id)
  out <- ifelse(!is_blank(wroot),  wroot,
                ifelse(!is_blank(cgroot), cgroot,
                       ifelse(!is_blank(pproot), pproot, NA_character_)))
  out
}

# --- ID helpers (robust) ---
normalize_id <- function(x) {
  x <- trimws(as.character(x))
  x[nchar(x) == 0] <- NA_character_
  toupper(x)
}

# "H-F0123-Y-S" -> "H-F0123"; "K-F0001-C-S" -> "K-F0001"
mk_root_id <- function(x) {
  x <- normalize_id(x)
  sub("-(Y|C)-S$", "", x, ignore.case = TRUE)
}

is_blank <- function(x) is.na(x) | x == ""

# ------- helpers to keep names clean across merges -------

# add "_3m"/"_6m" to every non-ID column that doesn't already end with a wave suffix
force_wave_suffix <- function(df, wave = c("3m","6m"),
                              exclude = c("p_participant_id","ppid_root","wecare_id","caregiver_id_3m")) {
  wave <- match.arg(wave)
  nm <- names(df)
  keep <- !nm %in% exclude
  need <- keep & !grepl("_(3m|6m)$", nm)
  nm[need] <- paste0(nm[need], "_", wave)
  names(df) <- nm
  df
}

# keep only the join key + wave-suffixed columns (drop unsuffixed FU fields)
prune_to_wave <- function(df, key) {
  stopifnot(key %in% names(df))
  wave_cols <- grep("_(3m|6m)$", names(df), value = TRUE)
  df[, c(key, wave_cols), drop = FALSE]
}

# Collapse .x/.y (and _x/_y) pairs, preferring "x" and filling from "y" when x is blank/NA
collapse_suffix_pairs <- function(df, prefer = c("x","y")) {
  prefer <- match.arg(prefer)
  nm <- names(df)
  pair_idx <- grepl("(\\.|_)[xy]$", nm)
  if (!any(pair_idx)) return(df)
  base_from <- function(n) sub("(\\.|_)x$|(\\.|_)y$", "", n)
  bases <- unique(base_from(nm[pair_idx]))
  is_blank <- function(x) is.na(x) | x == ""
  for (b in bases) {
    x_dot <- paste0(b, ".x"); y_dot <- paste0(b, ".y")
    x_ul  <- paste0(b, "_x"); y_ul  <- paste0(b, "_y")
    x_nm <- if (x_dot %in% nm) x_dot else if (x_ul %in% nm) x_ul else NA_character_
    y_nm <- if (y_dot %in% nm) y_dot else if (y_ul %in% nm) y_ul else NA_character_
    has_x <- !is.na(x_nm) && x_nm %in% nm
    has_y <- !is.na(y_nm) && y_nm %in% nm
    if (!has_x && !has_y) next
    winner <- if (has_x && has_y) {
      vx <- df[[x_nm]]; vy <- df[[y_nm]]
      if (prefer == "x") { vx[is_blank(vx)] <- vy[is_blank(vx)]; vx } else { vy[is_blank(vy)] <- vx[is_blank(vy)]; vy }
    } else if (has_x) df[[x_nm]] else df[[y_nm]]
    df[[b]] <- winner
    drop <- intersect(c(x_nm, y_nm), names(df))
    df[drop] <- NULL
    nm <- names(df)
  }
  df
}

# write which columns still end with .x/.y/_x/_y (for quick checks)
write_suffix_report <- function(df, label, out_dir = "data/checks/suffixes") {
  nm <- names(df)
  bad <- nm[grepl("(\\.|_)[xy]$", nm)]
  dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)
  utils::write.csv(data.frame(column = bad, stringsAsFactors = FALSE),
                   file.path(out_dir, paste0(label, "_suffix_pairs.csv")), row.names = FALSE, na = "")
}