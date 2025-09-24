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