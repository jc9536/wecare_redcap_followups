# WeCare Project 

Welcome! This guide walks you through **setup**, **configuration**, **what each file does**, and **how to run** the WeCare data pipeline and reports

---

## 1) What this project does (high level)

- **Pulls data from REDCap** (youth & caregiver projects).
- **Cleans and merges** those sources into one analysis-ready dataset.
- **Runs QC checks** and schema/label overlap audits.
- **Generates reports** (DSMB, enrollment/eligibility, bi-weekly recruitment tables, etc.).
- **Exports final outputs** to a folder you choose.

---

## 2) Prerequisites

### Software
- **R** ≥ 4.3 (https://cran.r-project.org)
- **RStudio** (optional but recommended: https://posit.co/download/rstudio-desktop/)

### R packages
We’ll install everything in one go (copy/paste into the R Console):

```r
install.packages(c(
  "tidyverse", "janitor", "lubridate", "gt", "rmarkdown", "knitr",
  "readr", "readxl", "httr", "jsonlite", "here", "rprojroot", "glue", "fs"
))
```

## 3) Project structure

Your repository should look like this:
```
WeCare/
│
├── config.user.example.R            ← See Step 4 in this README.md
├── config.user.R                    ← Your personal config file
│
├── R/
│   ├── 00_bootstrap.R
│   ├── 05_utils_io.R
│   ├── 10_fetch_redcap.R
│   ├── 20_baseline_cleaning.R
│   ├── 30_followups_attach.R
│   ├── 35_followups_export_audits.R
│   ├── 40_qc_checks.R
│   ├── 48_overlap_label_columns.R
│   ├── 49_baseline_coalesce_overlaps.R
│   ├── 50_merge_youth_caregiver.R
│   ├── 60_schema_overlap_report.R
│   ├── legacy_cleaning.R
│   └── run.R
│
├── RMarkdown/
│   ├── DSMB_report.Rmd
│   └── Enrollment_checks.Rmd
│
├── data/
├── output/
└── README.md
```

## 4) Configuration Setup

### Step 1: Copy the example file
```bash
cp config.user.example.R
```

### Step 2: Edit your personal configuration file  

Open `config.user.R` and fill in:  

```r
SHARED_URL <- "https://redcap..."

tokens <- c(
  youth_baseline     = "XXXXXXXXXXXXXXX",
  caregiver_baseline = "YYYYYYYYYYYYYYY",
  youth_followup     = "ZZZZZZZZZZZZZZZ",
  caregiver_followup = "WWWWWWWWWWWWWWW"
)

RAW_DIR    <- "data/raw"
OUT_DIR    <- "data/out"
CHECKS_DIR <- "data/checks"
```

### Step 3: Add to `.gitignore`  
```
config.user.R
```

---

## 5) Running the pipeline

### Option A: One-command run  
```
source("R/run.R")
```

This runs the full workflow:
1. Loads your config  
2. Pulls REDCap data  
3. Cleans + merges  
4. Runs QC checks  
5. Saves outputs to `OUT_DIR`

### Option B: Step-by-step  
```
source("R/00_bootstrap.R")
source("R/10_fetch_redcap.R")
source("R/20_baseline_cleaning.R")
source("R/49_baseline_coalesce_overlaps.R")
source("R/50_merge_youth_caregiver.R")
source("R/30_followups_attach.R")
source("R/40_qc_checks.R")
source("R/48_overlap_label_columns.R")
source("R/60_schema_overlap_report.R")
```

---

## 6) Running the reports  

Render R Markdown files:  
```
rmarkdown::render("RMarkdown/DSMB_report.Rmd")
rmarkdown::render("RMarkdown/Enrollment_checks.Rmd")
```
- Outputs formatted `gt` tables in HTML or simple text tables in the console  

---

## 7) File reference and purpose  

| File | Purpose |
|------|----------|
| `00_bootstrap.R` | Initializes environment + loads config |
| `05_utils_io.R` | File I/O helper utilities |
| `10_fetch_redcap.R` | Pulls data from REDCap using tokens |
| `20_baseline_cleaning.R` | Cleans baseline data |
| `30_followups_attach.R` | Attaches follow-up waves if present |
| `35_followups_export_audits.R` | (Optional) Exports REDCap audit logs |
| `40_qc_checks.R` | Runs QC checks and duplicates |
| `48_overlap_label_columns.R` | Finds overlapping / duplicated columns |
| `49_baseline_coalesce_overlaps.R` | Coalesces overlapping baseline fields |
| `50_merge_youth_caregiver.R` | Merges youth + caregiver datasets |
| `60_schema_overlap_report.R` | Summarizes schema and label overlaps |
| `run.R` | Orchestrates entire pipeline |
| `DSMB_report.Rmd` | DSMB report (enrollment, CONSORT, demographics) |
| `Enrollment_checks.Rmd` | Bi-weekly recruitment + eligibility tables |

---

## 8) Updating the REDCap URL & tokens  

When REDCap rotates shared URLs or tokens:  

1. Edit `config.user.R`  
2. Update:  
   ```
   SHARED_URL <- "https://redcap.XXX.edu/redcap_vXX.0.XX"
   tokens["youth_baseline"] <- "NEW_TOKEN"
   ```
3. Save and rerun the pipeline.

---

## 9) Outputs  

- **Clean data:** `OUT_DIR`  
- **QC reports:** `CHECKS_DIR`  
- **Merged dataset:** `MERGED_CSV_PATH`  
- **Rendered reports:** same directory as the Rmd or in `output/`  

---

## 10) Troubleshooting  

| Problem | Solution |
|----------|-----------|
| `is_rproj_root` not found | Use `here::here()` or run from project root |
| `file.exists(merged_path)` is not TRUE | Check paths and ensure file exists |
| `tabyl` error (“show_na must be TRUE/FALSE”) | Use `tabyl(df, var1, var2)` not `tabyl(df$var1, df$var2)` |
| “HTML Widget / shiny.tag” noise | Normal for `gt`; disappears in HTML output |
| Date format errors | Edit `parse_date_time(orders = c("Ymd","mdy","dmy"))` in scripts |

---

## 11) FAQ  

**Q:** Where do I edit tokens and URLs?  
**A:** In `config.user.R`

**Q:** I moved my RMarkdown files: do I need to edit paths?  
**A:** No, paths are project-root-relative via `here::here()`.

**Q:** Can I keep tokens private?  
**A:** Yes. `config.user.R` is already listed in `.gitignore`.

---

## 12) Getting help  

If you encounter issues:

1. Restart R  
2. Re-run:  
   ```r
   source("R/00_bootstrap.R")
   ```
3. Check paths and config values  
4. Re-install packages:  
   ```r
   update.packages(ask = FALSE)
   ```

---

## 13) Quick start summary  

```r
# 1. Install packages
install.packages(c(
  "tidyverse","janitor","lubridate","gt","rmarkdown","knitr","here","readr"
))

# 2. Copy and edit config
file.copy("config.user.example.R","config.user.R")

# 3. Set SHARED_URL, tokens, and output folders

# 4. Run pipeline
source("R/run.R")

# 5. Render reports
rmarkdown::render("RMarkdown/DSMB_report.Rmd")
rmarkdown::render("RMarkdown/Enrollment_checks.Rmd")
```

**Outputs** appear in `OUT_DIR`, and reports in `RMarkdown/`.  
