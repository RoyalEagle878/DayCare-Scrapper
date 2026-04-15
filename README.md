# DayCare Scrapper

## Overview
This project enriches daycare records with contact and match data using a layered pipeline:
- state portal and API adapters
- Google knowledge panel/search fallback
- Winnie fallback for unresolved records
- checkpointing and local run-state files to avoid repeating work

The main entry point is [enrich_daycare_data.py](.\enrich_daycare_data.py).

## Prerequisites
- Windows
- Python 3.9+
- Google Chrome installed at the path configured in [runtime_env.py](.\runtime_env.py)

## Install dependencies
From the project directory:

```powershell
cd .
python -m pip install -r requirements.txt
```

## Input and output files
Input:
- `DaycareBuildings_Input(in).csv`

Main generated files:
- `output\DaycareBuildings_Cleaned.csv`
- `output\DaycareBuildings_Enriched.csv`
- `output\enrichment.json`
- `output\google_enriched_staging.json`
- `output\google_bad_proxies.json`
- `output\google_miss.json`
- `logs\enrichment.log`

## End-to-end run
Run the full enrichment pipeline:

```powershell
python enrich_daycare_data.py
```

What the script does:
1. Cleans the input CSV.
2. Selects rows for the current run.
3. Runs the main enrichment pipeline.
4. Uses checkpoints while processing.
5. Writes the final enriched CSV to the `output` folder.

## Common run modes
Google-only mode:

```powershell
python enrich_daycare_data.py --google
```

Single PID in Google mode:

```powershell
python enrich_daycare_data.py --google --pid 2873164
```

Single PID with a visible browser:

```powershell
python enrich_daycare_data.py --google --pid 2873164 --headed
```

Selected states only:

```powershell
python enrich_daycare_data.py --states KS,TX,CA
```

## Single-PID Google output
If you run with both `--google` and `--pid`, the script processes only that PID and writes a PID-specific CSV in the `output` folder.

Example:

```powershell
python enrich_daycare_data.py --google --pid 2873164
```

Output:
- `output\DaycareBuildings_Enriched_PID_2873164.csv`

## Operational notes
- Successful rows are cached in checkpoint files and reused on reruns.
- Google misses are persisted in `output\google_miss.json` and skipped on later Google runs unless removed manually.
- Bad Google proxy hosts are persisted in `output\google_bad_proxies.json`.
- Google retries use exponential backoff.

## Troubleshooting
- If dependency installation fails, update `pip` first and rerun the install command.
- If browser automation fails, verify the Chrome path in [runtime_env.py](.\runtime_env.py).
- If Google mode skips a PID unexpectedly, check whether it exists in `output\google_miss.json`.
- If Google results are blocked frequently, review proxy settings and the bad-proxy registry.
