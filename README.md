# ⚡ U.S. Large Load Interconnection Tracker

A production-quality Streamlit application tracking **new U.S. electricity demand projects ≥100 MW** — primarily data centers and major industrial loads — across all major ISO/RTO regions.

## Features

- **Multi-source ETL pipeline** pulling from NYISO, PJM, CAISO, SPP, MISO, ERCOT, and FERC filings
- **Deterministic deduplication** using queue IDs + fuzzy matching
- **Daily auto-refresh** via GitHub Actions + manual "Refresh Now" button
- **Full provenance** — every row has at least one source URL; per-field provenance tracked
- **Confidence scoring**: high / medium / low (never hallucinate missing data)
- **Interactive dashboard**: MW by ISO, state, timeline; KPI cards
- **FERC docket tracker**: RM26-4 and other large-load rulemakings
- **Map view** with pydeck (requires source-provided coordinates)

## App Pages

| Tab | Description |
|-----|-------------|
| 📊 Dashboard | KPI cards, MW by ISO/state/year, recent changelog |
| 📋 Projects Table | Filterable/searchable table with row details + CSV export |
| 🗺️ Map | pydeck scatter plot with clustering and tooltips |
| 📄 Filings | FERC docket tracker with document list and keyword search |
| ⚙️ Sources | Scraper run status, data dictionary, confidence rules |

## Quick Start

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the refresh pipeline

```bash
python -m src.pipeline.refresh
```

This will:
- Download NYISO queue XLSX, PJM tables, CAISO/SPP/MISO/ERCOT pages, FERC filings
- Deduplicate and merge into `data/power_project.db`
- Write a changelog and `data/last_refresh_summary.json`

### 3. Launch the app

```bash
streamlit run app.py
```

Visit `http://localhost:8501`

## Configuration

Edit `config.yaml` to:
- Add/remove data sources
- Add FERC dockets to track
- Adjust dedup thresholds
- Set minimum MW threshold
- Enable geocoding

## Repo Structure

```
app.py                          # Main Streamlit app
config.yaml                     # All configuration
requirements.txt
README.md
data/                           # SQLite DB + cache (gitignored except DB)
  power_project.db
  last_refresh_summary.json
  download_cache/               # ETag/content-hash cache for downloads
src/
  models/
    project.py                  # Project Pydantic model + schema
    filing.py                   # Filing / docket models
    scraper_run.py              # Scraper run tracking model
  scrapers/
    base.py                     # Abstract base + utilities
    nyiso.py                    # NYISO queue XLSX parser
    pjm.py                      # PJM load forecast + discovery
    caiso.py                    # CAISO large loads initiative
    spp.py                      # SPP HILL / provisional load
    miso.py                     # MISO large loads committee
    ercot.py                    # ERCOT large load integration
    ferc_filings.py             # FERC docket tracker
  pipeline/
    refresh.py                  # Main ETL runner (also __main__)
    dedup.py                    # Deterministic + fuzzy dedup
  storage/
    database.py                 # SQLite layer (queries, upsert, history)
  utils/
    downloader.py               # Retry/ETag/hash downloader
    pdf_parser.py               # pdfplumber + pymupdf table extractor
    geocoder.py                 # Nominatim geocoding + state normalization
.github/workflows/
  daily_refresh.yml             # GitHub Actions daily cron
```

## Data Sources

| Source | ISO | Type | Project-level? |
|--------|-----|------|---------------|
| [NYISO Interconnection Queue](https://www.nyiso.com/documents/20142/1407078/NYISO-Interconnection-Queue.xlsx) | NYISO | XLSX | ✅ High confidence |
| [PJM Load Forecast Tables](https://www.pjm.com/planning/res-adeq/load-forecast) | PJM | XLSX | ⚠️ Partial |
| [PJM Large Load Adjustment PDF](https://www.pjm.com/-/media/DotCom/committees-groups/subcommittees/las/) | PJM | PDF | ⚠️ Partial |
| [CAISO Large Loads Initiative](https://www.caiso.com/generation-transmission/load/large-load) | CAISO | HTML + PDF | ⚠️ Aggregate/partial |
| [SPP Large Load Connection](https://www.spp.org) | SPP | HTML + PDF | ⚠️ Policy + partial |
| [MISO Large Loads Committee](https://www.misoenergy.org/engage/committees/large-loads/) | MISO | HTML + PDF | ⚠️ Meeting materials |
| [ERCOT Large Load Integration](https://www.ercot.com/services/rq/large-load-integration) | ERCOT | HTML | ⚠️ Forms/guides |
| [FERC RM26-4](https://www.ferc.gov/rm26-4) | FERC | Docket | 📄 Filings only |

## Confidence Rules

| Level | Criteria |
|-------|----------|
| **High** | Official queue row with explicit MW + date + POI fields |
| **Medium** | Official document with MW/date but partial location/POI |
| **Low** | Inferred category, incomplete date, or unstructured PDF text. MW is never invented. |

> ⚠️ `poi_text`, `substation`, `transmission_owner` fields are **never hallucinated**. They appear as `null`/`—` when not present in the source.

## Daily Refresh (GitHub Actions)

The workflow in `.github/workflows/daily_refresh.yml`:
1. Runs at 6:00 AM UTC daily (configurable)
2. Restores the SQLite DB from the previous artifact
3. Runs `python -m src.pipeline.refresh`
4. Uploads updated DB + summary JSON as artifacts
5. Commits the DB back to the repository

Manual trigger: Go to Actions → Daily Data Refresh → Run workflow

## Adding New Sources

1. Create `src/scrapers/my_source.py` extending `BaseScraper`
2. Implement `run() -> tuple[list[Project], ScraperRun]`
3. Add to `SCRAPER_REGISTRY` in `src/pipeline/refresh.py`
4. Add config entry in `config.yaml` under `sources:`

## Deployment

### Streamlit Cloud

1. Push to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect repo, set main file to `app.py`
4. The DB file (`data/power_project.db`) must be committed to the repo or mounted as a volume

### Docker

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
EXPOSE 8501
CMD ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
```

## License

MIT
