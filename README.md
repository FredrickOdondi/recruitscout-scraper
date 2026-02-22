# RecruitScout Job Scraper

A Python-based web scraper that aggregates job listings from multiple job boards using FastAPI and a beautiful frontend interface.

## Supported Job Boards

1. **Arbeitnow** - API-based job board
2. **Manfred** - GetManfred jobs
3. **Berlin Startup Jobs** - Engineering roles in Berlin
4. **Job4Good** - Social work jobs in Italy
5. **TuriJobs** - Spanish job market

## Features

- Scrape multiple job boards simultaneously
- Export results as CSV
- Beautiful web interface
- Select specific websites or scrape all
- Real-time scraping with async/await

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Run the application:
```bash
python main.py
```

3. Open your browser to:
```
http://localhost:8000
```

## Usage

1. Select which websites you want to scrape
2. Click "Scrape Jobs" button
3. View results in the table
4. Click "Export as CSV" to download the data

## API Endpoints

- `GET /` - Web interface
- `POST /api/scrape` - Scrape jobs (JSON body: `{"websites": ["arbeitnow", "manfred"]}`)
- `GET /api/export/csv` - Export all jobs as CSV
- `GET /health` - Health check

## Data Fields

Each job entry contains:
- **job_title**: The title of the position
- **date_posted**: When the job was posted
- **status**: Job status (default: Active)
- **website**: Source website

## Notes

- Web scraping may be affected by website structure changes
- Some websites may have rate limits or anti-scraping measures
- The scrapers use heuristics to find job listings and may need adjustment based on actual site structure
# recruitscout-scraper
