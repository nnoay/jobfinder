# 🎯 JobFinder — Multi-Source Job Aggregator

A simple job board that collects listings from **We Work Remotely**, **Indeed**, and **Glassdoor** into a single page with a black & red theme.

## Features

- 🔍 Search by job title or company
- 📍 Filter by location and job type
- 🔄 Auto-refreshes every hour
- ⚡ Manual scrape button
- 📱 Fully responsive
- 🎨 Black & red theme

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the app

```bash
python app.py
```

### 3. Open in browser

➡️ **http://localhost:5000**

## Usage

| Action | How |
|---|---|
| View jobs | Open `http://localhost:5000` |
| Force scrape | Click **"Scrape Now"** button or `POST /scrape` |
| API (JSON) | `GET /api/jobs` |

## API Endpoints

| Method | Route | Description |
|---|---|---|
| `GET` | `/` | Web interface |
| `GET` | `/api/jobs` | All jobs as JSON |
| `POST` | `/scrape` | Trigger scraping (optional JSON body: `{"search_term": "developer"}`) |

## Customization

Set the search term via environment variable:

```bash
export JOB_SEARCH="data scientist"
python app.py
```

## Notes

- Max **8 jobs per source** per scrape to be respectful
- Deduplication removes identical title + company combos
- If a source fails, the other two still load

## License

MIT
