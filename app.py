#!/usr/bin/env python3
"""
JobFinder v2 — Multi-source job aggregator
Gathers jobs from:
  1. We Work Remotely (RSS)
  2. RemoteOK (free JSON API)
  3. The Muse (free public API)
Single Flask app with auto-refresh every hour.
"""

import os
import re
import json
import time
import logging
import urllib.parse
from datetime import datetime, timezone, timedelta
from threading import Thread, Lock

import requests
import feedparser
from bs4 import BeautifulSoup
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS

# ─── Config ─────────────────────────────────────────────────────
SEARCH_TERM = os.environ.get("JOB_SEARCH", "developer")
SCRAPE_INTERVAL = 3600
CACHE_FILE = os.path.join(os.path.dirname(__file__), "jobs_cache.json")
MAX_PER_SOURCE = 8

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("jobfinder")

app = Flask(__name__)
CORS(app)

jobs_cache = []
last_updated = None
cache_lock = Lock()

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/html, */*",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch(url, timeout=15):
    for attempt in range(3):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except Exception as e:
            log.warning("Fetch failed (%s): %s (attempt %d/3)", url, e, attempt + 1)
            time.sleep(2)
    return None


def Job(title, company, location, jtype, salary, date, desc, url, source):
    return {
        "title": title, "company": company, "location": location,
        "type": jtype, "salary": salary, "date": date,
        "description": desc, "url": url, "source": source,
    }


def parse_date(text):
    if not text:
        return "Unknown"
    t = text.strip().lower()
    now = datetime.now(timezone.utc)
    if "just" in t or "now" in t or "moment" in t or "today" in t:
        return now.strftime("%Y-%m-%d")
    if "yesterday" in t:
        return (now - timedelta(days=1)).strftime("%Y-%m-%d")
    m = re.search(r"(\d+)\s*(min|minute|hour|day|week|month|year)s?\b", t)
    if m:
        n = int(m.group(1))
        u = m.group(2)
        d = {"min": timedelta(minutes=n), "minute": timedelta(minutes=n),
             "hour": timedelta(hours=n), "day": timedelta(days=n),
             "week": timedelta(weeks=n), "month": timedelta(days=30*n),
             "year": timedelta(days=365*n)}
        return (now - d.get(u, timedelta())).strftime("%Y-%m-%d")
    for fmt in ["%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d %b %Y",
                 "%b %d, %Y", "%d %B %Y", "%B %d, %Y"]:
        try:
            return datetime.strptime(t, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return text[:10]


def parse_salary(text):
    if not text:
        return "Not specified"
    m = re.search(r"[\$€£][\d,]+(?:\.\d+)?(?:\s*[-–]\s*[\$€£]?[\d,]+(?:\.\d+)?)?(?:k|K)?", text)
    if m:
        return m.group(0)
    m = re.search(r"(\d+)(k|K)\s*[-–]\s*(\d+)(k|K)", text)
    if m:
        return f"${m.group(1)}k - ${m.group(3)}k"
    m = re.search(r"\b(\d{2,3})\s*(?:k|K)\b", text)
    if m:
        return f"${m.group(1)}k"
    return "Not specified"


def deduplicate(jobs):
    seen = set()
    out = []
    for j in jobs:
        k = (j["title"].lower().strip(), j["company"].lower().strip())
        if k not in seen:
            seen.add(k)
            out.append(j)
    return out


# ─── Source 1: We Work Remotely (RSS) ────────────────────────────
def scrape_weworkremotely():
    log.info("Scraping We Work Remotely…")
    jobs = []
    try:
        feed = feedparser.parse("https://weworkremotely.com/remote-jobs.rss")
        for entry in feed.entries[:MAX_PER_SOURCE]:
            title = entry.get("title", "Untitled")
            company = entry.get("author", "Unknown")
            link = entry.get("link", "")
            desc_html = entry.get("summary", "")
            desc_text = BeautifulSoup(desc_html, "html.parser").get_text(separator=" ", strip=True)
            loc = "Remote"
            dl = desc_text.lower()
            if "worldwide" in dl or "anywhere" in dl:
                loc = "Remote (Worldwide)"
            elif "us" in dl and "remote" in dl:
                loc = "Remote (US)"
            salary = parse_salary(desc_text)
            description = desc_text[:150] + "…" if len(desc_text) > 150 else desc_text
            date_posted = parse_date(entry.get("published", ""))
            jtype = "Full-time"
            for tag in entry.get("tags", []):
                t = (tag.get("term", "") if isinstance(tag, dict) else str(tag)).lower()
                if "contract" in t:
                    jtype = "Contract"
                elif "part" in t:
                    jtype = "Part-time"
                elif "freelance" in t:
                    jtype = "Freelance"
            jobs.append(Job(title, company, loc, jtype, salary,
                           date_posted, description, link, "We Work Remotely"))
            time.sleep(0.2)
    except Exception as e:
        log.warning("WWR error: %s", e)
    log.info("  → %d jobs", len(jobs))
    return jobs


# ─── Source 2: RemoteOK (free API, no auth needed) ──────────────
def scrape_remoteok(search_term="developer"):
    log.info("Scraping RemoteOK…")
    jobs = []
    try:
        r = fetch("https://remoteok.com/api")
        if not r:
            return jobs
        data = r.json()
        if not isinstance(data, list):
            return jobs
        # Skip first element (it's a meta-header in RemoteOK API)
        items = data[1:] if len(data) > 1 and "slug" not in data[0] else data
        count = 0
        for item in items:
            if count >= MAX_PER_SOURCE:
                break
            # Filter by search term if provided
            tags_raw = item.get("tags", [])
            tag_str = " ".join(tags_raw) if isinstance(tags_raw, list) else str(tags_raw)
            desc_raw = item.get("description", "")
            desc_str = desc_raw if isinstance(desc_raw, str) else " ".join(desc_raw) if isinstance(desc_raw, list) else str(desc_raw)
            text = (item.get("position", "") + " " + desc_str + " " + tag_str).lower()
            if search_term and search_term.lower() not in text:
                if len(jobs) > MAX_PER_SOURCE // 2:
                    continue

            title = item.get("position", item.get("title", "Untitled"))
            company = item.get("company", "Unknown")
            link = item.get("url", "")
            loc = item.get("location", "Remote")
            salary = item.get("salary", "") or parse_salary(item.get("description", ""))
            desc = item.get("description", "")
            description = desc[:150] + "…" if len(desc) > 150 else desc
            # Extract date
            raw_date = item.get("date", "")
            date_posted = "Unknown"
            if raw_date:
                try:
                    if isinstance(raw_date, (int, float)):
                        date_posted = datetime.fromtimestamp(raw_date, tz=timezone.utc).strftime("%Y-%m-%d")
                    else:
                        date_posted = str(raw_date)[:10]
                except Exception:
                    pass
            jtype = "Full-time"
            tags_raw = item.get("tags", [])
            if isinstance(tags_raw, list):
                for t in tags_raw:
                    tl = t.lower() if isinstance(t, str) else ""
                    if "contract" in tl:
                        jtype = "Contract"
                    elif "part time" in tl or "part-time" in tl:
                        jtype = "Part-time"
            jobs.append(Job(title, company, loc, jtype,
                           salary, date_posted, description, link, "RemoteOK"))
            count += 1
            time.sleep(0.2)
    except Exception as e:
        log.warning("RemoteOK error: %s", e)
    log.info("  → %d jobs", len(jobs))
    return jobs


# ─── Source 3: The Muse (free public API) ────────────────────────
def scrape_muse(search_term="developer"):
    log.info("Scraping The Muse…")
    jobs = []
    try:
        q = urllib.parse.quote(search_term)
        r = fetch(f"https://www.themuse.com/api/public/jobs?page=1&search={q}&descending=true")
        if not r:
            return jobs
        data = r.json()
        for item in data.get("results", [])[:MAX_PER_SOURCE]:
            title = item.get("name", "Untitled")
            company = item.get("company", {}).get("name", "Unknown") if isinstance(item.get("company"), dict) else "Unknown"
            link = item.get("refs", {}).get("landing_page", "") if isinstance(item.get("refs"), dict) else ""

            locations = item.get("locations", [])
            loc_parts = []
            for loc in locations:
                parts = []
                if isinstance(loc, dict):
                    if loc.get("name"):
                        parts.append(loc["name"])
                    else:
                        if loc.get("city"): parts.append(loc["city"])
                        if loc.get("state"): parts.append(loc["state"])
                        if loc.get("country"): parts.append(loc["country"])
                if parts:
                    loc_parts.append(", ".join(parts))
            location = "; ".join(loc_parts) if loc_parts else "Remote"

            levels = item.get("levels", [])
            jtype = "Full-time"
            for lvl in levels:
                if isinstance(lvl, dict):
                    n = lvl.get("name", "").lower()
                    if "contract" in n:
                        jtype = "Contract"
                    elif "intern" in n:
                        jtype = "Internship"
                    elif "part" in n:
                        jtype = "Part-time"

            salary = parse_salary(item.get("contents", ""))

            pub_date = item.get("publication_date", "")
            date_posted = pub_date[:10] if pub_date else "Unknown"

            # Get description from contents
            contents = item.get("contents", "")
            if contents:
                clean = BeautifulSoup(contents, "html.parser").get_text(separator=" ", strip=True)
            else:
                clean = ""
            description = clean[:150] + "…" if len(clean) > 150 else clean

            jobs.append(Job(title, company, location, jtype,
                           salary, date_posted, description, link, "The Muse"))
            time.sleep(0.2)
    except Exception as e:
        log.warning("Muse error: %s", e)
    log.info("  → %d jobs", len(jobs))
    return jobs


# ─── Master Scraper ──────────────────────────────────────────────
def scrape_all(search_term=None):
    global jobs_cache, last_updated
    term = search_term or SEARCH_TERM
    log.info("═══ Scrape starting (term='%s') ═══", term)
    all_jobs = []
    all_jobs.extend(scrape_weworkremotely())
    time.sleep(1)
    all_jobs.extend(scrape_remoteok(term))
    time.sleep(1)
    all_jobs.extend(scrape_muse(term))
    all_jobs = deduplicate(all_jobs)

    def sk(j):
        d = j.get("date", "Unknown")
        return "0000" if d in ("Unknown", "Unknownn") else d
    all_jobs.sort(key=sk, reverse=True)

    with cache_lock:
        jobs_cache = all_jobs
        last_updated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    try:
        with open(CACHE_FILE, "w") as f:
            json.dump({"last_updated": last_updated, "jobs": jobs_cache}, f, indent=2)
    except Exception as e:
        log.warning("Cache write error: %s", e)

    log.info("═══ Done — %d jobs total ═══", len(jobs_cache))


def scrape_worker():
    while True:
        try:
            scrape_all()
        except Exception as e:
            log.error("Background scrape: %s", e)
        time.sleep(SCRAPE_INTERVAL)


# ─── Routes ──────────────────────────────────────────────────────
@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/jobs")
def api_jobs():
    with cache_lock:
        return jsonify({
            "last_updated": last_updated or "Never",
            "count": len(jobs_cache),
            "jobs": jobs_cache,
        })


@app.route("/scrape", methods=["POST"])
def trigger_scrape():
    term = request.json.get("search_term", SEARCH_TERM) if request.is_json else SEARCH_TERM
    Thread(target=scrape_all, args=(term,), daemon=True).start()
    return jsonify({"status": "started", "message": "Scraping in progress..."})


# ─── Startup ─────────────────────────────────────────────────────
def load_cache():
    global jobs_cache, last_updated
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE) as f:
                d = json.load(f)
                jobs_cache = d.get("jobs", [])
                last_updated = d.get("last_updated")
                log.info("Restored %d cached jobs", len(jobs_cache))
        except Exception as e:
            log.warning("Cache load: %s", e)


if __name__ == "__main__":
    load_cache()
    if not jobs_cache:
        scrape_all()
    Thread(target=scrape_worker, daemon=True).start()
    port = int(os.environ.get("PORT", 5000))
    log.info("JobFinder running on :%d", port)
    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)
