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


def Job(title, company, location, jtype, salary, date, desc, full_desc, url, source, category, skills):
    return {
        "title": title, "company": company, "location": location,
        "type": jtype, "salary": salary, "date": date,
        "description": desc, "full_description": full_desc,
        "url": url, "source": source,
        "category": category, "skills": skills,
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


# Category keywords lookup
CATEGORIES = {
    "Engineering": ["developer", "engineer", "software", "backend", "frontend", "fullstack",
                     "devops", "data scientist", "data analyst", "qa", "infrastructure", "sre",
                     "architect", "programmer", "coder", "tech lead", "engineering", "systems"],
    "Marketing": ["marketing", "growth", "seo", "sem", "content", "social media", "brand",
                   "pr", "communications", "digital marketing", "market", "campaign"],
    "Design": ["designer", "design", "ux", "ui", "product design", "graphic", "visual",
                "creative", "art", "figma", "sketch"],
    "Sales": ["sales", "account executive", "bdr", "sdr", "business development",
               "revenue", "partnership", "customer success", "account manager"],
    "Finance": ["finance", "accounting", "audit", "tax", "financial", "controller",
                 "cfo", "budget", "analyst - finance"],
    "HR / Recruiting": ["hr", "recruiter", "talent", "people", "human resources",
                         "hiring", "onboarding", "workforce"],
    "Product": ["product manager", "product owner", "pm", "product", "program manager"],
    "Operations": ["operations", "operations manager", "coordinator", "administrative",
                    "office manager", "logistics", "supply chain"],
    "Data": ["data scientist", "data engineer", "data analyst", "machine learning",
              "ml engineer", "analytics", "bi", "statistics"],
    "Medical / Health": ["doctor", "nurse", "medical", "healthcare", "clinical",
                          "pharma", "patient", "therapist", "surgeon"],
    "Legal": ["lawyer", "attorney", "legal", "compliance", "counsel", "paralegal"],
    "Education": ["teacher", "professor", "instructor", "education", "tutor",
                   "curriculum", "training"],
}

# Skills to detect in descriptions
SKILL_KEYWORDS = {
    # Languages
    "Python": r'\bpython\b', "JavaScript": r'\bjavascript\b', "TypeScript": r'\btypescript\b',
    "Java": r'\bjava\b', "C++": r'\bc\+\+\b', "C#": r'\bc#\b',
    "Ruby": r'\bruby\b', "PHP": r'\bphp\b', "Swift": r'\bswift\b', "Kotlin": r'\bkotlin\b',
    "Rust": r'\brust\b', "Go": r'\bgolang\b', "SQL": r'\bsql\b',
    # Frontend
    "React": r'\breact\b', "Angular": r'\bangular\b', "Vue.js": r'\bvue\b',
    "HTML/CSS": r'\bhtml\b',
    # Backend / Frameworks
    "Node.js": r'\bnode\.js\b', "Django": r'\bdjango\b', "Flask": r'\bflask\b',
    "Rails": r'\brails\b', "Spring": r'\bspring\b', "FastAPI": r'\bfastapi\b',
    ".NET": r'\b\.net\b',
    # Cloud / Infra
    "AWS": r'\baws\b', "GCP": r'\bgcp\b', "Azure": r'\bazure\b',
    "Docker": r'\bdocker\b', "Kubernetes": r'\bkubernetes\b', "Terraform": r'\bterraform\b',
    "CI/CD": r'\bci/cd\b',
    # Data / AI / ML
    "Machine Learning": r'\bmachine learning|ml\b',
    "Data Analysis": r'\bdata (analysis|science|analytics)\b',
    "Power BI": r'\bpower bi\b', "Tableau": r'\btableau\b',
    # Tools & Methods
    "Git": r'\bgit\b', "Linux": r'\blinux\b',
    "Agile / Scrum": r'\bagile|scrum\b', "REST API": r'\brest\b', "GraphQL": r'\bgraphql\b',
    "A/B Testing": r'\ba/b testing\b',
    # Business
    "Project Mgmt": r'\bproject management\b',
    "CRM": r'\bcrm\b', "SEO": r'\bseo\b',
}


def extract_job_meta(full_text, job_title, job_type):
    """Extract category and skills from job data."""
    combined = ((job_title or "") + " " + (full_text or "")).lower()

    # Detect category
    detected_cat = "Other"
    best_score = 0
    for cat, keywords in CATEGORIES.items():
        score = sum(2 if kw in combined else 0 for kw in keywords)
        if score > best_score:
            best_score = score
            detected_cat = cat

    # Detect skills from text (max 6)
    found_skills = []
    for skill, pattern in SKILL_KEYWORDS.items():
        if re.search(pattern, combined, re.I):
            found_skills.append(skill)
    found_skills = found_skills[:6]

    return detected_cat, found_skills


def extract_best_content(full_text, job_title):
    """Extract ultra-concise keyword preview + full clean expanded text."""
    if not full_text or len(full_text.strip()) < 20:
        return full_text or "", ""

    text = full_text.strip()
    lower = text.lower()

    bits = []

    # Tech skills detected
    skills_found = []
    for skill in ["Python","JavaScript","TypeScript","Java","C++","C#","Ruby","PHP","Rust","SQL",
                  "React","Angular","Vue.js","HTML/CSS","Node.js","Django","Flask","Rails","Spring",
                  "AWS","GCP","Azure","Docker","Kubernetes","Terraform","Git","Linux",
                  "Machine Learning","TensorFlow","PyTorch","Tableau","Power BI"]:
        sk = re.search(r'\b' + re.escape(skill.lower()) + r'\b', lower)
        if sk and skill not in skills_found:
            skills_found.append(skill)
    if skills_found:
        bits.append("Skills: " + ", ".join(skills_found[:4]))

    # Salary (only meaningful amounts: 4+ digits or 2 digits + k)
    sal = re.search(r'[$\u20ac\u00a3][\d,]+(?:k|K|,\d{3})(?:\s*[-\u2013]\s*[$\u20ac\u00a3]?[\d,]+(?:k|K|,\d{3}))?', text)
    if not sal:
        sal = re.search(r'[$\u20ac\u00a3]([5-9]\d{3}|\d{5,})(?:\s*[-\u2013]\s*[$\u20ac\u00a3]?[\d,]+)?', text)
    if sal:
        bits.append(sal.group(0))

    # Experience years
    exp = re.search(r'(\d+)\s*\+?\s*years?\s*(of\s*)?experience', lower)
    if exp:
        bits.append(exp.group(0).strip())

    # Remote / location
    if 'remote' in lower:
        bits.append("Remote")

    # Build preview
    if bits:
        preview = " • ".join(bits)
        if len(preview) > 150:
            preview = preview[:147] + "…"
        # If only 'Remote' found, add a short meaningful sentence too
        if preview.strip().lower() == 'remote':
            sentences = re.split(r'[.\n]', text)
            good = [s.strip() for s in sentences if len(s.strip()) > 40 and not re.search(r'(?i)(headquarters|url:|thank you|equal opportunity|diverse)', s)]
            if good:
                preview = good[0][:80] + "…" if len(good[0]) > 80 else good[0]
    else:
        # Grab a short meaningful sentence
        sentences = re.split(r'[.\n]', text)
        good = [s.strip() for s in sentences if len(s.strip()) > 40 and not re.search(r'(?i)(headquarters|url:|thank you|equal opportunity|diverse)', s)]
        preview = (good[0][:150] + "…") if good else (text[:150] + "…")
        if len(preview) <= 155:
            preview = preview.replace("…", "").strip()

    # Expanded: full clean text, no repeat of preview
    expanded = re.sub(r'(?i)\b(headquarters|url|compensation amount|updated|location|type)\s*:.*?(?=\b[a-z]|$)', '', text)
    expanded = re.sub(r'(?i)(thank you for|thanks for|we are an equal|all qualified applicants|diverse workforce|encourages applications).*?(\.|$)', '', expanded)
    expanded = expanded.strip()
    if not expanded:
        expanded = text
    preview_stripped = preview.replace('…', '').strip()
    if expanded.startswith(preview_stripped) and len(expanded) > len(preview_stripped):
        expanded = expanded[len(preview_stripped):].strip()
    if len(expanded) > 397:
        expanded = expanded[:397] + "…"
    if not expanded:
        expanded = preview

    return preview, expanded


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
            preview, expanded = extract_best_content(desc_text, title)
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
            category, skills = extract_job_meta(desc_text, title, jtype)
            jobs.append(Job(title, company, loc, jtype, salary,
                           date_posted, preview, expanded, link, "We Work Remotely", category, skills))
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
            desc_raw = item.get("description", "")
            desc = BeautifulSoup(desc_raw, "html.parser").get_text(separator=" ", strip=True) if desc_raw else ""
            preview, expanded = extract_best_content(desc, title)
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
            category, skills = extract_job_meta(desc, title, jtype)
            jobs.append(Job(title, company, loc, jtype,
                           salary, date_posted, preview, expanded, link, "RemoteOK", category, skills))
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
            preview, expanded = extract_best_content(clean, title)

            category, skills = extract_job_meta(clean, title, jtype)
            jobs.append(Job(title, company, location, jtype,
                           salary, date_posted, preview, expanded, link, "The Muse", category, skills))
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
