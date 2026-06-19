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
    """Extract smart preview + concise expandable text from a job description.
    Strips noise, picks the most work-relevant paragraphs.
    Returns (preview, expanded)."""
    if not full_text or len(full_text.strip()) < 20:
        return full_text or "", ""

    text = full_text.strip()

    # Common RSS/intro noise lines to filter out entirely
    noise_prefixes = (
        "headquarters:", "url:", "compensation amount:",
        "please mention the word", "when applying, tell them you found",
        "updated:", "location:", "type:",
    )

    # Corporate blabla — big penalty
    blabla_patterns = (
        "about the company", "our story", "company description",
        "at our company", "is an equal", "we are an equal",
        "thank you for", "thanks for", "our mission",
        "we're building a world", "we are a high growth",
        "we are dedicated to", "we are committed to",
        "we are looking to hire", "all qualified applicants",
        "diverse workforce", "we celebrate", "we strive",
        "join our team of", "we're proud to", "we're excited to",
        "does not discriminate", "encourages applications from",
    )

    # Keywords that signal real job content
    work_signal_words = (
        "salary", "requirements", "qualifications", "experience",
        "skills", "responsibilities", "what you\'ll do", "about you",
        "we\'re looking for", "you have", "you\'ll work", "you will",
        "must have", "nice to have", "preferred",
        "what we\'re looking for", "who you are",
        "key responsibilities", "role requirements",
        "your profile", "what you bring",
        "budget", "equity", "benefits", "compensation",
        "tech stack", "tools", "framework", "platform",
        "senior", "lead", "head of", "manager",
        "years of experience", "bachelor", "degree",
        "proficiency in", "expertise in", "knowledge of",
        "familiar with", "experience with", "fluent in",
    )

    # Tech keywords for bonus
    tech_keywords = (
        r'\b(python|javascript|react|node(\.js)?|aws|docker|sql|api|'
        r'kubernetes|git|linux|agile|typescript|java|c\+\+|ruby|'
        r'go|rust|swift|kotlin|flutter|html|css|mongodb|postgres|'
        r'redis|graphql|tensorflow|pytorch|docker|terraform|ansible|'
        r'ci/cd|rest|grpc|microservices|machine.?learning|ai)\b'
    )

    # Split into paragraphs (handle both newline-separated and flat text)
    paragraphs = [p.strip() for p in re.split(r'\n{2,}', text) if len(p.strip()) > 15]
    if len(paragraphs) <= 1:
        # Flat text — split on sentences and group, or split on noise markers
        # First try splitting on known noise markers (common in RSS feeds)
        markers = [
            r'(?i)headquarters:', r'(?i)url:', r'(?i)compensation amount:',
            r'(?i)about (the company|our)', r'(?i)job summary',
            r'(?i)updated:', r'(?i)location:', r'(?i)type:',
        ]
        split_points = []
        for m in markers:
            for match in re.finditer(m, text):
                split_points.append(match.start())
        if split_points:
            # Sort points, split text at those positions
            split_points = sorted(set(split_points))
            parts = []
            for i, sp in enumerate(split_points):
                end = split_points[i+1] if i+1 < len(split_points) else len(text)
                part = text[sp:end].strip()
                if len(part) > 20:
                    parts.append(part)
            if parts:
                paragraphs = parts
        if len(paragraphs) <= 1:
            # Final fallback: split on sentences
            sentences = [s.strip() + "." for s in text.split(". ") if len(s.strip()) > 30]
            if sentences:
                paragraphs = sentences

    def is_noise(p):
        """Check if paragraph is purely noise (no work content)."""
        pl = p.lower().strip()
        # Lines that are just metadata
        if pl.startswith(noise_prefixes):
            return True
        # Mostly URLs or location info
        if re.search(r'^https?://', pl) or re.match(r'^(headquarters|location|compensation|url)\s*:', pl, re.I):
            return True
        return False

    def score_para(p):
        pl = p.lower()
        score = 0

        # Penalize blabla heavily
        for bp in blabla_patterns:
            if bp in pl:
                score -= 10
                break

        # Penalize noise lines
        if is_noise(p):
            return -100  # Effectively eliminate

        # Boost for work signal words
        for sw in work_signal_words:
            if sw in pl:
                score += 3

        # Boost for matching job title keywords
        title_lower = job_title.lower() if job_title else ""
        title_words = set(re.findall(r'[a-zA-Z]+', title_lower))
        stopwords = {"a", "an", "the", "and", "or", "of", "in", "to", "for",
                     "with", "on", "at", "by", "is", "are", "it", "as",
                     "we", "our", "us", "you", "your", "job", "role",
                     "position", "new", "all", "about"}
        title_words -= stopwords
        para_words = set(re.findall(r'[a-zA-Z]+', pl))
        score += len(para_words & title_words) * 3

        # Bonus for salary mentioned
        if re.search(r'[\$€£]', p):
            score += 5

        # Bonus for tech keywords
        if re.search(tech_keywords, pl):
            score += 3

        # Boost for concise, focused paragraphs (50-250 chars)
        if 50 < len(p) < 250:
            score += 2
        elif len(p) > 400:
            score -= 1

        return score

    # Filter and score
    scored = [(score_para(p), p) for p in paragraphs if score_para(p) > -50]
    scored.sort(key=lambda x: x[0], reverse=True)

    if not scored:
        # Fallback: preview = first 200 chars, expanded = rest (no repeat)
        if len(text) > 200:
            preview = text[:200] + "…"
            expanded = text[200:]
        else:
            preview = text
            expanded = ""
        if len(expanded) > 397:
            expanded = expanded[:397] + "…"
        if not expanded:
            expanded = preview
        return preview, expanded

    # Preview: best scoring paragraph, max 200 chars
    best_para = scored[0][1]
    preview_source = best_para.strip()
    preview = preview_source[:200] + "…" if len(preview_source) > 200 else preview_source

    # Expanded: next best paragraphs up to 400 chars, SKIP the preview source
    expanded_parts = []
    length = 0
    for _, p in scored:
        if len(expanded_parts) >= 3:
            break
        # Skip the paragraph used for preview
        if p.strip() == preview_source:
            continue
        # Deduplicate
        if any(expanded_parts and (p[:60] in ep or ep[:60] in p) for ep in expanded_parts):
            continue
        if length + len(p) > 400:
            remaining = 400 - length - 3
            if remaining > 40:
                expanded_parts.append(p[:remaining] + "…")
            break
        expanded_parts.append(p)
        length += len(p)

    if expanded_parts:
        expanded = "\n\n".join(expanded_parts)
        if len(expanded) > 400:
            expanded = expanded[:397] + "…"
    else:
        # No other paragraphs — show remaining of preview source
        rest = preview_source[200:] if len(preview_source) > 200 else ""
        expanded = rest[:397] + "…" if len(rest) > 400 else rest

    # Final safety: strip any preview overlap from expanded
    preview_plain = preview.replace("…", "").strip()
    while expanded.startswith(preview_plain) and len(expanded) > len(preview_plain):
        expanded = expanded[len(preview_plain):].strip()
    if not expanded:
        # No rest — just return preview as both
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
