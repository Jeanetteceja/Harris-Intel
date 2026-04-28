"""
Texas Multi-County Motivated Seller Lead Scraper
=================================================
Counties : Harris, Fort Bend, Montgomery, Galveston, Dallas
Run      : python scraper/fetch.py
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import re
import zipfile
from datetime import datetime, timedelta
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright, TimeoutError as PWTimeout

try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

# =============================================================================
# COUNTY CONFIG — add more counties here any time!
# =============================================================================
COUNTIES = {
    "harris": {
        "name":      "Harris County",
        "state":     "TX",
        "clerk_url": "https://cclerk.hctx.net/web/search.aspx",
        "base_url":  "https://cclerk.hctx.net",
    },
    "fort_bend": {
        "name":      "Fort Bend County",
        "state":     "TX",
        "clerk_url": "https://www.fbctx.gov/departments/county_clerk/official_public_records/search.aspx",
        "base_url":  "https://www.fbctx.gov",
    },
    "montgomery": {
        "name":      "Montgomery County",
        "state":     "TX",
        "clerk_url": "https://mcrecords.co.montgomery.tx.us/",
        "base_url":  "https://mcrecords.co.montgomery.tx.us",
    },
    "galveston": {
        "name":      "Galveston County",
        "state":     "TX",
        "clerk_url": "https://www.galvestoncountytx.gov/county-clerk/official-public-records",
        "base_url":  "https://www.galvestoncountytx.gov",
    },
    "dallas": {
        "name":      "Dallas County",
        "state":     "TX",
        "clerk_url": "https://www.dallascounty.org/departments/countyclerk/officialrecords.php",
        "base_url":  "https://www.dallascounty.org",
    },
}

DOC_TYPE_MAP: dict[str, tuple[str, str]] = {
    "LP":       ("LP",      "Lis Pendens"),
    "NOFC":     ("NOFC",    "Notice of Foreclosure"),
    "TAXDEED":  ("TAXDEED", "Tax Deed"),
    "JUD":      ("JUD",     "Judgment"),
    "CCJ":      ("JUD",     "Certified Judgment"),
    "DRJUD":    ("JUD",     "Domestic Judgment"),
    "LNCORPTX": ("LN",     "Corp Tax Lien"),
    "LNIRS":    ("LN",     "IRS Lien"),
    "LNFED":    ("LN",     "Federal Lien"),
    "LN":       ("LN",     "Lien"),
    "LNMECH":   ("LN",     "Mechanic Lien"),
    "LNHOA":    ("LN",     "HOA Lien"),
    "MEDLN":    ("LN",     "Medicaid Lien"),
    "PRO":      ("PRO",    "Probate"),
    "NOC":      ("NOC",    "Notice of Commencement"),
    "RELLP":    ("RELLP",  "Release Lis Pendens"),
}

LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
OUTPUT_PATHS  = [Path("dashboard/records.json"), Path("data/records.json")]
GHL_OUTPUT    = Path("data/ghl_export.csv")

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 Chrome/122.0.0.0"})


# =============================================================================
# UTILITIES
# =============================================================================

def _normalise_date(raw: str) -> str:
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return raw.strip()


def _clean_amount(raw: str) -> float | None:
    s = re.sub(r"[^\d.]", "", raw or "")
    try:
        return float(s) if s else None
    except ValueError:
        return None


# =============================================================================
# SCORING
# =============================================================================

def compute_score_and_flags(rec: dict) -> tuple[int, list[str]]:
    flags: list[str] = []
    score = 30
    cat      = rec.get("cat", "")
    doc_type = rec.get("doc_type", "")
    amount   = rec.get("amount")
    filed    = rec.get("filed", "")
    owner    = (rec.get("owner") or "").upper()

    if cat == "LP":                                               flags.append("Lis pendens")
    if cat == "NOFC":                                             flags.append("Pre-foreclosure")
    if cat == "JUD":                                              flags.append("Judgment lien")
    if doc_type in ("LNCORPTX", "LNIRS", "LNFED", "TAXDEED"):   flags.append("Tax lien")
    if doc_type == "LNMECH":                                      flags.append("Mechanic lien")
    if cat == "PRO":                                              flags.append("Probate / estate")
    if re.search(r"\b(LLC|INC|CORP|LTD|TRUST)\b", owner):        flags.append("LLC / corp owner")

    try:
        if (datetime.now() - datetime.strptime(filed[:10], "%Y-%m-%d")).days <= 7:
            flags.append("New this week")
    except Exception:
        pass

    score += len(flags) * 10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    try:
        amt = float(amount)
        score += 15 if amt > 100_000 else 10 if amt > 50_000 else 0
    except (TypeError, ValueError):
        pass
    if "New this week" in flags:
        score += 5
    if rec.get("prop_address") or rec.get("mail_address"):
        score += 5

    return min(score, 100), flags


# =============================================================================
# SCRAPER
# =============================================================================

async def scrape_all_counties(date_from: str, date_to: str) -> list[dict]:
    all_records: list[dict] = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage"],
        )
        ctx = await browser.new_context(
            viewport={"width": 1280, "height": 900},
            user_agent="Mozilla/5.0 (X11; Linux x86_64) Chrome/122.0.0.0",
        )
        page = await ctx.new_page()

        for county_key, cfg in COUNTIES.items():
            log.info("── %s ──", cfg["name"])
            for dtype in DOC_TYPE_MAP:
                for attempt in range(1, 4):
                    try:
                        recs = await _search_county(page, cfg, dtype, date_from, date_to)
                        for r in recs:
                            r["county"]     = cfg["name"]
                            r["county_key"] = county_key
                        all_records.extend(recs)
                        if recs:
                            log.info("  %s %s → %d", cfg["name"], dtype, len(recs))
                        break
                    except PWTimeout:
                        log.warning("  timeout %s %s attempt %d", cfg["name"], dtype, attempt)
                    except Exception as exc:
                        log.warning("  error %s %s attempt %d: %s", cfg["name"], dtype, attempt, exc)
                    if attempt < 3:
                        await asyncio.sleep(3 * attempt)

        await browser.close()
    return all_records


async def _search_county(page, cfg: dict, dtype: str,
                          date_from: str, date_to: str) -> list[dict]:
    await page.goto(cfg["clerk_url"], timeout=30_000, wait_until="domcontentloaded")
    await page.wait_for_timeout(1_200)

    # Instrument type
    for sel in ['select[id*="Instrument"]', 'select[name*="Instrument"]',
                'select[id*="DocType"]',    'select[id*="instrument"]']:
        try:
            if await page.locator(sel).count():
                await page.select_option(sel, value=dtype, timeout=3_000)
                break
        except Exception:
            pass
    else:
        for sel in ['input[id*="Instrument"]', 'input[name*="DocType"]']:
            try:
                if await page.locator(sel).count():
                    await page.fill(sel, dtype, timeout=3_000)
                    break
            except Exception:
                pass

    # Date from
    for sel in ['input[id*="StartDate"]', 'input[name*="StartDate"]',
                'input[id*="DateFrom"]',  'input[id*="BeginDate"]',
                'input[id*="FiledFrom"]', 'input[name*="FromDate"]']:
        try:
            if await page.locator(sel).count():
                await page.fill(sel, date_from, timeout=3_000)
                break
        except Exception:
            pass

    # Date to
    for sel in ['input[id*="EndDate"]',  'input[name*="EndDate"]',
                'input[id*="DateTo"]',   'input[id*="StopDate"]',
                'input[id*="FiledTo"]',  'input[name*="ToDate"]']:
        try:
            if await page.locator(sel).count():
                await page.fill(sel, date_to, timeout=3_000)
                break
        except Exception:
            pass

    # Submit
    for sel in ['#btnSearch', 'input[value="Search"]',
                'button[type="submit"]', 'input[type="submit"]']:
        try:
            if await page.locator(sel).count():
                await page.click(sel, timeout=5_000)
                break
        except Exception:
            pass

    await page.wait_for_load_state("networkidle", timeout=25_000)

    records: list[dict] = []
    page_num = 0
    while True:
        page_num += 1
        html = await page.content()
        records.extend(_parse_table(html, dtype, cfg))
        if page_num >= 50:
            break
        nxt = page.locator(
            'a:text("Next"), a:text(">"), a:text("»"), '
            'input[value="Next"], .pagerNext, #lnkNext'
        ).first
        try:
            if not await nxt.is_visible(timeout=2_000):
                break
            await nxt.click(timeout=8_000)
            await page.wait_for_load_state("networkidle", timeout=20_000)
        except Exception:
            break

    return records


def _parse_table(html: str, dtype: str, cfg: dict) -> list[dict]:
    soup = BeautifulSoup(html, "lxml")
    records: list[dict] = []

    table = None
    for t in soup.find_all("table"):
        tid  = (t.get("id") or "").lower()
        tcls = " ".join(t.get("class") or []).lower()
        if any(k in tid + tcls for k in ("result", "grid", "data", "search", "record")):
            table = t
            break
    if not table:
        tables = soup.find_all("table")
        if not tables:
            return records
        table = max(tables, key=lambda t: len(t.find_all("tr")))

    rows = table.find_all("tr")
    if len(rows) < 2:
        return records

    headers = [th.get_text(" ", strip=True).upper()
               for th in rows[0].find_all(["th", "td"])]

    def col(cells, *kws):
        for kw in kws:
            for i, h in enumerate(headers):
                if kw in h and i < len(cells):
                    return cells[i].get_text(" ", strip=True)
        return ""

    cat, cat_label = DOC_TYPE_MAP.get(dtype, (dtype, dtype))

    for row in rows[1:]:
        cells = row.find_all(["td", "th"])
        if not cells or len(cells) < 2:
            continue
        try:
            doc_num = col(cells, "DOC", "INSTRUMENT", "NUMBER", "RECORD", "BOOK")
            filed   = col(cells, "FILE DATE", "RECORD DATE", "DATE", "FILED")
            grantor = col(cells, "GRANTOR", "OWNER", "FROM", "SELLER")
            grantee = col(cells, "GRANTEE", "TO", "LENDER", "BANK")
            legal   = col(cells, "LEGAL", "DESCR", "SUBDIV", "PROPERTY")
            amount  = col(cells, "AMOUNT", "CONSID", "DEBT", "FACE")

            href = ""
            link = row.find("a", href=True)
            if link:
                raw  = link["href"]
                href = raw if raw.startswith("http") else f"{cfg['base_url']}/{raw.lstrip('/')}"
                if not doc_num:
                    doc_num = link.get_text(strip=True)

            if not doc_num and not grantor:
                continue

            records.append({
                "doc_num":      doc_num.strip(),
                "doc_type":     dtype,
                "filed":        _normalise_date(filed) if filed else "",
                "cat":          cat,
                "cat_label":    cat_label,
                "owner":        grantor.strip(),
                "grantee":      grantee.strip(),
                "amount":       _clean_amount(amount),
                "legal":        legal.strip(),
                "clerk_url":    href,
                "prop_address": "",
                "prop_city":    "",
                "prop_state":   cfg["state"],
                "prop_zip":     "",
                "mail_address": "",
                "mail_city":    "",
                "mail_state":   cfg["state"],
                "mail_zip":     "",
            })
        except Exception as exc:
            log.debug("Row parse error: %s", exc)

    return records


# =============================================================================
# ENRICH / DEDUP / OUTPUT
# =============================================================================

def enrich(records: list[dict]) -> list[dict]:
    for rec in records:
        try:
            rec["score"], rec["flags"] = compute_score_and_flags(rec)
        except Exception:
            rec.setdefault("score", 30)
            rec.setdefault("flags", [])
    return records


def deduplicate(records: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out: list[dict] = []
    for r in records:
        key = f"{r.get('county_key')}|{r.get('doc_num')}|{r.get('doc_type')}|{r.get('filed')}"
        if key not in seen:
            seen.add(key)
            out.append(r)
    return out


def save_json(records: list[dict], date_from: str, date_to: str) -> None:
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Texas Multi-County Clerk Records",
        "counties":     list({r.get("county", "") for r in records}),
        "date_range":   {"from": date_from, "to": date_to},
        "total":        len(records),
        "with_address": sum(1 for r in records
                            if r.get("prop_address") or r.get("mail_address")),
        "records":      records,
    }
    for path in OUTPUT_PATHS:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        log.info("Saved → %s  (%d records)", path, len(records))


def save_ghl_csv(records: list[dict]) -> None:
    GHL_OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    fields = [
        "First Name","Last Name",
        "Mailing Address","Mailing City","Mailing State","Mailing Zip",
        "Property Address","Property City","Property State","Property Zip",
        "County","Lead Type","Document Type","Date Filed","Document Number",
        "Amount/Debt Owed","Seller Score","Motivated Seller Flags",
        "Source","Public Records URL",
    ]
    with open(GHL_OUTPUT, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields)
        w.writeheader()
        for r in records:
            parts = re.sub(r"[,]+"," ", r.get("owner","")).strip().split(None,1)
            w.writerow({
                "First Name":             parts[0] if parts else "",
                "Last Name":              parts[1] if len(parts)>1 else "",
                "Mailing Address":        r.get("mail_address",""),
                "Mailing City":           r.get("mail_city",""),
                "Mailing State":          r.get("mail_state","TX"),
                "Mailing Zip":            r.get("mail_zip",""),
                "Property Address":       r.get("prop_address",""),
                "Property City":          r.get("prop_city",""),
                "Property State":         r.get("prop_state","TX"),
                "Property Zip":           r.get("prop_zip",""),
                "County":                 r.get("county",""),
                "Lead Type":              r.get("cat_label",""),
                "Document Type":          r.get("doc_type",""),
                "Date Filed":             r.get("filed",""),
                "Document Number":        r.get("doc_num",""),
                "Amount/Debt Owed":       r.get("amount") or "",
                "Seller Score":           r.get("score",30),
                "Motivated Seller Flags": "; ".join(r.get("flags",[])),
                "Source":                 r.get("county","") + " Clerk",
                "Public Records URL":     r.get("clerk_url",""),
            })
    log.info("GHL CSV → %s  (%d rows)", GHL_OUTPUT, len(records))


# =============================================================================
# MAIN
# =============================================================================

async def main() -> None:
    now       = datetime.now()
    date_to   = now.strftime("%m/%d/%Y")
    date_from = (now - timedelta(days=LOOKBACK_DAYS)).strftime("%m/%d/%Y")

    log.info("Texas Multi-County Scraper | %s → %s", date_from, date_to)
    log.info("Counties: %s", ", ".join(c["name"] for c in COUNTIES.values()))

    raw      = await scrape_all_counties(date_from, date_to)
    unique   = deduplicate(raw)
    enriched = enrich(unique)
    enriched.sort(key=lambda r: r.get("score", 0), reverse=True)

    d_from = datetime.strptime(date_from, "%m/%d/%Y").strftime("%Y-%m-%d")
    d_to   = datetime.strptime(date_to,   "%m/%d/%Y").strftime("%Y-%m-%d")
    save_json(enriched, d_from, d_to)
    save_ghl_csv(enriched)

    log.info("── Summary ──")
    for cfg in COUNTIES.values():
        n = sum(1 for r in enriched if r.get("county") == cfg["name"])
        log.info("  %-25s %d leads", cfg["name"], n)
    log.info("TOTAL: %d | Done ✓", len(enriched))


if __name__ == "__main__":
    asyncio.run(main())
