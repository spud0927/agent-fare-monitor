"""
SFO ↔ SAN 2-night roundtrip fare monitor — Southwest sale-cycle aware.

Schedule:
  - Tuesday AM PT: Checks the advance sale window (21–70 days out)

Southwest drops fares every Tuesday and they revert Thursday, so the scan has
to run weekly on Tuesday. Sampling any less often than weekly would alias a
weekly signal: a date whose sale price swings $49/$69 week to week would be
observed at only half its sale events, with no way to tell which half — which
would also drag the historical baseline upward and corrupt the LLM comparison.

The Wednesday "last-minute" tier (7–14 days out) was removed. It was built on
the assumption that Southwest's Wanna Get Away Wednesday promo produced real
discounts on this route; in practice it never surfaced a deal worth acting on,
and it cost ~26 searches per billing cycle.

Uses SERPAPI for fares and Google Gemini Flash for evaluation.

Repo structure:
  sfo-san-fare-monitor/
  ├── fare_monitor/
  │   ├── __init__.py
  │   ├── monitor.py          ← this file
  │   ├── fare_history.db     ← auto-created, .gitignore it
  │   └── reports/            ← generated reports land here
  ├── .github/workflows/
  │   └── fare-monitor.yml    ← cron trigger
  └── requirements.txt        ← google-search-results, google-genai
"""

import os
import json
import sqlite3
import smtplib
import time
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta, date, timezone
from pathlib import Path
from enum import Enum

# ── Config ────────────────────────────────────────────────────────────────────

ORIGIN = "SFO"
DESTINATION = "SAN"
TRIP_NIGHTS = 2

# Valid departure days (Mon=0 ... Sun=6)
# Mon→Wed, Tue→Thu, Wed→Fri, Thu→Sat, Sun→Tue
# Excluded: Fri(4), Sat(5)
VALID_DEPARTURE_DAYS = {0, 1, 2, 3, 6}  # Mon, Tue, Wed, Thu, Sun

BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "fare_history.db"
REPORT_DIR = BASE_DIR / "reports"
REPORT_DIR.mkdir(exist_ok=True)
CACHE_DIR = BASE_DIR / "cache"
CACHE_DIR.mkdir(exist_ok=True)

# Set to True during development to reuse cached SERPAPI responses.
# Set to False (or use env var) for real runs.
USE_CACHE = os.environ.get("FARE_USE_CACHE", "false").lower() == "true"

# ── Budget ────────────────────────────────────────────────────────────────────
# ~36 date pairs per Tuesday sweep × ~4.3 Tuesdays = ~155 searches per cycle.
MONTHLY_SEARCH_BUDGET = 250

# Searches left untouched for swa-checker (southwest-tracker/swa-checker), which
# shares this SerpApi key. That repo re-prices flights already booked, so a
# missed run costs real money in unclaimed rebooking credit; this one is
# speculative shopping. When quota is tight, this monitor yields.
#
# Sized for swa-checker's worst case: flights are booked as one-ways, so each
# leg is its own search, and the itinerary list peaks around 20 legs. Its
# 18-day floor caps the practical per-run peak nearer 16, but the extra headroom
# is cheap insurance — swa-checker also runs twice a week (Tue and Wed) against
# this monitor's once, so it drains the shared pool faster than it looks.
RESERVE_FOR_OTHER_REPOS = 25

# ── LLM config ────────────────────────────────────────────────────────────────
GEMINI_MODEL = "gemini-2.5-flash"


# ── Sale tiers ────────────────────────────────────────────────────────────────

class SaleTier(Enum):
    # "last_minute" rows still exist in fare_history from before the Wednesday
    # tier was retired. The enum member is gone, so get_historical_summary()
    # simply never queries them — they stay in the DB but no longer influence
    # any baseline.
    ADVANCE = "advance"           # Tuesday scan: 21–70 days out


def get_sale_tier(run_day: int | None = None) -> SaleTier | None:
    """Determine which sale tier to scan based on the day of the week."""
    if run_day is None:
        run_day = date.today().weekday()
    if run_day == 1:    # Tuesday
        return SaleTier.ADVANCE
    return None


TIER_CONFIG = {
    SaleTier.ADVANCE: {
        "label": "Advance Sale (Tue sale)",
        "start_offset_days": 21,
        "end_offset_days": 70,
        "description": (
            "Tuesday advance sale window. Fares from 21–70 days out. "
            "Southwest typically drops prices across 2-3 months of inventory. "
            "Prices should be meaningfully lower than last-minute fares. "
            "A 'good deal' here is evaluated against a lower baseline."
        ),
    },
}


# ── Date generation ───────────────────────────────────────────────────────────

def generate_search_dates(tier: SaleTier) -> list[tuple[str, str]]:
    """Generate all valid (depart, return) pairs for the given sale tier."""
    today = date.today()
    config = TIER_CONFIG[tier]
    start = today + timedelta(days=config["start_offset_days"])
    end = today + timedelta(days=config["end_offset_days"])

    pairs = []
    current = start
    while current <= end:
        if current.weekday() in VALID_DEPARTURE_DAYS:
            return_date = current + timedelta(days=TRIP_NIGHTS)
            pairs.append((current.isoformat(), return_date.isoformat()))
        current += timedelta(days=1)

    return pairs


# ── Fare fetching (uses your existing SERPAPI setup) ──────────────────────────

def fetch_fares(depart_date: str, return_date: str) -> dict:
    """Fetch roundtrip fares from SERPAPI Google Flights for one date pair."""
    cache_file = CACHE_DIR / f"{ORIGIN}_{DESTINATION}_{depart_date}_{return_date}.json"

    # Return cached result if available and caching is enabled
    if USE_CACHE and cache_file.exists():
        results = json.loads(cache_file.read_text())
    else:
        from serpapi import GoogleSearch

        params = {
            "engine": "google_flights",
            "departure_id": ORIGIN,
            "arrival_id": DESTINATION,
            "outbound_date": depart_date,
            "return_date": return_date,
            "currency": "USD",
            "hl": "en",
            "type": "1",
            "api_key": os.environ["SERPAPI_KEY"],
        }
        results = GoogleSearch(params).get_dict()

        # get_dict() never raises on a non-2xx response — a rejected/out-of-quota
        # request comes back as a 200 with an "error" body, which would otherwise
        # look identical to a legitimate "no flights found" result.
        if "error" in results:
            raise RuntimeError(f"SerpApi error: {results['error']}")

        # Always write to cache so future debug runs can reuse
        cache_file.write_text(json.dumps(results, indent=2))

    best = results.get("best_flights", [])
    other = results.get("other_flights", [])

    return {
        "depart_date": depart_date,
        "return_date": return_date,
        "best_flights": best,
        "other_flights": other,
        "flight_count": len(best) + len(other),
    }


def fetch_all_fares(tier: SaleTier, date_pairs: list[tuple[str, str]] | None = None) -> list[dict]:
    """Fetch fares for the given date pairs (defaults to the tier's full window).

    generate_search_dates() returns pairs nearest-departure first, so a caller
    that trims the list keeps the dates the user can still act on.
    """
    if date_pairs is None:
        date_pairs = generate_search_dates(tier)
    config = TIER_CONFIG[tier]
    all_results = []

    print(f"  Window: {config['label']}")
    print(f"  Date pairs to search: {len(date_pairs)}\n")

    for depart, ret in date_pairs:
        try:
            result = fetch_fares(depart, ret)
            result["tier"] = tier.value
            all_results.append(result)
            print(f"    ✓ {depart} → {ret}: {result['flight_count']} options")
        except Exception as e:
            print(f"    ✗ {depart} → {ret}: {e}")
            print(f"  ⛔ Stopping this tier's scan early — a failed search usually "
                  f"means the SerpApi account is rate-limited or out of quota, and "
                  f"the remaining {len(date_pairs) - len(all_results) - 1} calls "
                  f"would just burn budget for nothing.")
            break

    return all_results


# ── Price history (SQLite) ────────────────────────────────────────────────────

def init_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fare_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scanned_at TEXT NOT NULL,
            tier TEXT NOT NULL,
            depart_date TEXT NOT NULL,
            return_date TEXT NOT NULL,
            price INTEGER,
            airline TEXT,
            stops INTEGER,
            duration_minutes INTEGER,
            raw_json TEXT
        )
    """)
    # Tracks which quota emails have gone out, so the "you're out of searches"
    # alert fires once per billing cycle rather than every Tuesday. Keyed by
    # cycle start, so it resets itself when the plan renews.
    conn.execute("""
        CREATE TABLE IF NOT EXISTS quota_notices (
            cycle_start TEXT NOT NULL,
            kind TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            PRIMARY KEY (cycle_start, kind)
        )
    """)
    conn.commit()
    return conn


def quota_notice_sent(conn: sqlite3.Connection, cycle_start: date, kind: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM quota_notices WHERE cycle_start = ? AND kind = ?",
        (cycle_start.isoformat(), kind),
    ).fetchone()
    return row is not None


def record_quota_notice(conn: sqlite3.Connection, cycle_start: date, kind: str):
    conn.execute(
        "INSERT OR REPLACE INTO quota_notices (cycle_start, kind, sent_at) VALUES (?,?,?)",
        (cycle_start.isoformat(), kind, datetime.now(timezone.utc).isoformat()),
    )
    conn.commit()


def save_fares(conn: sqlite3.Connection, all_results: list[dict]):
    """Persist today's fares for historical comparison."""
    now = datetime.now(timezone.utc).isoformat()
    rows = []
    for result in all_results:
        tier = result.get("tier", "unknown")
        for flight in result["best_flights"] + result["other_flights"]:
            price = flight.get("price")
            legs = flight.get("flights", [])
            airline = legs[0].get("airline", "Unknown") if legs else "Unknown"
            stops = max(len(legs) - 1, 0)
            duration = flight.get("total_duration", 0)

            rows.append((
                now, tier,
                result["depart_date"], result["return_date"],
                price, airline, stops, duration,
                json.dumps(flight),
            ))

    conn.executemany(
        """INSERT INTO fare_history
           (scanned_at, tier, depart_date, return_date, price, airline, stops, duration_minutes, raw_json)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        rows,
    )
    conn.commit()
    print(f"  💾 Saved {len(rows)} fare records to history.")


def get_historical_summary(conn: sqlite3.Connection) -> dict:
    """Pull summary stats from past scans, broken out by tier."""
    summary = {}
    for tier in SaleTier:
        rows = conn.execute("""
            SELECT
                MIN(price), ROUND(AVG(price),2), MAX(price),
                COUNT(*), COUNT(DISTINCT scanned_at),
                MIN(scanned_at), MAX(scanned_at)
            FROM fare_history
            WHERE price IS NOT NULL AND tier = ?
        """, (tier.value,)).fetchone()

        buckets = []
        if tier == SaleTier.ADVANCE:
            buckets = conn.execute("""
                SELECT
                    CAST((julianday(depart_date) - julianday(scanned_at)) / 7 AS INTEGER) as weeks_out,
                    MIN(price), ROUND(AVG(price),2), MAX(price), COUNT(*)
                FROM fare_history
                WHERE price IS NOT NULL AND tier = ?
                GROUP BY weeks_out
                ORDER BY weeks_out
            """, (tier.value,)).fetchall()

        summary[tier.value] = {
            "min": rows[0], "avg": rows[1], "max": rows[2],
            "total_fares": rows[3], "scan_count": rows[4],
            "first_scan": rows[5], "last_scan": rows[6],
            "by_weeks_out": [
                {"weeks_out": b[0], "min": b[1], "avg": b[2], "max": b[3], "count": b[4]}
                for b in buckets
            ] if buckets else [],
        }

    return summary


def get_cycle_start(renewal_date: date | None) -> date:
    """First day of the current SerpApi billing cycle.

    The plan renews on a fixed day of the month (the 13th on this account),
    NOT the 1st. Counting from the 1st meant that from the 1st through the
    12th the local tally reported a fresh 250 while the real account was
    still spending down the cycle that began on the 13th — the window in
    which this monitor happily burned through an already-exhausted quota.

    Falls back to the 1st only when the renewal date is unavailable, which
    is the old (wrong) behaviour but better than refusing to run.
    """
    today = date.today()
    if renewal_date is None:
        return today.replace(day=1)

    # renewal_date is the NEXT renewal; step back one month to find the start
    # of the cycle we're currently inside.
    day = renewal_date.day
    if today.day >= day:
        return today.replace(day=min(day, 28))
    prev = (today.replace(day=1) - timedelta(days=1))
    return prev.replace(day=min(day, 28))


def get_cycle_usage(conn: sqlite3.Connection, renewal_date: date | None) -> int:
    """Count searches this script has made in the current billing cycle.

    Note this only sees searches that returned at least one flight — a search
    that came back empty inserts no rows and is invisible here. It is a floor,
    not a true count, which is why get_serpapi_remaining() takes precedence.
    """
    cycle_start = get_cycle_start(renewal_date).isoformat()
    row = conn.execute("""
        SELECT COUNT(DISTINCT depart_date || '|' || return_date || '|' || scanned_at)
        FROM fare_history
        WHERE scanned_at >= ?
    """, (cycle_start,)).fetchone()
    return row[0] if row[0] else 0


def already_scanned_today(conn: sqlite3.Connection, tier: SaleTier) -> bool:
    """Check whether this tier has already been scanned today.

    Guards against a manual workflow_dispatch (or an accidental double
    schedule) re-running a full tier scan on top of one that already ran —
    the SerpApi budget doesn't reset just because someone re-triggered it.
    """
    today = date.today().isoformat()
    row = conn.execute(
        "SELECT COUNT(*) FROM fare_history WHERE tier = ? AND date(scanned_at) = ?",
        (tier.value, today),
    ).fetchone()
    return row[0] > 0


def get_serpapi_account() -> dict | None:
    """Ask SerpApi directly about quota and billing cycle.

    The local DB count in get_cycle_usage() only sees what this script has
    used and only counts searches that returned data — it's blind to usage
    from swa-checker (which shares this key) and to searches SerpApi rejected
    for being over quota. This is the only source that reflects reality.

    Does not consume a search.
    """
    from serpapi import GoogleSearch

    try:
        account = GoogleSearch({"api_key": os.environ["SERPAPI_KEY"]}).get_account()
    except Exception as e:
        print(f"  ⚠️  Could not reach SerpApi account API: {e}")
        return None

    if "error" in account:
        print(f"  ⚠️  SerpApi account API error: {account['error']}")
        return None

    return account


def parse_renewal_date(account: dict | None) -> date | None:
    """Pull plan_renewal_date out of the account payload, if present."""
    if not account:
        return None
    raw = account.get("plan_renewal_date")
    if not raw:
        return None
    try:
        return datetime.strptime(str(raw)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


# ── LLM evaluation (Gemini Flash) ────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a personal Southwest Airlines fare analyst for SFO ↔ SAN roundtrip fares
(2-night trips). You understand Southwest's pricing rhythm deeply.

## Southwest's sale cycle
- **Tuesday morning**: Southwest runs a sale on flights 21+ days out, typically
  impacting 2-3 months of fares. These "advance sale" prices are the lowest
  baseline for this route. Prices revert to normal on Thursday.
- The sale runs most Tuesdays whether or not Southwest announces it publicly.
- Crucially, the sale price for the SAME departure date varies week to week —
  a date at $49 one Tuesday may be $69 the next. So a fare being higher than
  a past observation does not mean the sale is absent; it means this week's
  discount on that date is shallower.

## You are evaluating: {tier_label}
{tier_description}

## Evaluation approach

- These are the cheapest fares available for this route; compare against the
  historical advance-tier data supplied below
- Good deals: prices at or near historical lows for this tier
- A fare that looks cheap in absolute terms but is average for this tier is NOT
  a deal
- The user books immediately when a good fare lands on a date that fits a trip
  they were already planning, so favour precision over volume — a false alarm
  costs them more than a missed marginal fare
- Nonstop strongly preferred on this short route
- Midday/early evening departures preferred over red-eyes or early AM
- Note if shifting dates by a day or two shows meaningful savings
- Consider day-of-week patterns (some days consistently cheaper)

{price_guidance}

## Output format
Return ONLY valid JSON — no markdown fences, no commentary outside the JSON:
{{
  "should_notify": true/false,
  "tier": "{tier_value}",
  "summary": "2-3 sentence natural language summary",
  "top_deals": [
    {{
      "depart_date": "YYYY-MM-DD",
      "return_date": "YYYY-MM-DD",
      "depart_day": "Monday",
      "return_day": "Wednesday",
      "price": 85,
      "airline": "...",
      "outbound_departure": "HH:MM",
      "outbound_arrival": "HH:MM",
      "return_departure": "HH:MM",
      "return_arrival": "HH:MM",
      "stops": 0,
      "why_good": "short explanation relative to this tier's baseline"
    }}
  ],
  "price_landscape": {{
    "lowest_today": 999,
    "median_today": 999,
    "highest_today": 999,
    "vs_historical": "comparison to past scans for this tier"
  }},
  "date_insights": "any patterns — e.g. Tuesdays are $20 cheaper than Thursdays",
  "recommendation": "specific advice: book now, wait for next week's sale, or watch a date"
}}

Set should_notify to true ONLY if there are deals genuinely worth acting on
relative to this tier's baseline. Limit top_deals to 5 max.
"""


def evaluate_with_llm(
    all_results: list[dict],
    tier: SaleTier,
    history: dict,
    price_guidance: str = "",
) -> dict:
    """Send today's fares + historical context to Gemini Flash for evaluation."""
    config = TIER_CONFIG[tier]

    system = SYSTEM_PROMPT.format(
        tier_label=config["label"],
        tier_description=config["description"],
        tier_value=tier.value,
        price_guidance=price_guidance,
    )

    # ── Preprocess: extract only what the LLM needs ──
    def slim_flight(flight: dict) -> dict | None:
        """Extract only evaluation-relevant fields from a SERPAPI flight."""
        price = flight.get("price")
        if price is None:
            return None

        legs = flight.get("flights", [])
        outbound = legs[0] if legs else {}
        ret_leg = legs[-1] if len(legs) > 1 else legs[0] if legs else {}

        return {
            "price": price,
            "airline": outbound.get("airline", "Unknown"),
            "stops": max(len(legs) - 1, 0),
            "total_duration_min": flight.get("total_duration"),
            "out_depart": outbound.get("departure_airport", {}).get("time", ""),
            "out_arrive": outbound.get("arrival_airport", {}).get("time", ""),
            "ret_depart": ret_leg.get("departure_airport", {}).get("time", ""),
            "ret_arrive": ret_leg.get("arrival_airport", {}).get("time", ""),
        }

    trimmed = []
    for r in all_results:
        all_flights = r["best_flights"] + r["other_flights"]
        slimmed = [f for f in (slim_flight(f) for f in all_flights) if f]
        # Sort by price — LLM only needs the cheapest options per date
        slimmed.sort(key=lambda x: x["price"])
        trimmed.append({
            "depart_date": r["depart_date"],
            "return_date": r["return_date"],
            "flights": slimmed[:8],  # top 8 cheapest per date pair
        })

    tier_history = history.get(tier.value, {})

    user_msg = f"""Today is {date.today().strftime('%A, %Y-%m-%d')} — this is the {config['label']} scan.

## Historical fare data for this tier ({tier.value})
{json.dumps(tier_history, indent=2)}

## Today's fare options ({len(trimmed)} date pairs searched)
{json.dumps(trimmed, indent=2)}

Evaluate these options against the {tier.value} baseline."""

    # ── Gemini API call (with retry for free tier flakiness) ──
    from google import genai
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

    max_retries = 3
    for attempt in range(max_retries):
        try:
            response = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=user_msg,
                config={
                    "system_instruction": system,
                    "temperature": 0.2,
                },
            )
            raw = response.text.strip()
            break
        except Exception as e:
            if attempt < max_retries - 1:
                wait = 30 * (attempt + 1)
                print(f"  ⚠️  Gemini error (attempt {attempt + 1}): {e}")
                print(f"      Retrying in {wait}s...")
                time.sleep(wait)
            else:
                print(f"  ❌ Gemini failed after {max_retries} attempts: {e}")
                raise

    # Clean potential markdown fences
    if raw.startswith("```"):
        raw = raw.split("\n", 1)[1].rsplit("```", 1)[0].strip()

    return json.loads(raw)


# ── Notification ──────────────────────────────────────────────────────────────

def format_report(evaluation: dict, tier: SaleTier) -> str:
    config = TIER_CONFIG[tier]
    landscape = evaluation.get("price_landscape", {})

    lines = [
        f"# ✈️ SFO ↔ SAN — {config['label']}",
        f"**{date.today().strftime('%A, %B %d, %Y')}**\n",
        f"## Summary\n{evaluation['summary']}\n",
    ]

    if landscape:
        lines.append(
            f"## Price Landscape\n"
            f"Today's range: **${landscape.get('lowest_today', '?')}** – "
            f"**${landscape.get('highest_today', '?')}** "
            f"(median ${landscape.get('median_today', '?')})\n"
            f"{landscape.get('vs_historical', '')}\n"
        )

    if evaluation.get("date_insights"):
        lines.append(f"## Date Patterns\n{evaluation['date_insights']}\n")

    if evaluation["top_deals"]:
        lines.append("## Top Deals\n")
        for i, deal in enumerate(evaluation["top_deals"], 1):
            lines.append(
                f"### {i}. ${deal['price']} — {deal['airline']} "
                f"({deal['depart_day']} {deal['depart_date']} → "
                f"{deal['return_day']} {deal['return_date']})\n"
                f"- Outbound: {deal.get('outbound_departure', '?')} → "
                f"{deal.get('outbound_arrival', '?')}\n"
                f"- Return: {deal.get('return_departure', '?')} → "
                f"{deal.get('return_arrival', '?')}\n"
                f"- Stops: {deal['stops']}\n"
                f"- {deal['why_good']}\n"
            )

    lines.append(f"## Recommendation\n{evaluation['recommendation']}")
    return "\n".join(lines)


def send_email(subject: str, html_body: str, fallback_text: str = "") -> bool:
    """Send one HTML email. Returns True on success.

    Shared by the fare report and the quota alerts so there is a single place
    where SMTP credentials and failure handling live.
    """
    sender_email = os.environ.get("EMAIL_SENDER")
    receiver_email = os.environ.get("EMAIL_RECIPIENT")
    password = os.environ.get("EMAIL_PASSWORD")

    if not all([sender_email, receiver_email, password]):
        print("⚠️  Email credentials not set. Skipping email send.")
        if fallback_text:
            print(fallback_text)
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, password)
            server.send_message(msg)
        print("📧 Email sent!")
        return True
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        if fallback_text:
            print(fallback_text)
        return False


def tier_label_for(tier_value: str) -> str:
    """Human label for a tier string coming back from the LLM.

    The LLM is told to echo the tier, but it can hallucinate — including the
    retired "last_minute" value, which is no longer a valid SaleTier and would
    raise ValueError mid-send. Degrade to a plain label instead of losing the
    whole report.
    """
    try:
        return TIER_CONFIG[SaleTier(tier_value)]["label"]
    except (ValueError, KeyError):
        return "Advance Sale (Tue sale)"


def send_report(report: str, evaluation: dict):
    """Send the report via email and save locally."""
    # Save markdown locally
    report_path = (
        REPORT_DIR / f"report_{date.today().isoformat()}_{evaluation['tier']}.md"
    )
    report_path.write_text(report)
    print(f"📄 Saved to {report_path}")

    # Build HTML email
    tier_label = evaluation.get("tier", "unknown")
    summary = evaluation.get("summary", "")
    landscape = evaluation.get("price_landscape", {})
    deals = evaluation.get("top_deals", [])

    deals_html = ""
    if deals:
        rows = ""
        for d in deals:
            rows += f"""<tr>
                <td>{d.get('depart_day','')[:3]} {d.get('depart_date','')} → {d.get('return_day','')[:3]} {d.get('return_date','')}</td>
                <td><strong>${d.get('price','?')}</strong></td>
                <td>{d.get('airline','')}</td>
                <td>{d.get('outbound_departure','?')} → {d.get('outbound_arrival','?')}</td>
                <td>{d.get('return_departure','?')} → {d.get('return_arrival','?')}</td>
                <td>{d.get('stops', 0)}</td>
                <td>{d.get('why_good','')}</td>
            </tr>"""
        deals_html = f"""
        <h2>Top Deals</h2>
        <table>
            <tr><th>Dates</th><th>Price</th><th>Airline</th><th>Outbound</th><th>Return</th><th>Stops</th><th>Why</th></tr>
            {rows}
        </table>"""

    html_body = f"""
    <html>
    <head>
        <style>
            body {{ font-family: sans-serif; color: #333; }}
            table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
            th, td {{ border: 1px solid #ddd; text-align: left; padding: 8px; }}
            th {{ background-color: #f2f2f2; }}
            tr:nth-child(even) {{ background-color: #f9f9f9; }}
            .landscape {{ background: #f0f7ff; padding: 12px; border-radius: 6px; margin: 12px 0; }}
        </style>
    </head>
    <body>
        <h1>✈️ SFO ↔ SAN — {tier_label_for(tier_label)}</h1>
        <p><em>{date.today().strftime('%A, %B %d, %Y')}</em></p>
        <h2>Summary</h2>
        <p>{summary}</p>
        <div class="landscape">
            <strong>Today's range:</strong> ${landscape.get('lowest_today','?')} – ${landscape.get('highest_today','?')}
            (median ${landscape.get('median_today','?')})<br>
            {landscape.get('vs_historical','')}
        </div>
        {deals_html}
        <h2>Recommendation</h2>
        <p>{evaluation.get('recommendation','')}</p>
        {f"<h2>Date Patterns</h2><p>{evaluation.get('date_insights','')}</p>" if evaluation.get('date_insights') else ""}
    </body>
    </html>
    """

    # Send email
    notify_flag = "🔔" if evaluation.get("should_notify") else "😴"
    subject = f"{notify_flag} SFO↔SAN {tier_label_for(tier_label)} — {summary[:60]}"
    send_email(subject, html_body, fallback_text=report)


# ── Quota alerts ──────────────────────────────────────────────────────────────

def google_flights_url(depart_date: str, return_date: str) -> str:
    """A Google Flights search link for one roundtrip date pair.

    Used in the manual-search emails so the user can click straight through to
    the search this run couldn't afford to make.
    """
    from urllib.parse import quote_plus

    q = (f"Flights from {ORIGIN} to {DESTINATION} on {depart_date} "
         f"through {return_date}")
    return f"https://www.google.com/travel/flights?q={quote_plus(q)}"


def _manual_search_table(date_pairs: list[tuple[str, str]]) -> str:
    rows = ""
    for depart, ret in date_pairs:
        d_day = datetime.strptime(depart, "%Y-%m-%d").strftime("%a")
        r_day = datetime.strptime(ret, "%Y-%m-%d").strftime("%a")
        rows += (f"<tr><td>{d_day} {depart} → {r_day} {ret}</td>"
                 f"<td><a href=\"{google_flights_url(depart, ret)}\">Search</a></td></tr>")
    return (f"<table><tr><th>Dates</th><th>Google Flights</th></tr>{rows}</table>")


QUOTA_EMAIL_CSS = """
body { font-family: sans-serif; color: #333; }
table { border-collapse: collapse; width: 100%; margin: 16px 0; }
th, td { border: 1px solid #ddd; text-align: left; padding: 8px; }
th { background-color: #f2f2f2; }
tr:nth-child(even) { background-color: #f9f9f9; }
.alert { background: #fff4f4; border-left: 4px solid #d33; padding: 12px; margin: 12px 0; }
.note { background: #f0f7ff; padding: 12px; border-radius: 6px; margin: 12px 0; }
"""


def send_quota_exhausted_alert(remaining: int, renewal: date | None, date_pairs):
    """First-time alert: the shared SerpApi quota is gone for this cycle."""
    renewal_str = renewal.strftime("%B %-d") if renewal else "the next renewal date"
    subject = f"🚨 SerpApi quota exhausted — SFO↔SAN scan skipped (resets {renewal_str})"
    html = f"""
    <html><head><style>{QUOTA_EMAIL_CSS}</style></head><body>
      <h1>🚨 SerpApi searches exhausted</h1>
      <div class="alert">
        <strong>{remaining} searches remaining.</strong> The SFO ↔ SAN fare scan
        did not run. Quota resets on <strong>{renewal_str}</strong>.
      </div>
      <p>This key is shared with <em>swa-checker</em>, which re-prices your booked
      flights. Both are affected until the plan renews.</p>
      <h2>Search these manually if you want this week's fares</h2>
      <p>{len(date_pairs)} date pairs the scan would have covered:</p>
      {_manual_search_table(date_pairs)}
      <div class="note">
        You'll get a shorter reminder on each run until quota resets, rather than
        this full alert again.
      </div>
    </body></html>
    """
    send_email(subject, html)


def send_manual_search_reminder(remaining: int, renewal: date | None, date_pairs):
    """Follow-up on every subsequent run while quota is still exhausted."""
    renewal_str = renewal.strftime("%B %-d") if renewal else "the next renewal date"
    subject = f"✋ SFO↔SAN — search manually this week (quota resets {renewal_str})"
    html = f"""
    <html><head><style>{QUOTA_EMAIL_CSS}</style></head><body>
      <h1>✋ Manual search needed this week</h1>
      <p>SerpApi is still out of searches (<strong>{remaining} left</strong>), so
      today's scan was skipped. Quota resets on <strong>{renewal_str}</strong>.</p>
      <p>Southwest's sale runs today and reverts Thursday — if you want this
      week's fares, search these {len(date_pairs)} date pairs by hand:</p>
      {_manual_search_table(date_pairs)}
    </body></html>
    """
    send_email(subject, html)


# ── Price guidance ────────────────────────────────────────────────────────────

PRICE_GUIDANCE = """\
## Price benchmarks (from user experience)
Rough guidelines for advance fares (Tuesday sale, 21+ days out) — adjust based
on accumulated historical data:

- Excellent: under $70 roundtrip
- Good: $70–$90
- Average: $90–$120
- Not worth flagging: $120+

As historical data accumulates, weight it more heavily than these static ranges.
"""


# ── Main ──────────────────────────────────────────────────────────────────────

def run_tier(tier: SaleTier, conn: sqlite3.Connection, eval_only: bool = False):
    """Run the full pipeline for a single tier."""
    config = TIER_CONFIG[tier]
    print(f"\n{'='*60}")
    print(f"  {config['label']}")
    print(f"{'='*60}\n")

    # Same-day guard — a manual re-trigger or an accidental double schedule
    # shouldn't burn through another full scan on top of one that already ran.
    if not eval_only and already_scanned_today(conn, tier):
        print(f"  ⚠️  {config['label']} was already scanned today — skipping "
              f"to avoid duplicating today's search budget.")
        return

    # Budget check
    account = get_serpapi_account() if not eval_only else None
    renewal = parse_renewal_date(account)
    used = get_cycle_usage(conn, renewal)
    local_remaining = MONTHLY_SEARCH_BUDGET - used

    real_remaining = account.get("total_searches_left") if account else None
    remaining = real_remaining if real_remaining is not None else local_remaining

    date_pairs = generate_search_dates(tier)
    expected = len(date_pairs)

    cycle_start = get_cycle_start(renewal)
    print(f"  📊 Cycle started {cycle_start} (renews {renewal or 'unknown'})")
    print(f"     {used} searches tracked locally this cycle; "
          f"{'unknown' if real_remaining is None else real_remaining} actually "
          f"remaining per SerpApi; {expected} needed for a full sweep")

    if not eval_only:
        # Keep a floor for swa-checker, which shares this key and guards
        # money already spent.
        spendable = remaining - RESERVE_FOR_OTHER_REPOS
        if spendable <= 0:
            print(f"  ⚠️  Only {remaining} searches left, all of which are reserved "
                  f"for swa-checker — skipping this run entirely.")
            # Alert once per billing cycle, then a shorter nudge every run after,
            # so an exhausted quota is never silent for a whole week.
            if quota_notice_sent(conn, cycle_start, "exhausted"):
                send_manual_search_reminder(remaining, renewal, date_pairs)
            else:
                send_quota_exhausted_alert(remaining, renewal, date_pairs)
                record_quota_notice(conn, cycle_start, "exhausted")
            return
        if expected > spendable:
            # Trim from the far end: dates closest to departure are the ones
            # the user can still act on, so they are scanned first. A date at
            # 70 days out has six more Tuesdays to reveal a low; a date at
            # 21 days out is on its last look.
            date_pairs = date_pairs[:spendable]
            print(f"  ✂️  Trimming sweep to {len(date_pairs)} nearest dates "
                  f"({expected} wanted, {spendable} spendable after reserving "
                  f"{RESERVE_FOR_OTHER_REPOS} for swa-checker).")

    # Fetch (or load from cache)
    if eval_only:
        print("\n📂 Loading from cache...")
        global USE_CACHE
        USE_CACHE = True
    else:
        print("\n📡 Fetching fares...")

    results = fetch_all_fares(tier, date_pairs)

    if not results:
        print("  No fare data retrieved for this tier.")
        return

    total = sum(r["flight_count"] for r in results)
    print(f"\n  Found {total} flights across {len(results)} date pairs.\n")

    # History
    history = get_historical_summary(conn)

    # Save (skip in eval-only to avoid duplicates)
    if not eval_only:
        save_fares(conn, results)

    # Evaluate
    print("🧠 Evaluating with Gemini Flash...\n")
    evaluation = evaluate_with_llm(results, tier, history, PRICE_GUIDANCE)

    # Report
    report = format_report(evaluation, tier)
    if evaluation["should_notify"]:
        print("🔔 Deals found!\n")
    else:
        print("😴 Nothing notable.\n")
    send_report(report, evaluation)


def main():
    import sys
    eval_only = "--eval-only" in sys.argv
    force = "--force" in sys.argv

    print(f"🛫 SFO ↔ SAN Fare Monitor — {datetime.now().isoformat()}")
    if eval_only:
        print("  ℹ️  Eval-only mode — skipping SERPAPI, using cached data.")

    conn = init_db()
    today_weekday = date.today().weekday()
    tier = get_sale_tier(today_weekday)

    if tier:
        run_tier(tier, conn, eval_only=eval_only)
    elif eval_only or force:
        # Off-schedule scan. This used to happen implicitly on any non-Tuesday
        # run, which meant a single stray workflow_dispatch cost a full sweep
        # of live searches. It now requires --force.
        if force and not eval_only:
            print("  ⚠️  --force: scanning off-schedule. Southwest fares revert "
                  "on Thursday, so a non-Tuesday sweep records post-sale prices "
                  "and will skew the historical baseline.")
        run_tier(SaleTier.ADVANCE, conn, eval_only=eval_only)
    else:
        print(f"  ℹ️  Not a Tuesday ({date.today():%A}) — nothing to scan. "
              f"Pass --force to sweep anyway, or --eval-only to re-evaluate cached data.")

    conn.close()


if __name__ == "__main__":
    main()
