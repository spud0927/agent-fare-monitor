"""
SFO ↔ SAN 2-night roundtrip fare monitor — Southwest sale-cycle aware.

Schedule:
  - Tuesday AM PT:  Checks the advance sale window (21–70 days out)
  - Wednesday AM PT: Checks the last-minute sale window (7–14 days out)

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
# 5 departure days/week × 7 weeks advance + 5 × 1 week last-minute
# = ~40 searches/week, ~173/month — fits 250 free tier with room
# for ~8-10 searches/week from other scripts (worst case ~243).
MONTHLY_SEARCH_BUDGET = 250

# ── LLM config ────────────────────────────────────────────────────────────────
GEMINI_MODEL = "gemini-2.5-flash"


# ── Sale tiers ────────────────────────────────────────────────────────────────

class SaleTier(Enum):
    LAST_MINUTE = "last_minute"   # Wednesday scan: 7–14 days out
    ADVANCE = "advance"           # Tuesday scan: 21–70 days out


def get_sale_tier(run_day: int | None = None) -> SaleTier | None:
    """Determine which sale tier to scan based on the day of the week."""
    if run_day is None:
        run_day = date.today().weekday()
    if run_day == 1:    # Tuesday
        return SaleTier.ADVANCE
    elif run_day == 2:  # Wednesday
        return SaleTier.LAST_MINUTE
    return None


TIER_CONFIG = {
    SaleTier.LAST_MINUTE: {
        "label": "Last-Minute (Wed sale)",
        "start_offset_days": 7,
        "end_offset_days": 14,
        "description": (
            "Wednesday last-minute sale window. Fares for 7–14 days out. "
            "Baseline prices are higher because of short booking window. "
            "A 'good deal' here means Southwest has discounted a near-term flight, "
            "which is unusual and worth grabbing."
        ),
    },
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


def fetch_all_fares(tier: SaleTier) -> list[dict]:
    """Fetch fares for every valid date pair in the tier's window."""
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
    conn.commit()
    return conn


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


def get_monthly_usage(conn: sqlite3.Connection) -> int:
    """Count how many searches have been made this calendar month."""
    first_of_month = date.today().replace(day=1).isoformat()
    row = conn.execute("""
        SELECT COUNT(DISTINCT depart_date || '|' || return_date || '|' || scanned_at)
        FROM fare_history
        WHERE scanned_at >= ?
    """, (first_of_month,)).fetchone()
    return row[0] if row[0] else 0


# ── LLM evaluation (Gemini Flash) ────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a personal Southwest Airlines fare analyst for SFO ↔ SAN roundtrip fares
(2-night trips). You understand Southwest's pricing rhythm deeply.

## Southwest's sale cycle
- **Tuesday morning**: Southwest runs a sale on flights 21+ days out, typically
  impacting 2-3 months of fares. These "advance sale" prices are the lowest
  baseline for this route.
- **Wednesday morning**: Southwest runs a last-minute sale for flights within the
  next ~2 weeks. These prices are higher than advance fares but represent
  discounts off the already-elevated last-minute baseline.

## You are evaluating: {tier_label}
{tier_description}

## Evaluation approach

You MUST evaluate deals against the appropriate baseline for this tier:

**For advance fares (Tuesday scan):**
- These should be the cheapest fares available
- Compare against historical advance-tier data
- Good deals: prices at or near historical lows for this tier
- A fare that looks cheap in absolute terms but is average for this tier is NOT a deal

**For last-minute fares (Wednesday scan):**
- Higher absolute prices are expected and normal
- Compare against historical last-minute-tier data
- Good deals: prices significantly below the last-minute average
- A $150 fare might be unremarkable for advance but excellent for last-minute

**For both tiers:**
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

    # ── Gemini API call ──
    from google import genai
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])

    response = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=user_msg,
        config={
            "system_instruction": system,
            "temperature": 0.2,
        },
    )

    raw = response.text.strip()

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
        <h1>✈️ SFO ↔ SAN — {TIER_CONFIG[SaleTier(tier_label)]['label']}</h1>
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
    sender_email = os.environ.get("EMAIL_SENDER")
    receiver_email = os.environ.get("EMAIL_RECIPIENT")
    password = os.environ.get("EMAIL_PASSWORD")

    if not all([sender_email, receiver_email, password]):
        print("⚠️  Email credentials not set. Skipping email send.")
        print(report)
        return

    notify_flag = "🔔" if evaluation.get("should_notify") else "😴"
    subject = f"{notify_flag} SFO↔SAN {TIER_CONFIG[SaleTier(tier_label)]['label']} — {summary[:60]}"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = receiver_email
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(sender_email, password)
            server.send_message(msg)
        print("📧 Email report sent!")
    except Exception as e:
        print(f"❌ Failed to send email: {e}")
        print(report)


# ── Price guidance ────────────────────────────────────────────────────────────

PRICE_GUIDANCE = """\
## Price benchmarks (from user experience)
These are rough guidelines — adjust based on accumulated historical data:

### Advance fares (Tuesday sale, 21+ days out)
- Excellent: under $70 roundtrip
- Good: $70–$90
- Average: $90–$120
- Not worth flagging: $120+

### Last-minute fares (Wednesday sale, 7–14 days out)
- Excellent: under $100 roundtrip
- Good: $100–$140
- Average: $140–$180
- Not worth flagging: $180+

As historical data accumulates, weight it more heavily than these static ranges.
"""


# ── Main ──────────────────────────────────────────────────────────────────────

def run_tier(tier: SaleTier, conn: sqlite3.Connection):
    """Run the full pipeline for a single tier."""
    config = TIER_CONFIG[tier]
    print(f"\n{'='*60}")
    print(f"  {config['label']}")
    print(f"{'='*60}\n")

    # Budget check
    used = get_monthly_usage(conn)
    remaining = MONTHLY_SEARCH_BUDGET - used
    expected = len(generate_search_dates(tier))
    print(f"  📊 Budget: {used}/{MONTHLY_SEARCH_BUDGET} used this month, "
          f"{remaining} remaining, {expected} needed for this run")

    if expected > remaining:
        print(f"  ⚠️  Only {remaining} searches left — skipping this run.")
        return

    # Fetch
    print("\n📡 Fetching fares...")
    results = fetch_all_fares(tier)

    if not results:
        print("  No fare data retrieved for this tier.")
        return

    total = sum(r["flight_count"] for r in results)
    print(f"\n  Found {total} flights across {len(results)} date pairs.\n")

    # History
    history = get_historical_summary(conn)

    # Save
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
    print(f"🛫 SFO ↔ SAN Fare Monitor — {datetime.now().isoformat()}")

    conn = init_db()
    today_weekday = date.today().weekday()
    tier = get_sale_tier(today_weekday)

    if tier:
        run_tier(tier, conn)
    else:
        # Manual run (not Tue/Wed) — scan both tiers
        print("  ℹ️  Manual run — scanning both tiers.")
        for t in SaleTier:
            run_tier(t, conn)

    conn.close()


if __name__ == "__main__":
    main()
