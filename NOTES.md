# Design notes — SerpApi budget and sale-cycle behaviour

Findings from the 2026-08-05 quota investigation. Read this before changing
anything that issues a SerpApi search or touches the scan window.

## The constraint you cannot see from inside this repo

**This repo shares one SerpApi key with [`swa-checker`](https://github.com/spud0927/swa-checker).**
Free tier, 250 searches/month. Nothing in this codebase reveals that — and
swa-checker is the *larger* consumer, because flights are booked as one-ways
(every leg is its own search) and it runs twice weekly to this repo's once.

- **The plan renews on the 13th, not the 1st.** `get_monthly_usage()` used to
  count from the 1st, so from the 1st through the 12th it reported a fresh 250
  against an already-spent account. That is the bug that caused the outage.
  `get_cycle_start()` now derives the boundary from `plan_renewal_date`.
- **`get_serpapi_account()` does not consume a search.** Check quota freely.
- **SerpApi returns HTTP 200 with an `"error"` body when out of quota.**
  `get_dict()` never raises, so a rejected request looks identical to a
  legitimate "no flights found" result.
- `RESERVE_FOR_OTHER_REPOS` holds back searches for swa-checker, which guards
  money already spent (rebooking credit) while this repo is speculative
  shopping. When quota is short this repo yields, and trims its sweep to the
  **nearest** departure dates — a date 21 days out is on its last look, one at
  70 days has six more Tuesdays coming.

## Budget

Measured over a 30-day cycle:

| | before | after |
|---|---|---|
| agent-fare-monitor (1 run/wk) | ~180 | **~154** |
| swa-checker (2 runs/wk, 14 legs) | ~120 | **~86** |
| **total vs 250 limit** | **~300** | **~240** |

At ~300/cycle the quota died ~25 days into a 30-day cycle — the "ran out a week
early" symptom. Current slack is **~10 searches**. Sweep size is ~36 date pairs
(21–70d, 5 valid departure weekdays).

## Why the schedule is what it is

**Weekly Tuesday sampling is mandatory, not a preference.** Southwest drops
fares Tuesday and reverts Thursday, and the sale price for the *same* departure
date varies week to week — $49 one Tuesday, $69 the next. Sampling biweekly
would **alias** that weekly signal: you would observe ~half the sale events with
no way to know which half, and `fare_history` minima would drift upward and
corrupt the LLM's baseline comparison. If usage must come down, cut the **date
set**, never the cadence.

**The Wednesday last-minute tier was removed.** It assumed Southwest's Wanna Get
Away Wednesday promo discounts this route; in practice it never surfaced an
actionable deal. Saved ~26/cycle. Historical `last_minute` rows remain in
`fare_history` but are no longer queried, so they cannot drag the advance
baseline around.

**Off-Tuesday sweeps require `--force`.** A non-Tuesday run previously fell
through to scanning both tiers, so one stray `workflow_dispatch` cost a full
sweep. Off-Tuesday prices are post-revert anyway and skew the baseline.

**Crons are staggered:** swa-checker at 14:15 UTC Tuesday, this repo at 14:30.
They previously collided at the same minute competing for shared quota.

## Why the 21–70d window was kept

Distribution of all-time lows by days-to-departure, from swa-checker's 37 legs:

```
<21d      8.1%
21-42d   43.2%   <- densest
43-56d   35.1%
57-70d    5.4%
71d+      8.1%   <- already outside this window
```

Narrowing to 21–56d looks defensible on those numbers (−43 searches/cycle) —
**but the coverage is too thin to act on.** Only 11 of 37 legs were ever
*observed* past 56 days, because swa-checker tracks a leg only once booked. The
57–70d bucket is under-sampled, not empty, and 8.1% of lows landing at 71d+
hints the far end holds more value than that data can see. **This repo is the
right instrument for that question** — it samples 21–70d on unbooked dates.
Revisit once a few cycles of `fare_history` have accumulated.

## Open questions / future steps

- **Resize the window using this repo's own data.** Once several cycles of
  21–70d history exist, test whether 57–70d ever produces a low that 21–56d
  would have missed. That is the honest basis for narrowing — not the
  booked-flight data above.
- **`google_flights_deals` engine.** Accepts a **comma-separated `outbound_date`**
  (a window of dates in one search) plus `trip_length`, `include_airlines=WN`,
  `max_price`, `stops`. Could collapse a 36-search sweep into a handful while
  keeping weekly cadence. Unverified: it is a *deals* engine, so it may not
  enumerate every date, and it needs `arrival_id` pinning confirmed. Response
  shape differs from `google_flights`, so parsing and the `fare_history` schema
  would need rework. **Prototype this before any further window cuts.**
- The plain `google_flights` engine has **no date grid, price calendar, or
  flexible-date parameter** — one search is one date pair, permanently. (A
  calendar-style API exists at searchapi.io, a different vendor and key.)
- **20-leg peak on swa-checker breaks the budget** (~277 vs 250). Options: fewer
  tracked legs, or SerpApi's paid tier (~$75/yr).
- **The quota-alert emails have never fired for real.** Both paths are tested
  with SMTP stubbed, but have not run against live credentials in Actions.

## Anti-patterns already ruled out

- **Do not gate the sweep on whether a sale is "announced."** Southwest runs the
  Tuesday sale most weeks whether or not it publishes a promo page.
  `southwest.com/special-offers/flight-deals/` is fetchable and does show sale
  state, but absence of a banner does not mean absence of a sale.
- **Do not scrape southwest.com for fares.** Already fought and lost against bot
  protection; the Low Fare Calendar is JS-rendered with no data in the HTML.
- **Do not judge an extra sample by its average.** Tue→Wed prices drift *upward*
  (+$531 net across 147 leg-weeks) yet Wednesday still set the all-time low for
  11 of 37 legs. Sampling targets the minimum, not the mean.
