import os, json, re, requests
from dotenv import load_dotenv
from datetime import date, timedelta
from tenacity import retry, wait_exponential, stop_after_attempt

# Load .env locally; in GitHub Actions/Render use env vars/secrets instead
load_dotenv()

# ----------------------------
# Required env vars (fail fast)
# ----------------------------
REQUIRED = ["SUPABASE_URL", "SUPABASE_SERVICE_ROLE", "FB_TOKEN", "ACCOUNT_ID"]
missing = [k for k in REQUIRED if not os.getenv(k)]
if missing:
    raise RuntimeError(f"Missing env vars: {', '.join(missing)}")

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SR_KEY       = os.environ["SUPABASE_SERVICE_ROLE"]
FB_TOKEN     = os.environ["FB_TOKEN"]
ACCOUNT_ID   = os.environ["ACCOUNT_ID"]  # without "act_"

# ----------------------------
# Facebook Graph API client
# ----------------------------
BASE = "https://graph.facebook.com/v19.0"
PURCHASE_PAT = re.compile(r"purchase", re.I)

def extract_purchase_count(actions):
    total = 0
    if actions:
        for a in actions:
            if PURCHASE_PAT.search(a.get("action_type", "")):
                try:
                    total += int(float(a.get("value", 0)))
                except Exception:
                    pass
    return total

def extract_purchase_value(action_values):
    total = 0.0
    if action_values:
        for av in action_values:
            if PURCHASE_PAT.search(av.get("action_type", "")):
                try:
                    total += float(av.get("value", 0))
                except Exception:
                    pass
    return total

# --- low-level fetch (HTTP, paginated) ---
@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5))
def _fetch(url, params):
    headers = {"Authorization": f"Bearer {FB_TOKEN}"}
    out = []
    while True:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            print("FB error:", r.text[:500])
            raise
        j = r.json()
        out.extend(j.get("data", []))
        next_url = (j.get("paging") or {}).get("next")
        if not next_url:
            break
        # after the first page, pass empty params because 'next' already encodes them
        url, params = next_url, {}
    return out

def fetch_insights(*args, **kwargs):
    """
    Two ways to call:
      fetch_insights("YYYY-MM-DD", "YYYY-MM-DD", time_increment=1)
      fetch_insights(date_preset="yesterday", time_increment=1)
    Uses ad-level, daily rows, explicit fields, and your campaign filter.
    """
    url = f"{BASE}/act_{ACCOUNT_ID}/insights"
    fields = ",".join([
        "date_start","date_stop",
        "ad_id","ad_name",
        "adset_id","adset_name",
        "campaign_id","campaign_name",
        "impressions","reach","spend","ctr",
        "actions","action_values"
    ])

    base_params = {
        "level": "ad",
        "fields": fields,
        "time_increment": kwargs.get("time_increment", 1),  # 1 for daily
        # NOTE: filtering is CASE-SENSITIVE. Adjust "Testing" as needed.
        "filtering": json.dumps([{
            "field": "campaign.name",
            "operator": "CONTAIN",
            "value": "Testing"
        }]),
        "action_report_time": "conversion",
        "action_attribution_windows": json.dumps(["7d_click","1d_view"]),
        "limit": 5000
    }

    if "date_preset" in kwargs:
        params = {**base_params, "date_preset": kwargs["date_preset"]}
    else:
        since, until = args  # must pass two args
        params = {
            **base_params,
            "time_range": json.dumps({"since": since, "until": until})
        }

    return _fetch(url, params)

# ----------------------------
# Supabase REST upsert (replaces SQLAlchemy)
# ----------------------------
TABLE = "fb_ad_daily"
ON_CONFLICT = ["ad_id", "date_start"]  # must match your table's unique/index

def _sb_headers():
    return {
        "apikey": SR_KEY,
        "Authorization": f"Bearer {SR_KEY}",
        "Content-Type": "application/json",
        # Upsert + minimal response for speed
        "Prefer": f"resolution=merge-duplicates,return=minimal,on_conflict={','.join(ON_CONFLICT)}",
        "Accept-Encoding": "gzip",
    }

def _nan_to_none(v):
    try:
        # NaN != NaN
        if isinstance(v, float) and (v != v):
            return None
    except Exception:
        pass
    return v

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def upsert_rows(rows: list[dict]) -> None:
    """
    Upsert to Supabase via PostgREST in chunks. Skips malformed rows without ad_id/date_start.
    """
    if not rows:
        print("[REST] no rows to upsert")
        return

    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    headers = _sb_headers()

    CHUNK = 1000
    total = len(rows)
    sent, skipped = 0, 0

    for i in range(0, total, CHUNK):
        batch = rows[i:i+CHUNK]
        payload = []
        for r in batch:
            ad_id = r.get("ad_id")
            date_start = r.get("date_start")
            if not ad_id or not date_start:
                skipped += 1
                continue

            purchases = extract_purchase_count(r.get("actions"))
            revenue   = extract_purchase_value(r.get("action_values"))

            row = {
                "ad_id": ad_id,
                "ad_name": r.get("ad_name"),
                "adset_id": r.get("adset_id"),
                "adset_name": r.get("adset_name"),
                "campaign_id": r.get("campaign_id"),
                "campaign_name": r.get("campaign_name"),
                "date_start": date_start,  # ISO date string
                "impressions": int(float(r.get("impressions") or 0)),
                "reach": int(float(r.get("reach") or 0)),
                "spend": float(r.get("spend") or 0.0),
                "ctr": float(r.get("ctr") or 0.0),
                "purchases": purchases,
                "revenue": revenue,
                "raw_actions": r.get("actions"),
                "raw_action_values": r.get("action_values"),
            }
            payload.append({k: _nan_to_none(v) for k, v in row.items()})

        if not payload:
            continue

        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=120)
        try:
            resp.raise_for_status()
        except requests.HTTPError:
            print("[REST] upsert failed:", resp.status_code, resp.text[:600])
            raise

        sent += len(payload)
        print(f"[REST] upserted {sent}/{total} (skipped {skipped})")

# ----------------------------
# job runners (same behavior)
# ----------------------------
def backfill(days=365, chunk_days=30):
    """
    One-time: pull ~1y history in ~30-day chunks (daily rows).
    Adjust the date window to your needs.
    """
    start = date.fromisoformat("2025-09-11")
    end   = date.fromisoformat("2025-09-21")
    cur = start
    while cur <= end:
        win_end = min(cur + timedelta(days=chunk_days-1), end)
        rows = fetch_insights(cur.isoformat(), win_end.isoformat(), time_increment=1)
        upsert_rows(rows)
        print(f"Loaded {cur}..{win_end}: {len(rows)} rows")
        cur = win_end + timedelta(days=1)

def daily(rolling_days=14):
    """
    Daily cron: re-pull a rolling window to capture late conversions.
    Keeps your original style using date_preset="yesterday".
    """
    rows = fetch_insights(date_preset="yesterday", time_increment=1)
    upsert_rows(rows)
    print(f"Refreshed yesterday: {len(rows)} rows")

if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--backfill", action="store_true")
    ap.add_argument("--days", type=int, default=365)
    ap.add_argument("--daily", action="store_true")
    ap.add_argument("--rolling-days", type=int, default=14)
    args = ap.parse_args()

    if args.backfill:
        backfill(days=args.days, chunk_days=30)
    if args.daily:
        daily(rolling_days=args.rolling_days)
