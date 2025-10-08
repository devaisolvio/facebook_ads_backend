import os, json, re, requests, socket, urllib.parse as u  # socket/urllib kept per your import list
from datetime import date, timedelta
from tenacity import retry, wait_exponential, stop_after_attempt
# from sqlalchemy import create_engine, text  # <- removed: not needed with REST

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
ACCOUNT_ID   = os.environ["ACCOUNT_ID"]  # without 'act_'

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
                except:
                    pass
    return total

def extract_purchase_value(action_values):
    total = 0.0
    if action_values:
        for av in action_values:
            if PURCHASE_PAT.search(av.get("action_type", "")):
                try:
                    total += float(av.get("value", 0))
                except:
                    pass
    return total

@retry(wait=wait_exponential(min=1, max=60), stop=stop_after_attempt(5))
def _fetch(url, params):
    headers = {"Authorization": f"Bearer {FB_TOKEN}"}
    out = []
    while True:
        r = requests.get(url, headers=headers, params=params, timeout=60)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            print("FB error:", r.text[:400])
            raise
        j = r.json()
        out.extend(j.get("data", []))
        next_url = (j.get("paging") or {}).get("next")
        if not next_url:
            break
        url, params = next_url, {}  # 'next' already includes all params
    return out

def fetch_insights(*args, **kwargs):
    """
    fetch_insights("2025-10-06", "2025-10-07", time_increment=1)
    fetch_insights(date_preset="yesterday", time_increment=1)
    """
    url = f"{BASE}/act_{ACCOUNT_ID}/insights"
    if "date_preset" in kwargs:
        params = {"date_preset": kwargs["date_preset"], "time_increment": kwargs.get("time_increment", 1)}
    else:
        since, until = args
        params = {"time_range": {"since": since, "until": until}, "time_increment": kwargs.get("time_increment", 1)}
    return _fetch(url, params)

# ----------------------------
# Supabase REST upsert (replaces SQLAlchemy)
# ----------------------------
TABLE = "fb_ad_daily"
ON_CONFLICT = ["ad_id", "date_start"]  # must match your unique index/constraint

def _sb_headers():
    return {
        "apikey": SR_KEY,
        "Authorization": f"Bearer {SR_KEY}",
        "Content-Type": "application/json",
        # Upsert + minimal response for speed; set on_conflict to your composite key
        "Prefer": f"resolution=merge-duplicates,return=minimal,on_conflict={','.join(ON_CONFLICT)}",
        "Accept-Encoding": "gzip",
    }

def _nan_to_none(v):
    # JSON can't carry NaN/Inf; make it None
    try:
        # catches float('nan') etc.
        if isinstance(v, float) and (v != v):
            return None
    except Exception:
        pass
    return v

def _map_row(r: dict) -> dict:
    purchases = extract_purchase_count(r.get("actions"))
    revenue   = extract_purchase_value(r.get("action_values"))
    return {
        "ad_id": r["ad_id"],
        "ad_name": r.get("ad_name"),
        "adset_id": r.get("adset_id"),
        "adset_name": r.get("adset_name"),
        "campaign_id": r.get("campaign_id"),
        "campaign_name": r.get("campaign_name"),
        "date_start": r["date_start"],  # ISO date string expected by your column type
        "impressions": int(float(r.get("impressions") or 0)),
        "reach": int(float(r.get("reach") or 0)),
        "spend": float(r.get("spend") or 0.0),
        "ctr": float(r.get("ctr") or 0.0),
        "purchases": purchases,
        "revenue": revenue,
        # store the raw arrays (JSONB column recommended)
        "raw_actions": r.get("actions"),
        "raw_action_values": r.get("action_values"),
        # let DB set updated_at via default/trigger, or add it here if you want
    }

@retry(wait=wait_exponential(min=1, max=30), stop=stop_after_attempt(5))
def upsert_rows(rows: list[dict]) -> None:
    """
    Upsert via Supabase REST in chunks.
    """
    if not rows:
        print("[REST] no rows to upsert")
        return

    url = f"{SUPABASE_URL}/rest/v1/{TABLE}"
    headers = _sb_headers()

    CHUNK = 1000  # tune based on row size
    total = len(rows)
    sent = 0
    for i in range(0, total, CHUNK):
        batch = rows[i:i+CHUNK]
        payload = [{k: _nan_to_none(v) for k, v in _map_row(r).items()} for r in batch]
        r = requests.post(url, headers=headers, data=json.dumps(payload), timeout=120)
        try:
            r.raise_for_status()
        except requests.HTTPError:
            print("[REST] upsert failed:", r.status_code, r.text[:600])
            raise
        sent += len(batch)
        print(f"[REST] upserted {sent}/{total}")

# ----------------------------
# Job runners (unchanged)
# ----------------------------
def backfill(days=365, chunk_days=30):
    """
    Example backfill (adjust dates!).
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
    """
    print("[CRON] fetching 'yesterday' from FB insights…")
    rows = fetch_insights(date_preset="yesterday", time_increment=1)
    print(f"[CRON] fetched rows: {len(rows)}")
    upsert_rows(rows)
    print(f"[CRON] done (rolling_days hint = {rolling_days})")

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
