import os, json, re, requests
from dotenv import load_dotenv
from datetime import date, timedelta
from tenacity import retry, wait_exponential, stop_after_attempt
from sqlalchemy import create_engine, text

load_dotenv()

DB_URL = os.environ["DB_URL"]
SSL = os.getenv("DB_SSLMODE", "require")
engine = create_engine(DB_URL, connect_args={"sslmode": SSL})

BASE = "https://graph.facebook.com/v19.0"
FB_TOKEN = os.getenv("FB_TOKEN", "")
ACCOUNT_ID = os.getenv("ACCOUNT_ID", "")  # without "act_" prefix

# --- purchase extraction helpers ---
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
            print("FB error:", r.text)
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
    Accepts either explicit dates (since/until) or a date_preset like 'yesterday'.
    Examples:
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

# --- DB upsert ---
UPSERT_SQL = text("""
insert into fb_ad_daily
  (ad_id, ad_name, adset_id, adset_name, campaign_id, campaign_name,
   date_start, impressions, reach, spend, ctr,
   purchases, revenue, raw_actions, raw_action_values, updated_at)
values
  (:ad_id, :ad_name, :adset_id, :adset_name, :campaign_id, :campaign_name,
   :date_start, :impressions, :reach, :spend, :ctr,
   :purchases, :revenue, :raw_actions, :raw_action_values, now())
on conflict (ad_id, date_start) do update set
  ad_name=excluded.ad_name,
  adset_id=excluded.adset_id, adset_name=excluded.adset_name,
  campaign_id=excluded.campaign_id, campaign_name=excluded.campaign_name,
  impressions=excluded.impressions, reach=excluded.reach,
  spend=excluded.spend, ctr=excluded.ctr,
  purchases=excluded.purchases, revenue=excluded.revenue,
  raw_actions=excluded.raw_actions, raw_action_values=excluded.raw_action_values,
  updated_at=now();
""")

def upsert_rows(rows):
    with engine.begin() as c:
        for r in rows:
            purchases = extract_purchase_count(r.get("actions"))
            revenue   = extract_purchase_value(r.get("action_values"))
            c.execute(UPSERT_SQL, dict(
                ad_id=r["ad_id"],
                ad_name=r.get("ad_name"),
                adset_id=r.get("adset_id"),
                adset_name=r.get("adset_name"),
                campaign_id=r.get("campaign_id"),
                campaign_name=r.get("campaign_name"),
                date_start=r["date_start"],
                impressions=int(float(r.get("impressions") or 0)),
                reach=int(float(r.get("reach") or 0)),
                spend=float(r.get("spend") or 0.0),
                ctr=float(r.get("ctr") or 0.0),
                purchases=purchases,
                revenue=revenue,
                raw_actions=json.dumps(r.get("actions")),
                raw_action_values=json.dumps(r.get("action_values")),
            ))

# --- job runners ---
def backfill(days=365, chunk_days=30):
    """
    One-time: pull ~1y history in ~30-day chunks (daily rows).
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
