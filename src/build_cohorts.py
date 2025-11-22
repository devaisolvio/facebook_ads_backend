import os, time, requests
import pandas as pd
import numpy as np
from datetime import date, timedelta
from dotenv import load_dotenv
from src.util import df_to_supabase

load_dotenv()

SUPABASE_URL    = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY    = os.getenv("SUPABASE_SERVICE_ROLE", "")
CAMPAIGN_FILTER = "Testing"      
PAGE_SIZE       = 1000
REQUEST_TIMEOUT = 60

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept-Encoding": "gzip",
}

# =========================
# Tiny timer
# =========================
class Timer:
    def __init__(self, label="PIPELINE"):
        self.label = label
        self.t0 = time.perf_counter()
        self.last = self.t0
        self.marks = {}
    def tick(self, name: str):
        now = time.perf_counter()
        step, total = now - self.last, now - self.t0
        self.marks[name] = {"step_seconds": round(step, 4), "total_seconds": round(total, 4)}
        print(f"[{self.label}] {name:<28} +{step:.3f}s  (total {total:.3f}s)")
        self.last = now

# =========================
# Fetch via PostgREST
# =========================
def fetch_fb_daily(
    since_iso: str,
    until_iso: str,
    session: requests.Session | None = None,
    timer: Timer | None = None
) -> pd.DataFrame:
    """
    Pull ad-level rows from fb_ad_daily between dates, filtered by
    campaign_name ILIKE *CAMPAIGN_FILTER*.
    """
    sess = session or requests.Session()
    base = f"{SUPABASE_URL}/rest/v1/fb_ad_daily"
    params = {
        "select": (
            "ad_id,ad_name,adset_id,adset_name,"
            "campaign_id,campaign_name,"
            "date_start,impressions,spend,purchases,revenue"
        ),
        "and": f"(date_start.gte.{since_iso},date_start.lte.{until_iso},"
               f"campaign_name.ilike.*{CAMPAIGN_FILTER}*)",
        "order": "date_start.asc",
    }

    rows, start = [], 0
    page = 1
    while True:
        rng = {"Range-Unit": "items", "Range": f"{start}-{start+PAGE_SIZE-1}"}
        r = sess.get(base, headers={**HEADERS, **rng}, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        batch = r.json()
        rows.extend(batch)
        print(f"[FETCH] page {page:<3} rows={len(batch):>6}")
        if len(batch) < PAGE_SIZE:
            break
        start += PAGE_SIZE
        page  += 1

    df = pd.DataFrame(rows)
    if timer: timer.tick(f"fetch total (rows={len(df)})")
    return df

# =========================
# Cohort prep → ad-week facts
# =========================

HIT_MIN_PURCHASES = 10
HIT_MIN_ROAS = 1.4

def prepare_ad_weeks(df_src: pd.DataFrame, timer=None) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Returns:
      gb: per (cohort_week, week_offset, ad_id) with spend/purchases/revenue, roas, hit_bucket, hit_cum
      launch_dims: names frozen at launch
    """
    if df_src.empty:
        raise SystemExit("No data")

    df = df_src.copy()

    # --- types ---
    df["date_start"] = pd.to_datetime(df["date_start"])
    for c in ("impressions", "purchases"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype(int)
    for c in ("spend", "revenue"):
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)
    if timer: timer.tick("type conversion")

    # --- launch day per ad = first day with spend > 0 ---
    launch = (
        df[df["spend"] > 0]
        .groupby("ad_id", as_index=False)["date_start"].min()
        .rename(columns={"date_start": "first_spend_date"})
    )
    df = df.merge(launch, on="ad_id", how="inner")

    # --- keep only first 28 days after launch ---
    delta = (df["date_start"] - df["first_spend_date"]).dt.days
    df = df[(delta >= 0) & (delta < 28)].copy()

    # --- week bucket 1..4, cohort label = Monday of launch week ---
    df["week_offset"] = 1 + (delta // 7)
    df = df[df["week_offset"].between(1, 4)].copy()
    df["cohort_week"] = (
        df["first_spend_date"] - pd.to_timedelta(df["first_spend_date"].dt.dayofweek, unit="D")
    ).dt.date
    if timer: timer.tick("window & offsets")

    # --- aggregate to ad × week bucket ---
    gb = (
        df.groupby(["cohort_week", "week_offset", "ad_id"], as_index=False)
          .agg(spend=("spend","sum"),
               purchases=("purchases","sum"),
               revenue=("revenue","sum"))
    )
    if timer: timer.tick("groupby ad×week")

    # --- ROAS + hit flags (conservative: spend==0 → roas=0) ---
    gb["roas"] = np.where(gb["spend"] > 0, gb["revenue"] / gb["spend"], 0.0)
    gb["hit_bucket"] = ((gb["purchases"] >= HIT_MIN_PURCHASES) & (gb["roas"] >= HIT_MIN_ROAS)).astype(int)

    # --- sticky/cumulative hit per ad across weeks ---
    gb = gb.sort_values(["cohort_week","ad_id","week_offset"])
    gb["hit_cum"] = gb.groupby(["cohort_week","ad_id"])["hit_bucket"].cummax()
    if timer: timer.tick("hit logic & cumulative")

    # --- names frozen at launch (for stable filters) ---
    launch_dims = (
        df.loc[df["date_start"] == df["first_spend_date"],
               ["ad_id","ad_name","adset_id","adset_name","campaign_id","campaign_name"]]
          .drop_duplicates("ad_id")
          .rename(columns={
              "ad_name":"ad_name_at_launch",
              "adset_name":"adset_name_at_launch",
              "campaign_name":"campaign_name_at_launch",
          })
    )

    # Optional guardrails (comment out in prod if you like)
    assert gb["week_offset"].between(1,4).all(), "week_offset out of 1..4"
    # Monotonicity check (cohort % won’t decrease when you compute grid later)
    # (You can do the grid check after you aggregate to cohort level.)

    return gb, launch_dims

def build_outputs(gb: pd.DataFrame, launch_dims: pd.DataFrame, timer=None) -> pd.DataFrame:
    
    """
    Build ONLY the ad-week facts table. (No cohort grid aggregation here.)
    """
    cohort_ad_week = (
        gb.merge(launch_dims, on="ad_id", how="left")[
            ["cohort_week","week_offset","ad_id",
                "adset_id","adset_name_at_launch",
             "campaign_id","campaign_name_at_launch",
             "spend","purchases","revenue","roas","hit_bucket","hit_cum"]
        ]
    )
    if timer: timer.tick("build cohort_ad_week")
    return cohort_ad_week





def upsert_snapshot(data: pd.DataFrame | list[dict]):
    """Upsert data to Supabase via RPC function."""
    if isinstance(data, pd.DataFrame):
        rows = df_to_supabase(data)
    else:
        rows = data
    
    if not rows:
        print("Warning: No rows to upsert")
        return None
    
    print(f"Upserting {len(rows)} rows...")
    
    payload = {"p_rows": rows}
    
    try:
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/rpc/upsert_cohort_ad_week_snapshots",
            headers=HEADERS,
            json=payload,
            timeout=60,
        )
        r.raise_for_status()
        print(f"✓ Successfully upserted {len(rows)} rows")
        return r.json() if r.content else None
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e}")
        print(f"Response: {r.text}")
        print(f"First row sample: {rows[0] if rows else 'No rows'}")
        raise
    except Exception as e:
        print(f"Error upserting data: {e}")
        raise


def build_cohorts_function():
    """Main pipeline to build cohorts and upsert to database."""
    T = Timer("PIPELINE")
    session = requests.Session()
    
    since = (date.today() - timedelta(days=365)).isoformat()
    until = date.today().isoformat()
    
    df_src = fetch_fb_daily(since, until, session=session, timer=T)
    if df_src.empty:
        raise SystemExit("No source data returned")
    
    gb, launch_dims = prepare_ad_weeks(df_src, timer=T)
    cohort_ad_week = build_outputs(gb, launch_dims, timer=T)
    pd.DataFrame(cohort_ad_week).to_csv("cohort.csv")
    
    # Upsert to database
    upsert_snapshot(cohort_ad_week) 
    
    return cohort_ad_week


