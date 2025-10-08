import os, time, requests
import pandas as pd
import numpy as np
from datetime import date, timedelta
from dotenv import load_dotenv

# =========================
# Config / Env
# =========================
load_dotenv()

SUPABASE_URL    = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY    = os.getenv("SUPABASE_SERVICE_ROLE", "")
CAMPAIGN_FILTER = "Testing"      # case-insensitive (ilike)
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
             "ad_name_at_launch","adset_id","adset_name_at_launch",
             "campaign_id","campaign_name_at_launch",
             "spend","purchases","revenue","roas","hit_bucket","hit_cum"]
        ]
    )
    if timer: timer.tick("build cohort_ad_week")
    return cohort_ad_week
# =========================
# Entry point (returns a DataFrame)
# =========================
def build_cohorts_function():
    T = Timer("PIPELINE")
    session = requests.Session()

    since = (date.today() - timedelta(days=365)).isoformat()
    until = date.today().isoformat()

    df_src = fetch_fb_daily(since, until, session=session, timer=T)
    if df_src.empty:
        raise SystemExit("No source data returned")

    gb, launch_dims = prepare_ad_weeks(df_src, timer=T)
    cohort_ad_week = build_outputs(gb, launch_dims, timer=T)
    return cohort_ad_week


def temp(
    since_iso: str = "2024-10-28",
    until_iso: str = "2024-11-08",
    chunk_days: int | None = None,
    cohort_iso: str = "2024-10-28",   # <- which cohort week to inspect
    week_n: int = 3                   # <- which week (1..4)
):
    """
    TEMP USE ONLY: Fetch a narrow window, build ad-week facts, and write CSVs:
      - data_to_check.csv                  (raw daily rows in the window)
      - result.csv                         (ad-week facts after launch logic)
      - cohort_{cohort_iso}_week{n}_rows.csv     (raw rows for that cohort+week)
      - cohort_{cohort_iso}_week{n}_summary.csv  (summary with correct denominator)
    Returns (df_src, cohort_ad_week).
    """
    session = requests.Session()

    def _fetch_range(since_s: str, until_s: str) -> pd.DataFrame:
        return fetch_fb_daily(since_s, until_s, session=session)

    # fetch window (optionally in chunks)
    if chunk_days:
        from datetime import timedelta, date as _date
        parts = []
        cur = _date.fromisoformat(since_iso)
        end = _date.fromisoformat(until_iso)
        while cur <= end:
            win_end = min(cur + timedelta(days=chunk_days - 1), end)
            print(f"Fetching {cur}..{win_end}")
            parts.append(_fetch_range(cur.isoformat(), win_end.isoformat()))
            cur = win_end + timedelta(days=1)
        df_src = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()
    else:
        df_src = _fetch_range(since_iso, until_iso)

    if df_src.empty:
        print("No source data returned for the given window.")
        return df_src, pd.DataFrame()

    # save raw window for manual checking
    df_src.to_csv("data_to_check.csv", index=False)

    # build ad-week facts
    gb, launch_dims = prepare_ad_weeks(df_src)
    cohort_ad_week = build_outputs(gb, launch_dims)

    # save full ad-week facts
    cohort_ad_week.to_csv("result.csv", index=False)
    print(f"Saved data_to_check.csv ({len(df_src)} rows), result.csv ({len(cohort_ad_week)} rows)")

    # -------- write CSVs for one cohort week --------
    wk_df = cohort_ad_week[
        (cohort_ad_week["cohort_week"] == cohort_iso) &
        (cohort_ad_week["week_offset"] == week_n)
    ].copy()
    wk_df.to_csv(f"cohort_{cohort_iso}_week{week_n}_rows.csv", index=False)

    # Denominator = ALL ads that launched in this cohort (not just those with week_n rows)
    ads_all = (cohort_ad_week.loc[cohort_ad_week["cohort_week"] == cohort_iso, "ad_id"]
               .drop_duplicates()
               .sort_values())

    # hit_cum per ad at week_n (0 if the ad has no row for that week)
    if not wk_df.empty:
        wk_hits = (wk_df.drop_duplicates("ad_id")
                      .set_index("ad_id")["hit_cum"]
                      .reindex(ads_all, fill_value=0))
        week_hit_pct = round(wk_hits.mean() * 100, 1)
        totals = wk_df[["spend", "purchases", "revenue"]].sum(numeric_only=True)
    else:
        week_hit_pct = 0.0
        totals = pd.Series({"spend": 0.0, "purchases": 0, "revenue": 0.0})

    summary_df = pd.DataFrame([{
        "cohort_week": cohort_iso,
        "week_offset": week_n,
        "total_ads": int(len(ads_all)),
        "week_hit_pct": float(week_hit_pct),
        "spend_sum": float(totals.get("spend", 0.0)),
        "purchases_sum": int(totals.get("purchases", 0)),
        "revenue_sum": float(totals.get("revenue", 0.0)),
    }])
    summary_df.to_csv(f"cohort_{cohort_iso}_week{week_n}_summary.csv", index=False)
    # -----------------------------------------------

    return df_src, cohort_ad_week
