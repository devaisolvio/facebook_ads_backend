# app.py
import time,requests
from flask import Flask, jsonify,request
from flask_cors import CORS
import pandas as pd
import os
from src.build_cohorts import build_cohorts_function      # your existing builder
from src.util  import df_to_records                 # <- reuse the previous util

app = Flask(__name__)
CORS(app)  # keep wide-open for dev; restrict origins in prod
SUPABASE_URL    = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY    = os.getenv("SUPABASE_SERVICE_ROLE", "")

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept-Encoding": "gzip",
}

TABLE = "cohort_ad_week_snapshots"   # the snapshot table you created
PAGE_SIZE = 1000
REQUEST_TIMEOUT = 60


def fetch_ad_weeks_from_db(
    since_iso: str | None = None,
    until_iso: str | None = None,
    campaign_like: str | None = None,
    ad_like: str | None = None,
) -> pd.DataFrame:
    """
    Read from snapshot table with optional filters.
    Returns a DataFrame with the same columns your frontend expects.
    """
    base = f"{SUPABASE_URL}/rest/v1/{TABLE}"

    # Build PostgREST filters
    and_parts = []
    if since_iso: and_parts.append(f"cohort_week.gte.{since_iso}")
    if until_iso: and_parts.append(f"cohort_week.lte.{until_iso}")
    if campaign_like: and_parts.append(f"campaign_name_at_launch.ilike.*{campaign_like}*")
    if ad_like: and_parts.append(f"ad_name_at_launch.ilike.*{ad_like}*")
    and_param = f"({','.join(and_parts)})" if and_parts else None

    params = {
        "select": ",".join([
            "cohort_week","week_offset","ad_id"
            ,"adset_name_at_launch","ad_name_at_launch",
            "campaign_id","campaign_name_at_launch",
            "spend","purchases","revenue","roas","hit_bucket","hit_cum"
        ]),
        "order": "cohort_week.asc,week_offset.asc,ad_id.asc",
    }
    if and_param:
        params["and"] = and_param

    rows, start, page = [], 0, 1
    while True:
        rng = {"Range-Unit": "items", "Range": f"{start}-{start+PAGE_SIZE-1}"}
        r = requests.get(base, headers={**HEADERS, **rng}, params=params, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        batch = r.json()
        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break
        start += PAGE_SIZE
        page  += 1

    return pd.DataFrame(rows)

@app.get("/api/ad-weeks")
def ad_weeks():
    # Optional query params for light filtering (kept simple)
    since = request.args.get("since")   # "YYYY-MM-DD"
    until = request.args.get("until")   # "YYYY-MM-DD"
    camp  = request.args.get("campaign")
    ad    = request.args.get("ad")

    df = fetch_ad_weeks_from_db(since_iso=since, until_iso=until, campaign_like=camp, ad_like=ad)

    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "count": int(len(df)),
        "rows": df_to_records(df),  # same shape as before
    }
    return jsonify(payload)

@app.get("/healthz")
def healthz():
    return jsonify({"ok": True})

if __name__ == "__main__":
    # Dev server; use gunicorn/waitress for production
    app.run(host="0.0.0.0", port=8000, debug=True)
