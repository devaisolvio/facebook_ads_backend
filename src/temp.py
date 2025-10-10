# test_cohort_audit.py
import requests
import pandas as pd
import numpy as np

from fb_client import fetch_insights
from build_cohorts import prepare_ad_weeks, build_outputs

# --- helpers to extract purchases/revenue from FB actions ---
def _extract_purchase_count(actions):
    total = 0
    if actions:
        for a in actions:
            if "purchase" in str(a.get("action_type", "")).lower():
                try:
                    total += int(float(a.get("value", 0)))
                except:
                    pass
    return total

def _extract_purchase_value(action_values):
    total = 0.0
    if action_values:
        for av in action_values:
            if "purchase" in str(av.get("action_type", "")).lower():
                try:
                    total += float(av.get("value", 0))
                except:
                    pass
    return total

def _to_expected_df(rows_or_df) -> pd.DataFrame:
    import pandas as pd
    import numpy as np

    if isinstance(rows_or_df, pd.DataFrame):
        df = rows_or_df.copy()
    else:
        df = pd.DataFrame(rows_or_df)

    # Ensure object dtype so we can store lists
    for col in ("actions", "action_values"):
        if col in df.columns:
            df[col] = df[col].astype("object")
            # Coerce non-lists (NaN/None/str/dict) to empty list
            df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])

    # Derive purchases/revenue if missing (or if present but all NaN)
    if "purchases" not in df.columns or df["purchases"].isna().all():
        df["purchases"] = df.get("actions", pd.Series([], dtype="object")).apply(_extract_purchase_count)
    if "revenue" not in df.columns or df["revenue"].isna().all():
        df["revenue"] = df.get("action_values", pd.Series([], dtype="object")).apply(_extract_purchase_value)

    # Ensure required columns exist
    needed = [
        "ad_id","ad_name","adset_id","adset_name",
        "campaign_id","campaign_name",
        "date_start","impressions","spend","purchases","revenue"
    ]
    for col in needed:
        if col not in df.columns:
            if col in ("impressions","purchases"):
                df[col] = 0
            elif col in ("spend","revenue"):
                df[col] = 0.0
            else:
                df[col] = None

    # Minimal typing & cleaning
    df["date_start"] = pd.to_datetime(df["date_start"], errors="coerce")
    df = df.dropna(subset=["ad_id","date_start"])
    df["date_start"] = df["date_start"].dt.strftime("%Y-%m-%d")
    df["impressions"] = pd.to_numeric(df["impressions"], errors="coerce").fillna(0).astype(int)
    df["purchases"]   = pd.to_numeric(df["purchases"],   errors="coerce").fillna(0).astype(int)
    df["spend"]       = pd.to_numeric(df["spend"],       errors="coerce").fillna(0.0)
    df["revenue"]     = pd.to_numeric(df["revenue"],     errors="coerce").fillna(0.0)

    return df[needed]


if __name__ == "__main__":
    session = requests.Session()

    SINCE = "2024-10-28"
    UNTIL = "2024-11-08"

    # 1) Fetch from Facebook API (raw)
    raw = fetch_insights(SINCE, UNTIL, session=session)

    # 2) Normalize to the schema your cohort pipeline expects
    df_src = _to_expected_df(raw)
    df_src.to_csv("audit_raw.csv", index=False)

    # 3) Build ad-week facts → save
    gb, launch_dims = prepare_ad_weeks(df_src)
    ad_week = build_outputs(gb, launch_dims)
    ad_week.to_csv("audit_ad_week.csv", index=False)

    print("Wrote audit_raw.csv and audit_ad_week.csv")
