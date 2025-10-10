# utils.py
import numpy as np

def df_to_records(df):
    out = []
    for _, r in df.iterrows():
        obj = {}
        for k, v in r.items():
            if hasattr(v, "isoformat"):         # datetime/date
                obj[k] = v.isoformat()
            elif isinstance(v, (np.integer,)):
                obj[k] = int(v)
            elif isinstance(v, (np.floating,)):
                obj[k] = float(v)
            else:
                obj[k] = v
        out.append(obj)
    return out



# utils.py
import numpy as np
import pandas as pd



def df_to_supabase(df: pd.DataFrame) -> list[dict]:
    """Convert DataFrame to Supabase-compatible list of dicts."""
    rows = []
    for _, r in df.iterrows():
        obj = {}
        for k, v in r.items():
            # Handle timestamps/dates
            if isinstance(v, (pd.Timestamp,)) or hasattr(v, "isoformat"):
                obj[k] = v.isoformat()
            # Handle numpy integers
            elif isinstance(v, np.integer):
                obj[k] = int(v)
            # Handle numpy floats
            elif isinstance(v, np.floating):
                obj[k] = None if pd.isna(v) or not np.isfinite(v) else float(v)
            # Handle numpy booleans
            elif isinstance(v, (np.bool_,)):
                obj[k] = bool(v)
            # Handle regular Python floats
            elif isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                obj[k] = None
            # Handle None/NaN values
            elif pd.isna(v):
                obj[k] = None
            else:
                obj[k] = v
        rows.append(obj)
    return rows
