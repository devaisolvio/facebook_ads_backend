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
