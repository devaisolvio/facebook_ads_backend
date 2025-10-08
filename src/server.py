# app.py
import time
from flask import Flask, jsonify
from flask_cors import CORS

from build_cohorts import build_cohorts_function      # your existing builder
from util  import df_to_records                 # <- reuse the previous util

app = Flask(__name__)
CORS(app)  # keep wide-open for dev; restrict origins in prod

    

@app.get("/api/ad-weeks")
def ad_weeks():
    df = build_cohorts_function()  # returns DataFrame
    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "count": int(len(df)),
        "rows": df_to_records(df),  # <- serialize via your util
    }
    return jsonify(payload)

@app.get("/healthz")
def healthz():
    return jsonify({"ok": True})

if __name__ == "__main__":
    # Dev server; use gunicorn/waitress for production
    app.run(host="0.0.0.0", port=8000, debug=True)
