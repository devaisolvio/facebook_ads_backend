from datetime import datetime
from zoneinfo import ZoneInfo
import sys

# Run only at 08:00 Europe/Berlin (handles CET/CEST automatically)
now = datetime.now(ZoneInfo("Europe/Berlin"))
if now.hour != 8:
    print(f"Skip: local time is {now.strftime('%Y-%m-%d %H:%M:%S %Z')}, not 08:00.")
    sys.exit(0)

from src.fb_client import daily

if __name__ == "__main__":
    print("Starting daily job…")
    daily()
    print("Daily job done ✅")
