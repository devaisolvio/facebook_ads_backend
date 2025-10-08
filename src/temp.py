import os
from supabase import create_client
from dotenv import load_dotenv
import time

load_dotenv()

url = os.environ["SUPABASE_URL"]
key = os.environ["SUPABASE_ANON_KEY"]  # or SERVICE_ROLE_KEY on the server
client = create_client(url, key)

print("Api Request Started" )
start = time.perf_counter()
resp = client.table("fb_ad_daily").select("*").execute()
end = time.perf_counter()
print(resp.data,"\n\n",end- start)
