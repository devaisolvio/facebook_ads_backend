import requests
import os
import time
import pandas as pd
# Your Supabase credentials
SUPABASE_URL = "https://zkkkjlveoyaebhldiluf.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Inpra2tqbHZlb3lhZWJobGRpbHVmIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc1OTQ2NjI0NiwiZXhwIjoyMDc1MDQyMjQ2fQ.wZZ9mKUnYvPjyglh0sEyUx0aQXsPYonUJ-u4ebgc5QQ"  # Get this from your Supabase dashboard

# Headers for authentication
headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}"
}

# Example: Get all rows from a table
def get_all_data(table_name):
    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return None

start_time = time.time()
data = get_all_data("fb_ad_daily")

df = pd.DataFrame(data)
print(df.info())
print(df.columns)
df = df[df['campaign_name'].str.contains('testing', case=False, na=False)]
df['roas'] = df['spend'] / df['revenue']




# Convert date_start to datetime
df['date_start'] = pd.to_datetime(df['date_start'])

# Filter for testing campaigns and spend > 0
df = df[df['campaign_name'].str.contains('testing', case=False, na=False)]
df = df[df['spend'] > 0].copy()

# Step 1: Get first spend date per ad (ad_launches CTE)
ad_launches = df.groupby('ad_id')['date_start'].min().reset_index()
ad_launches.columns = ['ad_id', 'first_spend_date']

# Step 2: Merge first_spend_date back to main dataframe
df = df.merge(ad_launches, on='ad_id', how='inner')

# IMPORTANT: Convert first_spend_date to datetime (it's already datetime from the groupby)
# But let's ensure it's datetime in case
df['first_spend_date'] = pd.to_datetime(df['first_spend_date'])

# Step 3: Filter for only data within 28 days of launch
df = df[df['date_start'] >= df['first_spend_date']].copy()
df = df[df['date_start'] < df['first_spend_date'] + pd.Timedelta(days=28)].copy()

# Step 4: Calculate week_offset (1-based, so week 1 = days 0-6)
df['days_since_launch'] = (df['date_start'] - df['first_spend_date']).dt.days
df['week_offset'] = 1 + (df['days_since_launch'] // 7)

# Step 5: Calculate cohort_week (Monday of launch week)
df['cohort_week'] = df['first_spend_date'] - pd.to_timedelta(df['first_spend_date'].dt.dayofweek, unit='D')

# Step 6: Filter for weeks 1-4 only
df = df[df['week_offset'].between(1, 4)].copy()

# Step 7: Calculate ROAS (note: your original had it backwards - should be revenue/spend)
df['roas'] = df['revenue'] / df['spend']

# Select final columns (matching SQL output)
final_columns = [
    'ad_id', 'ad_name', 'adset_id', 'adset_name', 
    'campaign_id', 'campaign_name', 'date_start',
    'impressions', 'spend', 'purchases', 'revenue',
    'first_spend_date', 'week_offset', 'cohort_week', 'roas'
]

df = df[final_columns]


# Simpler approach - calculate hit rate as ads with at least 1 purchase / total ads
def create_simple_dashboard(df):
    results = []
    
    # Get unique cohort weeks
    cohort_weeks = sorted(df['cohort_week'].unique(), reverse=True)
    
    for cohort in cohort_weeks:
        cohort_data = df[df['cohort_week'] == cohort]
        total_assets = cohort_data['ad_id'].nunique()
        total_hits = cohort_data['purchases'].sum()
        
        row = {
            'Date': cohort.strftime('%d-%b-%y'),
            'Assets': total_assets,
            'Hits': int(total_hits)
        }
        
        # Calculate hit rate for each week
        for week in range(1, 5):
            week_data = cohort_data[cohort_data['week_offset'] == week]
            # Count ads that had at least 1 purchase in this week
            ads_with_purchases = (week_data.groupby('ad_id')['purchases'].sum() > 0).sum()
            hit_rate = (ads_with_purchases / total_assets * 100) if total_assets > 0 else 0
            row[f'Week {week} (+{week*7})'] = round(hit_rate, 1)
        
        results.append(row)
    
    # Add total row
    total_assets = df['ad_id'].nunique()
    total_row = {
        'Date': 'Total',
        'Assets': total_assets,
        'Hits': int(df['purchases'].sum())
    }
    
    for week in range(1, 5):
        week_data = df[df['week_offset'] == week]
        ads_with_purchases = (week_data.groupby('ad_id')['purchases'].sum() > 0).sum()
        hit_rate = (ads_with_purchases / total_assets * 100) if total_assets > 0 else 0
        total_row[f'Week {week} (+{week*7})'] = round(hit_rate, 1)
    
    results.append(total_row)
    
    return pd.DataFrame(results)



print(f"\nFinal shape: {df.shape}")
print(df.head())
df.to_csv("fb_ad_daily_testing.csv", index=False)

end_time = time.time()
print(f"Time taken for initial data extraction: {end_time - start_time} seconds")

# Use it
dashboard_df = create_simple_dashboard(df)
print(dashboard_df.to_string(index=False))

end_time = time.time()
print(f"Time taken for dashboard creation: {end_time - start_time} seconds")