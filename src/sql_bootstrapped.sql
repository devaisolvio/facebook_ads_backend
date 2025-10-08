-- Raw daily facts (one row per ad per day)
create table if not exists fb_ad_daily (
  ad_id            text not null,
  ad_name          text,
  adset_id         text,
  adset_name       text,
  campaign_id      text,
  campaign_name    text,
  date_start       date not null,
  impressions      bigint default 0,
  spend            numeric(18,6) default 0,
  reach           bigint default 0,
  purchases        bigint default 0,
  revenue          numeric(18,6) default 0,
  raw_actions      jsonb,
  raw_action_values jsonb,
  updated_at       timestamptz default now(),
  primary key (ad_id, date_start)
);

-- Final small table your React app reads (pandas rewrites it daily)
create table if not exists cohort_grid (
  cohort_week date,
  week_offset int,
  hit_pct numeric,
  total_ads int,
  -- optional denormalized filters:
  campaign_id text,
  campaign_name text
);
create index if not exists idx_cohort_grid_week on cohort_grid(cohort_week, week_offset);
