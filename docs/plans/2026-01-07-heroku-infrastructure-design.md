# Heroku Infrastructure Design

**Date:** 2026-01-07
**Purpose:** Deploy full data stack on Heroku: Meltano pipelines, dbt transformations, and Metabase dashboards

## Overview

Two Heroku apps running the complete Reluvia data pipeline:

| App | Purpose | Dyno Type | Cost |
|-----|---------|-----------|------|
| `reluvia-dwh` | Data pipelines (Meltano + Python + dbt) | One-off dynos via Scheduler | ~$0.50/month |
| `reluvia-metabase` | Dashboards | Eco | $5/month |

**Total estimated cost:** ~$10-11/month (including Postgres Essential-0 at $5)

## Architecture

```
┌─────────────────┐    ┌─────────────────┐
│  Meltano        │    │  Python script  │
│  (Stripe,       │    │  (FunnelFox)    │
│   Amplitude)    │    │                 │
└────────┬────────┘    └────────┬────────┘
         │  (parallel)          │
         └──────────┬───────────┘
                    ▼
            ┌───────────────┐
            │     dbt       │
            │  (build marts)│
            └───────────────┘
                    │
                    ▼
            ┌───────────────┐
            │   Metabase    │
            │  (dashboards) │
            └───────────────┘
```

**Schedule:** Daily at 02:00 UTC via Heroku Scheduler

## Project Structure

```
DWH/
├── Procfile                    # Heroku process definitions
├── runtime.txt                 # Python version
├── requirements.txt            # Python dependencies
├── deploy/
│   └── heroku_setup.sh         # One-time setup script
├── bin/
│   └── run_pipeline.sh         # Daily orchestration script
├── reluvia/                    # Meltano project
│   └── meltano.yml             # Uses $DATABASE_URL
├── dbt/                        # dbt project
│   ├── profiles.yml            # Heroku-compatible config
│   └── models/marts/           # Analytics marts
└── raw_funnelfox.py            # FunnelFox extraction script
```

## Data Pipeline App (`reluvia-dwh`)

### Procfile

```
pipeline: bin/run_pipeline.sh
meltano: cd reluvia && meltano run tap-stripe tap-amplitude target-postgres
funnelfox: python raw_funnelfox.py
dbt: cd dbt && dbt run
```

### Orchestration Script (`bin/run_pipeline.sh`)

Runs daily via Heroku Scheduler:
1. Runs Meltano and FunnelFox Python script in parallel
2. Waits for both to complete
3. Runs dbt transformations
4. Logs timestamps and errors

Key features:
- Parallel extraction saves time
- Fails fast if any step errors
- dbt only runs after data is loaded

### Environment Variables

Set via `heroku config:set -a reluvia-dwh`:

| Variable | Description |
|----------|-------------|
| `DATABASE_URL` | Auto-set by Heroku Postgres add-on |
| `TAP_STRIPE_API_KEY` | Stripe API key |
| `TAP_AMPLITUDE_API_KEY` | Amplitude API key |
| `TAP_AMPLITUDE_SECRET_KEY` | Amplitude secret key |
| `FUNNELFOX_API_KEY` | FunnelFox credentials |

## Metabase App (`reluvia-metabase`)

### Deployment

Uses official Metabase buildpack, connects to the same Postgres database as the DWH.

### Configuration

| Variable | Value |
|----------|-------|
| `MB_DB_TYPE` | `postgres` |
| `MB_DB_CONNECTION_URI` | DWH Postgres URL |
| `JAVA_OPTS` | `-Xmx300m` (memory limit for eco dyno) |

### Dyno Sizing

- **Eco ($5/month):** Sleeps after 30 min inactivity, 10-15s cold start
- **Upgrade path:** Basic ($7) or Standard-1X ($25) for always-on

## Monitoring & Error Handling

### Papertrail Logging

```bash
heroku addons:create papertrail:choklad -a reluvia-dwh
```

Features:
- Search logs for "ERROR" or "PIPELINE_FAILED"
- Email alerts when patterns match
- Log retention beyond Heroku's 1500-line limit

### Failure Notification

Pipeline script logs `PIPELINE_FAILED: <reason>` on errors. Papertrail alert triggers email notification.

Optional Slack webhook can be added later.

## Deployment Script

One-time setup via `deploy/heroku_setup.sh`:

### Part 1: Data Pipeline App

```bash
heroku create reluvia-dwh --region eu
heroku addons:create heroku-postgresql:essential-0 -a reluvia-dwh
heroku addons:create scheduler:standard -a reluvia-dwh
heroku config:set -a reluvia-dwh \
  TAP_STRIPE_API_KEY="sk_live_xxx" \
  TAP_AMPLITUDE_API_KEY="xxx" \
  TAP_AMPLITUDE_SECRET_KEY="xxx" \
  FUNNELFOX_API_KEY="xxx"
git push heroku main
heroku addons:open scheduler -a reluvia-dwh
# → Add job: bin/run_pipeline.sh at 02:00 UTC
```

### Part 2: Metabase App

```bash
heroku create reluvia-metabase --region eu
heroku buildpacks:add https://github.com/metabase/metabase-buildpack -a reluvia-metabase
heroku config:set -a reluvia-metabase \
  MB_DB_TYPE="postgres" \
  MB_DB_CONNECTION_URI="$(heroku config:get DATABASE_URL -a reluvia-dwh)" \
  JAVA_OPTS="-Xmx300m"
# Deploy from Metabase buildpack
```

## Heroku Resources Summary

| Resource | Cost |
|----------|------|
| Postgres Essential-0 | $5/month |
| Metabase Eco dyno | $5/month |
| Scheduler | Free |
| Papertrail Choklad | Free |
| One-off dyno usage | ~$0.50/month |
| **Total** | **~$10-11/month** |

## Identity Linking (from CLAUDE.md)

- **Primary Link:** `raw_funnelfox.sessions.profile_id` is Master ID
- **Stripe Link:** `raw_stripe.charges.id` ↔ `raw_funnelfox.subscriptions.psp_id`
- **Amplitude Link:** `raw_amplitude.events.user_id` ↔ `profile_id`
- **Fallback:** Use `email` or `device_id` when `user_id` is null

## Next Steps

1. Create deployment files (`deploy/heroku_setup.sh`, `bin/run_pipeline.sh`)
2. Update Procfile
3. Configure dbt profiles.yml for Heroku
4. Update meltano.yml to use `$DATABASE_URL`
5. Test locally, then deploy
6. Configure Heroku Scheduler job
7. Set up Metabase and connect to analytics schema
