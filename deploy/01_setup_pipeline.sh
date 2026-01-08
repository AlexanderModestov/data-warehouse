#!/bin/bash
# Step 1: Data Pipeline Setup
# Deploys to existing reluvia-bi app (Postgres & credentials already configured)
set -e

echo "============================================"
echo "Reluvia DWH - Pipeline Setup (Step 1 of 2)"
echo "============================================"

# Configuration
APP_NAME="reluvia-bi"

# ============================================
# Verify App Exists
# ============================================
echo ""
echo ">>> Verifying app: $APP_NAME"

if ! heroku apps:info -a "$APP_NAME" > /dev/null 2>&1; then
  echo "ERROR: App $APP_NAME does not exist"
  exit 1
fi
echo "  App exists"

# Verify DATABASE_URL
if ! heroku config:get DATABASE_URL -a "$APP_NAME" > /dev/null 2>&1; then
  echo "ERROR: DATABASE_URL not set on $APP_NAME"
  exit 1
fi
echo "  DATABASE_URL is configured"

# ============================================
# Add Scheduler (if not exists)
# ============================================
echo ""
echo ">>> Adding Heroku Scheduler..."

if heroku addons -a "$APP_NAME" | grep -q "scheduler"; then
  echo "  Scheduler already exists"
else
  heroku addons:create scheduler:standard -a "$APP_NAME"
fi

# ============================================
# Add Papertrail (if not exists)
# ============================================
echo ""
echo ">>> Adding Papertrail for logging..."

if heroku addons -a "$APP_NAME" | grep -q "papertrail"; then
  echo "  Papertrail already exists"
else
  heroku addons:create papertrail:choklad -a "$APP_NAME"
fi

# ============================================
# Deploy Code
# ============================================
echo ""
echo ">>> Setting up git remote..."

git remote remove heroku 2>/dev/null || true
heroku git:remote -a "$APP_NAME"

echo ""
echo ">>> Deploying code to Heroku..."

git push heroku main

# ============================================
# Configure Scheduler
# ============================================
echo ""
echo ">>> Opening Scheduler configuration..."
echo ""
echo "  ADD THIS JOB MANUALLY:"
echo "  ┌───────────────────────────────────────┐"
echo "  │ Command:   deploy/run_pipeline.sh    │"
echo "  │ Schedule:  Daily at 02:00 UTC        │"
echo "  └───────────────────────────────────────┘"
echo ""

heroku addons:open scheduler -a "$APP_NAME"

# ============================================
# Summary
# ============================================
echo ""
echo "============================================"
echo "Pipeline Setup Complete!"
echo "============================================"
echo ""
echo "App: $APP_NAME"
echo ""
echo "NEXT STEPS:"
echo ""
echo "1. Configure the Scheduler job (browser window opened)"
echo "   Command:  deploy/run_pipeline.sh"
echo "   Schedule: Daily at 02:00 UTC"
echo ""
echo "2. Test the pipeline manually:"
echo "   heroku run deploy/run_pipeline.sh -a $APP_NAME"
echo ""
echo "3. View logs:"
echo "   heroku logs --tail -a $APP_NAME"
echo ""
echo "When ready for Metabase, run: ./deploy/02_setup_metabase.sh"
echo ""
