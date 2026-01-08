#!/bin/bash
# Step 2: Metabase Setup
# Creates separate app for Metabase, attaches existing Postgres
set -e

echo "============================================"
echo "Reluvia DWH - Metabase Setup (Step 2 of 2)"
echo "============================================"

# Configuration
APP_NAME_METABASE="reluvia-metabase"
APP_NAME_POSTGRES="reluvia-bi"  # App that owns the Postgres
REGION="eu"

# ============================================
# Create Metabase App
# ============================================
echo ""
echo ">>> Creating Metabase app: $APP_NAME_METABASE"

if heroku apps:info -a "$APP_NAME_METABASE" > /dev/null 2>&1; then
  echo "  App already exists"
else
  heroku create "$APP_NAME_METABASE" --region "$REGION"
fi

# ============================================
# Add Metabase Buildpack
# ============================================
echo ""
echo ">>> Adding Metabase buildpack..."

heroku buildpacks:clear -a "$APP_NAME_METABASE" 2>/dev/null || true
heroku buildpacks:add https://github.com/metabase/metabase-buildpack -a "$APP_NAME_METABASE"

# ============================================
# Attach Postgres
# ============================================
echo ""
echo ">>> Attaching Postgres from $APP_NAME_POSTGRES..."

POSTGRES_ADDON=$(heroku addons -a "$APP_NAME_POSTGRES" --json | python -c "import sys, json; addons = json.load(sys.stdin); print(next((a['name'] for a in addons if 'postgresql' in a['addon_service']['name']), ''))" 2>/dev/null)

if [ -z "$POSTGRES_ADDON" ]; then
  echo "ERROR: No Postgres addon found on $APP_NAME_POSTGRES"
  exit 1
fi

echo "  Found: $POSTGRES_ADDON"
heroku addons:attach "$POSTGRES_ADDON" -a "$APP_NAME_METABASE" --as DATABASE 2>/dev/null || echo "  Already attached"

# ============================================
# Configure Metabase
# ============================================
echo ""
echo ">>> Configuring Metabase..."

heroku config:set -a "$APP_NAME_METABASE" \
  MB_DB_TYPE="postgres" \
  JAVA_OPTS="-Xmx300m"

# ============================================
# Deploy Metabase
# ============================================
echo ""
echo ">>> Deploying Metabase..."
echo "    (This takes 3-5 minutes - Metabase is a large app)"

# Metabase buildpack needs an empty deploy to trigger
cd /tmp
rm -rf metabase-deploy 2>/dev/null || true
mkdir metabase-deploy
cd metabase-deploy
git init
git commit --allow-empty -m "Deploy Metabase"
heroku git:remote -a "$APP_NAME_METABASE"
git push heroku main --force

cd - > /dev/null

# ============================================
# Summary
# ============================================
echo ""
echo "============================================"
echo "Metabase Setup Complete!"
echo "============================================"
echo ""
echo "App: $APP_NAME_METABASE"
echo "URL: https://$APP_NAME_METABASE.herokuapp.com"
echo ""
echo "NEXT STEPS:"
echo ""
echo "1. Open Metabase:"
echo "   heroku open -a $APP_NAME_METABASE"
echo ""
echo "2. Complete initial setup in browser:"
echo "   - Create admin account"
echo "   - Add database connection (use same Postgres URL)"
echo "   - Point to 'analytics' schema for the dbt marts"
echo ""
echo "3. View logs if issues:"
echo "   heroku logs --tail -a $APP_NAME_METABASE"
echo ""
echo "Note: Eco dynos sleep after 30 min inactivity."
echo "First load may take 10-15 seconds to wake up."
echo ""
