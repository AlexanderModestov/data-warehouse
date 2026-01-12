# Stripe Payments Dashboard

Streamlit dashboard for viewing Stripe payment statistics.

## Local Development

1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Create `.streamlit/secrets.toml`:
   ```toml
   DASHBOARD_PASSWORD = "your-password"
   ```

3. Run the app:
   ```bash
   streamlit run app.py
   ```

## Deployment to Heroku

1. Create Heroku app:
   ```bash
   heroku create your-app-name
   ```

2. Set environment variables:
   ```bash
   heroku config:set DASHBOARD_PASSWORD=your-secure-password
   ```

3. Deploy:
   ```bash
   git subtree push --prefix dashboard heroku main
   ```

## Switching to Real Data

When dbt marts are ready, update `lib/queries.py`:

1. Add database connection in `lib/db.py`
2. Replace mock data calls with SQL queries
3. Update `.streamlit/secrets.toml` with `DATABASE_URL`

## Pages

- **Overview** - Revenue, success rate, failure breakdown
- **Payments** - Detailed payment explorer with filters
