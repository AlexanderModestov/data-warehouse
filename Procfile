# Data Pipeline Processes (run via Heroku Scheduler)
pipeline: deploy/run_pipeline.sh
meltano: cd reluvia && meltano run tap-stripe tap-amplitude target-postgres
funnelfox: python raw_funnelfox.py
dbt: cd dbt && dbt run --profiles-dir .
