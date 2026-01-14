@echo off
REM Wrapper to run dbt with .env variables loaded
REM Usage: run_dbt.bat run --select mart_stripe_payments

cd /d %~dp0
python run_dbt.py %*
