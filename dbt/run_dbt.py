#!/usr/bin/env python
"""
Wrapper script to run dbt with environment variables from .env file.

Usage:
    python run_dbt.py run --select mart_stripe_payments
    python run_dbt.py test
    python run_dbt.py debug
"""
import os
import sys
import subprocess
from pathlib import Path

# Load .env from project root (one level up from dbt folder)
from dotenv import load_dotenv

project_root = Path(__file__).parent.parent
env_file = project_root / '.env'

if env_file.exists():
    load_dotenv(env_file)
    print(f"Loaded environment from {env_file}")
else:
    print(f"Warning: {env_file} not found")

# Run dbt with all arguments passed to this script
# Pass current environment (including loaded .env vars) to subprocess
result = subprocess.run(
    ['dbt'] + sys.argv[1:],
    cwd=Path(__file__).parent,
    env=os.environ.copy(),  # Pass environment variables to subprocess
    shell=True
)
sys.exit(result.returncode)
