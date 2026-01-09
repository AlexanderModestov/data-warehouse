#!/bin/bash
# Helper script to run Meltano pipelines with the correct schema

case "$1" in
  stripe)
    TARGET_SCHEMA=raw_stripe meltano run tap-stripe target-postgres
    ;;
  amplitude)
    TARGET_SCHEMA=raw_amplitude meltano run tap-amplitude fix-amplitude-types target-postgres
    ;;
  facebook)
    TARGET_SCHEMA=raw_facebook meltano run tap-postgres target-postgres
    ;;
  funnelfox)
    TARGET_SCHEMA=raw_funnelfox meltano run tap-funnelfox target-postgres
    ;;
  *)
    echo "Usage: $0 {stripe|amplitude|facebook|funnelfox}"
    echo "Example: $0 stripe"
    exit 1
    ;;
esac
