@echo off
REM Helper script to run Meltano pipelines with the correct schema

if "%1"=="stripe" (
    set "TARGET_SCHEMA=raw_stripe" && meltano run tap-stripe target-postgres
) else if "%1"=="amplitude" (
    set "TARGET_SCHEMA=raw_amplitude" && meltano run tap-amplitude fix-amplitude-types target-postgres
) else if "%1"=="facebook" (
    set "TARGET_SCHEMA=raw_facebook" && meltano run tap-postgres target-postgres
) else if "%1"=="funnelfox" (
    set "TARGET_SCHEMA=raw_funnelfox" && meltano run tap-funnelfox target-postgres
) else (
    echo Usage: %0 {stripe^|amplitude^|facebook^|funnelfox}
    echo Example: %0 stripe
    exit /b 1
)
