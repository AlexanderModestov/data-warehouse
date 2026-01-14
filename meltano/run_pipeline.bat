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
) else if "%1"=="appsflyer" (
    set "TARGET_SCHEMA=raw_appsflyer" && meltano run tap-appsflyer-ios target-postgres && meltano run tap-appsflyer-android target-postgres
) else if "%1"=="appsflyer-ios" (
    set "TARGET_SCHEMA=raw_appsflyer" && meltano run tap-appsflyer-ios target-postgres
) else if "%1"=="appsflyer-android" (
    set "TARGET_SCHEMA=raw_appsflyer" && meltano run tap-appsflyer-android target-postgres
) else (
    echo Usage: %0 {stripe^|amplitude^|facebook^|funnelfox^|appsflyer^|appsflyer-ios^|appsflyer-android}
    echo Example: %0 stripe
    exit /b 1
)
