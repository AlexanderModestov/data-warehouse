@echo off
REM Run Amplitude pipeline with Unicode sanitization (fully file-based approach)
REM Each step writes to a file to avoid Windows pipe issues

set TARGET_SCHEMA=raw_amplitude
set PYTHONIOENCODING=utf-8

echo Starting Amplitude pipeline with Unicode sanitization...

REM Activate virtual environment
call ..\.venv\Scripts\activate

REM Step 1: Extract data from tap to intermediate file (only stdout, not stderr)
echo Step 1: Extracting data from Amplitude...
meltano invoke tap-amplitude > amplitude_raw.json
if %ERRORLEVEL% NEQ 0 (
    echo Error during extraction.
    exit /b 1
)

REM Step 2: Sanitize Unicode (only stdout to file)
echo Step 2: Sanitizing Unicode characters...
python sanitize_unicode.py < amplitude_raw.json > amplitude_sanitized.json
if %ERRORLEVEL% NEQ 0 (
    echo Error during sanitization.
    exit /b 1
)

REM Step 3: Transform with mapper (only stdout to file)
echo Step 3: Applying type transformations...
meltano invoke fix-amplitude-types < amplitude_sanitized.json > amplitude_mapped.json
if %ERRORLEVEL% NEQ 0 (
    echo Error during mapping.
    exit /b 1
)

REM Step 4: Load to PostgreSQL
echo Step 4: Loading to PostgreSQL...
meltano invoke target-postgres < amplitude_mapped.json
if %ERRORLEVEL% NEQ 0 (
    echo Error during loading to PostgreSQL.
    exit /b 1
)

REM Clean up intermediate files (uncomment to enable)
REM del amplitude_raw.json
REM del amplitude_sanitized.json
REM del amplitude_mapped.json

echo Pipeline completed successfully.
