# DWH - Data Warehouse

## Meltano Setup

This project uses Meltano for data integration and ELT pipelines.

## Prerequisites

- Python 3.8 or higher
- Meltano installed (`pip install meltano`)

## Project Initialization

If starting from scratch, initialize a Meltano project:

```bash
meltano init --no-usage-stats .
```

## Adding Extractors (Taps)

Extractors are data sources that pull data from various systems.

### Available Extractors

To see all available extractors:

```bash
meltano hub search extractors
```

### Adding an Extractor

```bash
meltano add extractor <extractor-name>
```

### Common Extractors Examples

```bash
# Amplitude
meltano add extractor tap-amplitude

# PostgreSQL
meltano add extractor tap-postgres

# Google Analytics
meltano add extractor tap-google-analytics

# Salesforce
meltano add extractor tap-salesforce

# MySQL
meltano add extractor tap-mysql

# MongoDB
meltano add extractor tap-mongodb
```

### Configuring an Extractor

After adding an extractor, configure it with required credentials:

```bash
# Interactive configuration
meltano config <extractor-name> set

# Or set specific values
meltano config <extractor-name> set <setting-name> <value>
```

Example for tap-amplitude:
```bash
meltano config tap-amplitude set airbyte_config.api_key YOUR_API_KEY
meltano config tap-amplitude set airbyte_config.secret_key YOUR_SECRET_KEY
meltano config tap-amplitude set airbyte_config.data_region US
meltano config tap-amplitude set airbyte_config.start_date 2024-01-01
```

## Adding Loaders (Targets)

Loaders are destinations where extracted data is loaded.

### Available Loaders

To see all available loaders:

```bash
meltano hub search loaders
```

### Adding a Loader

```bash
meltano add loader <loader-name>
```

### Common Loaders Examples

```bash
# JSONL (for testing/development)
meltano add loader target-jsonl

# PostgreSQL
meltano add loader target-postgres

# Snowflake
meltano add loader target-snowflake

# BigQuery
meltano add loader target-bigquery

# CSV
meltano add loader target-csv

# Parquet
meltano add loader target-parquet
```

### Configuring a Loader

After adding a loader, configure it with connection details:

```bash
# Interactive configuration
meltano config <loader-name> set

# Or set specific values
meltano config <loader-name> set <setting-name> <value>
```

Example for target-postgres:
```bash
meltano config target-postgres set host localhost
meltano config target-postgres set port 5432
meltano config target-postgres set user postgres
meltano config target-postgres set password YOUR_PASSWORD
meltano config target-postgres set database dwh
meltano config target-postgres set default_target_schema public
```

Example for target-jsonl (simplest for testing):
```bash
meltano config target-jsonl set destination_path output
```

## Running Pipelines

Once you have configured an extractor and loader, run the pipeline:

```bash
meltano run <extractor-name> <loader-name>
```

Example:
```bash
meltano run tap-amplitude target-jsonl
```

## Testing Configuration

Test your extractor configuration:

```bash
meltano invoke <extractor-name> --discover
```

Test your loader configuration:

```bash
meltano invoke <loader-name> --help
```

## Viewing Current Configuration

To see current configuration for a plugin:

```bash
meltano config <plugin-name> list
```

To see the entire project configuration:

```bash
cat meltano.yml
```

## Scheduling Pipelines

To schedule pipelines, add orchestrator (e.g., Airflow):

```bash
meltano add orchestrator airflow
```

## Useful Commands

```bash
# List all installed plugins
meltano list

# Remove a plugin
meltano remove extractor <extractor-name>
meltano remove loader <loader-name>

# Update a plugin
meltano upgrade extractor <extractor-name>

# View logs
meltano invoke <plugin-name> --log-level debug
```

## Environment Variables

You can use environment variables for sensitive configuration:

```bash
# Set environment variable
export TAP_AMPLITUDE_API_KEY=your_key

# Or use .env file (add to .gitignore!)
echo "TAP_AMPLITUDE_API_KEY=your_key" >> .env
```

Then reference in meltano.yml:
```yaml
config:
  api_key: $TAP_AMPLITUDE_API_KEY
```

## Documentation

- [Meltano Documentation](https://docs.meltano.com/)
- [Meltano Hub](https://hub.meltano.com/)
- [Meltano Community Slack](https://meltano.com/slack)


meltano init
