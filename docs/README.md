# Data.gov Metadata Pipeline & Datasets Explorer

A Python data pipeline that crawls Data.gov dataset metadata, normalizes it into a relational model, and persists it to MySQL for analysis and downstream applications.

## What This Project Does (So far .. 😄)
- Fetches dataset metadata from the Data.gov CKAN API.
- Persists publishers, datasets, topics, tags, resources, and ingestion run metadata.
- Uses idempotent upserts to safely re-run ingestion without duplicating primary keys.
- Seeds application users from a CSV file.
- Exports database tables to CSV for reporting and offline analysis.

## Tech Stack
- Python 3.11+
- SQLAlchemy 2.x
- MySQL Connector/Python
- Requests

## Project Structure
```text
src/data_gov_datasets_explorer/
  crawler/               # Crawl + ingestion orchestration and helpers
  seeding/               # CSV-based user seeding utilities
  export_db/             # Database table CSV export tools
  db.py                  # SQLAlchemy engine/session setup
  models.py              # ORM models and enums
  logger.py              # App logger wrapper
  main.py                # Current top-level app entry in package
```

## Prerequisites
- Python 3.11 or newer
- MySQL server
- A database matching your environment configuration

## Configuration
Create and adjust your `.env` using `.env.example` values:

`DATABASE_URL` takes precedence if set.

## Installation
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .
```

If editable install is not desired, use:

```bash
pip install .
```

## Running the Project
Run the Data.gov crawler ingestion:

```bash
python -m data_gov_datasets_explorer.crawler.crawler
```

Run user seeding from `seeding/users.csv`:

```bash
python -m data_gov_datasets_explorer.seeding.users
```

Export all DB tables to CSV:

```bash
python -m data_gov_datasets_explorer.export_db.export_db_csv
```

## Logs
The project writes structured logs to console and file. Typical files include:
- `crawler.log`
- `seeding-users.log`
- `export_db_csv.log`
