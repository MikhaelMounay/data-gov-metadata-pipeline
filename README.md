# Data.gov Metadata Pipeline & Datasets Explorer

A Python data pipeline that crawls Data.gov dataset metadata, normalizes it into a relational model, and persists it to MySQL for analysis and downstream applications.

## What This Project Does (So far .. 😄)
- Fetches dataset metadata from the Data.gov CKAN API.
- Persists publishers, datasets, topics, tags, resources, and ingestion run metadata.
- Uses idempotent upserts to safely re-run ingestion without duplicating primary keys.
- Seeds application users from a CSV file.
- Exports database tables to CSV for reporting and offline analysis.
- Provides a multi-page web client for data operations, discovery, and analytics.
- Supports portable runtime with Docker and Docker Compose.

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

Run with Docker (portable setup):

```bash
docker compose up --build
```

Then open:

```text
http://127.0.0.1:8000
```

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

Run the client web app:

```bash
python -m data_gov_datasets_explorer.webapp.app
```

Then open:

```text
http://127.0.0.1:8000
```

The client app supports:
- Registering an application user.
- Creating a project for a user.
- Adding dataset usage entries for an existing user project.
- Selecting datasets for usage through a searchable picker backed by the full dataset catalog.
- Viewing user usage history.
- Querying datasets by organization type, format, and tag.
- Viewing top organizations and top datasets by user usage.
- Viewing grouped totals by organization, topic, format, and organization type.
- Viewing usage distribution by project type.
- Viewing top 10 tags by project type.

Navigation is split by purpose:
- Actions: register users and add usage records.
- Discovery: user usage and dataset lookups.
- Analytics: organization, project, and tag/topic reports.

## Logs
The project writes structured logs to console and file. Typical files include:
- `crawler.log`
- `seeding-users.log`
- `export_db_csv.log`
