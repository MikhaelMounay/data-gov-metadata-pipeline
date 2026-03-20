# Data.gov Metadata Pipeline & Datasets Explorer

A Python project that ingests Data.gov metadata into MySQL and provides a web client for operational workflows and analytics.

## What This Project Does
- Crawls Data.gov CKAN metadata.
- Persists publishers, datasets, topics, tags, resources, and ingestion metadata into MySQL.
- Uses idempotent upserts to support safe re-ingestion.
- Seeds application users from CSV.
- Exports database tables to CSV for reporting.
- Provides a multi-page web application for:
  - user registration,
  - project creation,
  - dataset usage tracking,
  - dataset exploration,
  - analytics dashboards and ranking reports.
- Supports containerized runtime using Docker and Docker Compose.

## Tech Stack
- Python 3.11+
- SQLAlchemy 2.x
- MySQL Connector/Python
- Flask
- Waitress
- python-dotenv
- Requests
- Docker / Docker Compose

## Project Structure
```text
src/data_gov_datasets_explorer/
  crawler/               # Crawl + ingestion orchestration and helpers
  seeding/               # CSV-based user seeding utilities
  export_db/             # Database table CSV export tools
  webapp/                # Flask app, templates, static assets, service layer
  db.py                  # SQLAlchemy engine/session setup
  models.py              # ORM models and enums
  logger.py              # App logger wrapper
  main.py                # Unified runtime entry (dev/prod web app server)

migrations/
  datagov_datasets-creation_sql.sql
```

## Prerequisites
- Python 3.11+ (for local run)
- MySQL server and target database
- Docker Desktop (optional, for containerized run)

## Configuration
Create `.env` from `.env.example` and adjust values as needed.

Key variables:
- `DATABASE_URL` (takes precedence over split DB vars)
- `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`
- `WEBAPP_PORT`
- `FLASK_SECRET_KEY`
- `ENVIRONMENT` (`development` or `production`)

## Installation (Local)
```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .
```

If editable install is not desired:

```bash
pip install .
```

## Running the Project (Local)
Run crawler ingestion:

```bash
python -m data_gov_datasets_explorer.crawler.crawler
```

Seed users from CSV:

```bash
python -m data_gov_datasets_explorer.seeding.users
```

Export all DB tables to CSV:

```bash
python -m data_gov_datasets_explorer.export_db.export_db_csv
```

Run the web app (uses `ENVIRONMENT` to choose Flask dev server or Waitress):

```bash
python -m data_gov_datasets_explorer.main
```

Open:

```text
http://127.0.0.1:8000
```

## Running with Docker
Start the web app container:

```bash
docker compose up --build
```

Open:

```text
http://127.0.0.1:8000
```

Note: the current `docker-compose.yml` runs the web service only and expects database connectivity from your configured `.env` values.

## Web App Capabilities
- Register application users.
- Create projects per user.
- Add dataset usage to existing projects.
- Search/select datasets from the full catalog.
- View user usage history.
- Query datasets by organization type, format, and tag.
- View top organizations and top datasets by user adoption.
- View grouped totals by organization, topic, format, and organization type.
- View usage distribution by project type.
- View top 10 tags by project type.

Navigation areas:
- Actions: user/project/usage creation
- Discovery: user and dataset lookups
- Analytics: organization/project/tag-topic reporting

## Logs
Typical log files include:
- `crawler.log`
- `seeding-users.log`
- `export_db_csv.log`
