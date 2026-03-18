import time
import uuid
import requests
from datetime import datetime

from sqlalchemy import inspect
from sqlalchemy.dialects.mysql import insert as mysql_insert

from db import SessionLocal, get_session
from crawler_builders import build_dataset, build_publisher, build_tags, build_topics, build_resources
from models import _IngestionItem, _IngestionRun


DATASETS_URL = "https://catalog.data.gov/api/3/action/package_search"
ORGANIZATION_LIST_URL = "https://catalog.data.gov/api/3/action/organization_list"
ORGANIZATION_SHOW_URL = "https://catalog.data.gov/api/3/action/organization_show"
MAX_RETRIES = 3
DATASET_COUNT = 1000
DATASET_START = 0

_organization_cache: dict[str, dict] = {}
_organization_fetch_failures: set[str] = set()


# Convert a SQLAlchemy ORM instance into a plain dict of column values.
def _row_from_model(model_obj) -> dict:
    mapper = inspect(model_obj.__class__)
    return {
        attr.columns[0].name: getattr(model_obj, attr.key)
        for attr in mapper.column_attrs
    }


# Build the MySQL ON DUPLICATE KEY UPDATE mapping for non-primary-key columns.
def _on_duplicate_update_map(insert_stmt, table) -> dict:
    primary_key_columns = {column.key for column in table.primary_key.columns}
    update_map = {
        column.key: getattr(insert_stmt.inserted, column.key)
        for column in table.columns
        if column.key not in primary_key_columns
    }

    # Composite-key-only tables have no mutable non-PK columns; this keeps SQL valid.
    if not update_map:
        first_pk = next(iter(primary_key_columns))
        update_map[first_pk] = getattr(insert_stmt.inserted, first_pk)

    return update_map


# Upsert a single ORM object using MySQL ON DUPLICATE KEY UPDATE semantics.
def _upsert_one(session, model_obj) -> None:
    table = model_obj.__table__
    row = _row_from_model(model_obj)

    insert_stmt = mysql_insert(table).values(row)
    update_map = _on_duplicate_update_map(insert_stmt, table)
    session.execute(insert_stmt.on_duplicate_key_update(**update_map))


# Upsert a list of ORM objects in one statement using MySQL ON DUPLICATE KEY UPDATE.
def _upsert_many(session, model_objects: list) -> None:
    if not model_objects:
        return

    table = model_objects[0].__table__
    rows = [_row_from_model(item) for item in model_objects]

    insert_stmt = mysql_insert(table).values(rows)
    update_map = _on_duplicate_update_map(insert_stmt, table)
    session.execute(insert_stmt.on_duplicate_key_update(**update_map))


# Fetch the configured batch of datasets from data.gov and validate the API response.
def fetch_datasets() -> list[dict]:
    response = requests.get(
        DATASETS_URL,
        params={"rows": DATASET_COUNT, "start": DATASET_START},
        timeout=30,
    )
    response.raise_for_status()
    payload = response.json()

    if not payload.get("success"):
        raise RuntimeError("Data.gov API returned an unsuccessful response")

    results = payload.get("result", {}).get("results", [])
    if not results:
        raise RuntimeError("No datasets returned by data.gov API")

    # Preload organization metadata once so dataset persistence can do O(1) local lookups.
    preload_organizations_lookup()

    return results


# Bulk-load organization metadata for fast publisher enrichment.
def preload_organizations_lookup() -> None:
    try:
        response = requests.get(
            ORGANIZATION_LIST_URL,
            params={"all_fields": True, "include_extras": True, "limit": 1000},
            timeout=60,
        )
        response.raise_for_status()
        payload = response.json()

        if not payload.get("success"):
            raise RuntimeError("Data.gov organization_list returned an unsuccessful response")

        organizations = payload.get("result", [])
        if not isinstance(organizations, list):
            raise RuntimeError("Data.gov organization_list returned an invalid result payload")

        for item in organizations:
            if not isinstance(item, dict):
                continue
            organization_id = item.get("id")
            if organization_id:
                _organization_cache[organization_id] = item
    except Exception:
        # Keep crawl resilient; missing entries can still be fetched via organization_show fallback.
        return


# Fetch full publisher details once per organization id.
def fetch_organization_details(organization_id: str) -> dict | None:
    if organization_id in _organization_cache:
        return _organization_cache[organization_id]

    if organization_id in _organization_fetch_failures:
        return None

    try:
        response = requests.post(
            ORGANIZATION_SHOW_URL,
            json={"id": organization_id},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()

        if not payload.get("success"):
            raise RuntimeError("Data.gov organization_show returned an unsuccessful response")

        result = payload.get("result")
        if not isinstance(result, dict):
            raise RuntimeError("Data.gov organization_show returned an invalid result payload")

        _organization_cache[organization_id] = result
        return result
    except Exception:
        _organization_fetch_failures.add(organization_id)
        return None


# Record one datasets processing result in the ingestion tracking table.
def upsert_ingestion_item(
    run_id: str,
    dataset_id: str,
    status: str,
    attempt_count: int,
    error_message: str | None,
) -> None:
    with get_session() as session:
        _upsert_one(
            session,
            _IngestionItem(
                run_id=run_id,
                dataset_id=dataset_id,
                status=status,
                attempt_count=attempt_count,
                processed_at=datetime.utcnow(),
                error_message=error_message,
            ),
        )


# Persist one dataset and related entities with retries and per-attempt transaction boundaries.
def persist_dataset_with_retry(run_id: str, dataset_payload: dict) -> tuple[bool, int, str | None, str]:
    dataset_id = dataset_payload.get("id") or f"missing-id-{uuid.uuid4()}"
    last_error: str | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # One transaction per dataset attempt.
            with SessionLocal.begin() as session:
                dataset_organization = dataset_payload.get("organization") or {}
                organization_id = dataset_organization.get("id")
                full_organization = (
                    fetch_organization_details(organization_id) if organization_id else None
                )

                publisher = build_publisher(dataset_payload, full_organization)
                dataset = build_dataset(dataset_payload)
                topics = build_topics(dataset_payload, dataset.id)
                tags = build_tags(dataset_payload, dataset.id)
                resources = build_resources(dataset_payload, dataset.id)

                if publisher is not None:
                    _upsert_one(session, publisher)

                _upsert_one(session, dataset)

                _upsert_many(session, topics)

                _upsert_many(session, tags)

                _upsert_many(session, resources)

            upsert_ingestion_item(
                run_id=run_id,
                dataset_id=dataset_id,
                status="success",
                attempt_count=attempt,
                error_message=None,
            )
            return True, attempt, None, dataset_id
        except Exception as exc:
            last_error = f"{type(exc).__name__}: {exc}"
            if attempt < MAX_RETRIES:
                time.sleep(0.5 * (2 ** (attempt - 1)))

    upsert_ingestion_item(
        run_id=run_id,
        dataset_id=dataset_id,
        status="failed",
        attempt_count=MAX_RETRIES,
        error_message=last_error,
    )
    return False, MAX_RETRIES, last_error, dataset_id


# Create and persist a new ingestion run entry, then return its generated run id.
def start_run(total_target: int) -> str:
    run_id = str(uuid.uuid4())
    with get_session() as session:
        session.add(
            _IngestionRun(
                id=run_id,
                status="running",
                started_at=datetime.utcnow(),
                total_target=total_target,
                total_success=0,
                total_failed=0,
            )
        )
    return run_id


# Finalize an ingestion run with totals, status, and optional error details.
def finish_run(
    run_id: str,
    total_success: int,
    total_failed: int,
    error_message: str | None = None,
) -> None:
    with get_session() as session:
        run = session.get(_IngestionRun, run_id)
        if run is None:
            return

        run.finished_at = datetime.utcnow()
        run.total_success = total_success
        run.total_failed = total_failed
        run.status = "failed" if (error_message or total_failed > 0) else "success"
        run.error_message = error_message
