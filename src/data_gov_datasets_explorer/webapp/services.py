from __future__ import annotations

from datetime import date

from sqlalchemy import and_, desc, func, select

from data_gov_datasets_explorer.db import get_session
from data_gov_datasets_explorer.models import (
    AppUser,
    Dataset,
    DatasetTag,
    DatasetTopic,
    Project,
    ProjectCategoryEnum,
    ProjectDatasets,
    Publisher,
    Resource,
)


def parse_birthdate(raw_birthdate: str | None) -> date | None:
    if not raw_birthdate:
        return None
    return date.fromisoformat(raw_birthdate)


def normalize_project_category(raw_value: str | None) -> ProjectCategoryEnum | None:
    if not raw_value:
        return None

    normalized = raw_value.strip().lower()
    for option in ProjectCategoryEnum:
        if option.value == normalized:
            return option
    raise ValueError(
        "Invalid project category. Use one of: analytics, machine learning, field research."
    )


# Register a user in AppUser.
"""
INSERT INTO AppUser (Email, Username, Gender, Birthdate, Country)
VALUES ('user@example.com', 'alice', 'female', '1998-05-01', 'USA');
"""
def register_user(
    email: str,
    username: str | None,
    gender: str | None,
    birthdate: str | None,
    country: str | None,
) -> str:
    with get_session() as session:
        existing = session.get(AppUser, email)
        if existing:
            raise ValueError(f"A user with email '{email}' already exists.")

        user = AppUser(
            email=email,
            username=username or None,
            gender=gender or None,
            birthdate=parse_birthdate(birthdate),
            country=country or None,
        )
        session.add(user)

    return f"User '{email}' registered successfully."


def create_project(
    email: str,
    project_name: str,
    project_category_raw: str | None,
) -> str:
    project_category = normalize_project_category(project_category_raw)

    with get_session() as session:
        user = session.get(AppUser, email)
        if user is None:
            raise ValueError(f"No user found for email '{email}'.")

        existing = session.get(Project, {"app_user_email": email, "name": project_name})
        if existing:
            raise ValueError(
                f"Project '{project_name}' already exists for user '{email}'."
            )

        session.add(
            Project(
                app_user_email=email,
                name=project_name,
                project_category=project_category,
            )
        )

    return "Project created successfully."


# Add a new user usage for a given dataset.
"""
INSERT INTO ProjectDatasets (AppUserEmail, ProjectName, DatasetId)
VALUES ('user@example.com', 'Project A', 'dataset-id');
"""
def add_dataset_usage(
    email: str,
    project_name: str,
    dataset_id: str,
) -> str:
    with get_session() as session:
        user = session.get(AppUser, email)
        if user is None:
            raise ValueError(f"No user found for email '{email}'.")

        dataset = session.get(Dataset, dataset_id)
        if dataset is None:
            raise ValueError(f"No dataset found for id '{dataset_id}'.")

        project = session.get(Project, {"app_user_email": email, "name": project_name})
        if project is None:
            raise ValueError(
                f"Project '{project_name}' does not exist for '{email}'. Create it first."
            )

        usage_link = session.get(
            ProjectDatasets,
            {
                "app_user_email": email,
                "project_name": project_name,
                "dataset_id": dataset_id,
            },
        )
        if usage_link:
            raise ValueError("This dataset usage already exists for the user project.")

        session.add(
            ProjectDatasets(
                app_user_email=email,
                project_name=project_name,
                dataset_id=dataset_id,
            )
        )

    return "Dataset usage added successfully."


# View existing usage information for the user.
"""
SELECT
  p.AppUserEmail,
  p.Name AS project_name,
  p.ProjectCategory,
  d.Id AS dataset_id,
  d.Name AS dataset_name
FROM Project p
JOIN ProjectDatasets pd
  ON pd.AppUserEmail = p.AppUserEmail
 AND pd.ProjectName = p.Name
JOIN Dataset d
  ON d.Id = pd.DatasetId
WHERE p.AppUserEmail = 'user@example.com'
ORDER BY p.Name, d.Name;
"""
def fetch_user_usage(email: str) -> list[dict[str, str | None]]:
    with get_session() as session:
        stmt = (
            select(
                Project.app_user_email,
                Project.name.label("project_name"),
                Project.project_category,
                Dataset.id.label("dataset_id"),
                Dataset.name.label("dataset_name"),
            )
            .join(
                ProjectDatasets,
                and_(
                    ProjectDatasets.app_user_email == Project.app_user_email,
                    ProjectDatasets.project_name == Project.name,
                ),
            )
            .join(Dataset, Dataset.id == ProjectDatasets.dataset_id)
            .where(Project.app_user_email == email)
            .order_by(Project.name, Dataset.name)
        )

        rows = session.execute(stmt).all()
        return [
            {
                "app_user_email": row.app_user_email,
                "project_name": row.project_name,
                "project_category": (
                    row.project_category.value if row.project_category else "unassigned"
                ),
                "dataset_id": row.dataset_id,
                "dataset_name": row.dataset_name,
            }
            for row in rows
        ]


# View datasets by organization type (federal, state, city, etc.).
"""
SELECT
  d.Id AS dataset_id,
  d.Name AS dataset_name,
  p.Name AS organization,
  p.Type AS organization_type
FROM Dataset d
JOIN Publisher p
  ON p.Id = d.PublisherId
WHERE LOWER(p.Type) = LOWER('federal')
ORDER BY d.Name;
"""
def datasets_by_org_type(org_type: str) -> list[dict[str, str | None]]:
    with get_session() as session:
        stmt = (
            select(
                Dataset.id.label("dataset_id"),
                Dataset.name.label("dataset_name"),
                Publisher.name.label("organization"),
                Publisher.type.label("organization_type"),
            )
            .join(Publisher, Dataset.publisher_id == Publisher.id)
            .where(func.lower(Publisher.type) == org_type.lower())
            .order_by(Dataset.name)
        )

        return [dict(row._mapping) for row in session.execute(stmt).all()]


# View top 5 contributing organizations by number of datasets provided.
"""
SELECT
  p.Name AS organization,
  p.Type AS organization_type,
  COUNT(d.Id) AS dataset_count
FROM Publisher p
JOIN Dataset d
  ON d.PublisherId = p.Id
GROUP BY p.Id, p.Name, p.Type
ORDER BY dataset_count DESC, p.Name
LIMIT 5;
"""
def top_5_organizations() -> list[dict[str, str | int | None]]:
    with get_session() as session:
        stmt = (
            select(
                Publisher.name.label("organization"),
                Publisher.type.label("organization_type"),
                func.count(Dataset.id).label("dataset_count"),
            )
            .join(Dataset, Dataset.publisher_id == Publisher.id)
            .group_by(Publisher.id, Publisher.name, Publisher.type)
            .order_by(desc("dataset_count"), Publisher.name)
            .limit(5)
        )
        return [dict(row._mapping) for row in session.execute(stmt).all()]


# View datasets available in a given format.
"""
SELECT
  d.Id AS dataset_id,
  d.Name AS dataset_name,
  r.Format AS format,
  p.Name AS organization
FROM Dataset d
JOIN Resource r
  ON r.DatasetId = d.Id
LEFT JOIN Publisher p
  ON p.Id = d.PublisherId
WHERE LOWER(r.Format) = LOWER('csv')
ORDER BY d.Name;
"""
def datasets_by_format(dataset_format: str) -> list[dict[str, str | None]]:
    with get_session() as session:
        stmt = (
            select(
                Dataset.id.label("dataset_id"),
                Dataset.name.label("dataset_name"),
                Resource.format.label("format"),
                Publisher.name.label("organization"),
            )
            .join(Resource, Resource.dataset_id == Dataset.id)
            .join(Publisher, Dataset.publisher_id == Publisher.id, isouter=True)
            .where(func.lower(Resource.format) == dataset_format.lower())
            .order_by(Dataset.name)
        )
        return [dict(row._mapping) for row in session.execute(stmt).all()]


# View datasets associated with a given input tag.
"""
SELECT
  d.Id AS dataset_id,
  d.Name AS dataset_name,
  t.Tag AS tag,
  p.Name AS organization
FROM Dataset d
JOIN DatasetTag t
  ON t.DatasetId = d.Id
LEFT JOIN Publisher p
  ON p.Id = d.PublisherId
WHERE LOWER(t.Tag) = LOWER('water')
ORDER BY d.Name;
"""
def datasets_by_tag(tag: str) -> list[dict[str, str | None]]:
    with get_session() as session:
        stmt = (
            select(
                Dataset.id.label("dataset_id"),
                Dataset.name.label("dataset_name"),
                DatasetTag.tag.label("tag"),
                Publisher.name.label("organization"),
            )
            .join(DatasetTag, DatasetTag.dataset_id == Dataset.id)
            .join(Publisher, Dataset.publisher_id == Publisher.id, isouter=True)
            .where(func.lower(DatasetTag.tag) == tag.lower())
            .order_by(Dataset.name)
        )
        return [dict(row._mapping) for row in session.execute(stmt).all()]


# Show total datasets contributed by organization, topic, format, and organization type.
"""
SELECT p.Name AS group_value, COUNT(d.Id) AS total
FROM Publisher p
JOIN Dataset d ON d.PublisherId = p.Id
GROUP BY p.Id, p.Name
ORDER BY total DESC, p.Name;

SELECT dt.Topic AS group_value, COUNT(*) AS total
FROM DatasetTopic dt
GROUP BY dt.Topic
ORDER BY total DESC, dt.Topic;

SELECT r.Format AS group_value, COUNT(DISTINCT r.DatasetId) AS total
FROM Resource r
WHERE r.Format IS NOT NULL
GROUP BY r.Format
ORDER BY total DESC, r.Format;

SELECT p.Type AS group_value, COUNT(d.Id) AS total
FROM Publisher p
JOIN Dataset d ON d.PublisherId = p.Id
WHERE p.Type IS NOT NULL
GROUP BY p.Type
ORDER BY total DESC, p.Type;
"""
def totals_grouped() -> dict[str, list[dict[str, str | int | None]]]:
    with get_session() as session:
        by_org_stmt = (
            select(Publisher.name.label("group_value"), func.count(Dataset.id).label("total"))
            .join(Dataset, Dataset.publisher_id == Publisher.id)
            .group_by(Publisher.id, Publisher.name)
            .order_by(desc("total"), Publisher.name)
        )

        by_topic_stmt = (
            select(DatasetTopic.topic.label("group_value"), func.count().label("total"))
            .group_by(DatasetTopic.topic)
            .order_by(desc("total"), DatasetTopic.topic)
        )

        by_format_stmt = (
            select(
                Resource.format.label("group_value"),
                func.count(func.distinct(Resource.dataset_id)).label("total"),
            )
            .where(Resource.format.is_not(None))
            .group_by(Resource.format)
            .order_by(desc("total"), Resource.format)
        )

        by_org_type_stmt = (
            select(Publisher.type.label("group_value"), func.count(Dataset.id).label("total"))
            .join(Dataset, Dataset.publisher_id == Publisher.id)
            .where(Publisher.type.is_not(None))
            .group_by(Publisher.type)
            .order_by(desc("total"), Publisher.type)
        )

        return {
            "organization": [
                dict(row._mapping) for row in session.execute(by_org_stmt).all()
            ],
            "topic": [dict(row._mapping) for row in session.execute(by_topic_stmt).all()],
            "format": [dict(row._mapping) for row in session.execute(by_format_stmt).all()],
            "organization_type": [
                dict(row._mapping) for row in session.execute(by_org_type_stmt).all()
            ],
        }


# Show top 5 datasets by number of users who are using them.
"""
SELECT
  d.Id AS dataset_id,
  d.Name AS dataset_name,
  COUNT(DISTINCT pd.AppUserEmail) AS user_count
FROM Dataset d
JOIN ProjectDatasets pd
  ON pd.DatasetId = d.Id
GROUP BY d.Id, d.Name
ORDER BY user_count DESC, d.Name
LIMIT 5;
"""
def top_5_datasets_by_users() -> list[dict[str, str | int | None]]:
    with get_session() as session:
        stmt = (
            select(
                Dataset.id.label("dataset_id"),
                Dataset.name.label("dataset_name"),
                func.count(func.distinct(ProjectDatasets.app_user_email)).label("user_count"),
            )
            .join(ProjectDatasets, ProjectDatasets.dataset_id == Dataset.id)
            .group_by(Dataset.id, Dataset.name)
            .order_by(desc("user_count"), Dataset.name)
            .limit(5)
        )
        return [dict(row._mapping) for row in session.execute(stmt).all()]


# Show distribution of dataset usage by project type.
"""
SELECT
  p.ProjectCategory AS project_type,
  COUNT(pd.DatasetId) AS usage_count
FROM Project p
JOIN ProjectDatasets pd
  ON pd.AppUserEmail = p.AppUserEmail
 AND pd.ProjectName = p.Name
GROUP BY p.ProjectCategory
ORDER BY usage_count DESC;
"""
def usage_distribution_by_project_type() -> list[dict[str, str | int | None]]:
    with get_session() as session:
        stmt = (
            select(
                Project.project_category.label("project_type"),
                func.count(ProjectDatasets.dataset_id).label("usage_count"),
            )
            .join(
                ProjectDatasets,
                and_(
                    ProjectDatasets.app_user_email == Project.app_user_email,
                    ProjectDatasets.project_name == Project.name,
                ),
            )
            .group_by(Project.project_category)
            .order_by(desc("usage_count"))
        )

        rows = session.execute(stmt).all()
        return [
            {
                "project_type": row.project_type.value if row.project_type else "unassigned",
                "usage_count": row.usage_count,
            }
            for row in rows
        ]


# Display top 10 tags associated with every project type.
"""
WITH ranked AS (
  SELECT
    p.ProjectCategory AS project_type,
    dt.Tag AS tag,
    COUNT(dt.Tag) AS usage_count,
    ROW_NUMBER() OVER (
      PARTITION BY p.ProjectCategory
      ORDER BY COUNT(dt.Tag) DESC
    ) AS rank_num
  FROM Project p
  JOIN ProjectDatasets pd
    ON pd.AppUserEmail = p.AppUserEmail
   AND pd.ProjectName = p.Name
  JOIN DatasetTag dt
    ON dt.DatasetId = pd.DatasetId
  GROUP BY p.ProjectCategory, dt.Tag
)
SELECT project_type, tag, usage_count, rank_num
FROM ranked
WHERE rank_num <= 10
ORDER BY project_type, rank_num;
"""
def top_10_tags_by_project_type() -> list[dict[str, str | int]]:
    with get_session() as session:
        usage_count = func.count(DatasetTag.tag)

        ranked_stmt = (
            select(
                Project.project_category.label("project_type"),
                DatasetTag.tag.label("tag"),
                usage_count.label("usage_count"),
                func.row_number()
                .over(
                    partition_by=Project.project_category,
                    order_by=usage_count.desc(),
                )
                .label("rank"),
            )
            .join(
                ProjectDatasets,
                and_(
                    ProjectDatasets.app_user_email == Project.app_user_email,
                    ProjectDatasets.project_name == Project.name,
                ),
            )
            .join(DatasetTag, DatasetTag.dataset_id == ProjectDatasets.dataset_id)
            .group_by(Project.project_category, DatasetTag.tag)
            .subquery()
        )

        stmt = (
            select(
                ranked_stmt.c.project_type,
                ranked_stmt.c.tag,
                ranked_stmt.c.usage_count,
                ranked_stmt.c.rank,
            )
            .where(ranked_stmt.c.rank <= 10)
            .order_by(ranked_stmt.c.project_type, ranked_stmt.c.rank)
        )

        rows = session.execute(stmt).all()
        results: list[dict[str, str | int]] = []
        for row in rows:
            project_type = row.project_type.value if row.project_type else "unassigned"
            results.append(
                {
                    "project_type": project_type,
                    "tag": row.tag,
                    "usage_count": row.usage_count,
                    "rank": row.rank,
                }
            )
        return results


def dataset_reference(limit: int = 200) -> list[dict[str, str]]:
    with get_session() as session:
        stmt = select(Dataset.id, Dataset.name).order_by(Dataset.name).limit(limit)
        rows = session.execute(stmt).all()
        return [{"id": row.id, "name": row.name} for row in rows]


def dataset_count() -> int:
    with get_session() as session:
        stmt = select(func.count(Dataset.id))
        return int(session.execute(stmt).scalar_one() or 0)


def search_datasets(query: str, limit: int = 30) -> list[dict[str, str]]:
    normalized = query.strip().lower()

    with get_session() as session:
        stmt = select(Dataset.id, Dataset.name)
        if normalized:
            like_term = f"%{normalized}%"
            stmt = stmt.where(
                func.lower(Dataset.name).like(like_term)
                | func.lower(Dataset.id).like(like_term)
            )

        stmt = stmt.order_by(Dataset.name).limit(limit)
        rows = session.execute(stmt).all()
        return [{"id": row.id, "name": row.name} for row in rows]
