from __future__ import annotations

from datetime import datetime
import hashlib
import unicodedata

from src.models import AccessLevelEnum, Dataset, DatasetTag, DatasetTopic, Publisher, Resource


_ALLOWED_CONTROL_CHARS = {"\t", "\n", "\r"}


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None

    normalized_value = unicodedata.normalize("NFKC", str(value))
    cleaned_chars: list[str] = []
    for char in normalized_value:
        if char in _ALLOWED_CONTROL_CHARS:
            cleaned_chars.append(char)
            continue

        # Drop invisible/control-ish Unicode classes (Cc, Cf, Cs, Co, Cn).
        if unicodedata.category(char).startswith("C"):
            continue
        cleaned_chars.append(char)

    cleaned_value = "".join(cleaned_chars).strip()
    if not cleaned_value:
        return None

    # Keep persistence strict to valid UTF-8 bytes.
    cleaned_value = cleaned_value.encode("utf-8", "ignore").decode("utf-8", "ignore")
    return cleaned_value or None

def parse_date(value: str | None):
    cleaned_value = clean_text(value)
    if not cleaned_value:
        return None
    try:
        return datetime.fromisoformat(cleaned_value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def get_extra(dataset: dict, key: str):
    for item in dataset.get("extras", []):
        if clean_text(item.get("key")) == key:
            return clean_text(item.get("value"))
    return None


def normalize_access_level(value: str | None):
    cleaned_value = clean_text(value)
    if not cleaned_value:
        return None

    normalized = cleaned_value.strip().lower()
    for enum_value in AccessLevelEnum:
        if enum_value.value == normalized:
            return enum_value
    return None


def get_extra_value(extras: list[dict] | None, key: str) -> str | None:
    if not extras:
        return None

    for item in extras:
        if clean_text(item.get("key")) == key:
            return clean_text(item.get("value"))
    return None


def build_publisher(dataset_payload: dict, organization_payload: dict | None = None) -> Publisher | None:
    dataset_organization = dataset_payload.get("organization") or {}
    full_organization = organization_payload or {}

    publisher_id = clean_text(full_organization.get("id") or dataset_organization.get("id"))
    if not publisher_id:
        return None

    organization_extras = full_organization.get("extras") if isinstance(full_organization, dict) else None

    email = (
        full_organization.get("email")
        or get_extra_value(organization_extras, "email")
        or get_extra_value(organization_extras, "email_list")
    )
    phone = full_organization.get("phone") or get_extra_value(organization_extras, "phone")
    website_url = (
        full_organization.get("website")
        or get_extra_value(organization_extras, "website")
        or get_extra_value(organization_extras, "website_url")
    )

    return Publisher(
        id=publisher_id,
        name=(
            clean_text(full_organization.get("name"))
            or clean_text(full_organization.get("title"))
            or clean_text(dataset_organization.get("name"))
            or clean_text(dataset_organization.get("title"))
            or "Unknown"
        ),
        title=(
            clean_text(full_organization.get("title"))
            or clean_text(full_organization.get("display_name"))
            or clean_text(dataset_organization.get("title"))
        ),
        description=clean_text(full_organization.get("description")) or clean_text(dataset_organization.get("description")),
        type=(
            get_extra_value(organization_extras, "organization_type")
            or clean_text(full_organization.get("type"))
            or clean_text(dataset_organization.get("type"))
        ),
        email=clean_text(email),
        phone=clean_text(phone),
        website_url=clean_text(website_url),
    )


def build_dataset(dataset_payload: dict) -> Dataset:
    organization = dataset_payload.get("organization") or {}
    dataset_id = clean_text(dataset_payload.get("id"))
    if not dataset_id:
        raise ValueError("Dataset id is missing or invalid after cleaning")

    return Dataset(
        id=dataset_id,
        name=clean_text(dataset_payload.get("name")) or clean_text(dataset_payload.get("title")) or "Untitled",
        description=clean_text(dataset_payload.get("notes")),
        license=clean_text(dataset_payload.get("license_title")) or clean_text(dataset_payload.get("license_id")),
        creation_date=parse_date(dataset_payload.get("metadata_created")),
        update_date=parse_date(dataset_payload.get("metadata_modified")),
        maintainer=clean_text(dataset_payload.get("maintainer")),
        access_level=normalize_access_level(get_extra(dataset_payload, "accessLevel")),
        publisher_id=clean_text(organization.get("id")),
    )


def build_topics(dataset_payload: dict, dataset_id: str) -> list[DatasetTopic]:
    topics: list[DatasetTopic] = []
    for group in dataset_payload.get("groups", []):
        topic_name = clean_text(group.get("title")) or clean_text(group.get("name"))
        if topic_name:
            topics.append(DatasetTopic(dataset_id=dataset_id, topic=topic_name))
    return topics


def build_tags(dataset_payload: dict, dataset_id: str) -> list[DatasetTag]:
    tags: list[DatasetTag] = []
    for tag in dataset_payload.get("tags", []):
        tag_name = clean_text(tag.get("name"))
        if tag_name:
            tags.append(DatasetTag(dataset_id=dataset_id, tag=tag_name))
    return tags


def build_resources(dataset_payload: dict, dataset_id: str) -> list[Resource]:
    resources: list[Resource] = []
    for item in dataset_payload.get("resources", []):
        resource_url = clean_text(item.get("url"))
        if not resource_url:
            continue
        url_hash = hashlib.sha256(resource_url.encode("utf-8")).digest()
        resources.append(
            Resource(
                dataset_id=dataset_id,
                url_hash=url_hash,
                url=resource_url,
                name=clean_text(item.get("name")),
                format=clean_text(item.get("format")),
                description=clean_text(item.get("description")),
            )
        )
    return resources
