from __future__ import annotations
from datetime import date, datetime
from enum import Enum
from sqlalchemy import Date, DateTime, Enum as SAEnum, ForeignKey, ForeignKeyConstraint, Integer, LargeBinary, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

class Base(DeclarativeBase):
	pass


class AccessLevelEnum(str, Enum):
	PUBLIC = "public"
	RESTRICTED_PUBLIC = "restricted public"
	NON_PUBLIC = "non-public"


class ProjectCategoryEnum(str, Enum):
	ANALYTICS = "analytics"
	MACHINE_LEARNING = "machine learning"
	FIELD_RESEARCH = "field research"


class Publisher(Base):
	__tablename__ = "Publisher"

	id: Mapped[str] = mapped_column("Id", String(255), primary_key=True)
	name: Mapped[str] = mapped_column("Name", String(255), nullable=False)
	title: Mapped[str | None] = mapped_column("Title", String(255))
	description: Mapped[str | None] = mapped_column("Description", Text)
	type: Mapped[str | None] = mapped_column("Type", String(255))
	email: Mapped[str | None] = mapped_column("Email", String(1024))
	phone: Mapped[str | None] = mapped_column("Phone", String(1024))
	website_url: Mapped[str | None] = mapped_column("WebsiteURL", String(2048))

	datasets: Mapped[list[Dataset]] = relationship(back_populates="publisher")


class Dataset(Base):
	__tablename__ = "Dataset"

	id: Mapped[str] = mapped_column("Id", String(255), primary_key=True)
	name: Mapped[str] = mapped_column("Name", String(255), nullable=False)
	description: Mapped[str | None] = mapped_column("Description", Text)
	license: Mapped[str | None] = mapped_column("License", String(255))
	creation_date: Mapped[date | None] = mapped_column("CreationDate", Date)
	update_date: Mapped[date | None] = mapped_column("UpdateDate", Date)
	maintainer: Mapped[str | None] = mapped_column("Maintainer", String(255))
	access_level: Mapped[AccessLevelEnum | None] = mapped_column(
		"AccessLevel",
		SAEnum(
			AccessLevelEnum,
			values_callable=lambda x: [e.value for e in x],
			name="access_level_enum",
		),
	)
	publisher_id: Mapped[str | None] = mapped_column(
		"PublisherId",
		String(255),
		ForeignKey("Publisher.Id", ondelete="SET NULL", onupdate="CASCADE"),
	)

	publisher: Mapped[Publisher | None] = relationship(back_populates="datasets")
	topics: Mapped[list[DatasetTopic]] = relationship(back_populates="dataset")
	tags: Mapped[list[DatasetTag]] = relationship(back_populates="dataset")
	resources: Mapped[list[Resource]] = relationship(back_populates="dataset")
	project_links: Mapped[list[ProjectDatasets]] = relationship(back_populates="dataset")


class DatasetTopic(Base):
	__tablename__ = "DatasetTopic"

	dataset_id: Mapped[str] = mapped_column(
		"DatasetId",
		String(255),
		ForeignKey("Dataset.Id"),
		primary_key=True,
	)
	topic: Mapped[str] = mapped_column("Topic", String(255), primary_key=True)

	dataset: Mapped[Dataset] = relationship(back_populates="topics")


class DatasetTag(Base):
	__tablename__ = "DatasetTag"

	dataset_id: Mapped[str] = mapped_column(
		"DatasetId",
		String(255),
		ForeignKey("Dataset.Id"),
		primary_key=True,
	)
	tag: Mapped[str] = mapped_column("Tag", String(255), primary_key=True)

	dataset: Mapped[Dataset] = relationship(back_populates="tags")


class Resource(Base):
	__tablename__ = "Resource"

	dataset_id: Mapped[str] = mapped_column(
		"DatasetId",
		String(255),
		ForeignKey("Dataset.Id"),
		primary_key=True,
	)
	url_hash: Mapped[bytes] = mapped_column("URLHash", LargeBinary(32), primary_key=True)
	url: Mapped[str] = mapped_column("URL", String(2048), nullable=False)
	name: Mapped[str | None] = mapped_column("Name", String(1024))
	format: Mapped[str | None] = mapped_column("Format", String(100))
	description: Mapped[str | None] = mapped_column("Description", Text)

	dataset: Mapped[Dataset] = relationship(back_populates="resources")


class AppUser(Base):
	__tablename__ = "AppUser"

	email: Mapped[str] = mapped_column("Email", String(255), primary_key=True)
	username: Mapped[str | None] = mapped_column("Username", String(255))
	gender: Mapped[str | None] = mapped_column("Gender", String(100))
	birthdate: Mapped[date | None] = mapped_column("Birthdate", Date)
	country: Mapped[str | None] = mapped_column("Country", String(100))

	projects: Mapped[list[Project]] = relationship(back_populates="app_user")


class Project(Base):
	__tablename__ = "Project"

	app_user_email: Mapped[str] = mapped_column(
		"AppUserEmail",
		String(255),
		ForeignKey("AppUser.Email"),
		primary_key=True,
	)
	name: Mapped[str] = mapped_column("Name", String(255), primary_key=True)
	project_category: Mapped[ProjectCategoryEnum | None] = mapped_column(
		"ProjectCategory",
		SAEnum(
			ProjectCategoryEnum,
			values_callable=lambda x: [e.value for e in x],
			name="project_category_enum",
		),
	)

	app_user: Mapped[AppUser] = relationship(back_populates="projects")
	datasets_links: Mapped[list[ProjectDatasets]] = relationship(back_populates="project")


class ProjectDatasets(Base):
	__tablename__ = "ProjectDatasets"

	app_user_email: Mapped[str] = mapped_column("AppUserEmail", String(255), primary_key=True)
	project_name: Mapped[str] = mapped_column("ProjectName", String(255), primary_key=True)
	dataset_id: Mapped[str] = mapped_column(
		"DatasetId",
		String(255),
		ForeignKey("Dataset.Id"),
		primary_key=True,
	)

	__table_args__ = (
		ForeignKeyConstraint(
			["AppUserEmail", "ProjectName"],
			["Project.AppUserEmail", "Project.Name"],
			name="fk_pd_project",
		),
	)

	project: Mapped[Project] = relationship(back_populates="datasets_links")
	dataset: Mapped[Dataset] = relationship(back_populates="project_links")


class _IngestionRun(Base):
	__tablename__ = "_IngestionRun"

	id: Mapped[str] = mapped_column("Id", String(36), primary_key=True)
	status: Mapped[str] = mapped_column("Status", String(32), nullable=False)
	started_at: Mapped[datetime] = mapped_column("StartedAt", DateTime, nullable=False)
	finished_at: Mapped[datetime | None] = mapped_column("FinishedAt", DateTime)
	total_target: Mapped[int] = mapped_column("TotalTarget", Integer, nullable=False, default=0)
	total_success: Mapped[int] = mapped_column("TotalSuccess", Integer, nullable=False, default=0)
	total_failed: Mapped[int] = mapped_column("TotalFailed", Integer, nullable=False, default=0)
	error_message: Mapped[str | None] = mapped_column("ErrorMessage", Text)

	items: Mapped[list[_IngestionItem]] = relationship(back_populates="run")


class _IngestionItem(Base):
	__tablename__ = "_IngestionItem"

	run_id: Mapped[str] = mapped_column(
		"RunId",
		String(36),
		ForeignKey("_IngestionRun.Id", ondelete="CASCADE"),
		primary_key=True,
	)
	dataset_id: Mapped[str] = mapped_column("DatasetId", String(255), primary_key=True)
	status: Mapped[str] = mapped_column("Status", String(32), nullable=False)
	attempt_count: Mapped[int] = mapped_column("AttemptCount", Integer, nullable=False, default=1)
	processed_at: Mapped[datetime] = mapped_column("ProcessedAt", DateTime, nullable=False)
	error_message: Mapped[str | None] = mapped_column("ErrorMessage", Text)

	run: Mapped[_IngestionRun] = relationship(back_populates="items")
