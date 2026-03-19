from __future__ import annotations

import os
from contextlib import contextmanager
from collections.abc import Iterator

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session, sessionmaker

from data_gov_datasets_explorer.models import Base


def build_database_url() -> str:
	configured_url = os.getenv("DATABASE_URL")
	if configured_url:
		return configured_url

	host = os.getenv("DB_HOST", "127.0.0.1")
	port = os.getenv("DB_PORT", "3306")
	user = os.getenv("DB_USER", "root")
	password = os.getenv("DB_PASSWORD", "root")
	database = os.getenv("DB_NAME", "datagov_datasets")

	return f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}?charset=utf8mb4"


DATABASE_URL = build_database_url()

engine: Engine = create_engine(
	DATABASE_URL,
	pool_pre_ping=True,
)

SessionLocal = sessionmaker(
	bind=engine,
	autoflush=False,
	autocommit=False,
	expire_on_commit=False,
)


def init_db() -> None:
	Base.metadata.create_all(bind=engine)


@contextmanager
def get_session() -> Iterator[Session]:
	session = SessionLocal()
	try:
		yield session
		session.commit()
	except Exception:
		session.rollback()
		raise
	finally:
		session.close()
