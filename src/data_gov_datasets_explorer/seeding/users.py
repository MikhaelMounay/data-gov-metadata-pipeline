from __future__ import annotations

import csv
from datetime import date
from pathlib import Path

from data_gov_datasets_explorer.db import SessionLocal, init_db
from data_gov_datasets_explorer.logger import AppLogger
from data_gov_datasets_explorer.models import AppUser


log = AppLogger("seeding-users")


def _clean_text(value: str | None) -> str | None:
	if value is None:
		return None

	cleaned = str(value).strip()
	return cleaned or None


def _parse_birthdate(value: str | None) -> date | None:
	cleaned = _clean_text(value)
	if not cleaned:
		return None

	try:
		return date.fromisoformat(cleaned)
	except ValueError:
		return None


def _upsert_user(row: dict[str, str]) -> str:
	email = _clean_text(row.get("email"))
	if not email:
		raise ValueError("Missing required column: email")

	username = _clean_text(row.get("username"))
	gender = _clean_text(row.get("gender"))
	country = _clean_text(row.get("country"))

	raw_birthdate = row.get("birthdate")
	birthdate = _parse_birthdate(raw_birthdate)
	if _clean_text(raw_birthdate) and birthdate is None:
		raise ValueError(f"Invalid birthdate format for email={email}: {raw_birthdate}")

	with SessionLocal.begin() as session:
		existing_user = session.get(AppUser, email)
		if existing_user is None:
			session.add(
				AppUser(
					email=email,
					username=username,
					gender=gender,
					birthdate=birthdate,
					country=country,
				)
			)
			return "inserted"

		existing_user.username = username
		existing_user.gender = gender
		existing_user.birthdate = birthdate
		existing_user.country = country
		return "updated"


def seed_users(csv_path: Path) -> tuple[int, int, int]:
	inserted = 0
	updated = 0
	skipped = 0

	with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
		reader = csv.DictReader(csv_file)

		required_columns = {"email", "username", "gender", "birthdate", "country"}
		missing_columns = required_columns.difference(reader.fieldnames or [])
		if missing_columns:
			missing = ", ".join(sorted(missing_columns))
			raise ValueError(f"CSV is missing required columns: {missing}")

		for row_index, row in enumerate(reader, start=2):
			try:
				status = _upsert_user(row)
				if status == "inserted":
					inserted += 1
				else:
					updated += 1
			except Exception as exc:
				skipped += 1
				log.warning(f"Skipping row {row_index}: {type(exc).__name__}: {exc}")

	return inserted, updated, skipped


def main() -> None:
	csv_path = Path(__file__).with_name("users.csv")

	log.info("Step 1/3: Initializing database metadata")
	init_db()

	log.info(f"Step 2/3: Seeding users from {csv_path}")
	inserted, updated, skipped = seed_users(csv_path)

	log.info("Step 3/3: User seeding completed")
	print("User seeding summary")
	print(f"CSV file: {csv_path}")
	print(f"Inserted: {inserted}")
	print(f"Updated: {updated}")
	print(f"Skipped: {skipped}")


if __name__ == "__main__":
	main()
