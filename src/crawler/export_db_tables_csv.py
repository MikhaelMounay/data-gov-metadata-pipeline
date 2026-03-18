from __future__ import annotations

import csv
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path

from sqlalchemy import MetaData, Table, select

from src.db import engine


def serialize_value(value) -> str:
    if value is None:
        return ""
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value).hex()
    return str(value)


def export_all_tables(output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    metadata = MetaData()
    metadata.reflect(bind=engine)

    with engine.connect() as connection:
        for table_name in sorted(metadata.tables.keys()):
            table = Table(table_name, metadata, autoload_with=engine)
            rows = connection.execute(select(table)).mappings().all()

            output_path = output_dir / f"{table_name}.csv"
            column_names = [column.name for column in table.columns]

            with output_path.open("w", newline="", encoding="utf-8-sig") as csv_file:
                writer = csv.writer(csv_file)
                writer.writerow(column_names)
                for row in rows:
                    writer.writerow([serialize_value(row.get(column)) for column in column_names])

            print(f"Exported {table_name}: {len(rows)} rows -> {output_path}")


if __name__ == "__main__":
    export_all_tables(Path("exports"))
