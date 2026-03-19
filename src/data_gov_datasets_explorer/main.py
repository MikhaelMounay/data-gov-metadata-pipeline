from pathlib import Path

from data_gov_datasets_explorer.export_db.export_db_csv import export_all_tables


if __name__ == "__main__":
    export_all_tables(Path("exports"))
