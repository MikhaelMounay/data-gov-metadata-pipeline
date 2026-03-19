import os

from data_gov_datasets_explorer.webapp.app import app


if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=int(os.getenv("WEBAPP_PORT", "8000")),
        debug=True,
    )
