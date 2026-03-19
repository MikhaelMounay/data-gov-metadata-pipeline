import os
import dotenv
import waitress
import logging

from data_gov_datasets_explorer.webapp import __main__ as webapp


if __name__ == "__main__":
    dotenv.load_dotenv()  # Load environment variables from .env file

    if os.getenv("ENVIRONMENT", "development") == "production":
        logging.basicConfig(level=logging.INFO)
        waitress.serve(webapp.app, host="0.0.0.0", port=int(os.getenv("WEBAPP_PORT", "8000")))
    else:
        webapp.app.run(
            host="127.0.0.1",
            port=int(os.getenv("WEBAPP_PORT", "8000")),
            debug=True,
        )
