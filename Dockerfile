FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    ENVIRONMENT=production \
    WEBAPP_PORT=8000 \
    PYTHONPATH=/app/src

WORKDIR /app

# Copy sources needed to install project and its dependencies.
COPY pyproject.toml README.md ./
COPY poetry.lock ./
COPY src ./src

RUN python -m pip install --upgrade pip \
    && pip install .

EXPOSE 8000

CMD ["python", "-m", "data_gov_datasets_explorer.main"]
