FROM python:3.12-slim

WORKDIR /app

ENV POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_CACHE_DIR='/var/cache/poetry'

RUN pip install poetry

COPY pyproject.toml poetry.lock* ./

RUN poetry install --no-root

COPY ./app ./app

CMD ["poetry", "run", "uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]