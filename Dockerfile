FROM python:3.14-slim

ENV PYTHONUNBUFFERED=1 \
    APP_HOST=0.0.0.0 \
    APP_PORT=8080 \
    IDEMPOTENCY_DB_PATH=/data/idempotency.sqlite3

RUN pip install --no-cache-dir --upgrade pip uv

WORKDIR /app

COPY pyproject.toml uv.lock* ./

RUN uv pip install --system --no-cache-dir --no-verify-hashes .

COPY main.py project_metadata.py ./
COPY gateway/ ./gateway/

VOLUME ["/data"]

EXPOSE 8080

CMD ["python", "main.py"]
