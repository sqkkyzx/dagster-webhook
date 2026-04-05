FROM python:3.14-slim

ENV DAGSTER_HOME=/opt/dagster/dagster_home/ \
    PYTHONUNBUFFERED=1

RUN pip install --no-cache-dir --upgrade pip uv

WORKDIR $DAGSTER_HOME

COPY pyproject.toml uv.lock* ./

RUN uv pip install --system --no-cache-dir --no-verify-hashes .

EXPOSE 4000

ENTRYPOINT ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000"]

CMD ["-f", "definitions.py"]
