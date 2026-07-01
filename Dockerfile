FROM python:3.12-slim

WORKDIR /app

# Running scripts as `python consumer/consumer.py` (see docker-compose.yml
# `command:` overrides) only puts consumer/'s own directory on sys.path,
# NOT /app itself - so top-level packages like `database`, `core`, and
# `telemetry` would fail to import. Setting PYTHONPATH fixes that without
# needing to switch every service to `python -m package.module`.
ENV PYTHONPATH=/app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Actual entrypoint (consumer/consumer.py vs main.py) is chosen per
# service via the `command:` override in docker-compose.yml.
CMD ["python", "main.py"]
