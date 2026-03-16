FROM python:3.12-slim

WORKDIR /app

# Install dependencies
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir flask gunicorn google-cloud-bigquery google-cloud-pubsub

# Copy source
COPY src/ src/

# Environment
ENV PYTHONUNBUFFERED=1 \
    PORT=8080

EXPOSE 8080

# Run with gunicorn for production
CMD exec gunicorn --bind :$PORT --workers 1 --threads 2 --timeout 60 src.subscriber:app
