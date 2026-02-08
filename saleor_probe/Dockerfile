FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install deps
COPY pyproject.toml /app/pyproject.toml
RUN pip install --no-cache-dir -U pip && \
    pip install --no-cache-dir .

# Copy source
COPY probe /app/probe

# Re-install local package to include source (simple approach)
RUN pip install --no-cache-dir .

ENTRYPOINT ["saleor-synth-probe"]