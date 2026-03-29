FROM python:3.12-slim

WORKDIR /app

# Install system dependencies (tzdata for timezone support, curl for healthcheck)
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata curl && \
    rm -rf /var/lib/apt/lists/*

# Copy dependency definition and source for install
COPY pyproject.toml README.md /app/
COPY src /app/src

# Install Python dependencies (pinned pip version for reproducibility)
RUN pip install --no-cache-dir "pip>=24.0,<25.0" && \
    pip install --no-cache-dir .

# Create non-root user and switch to it
RUN groupadd -r gml && useradd -r -g gml -m gml && \
    chown -R gml:gml /app
USER gml

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8777/api/stats || exit 1

ENTRYPOINT ["gml"]
