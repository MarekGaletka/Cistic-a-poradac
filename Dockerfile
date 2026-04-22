FROM python:3.12-slim

WORKDIR /app

# Install system dependencies (tzdata for timezone support, curl for healthcheck)
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata curl && \
    rm -rf /var/lib/apt/lists/*

# Copy dependency definition first (layer cache: deps change less than src)
COPY pyproject.toml README.md /app/

# Pre-install build tools (cached until pyproject.toml changes)
RUN pip install --no-cache-dir "pip>=24.0,<25.0" "setuptools>=68" "wheel"

# Copy source and install package
COPY src /app/src
RUN pip install --no-cache-dir .

# Create non-root user and switch to it
RUN groupadd -r gml && useradd -r -g gml -m gml && \
    chown -R gml:gml /app
USER gml

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8777/api/stats || exit 1

ENTRYPOINT ["gml"]
