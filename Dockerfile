FROM python:3.12-slim

WORKDIR /app

# Install system dependencies (tzdata for timezone support, curl for healthcheck)
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata curl && \
    rm -rf /var/lib/apt/lists/*

# Copy dependency definition first for better layer caching
COPY pyproject.toml README.md /app/

# Install Python dependencies (cached unless pyproject.toml changes)
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir .

# Copy source code (changes more frequently)
COPY src /app/src

# Re-install to register the package with source code
RUN pip install --no-cache-dir --no-deps .

# Create non-root user and switch to it
RUN groupadd -r gml && useradd -r -g gml -m gml && \
    chown -R gml:gml /app
USER gml

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8777/api/stats || exit 1

ENTRYPOINT ["gml"]
