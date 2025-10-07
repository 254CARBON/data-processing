## Base Python runtime (slim image for smaller footprint).
FROM python:3.11-slim

# Set working directory for sources and runtime.
WORKDIR /app

# Install toolchain packages needed by native dependencies.
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage layer caching between builds.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy all application source code into the image.
COPY . .

# Minimal env for predictable Python behavior inside containers.
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expose service HTTP port (health/metrics served by the app).
EXPOSE 8083

# Container health check hitting the service's health endpoint.
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8083/health || exit 1

# Default command: start the aggregation service.
CMD ["python", "-m", "app.main"]

