## Base Python runtime image (slim for smaller size).
FROM python:3.11-slim

# Set working directory for app code and runtime.
WORKDIR /app

# Install basic build tools required for native deps.
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency list separately to leverage Docker layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source.
COPY . .

# Standard environment for predictable Python behavior.
ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

# Expose service HTTP port (health/metrics endpoints included).
EXPOSE 8084

# Health check: verifies service is responding on the health endpoint.
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8084/health || exit 1

# Default command: start the projection service.
CMD ["python", "-m", "app.main"]

