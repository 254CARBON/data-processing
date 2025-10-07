## Base Python image (slim variant for smaller image size).
FROM python:3.11-slim

# Set working directory for application code and runtime.
WORKDIR /app

# Install build toolchain dependencies required by some Python wheels.
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency manifest to leverage Docker layer caching of installs.
COPY requirements.txt .

# Install pinned Python dependencies; avoid caching to keep layers slim.
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source code into the container image.
COPY . .

# Drop privileges by running as an unprivileged user.
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expose service, health, and metrics ports (for documentation/ops tools).
EXPOSE 8201 8080 9090

# Health check: probe the HTTP health endpoint to signal readiness.
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1

# Default command to run the enrichment service.
CMD ["python", "-m", "app.main"]

