# API Reference

This document provides comprehensive API documentation for all services in the data processing pipeline.

## Overview

All services expose HTTP APIs for health checks, metrics, and service-specific endpoints. The APIs follow RESTful conventions and return JSON responses.

## Common Endpoints

All services implement these common endpoints:

### Health Check

```http
GET /health
```

**Response**:
```json
{
  "healthy": true,
  "timestamp": "2024-01-01T00:00:00Z",
  "service": "normalization-service",
  "version": "1.0.0",
  "dependencies": {
    "kafka": "healthy",
    "clickhouse": "healthy"
  }
}
```

### Readiness Check

```http
GET /health/ready
```

**Response**:
```json
{
  "ready": true,
  "timestamp": "2024-01-01T00:00:00Z",
  "checks": {
    "kafka_connection": "ok",
    "database_connection": "ok"
  }
}
```

### Liveness Check

```http
GET /health/live
```

**Response**:
```json
{
  "alive": true
}
```

### Metrics

```http
GET /metrics
```

**Response**: Prometheus-formatted metrics
```
# HELP normalization_service_requests_total Total number of requests
# TYPE normalization_service_requests_total counter
normalization_service_requests_total{method="GET",endpoint="/health",status="200"} 42

# HELP normalization_service_messages_processed_total Total messages processed
# TYPE normalization_service_messages_processed_total counter
normalization_service_messages_processed_total{topic="ingestion.market.raw.v1",status="success"} 1000
```

## Normalization Service

**Base URL**: `http://localhost:8080`

### Service Information

```http
GET /info
```

**Response**:
```json
{
  "service": "normalization-service",
  "version": "1.0.0",
  "environment": "local",
  "started_at": "2024-01-01T00:00:00Z",
  "uptime_seconds": 3600
}
```

### Processing Statistics

```http
GET /stats
```

**Response**:
```json
{
  "messages_processed": 10000,
  "messages_failed": 50,
  "processing_rate_per_second": 100.5,
  "average_processing_time_ms": 5.2,
  "error_rate_percent": 0.5,
  "last_processing_time": "2024-01-01T00:00:00Z"
}
```

### Supported Markets

```http
GET /markets
```

**Response**:
```json
{
  "supported_markets": [
    {
      "name": "MISO",
      "description": "Midcontinent Independent System Operator",
      "supported_formats": ["raw", "normalized"],
      "sample_instruments": ["NG_HH_BALMO", "NG_HH_PROMPT"]
    },
    {
      "name": "CAISO",
      "description": "California Independent System Operator",
      "supported_formats": ["raw", "normalized"],
      "sample_instruments": ["POWER_PEAK_DAY_AHEAD", "POWER_OFF_PEAK_DAY_AHEAD"]
    }
  ]
}
```

### Data Quality Rules

```http
GET /quality-rules
```

**Response**:
```json
{
  "quality_rules": [
    {
      "name": "price_positive",
      "description": "Price must be positive",
      "enabled": true,
      "threshold": 0.0
    },
    {
      "name": "volume_non_negative",
      "description": "Volume must be non-negative",
      "enabled": true,
      "threshold": 0.0
    },
    {
      "name": "late_arrival_threshold",
      "description": "Maximum allowed late arrival time",
      "enabled": true,
      "threshold": 300
    }
  ]
}
```

## Enrichment Service

**Base URL**: `http://localhost:8081`

### Service Information

```http
GET /info
```

**Response**:
```json
{
  "service": "enrichment-service",
  "version": "1.0.0",
  "environment": "local",
  "started_at": "2024-01-01T00:00:00Z",
  "uptime_seconds": 3600
}
```

### Enrichment Statistics

```http
GET /stats
```

**Response**:
```json
{
  "enrichment_count": 5000,
  "enrichment_failures": 25,
  "cache_hits": 4500,
  "cache_misses": 500,
  "cache_hit_rate_percent": 90.0,
  "average_enrichment_time_ms": 2.1,
  "last_enrichment_time": "2024-01-01T00:00:00Z"
}
```

### Taxonomy Classification

```http
GET /taxonomy/{instrument_id}
```

**Parameters**:
- `instrument_id` (string): Instrument identifier

**Response**:
```json
{
  "instrument_id": "NG_HH_BALMO",
  "taxonomy": {
    "commodity": "natural_gas",
    "region": "midwest",
    "product_tier": "balmo",
    "confidence": 0.95,
    "method": "rule_based"
  },
  "metadata": {
    "unit": "MMBtu",
    "contract_size": 1000,
    "tick_size": 0.001
  }
}
```

### Cache Status

```http
GET /cache/status
```

**Response**:
```json
{
  "cache_enabled": true,
  "cache_size": 1000,
  "cache_hit_rate": 0.9,
  "cache_ttl_seconds": 3600,
  "last_refresh": "2024-01-01T00:00:00Z"
}
```

### Refresh Cache

```http
POST /cache/refresh
```

**Response**:
```json
{
  "success": true,
  "refreshed_count": 1000,
  "refresh_time_ms": 150.5
}
```

## Aggregation Service

**Base URL**: `http://localhost:8082`

### Service Information

```http
GET /info
```

**Response**:
```json
{
  "service": "aggregation-service",
  "version": "1.0.0",
  "environment": "local",
  "started_at": "2024-01-01T00:00:00Z",
  "uptime_seconds": 3600
}
```

### Aggregation Statistics

```http
GET /stats
```

**Response**:
```json
{
  "bars_generated": 50000,
  "active_windows": 1000,
  "processing_rate_per_second": 50.0,
  "average_processing_time_ms": 10.5,
  "last_processing_time": "2024-01-01T00:00:00Z",
  "intervals": {
    "1m": {"count": 30000, "rate": 30.0},
    "5m": {"count": 15000, "rate": 15.0},
    "1h": {"count": 5000, "rate": 5.0}
  }
}
```

### Time Windows

```http
GET /windows
```

**Response**:
```json
{
  "active_windows": [
    {
      "instrument_id": "NG_HH_BALMO",
      "tenant_id": "tenant_1",
      "interval": "1m",
      "start": "2024-01-01T00:00:00Z",
      "end": "2024-01-01T00:01:00Z",
      "tick_count": 100,
      "is_complete": false
    }
  ],
  "total_windows": 1000
}
```

### Bar Intervals

```http
GET /intervals
```

**Response**:
```json
{
  "supported_intervals": [
    {
      "interval": "1m",
      "description": "1-minute bars",
      "enabled": true
    },
    {
      "interval": "5m",
      "description": "5-minute bars",
      "enabled": true
    },
    {
      "interval": "1h",
      "description": "1-hour bars",
      "enabled": true
    }
  ]
}
```

### Curve Calculations

```http
GET /curves/{instrument_id}
```

**Parameters**:
- `instrument_id` (string): Instrument identifier

**Response**:
```json
{
  "instrument_id": "NG_HH_BALMO",
  "curve_points": [
    {
      "tenor": "M1",
      "price": 120.50,
      "timestamp": "2024-01-01T00:00:00Z"
    },
    {
      "tenor": "M2",
      "price": 121.00,
      "timestamp": "2024-01-01T00:00:00Z"
    }
  ],
  "last_updated": "2024-01-01T00:00:00Z"
}
```

## Projection Service

**Base URL**: `http://localhost:8083`

### Service Information

```http
GET /info
```

**Response**:
```json
{
  "service": "projection-service",
  "version": "1.0.0",
  "environment": "local",
  "started_at": "2024-01-01T00:00:00Z",
  "uptime_seconds": 3600
}
```

### Projection Statistics

```http
GET /stats
```

**Response**:
```json
{
  "projections_updated": 25000,
  "cache_invalidations": 500,
  "materialized_views_refreshed": 100,
  "average_refresh_time_ms": 25.5,
  "last_update_time": "2024-01-01T00:00:00Z"
}
```

### Latest Prices

```http
GET /prices/latest
```

**Query Parameters**:
- `instrument_id` (optional): Filter by instrument
- `tenant_id` (optional): Filter by tenant
- `limit` (optional): Limit results (default: 100)

**Response**:
```json
{
  "latest_prices": [
    {
      "instrument_id": "NG_HH_BALMO",
      "tenant_id": "tenant_1",
      "price": 120.50,
      "volume": 1000,
      "timestamp": "2024-01-01T00:00:00Z",
      "source": "aggregation-service"
    }
  ],
  "total_count": 1,
  "last_updated": "2024-01-01T00:00:00Z"
}
```

### Curve Snapshots

```http
GET /curves/snapshots
```

**Query Parameters**:
- `instrument_id` (optional): Filter by instrument
- `tenant_id` (optional): Filter by tenant
- `limit` (optional): Limit results (default: 100)

**Response**:
```json
{
  "curve_snapshots": [
    {
      "instrument_id": "NG_HH_BALMO",
      "tenant_id": "tenant_1",
      "curve_points": [
        {
          "tenor": "M1",
          "price": 120.50
        },
        {
          "tenor": "M2",
          "price": 121.00
        }
      ],
      "snapshot_time": "2024-01-01T00:00:00Z"
    }
  ],
  "total_count": 1
}
```

### Cache Management

```http
GET /cache/status
```

**Response**:
```json
{
  "cache_enabled": true,
  "cache_size": 5000,
  "cache_hit_rate": 0.85,
  "cache_ttl_seconds": 300,
  "last_cleanup": "2024-01-01T00:00:00Z"
}
```

### Invalidate Cache

```http
POST /cache/invalidate
```

**Request Body**:
```json
{
  "instrument_id": "NG_HH_BALMO",
  "tenant_id": "tenant_1",
  "reason": "manual_invalidation"
}
```

**Response**:
```json
{
  "success": true,
  "invalidated_keys": ["NG_HH_BALMO_tenant_1"],
  "invalidation_time_ms": 5.2
}
```

## Error Responses

All services return consistent error responses:

### 400 Bad Request

```json
{
  "error": "bad_request",
  "message": "Invalid request parameters",
  "details": {
    "field": "instrument_id",
    "reason": "required field missing"
  },
  "timestamp": "2024-01-01T00:00:00Z"
}
```

### 404 Not Found

```json
{
  "error": "not_found",
  "message": "Resource not found",
  "details": {
    "resource": "instrument",
    "id": "INVALID_INSTRUMENT"
  },
  "timestamp": "2024-01-01T00:00:00Z"
}
```

### 500 Internal Server Error

```json
{
  "error": "internal_error",
  "message": "Internal server error",
  "details": {
    "service": "normalization-service",
    "operation": "parse_market_data"
  },
  "timestamp": "2024-01-01T00:00:00Z"
}
```

### 503 Service Unavailable

```json
{
  "error": "service_unavailable",
  "message": "Service temporarily unavailable",
  "details": {
    "service": "normalization-service",
    "reason": "database_connection_failed"
  },
  "timestamp": "2024-01-01T00:00:00Z"
}
```

## Rate Limiting

All services implement rate limiting:

- **Default**: 1000 requests per minute per IP
- **Headers**: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`
- **Response**: 429 Too Many Requests when limit exceeded

## Authentication

### API Key Authentication

```http
Authorization: Bearer <api_key>
```

### Service-to-Service Authentication

```http
X-Service-Token: <service_token>
```

## WebSocket Endpoints

### Real-time Updates

```javascript
// Connect to WebSocket
const ws = new WebSocket('ws://localhost:8083/ws/prices');

// Listen for updates
ws.onmessage = function(event) {
  const data = JSON.parse(event.data);
  console.log('Price update:', data);
};

// Subscribe to specific instrument
ws.send(JSON.stringify({
  action: 'subscribe',
  instrument_id: 'NG_HH_BALMO',
  tenant_id: 'tenant_1'
}));
```

**WebSocket Message Format**:
```json
{
  "type": "price_update",
  "data": {
    "instrument_id": "NG_HH_BALMO",
    "tenant_id": "tenant_1",
    "price": 120.50,
    "timestamp": "2024-01-01T00:00:00Z"
  }
}
```

## SDK Examples

### Python SDK

```python
import requests

# Initialize client
client = DataProcessingClient(
    base_url="http://localhost:8080",
    api_key="your_api_key"
)

# Get service health
health = client.get_health()
print(f"Service healthy: {health['healthy']}")

# Get processing statistics
stats = client.get_stats()
print(f"Messages processed: {stats['messages_processed']}")

# Get latest prices
prices = client.get_latest_prices(
    instrument_id="NG_HH_BALMO",
    limit=10
)
for price in prices['latest_prices']:
    print(f"{price['instrument_id']}: {price['price']}")
```

### JavaScript SDK

```javascript
import { DataProcessingClient } from '@254carbon/data-processing-sdk';

// Initialize client
const client = new DataProcessingClient({
  baseUrl: 'http://localhost:8080',
  apiKey: 'your_api_key'
});

// Get service health
const health = await client.getHealth();
console.log(`Service healthy: ${health.healthy}`);

// Get processing statistics
const stats = await client.getStats();
console.log(`Messages processed: ${stats.messages_processed}`);

// Get latest prices
const prices = await client.getLatestPrices({
  instrumentId: 'NG_HH_BALMO',
  limit: 10
});
prices.latestPrices.forEach(price => {
  console.log(`${price.instrumentId}: ${price.price}`);
});
```

## Testing

### Health Check Test

```bash
curl -X GET http://localhost:8080/health
```

### Load Testing

```bash
# Install hey (HTTP load testing tool)
go install github.com/rakyll/hey@latest

# Run load test
hey -n 1000 -c 10 http://localhost:8080/health
```

### Integration Testing

```python
import pytest
import requests

def test_service_health():
    response = requests.get("http://localhost:8080/health")
    assert response.status_code == 200
    assert response.json()["healthy"] == True

def test_metrics_endpoint():
    response = requests.get("http://localhost:8080/metrics")
    assert response.status_code == 200
    assert "normalization_service_requests_total" in response.text
```

## Versioning

### API Versioning

- **Current Version**: v1
- **Version Header**: `Accept: application/vnd.254carbon.v1+json`
- **URL Versioning**: `/api/v1/endpoint`

### Backward Compatibility

- New fields are additive only
- Deprecated fields are marked but not removed
- Breaking changes require major version bump

## OpenAPI Specification

The complete OpenAPI 3.0 specification is available at:

- **Normalization Service**: `http://localhost:8080/openapi.json`
- **Enrichment Service**: `http://localhost:8081/openapi.json`
- **Aggregation Service**: `http://localhost:8082/openapi.json`
- **Projection Service**: `http://localhost:8083/openapi.json`

## Support

For API support and questions:

- **Documentation**: Check this API reference
- **Issues**: Report bugs via GitHub Issues
- **Community**: Join our Slack channel
- **Enterprise**: Contact support@254carbon.com

