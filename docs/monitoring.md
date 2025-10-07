# Monitoring Infrastructure

This document describes the monitoring infrastructure for the data processing pipeline, including Prometheus metrics collection, Grafana dashboards, and alerting.

## Overview

The monitoring stack consists of:

- **Prometheus**: Metrics collection and storage
- **Grafana**: Visualization and dashboards
- **Node Exporter**: System metrics
- **cAdvisor**: Container metrics
- **Alert Manager**: Alerting (optional)

## Quick Start

### 1. Start Monitoring Services

```bash
# Start all monitoring services
python scripts/setup_monitoring.py

# Or start specific services
docker-compose up -d prometheus grafana node-exporter cadvisor
```

### 2. Access Monitoring Interfaces

- **Prometheus**: http://localhost:9090
- **Grafana**: http://localhost:3000 (admin/admin)
- **Node Exporter**: http://localhost:9100/metrics
- **cAdvisor**: http://localhost:8080

### 3. Test Monitoring

```bash
# Test all monitoring components
python scripts/test_monitoring.py
```

## Prometheus Configuration

### Metrics Collection

Prometheus scrapes metrics from the following targets:

- **Data Processing Services**: Port 8080 on each service
- **Node Exporter**: Port 9100
- **cAdvisor**: Port 8080
- **Infrastructure Services**: Kafka, ClickHouse, PostgreSQL, Redis

### Configuration Files

- `monitoring/prometheus.yml`: Main Prometheus configuration
- `monitoring/alert_rules.yml`: Alert rules and thresholds

### Key Metrics

#### Service Metrics
- `{service}_health_status`: Service health (1=healthy, 0=unhealthy)
- `{service}_requests_total`: Total HTTP requests
- `{service}_request_duration_seconds`: Request latency
- `{service}_messages_processed_total`: Messages processed
- `{service}_processing_duration_seconds`: Message processing time
- `{service}_errors_total`: Error count by type
- `{service}_memory_usage_bytes`: Memory usage
- `{service}_active_connections`: Active connections

#### Business Metrics
- `normalization_service_messages_processed_total`: Raw messages processed
- `enrichment_service_cache_hits_total`: Cache hit rate
- `aggregation_service_bars_generated_total`: OHLC bars generated
- `projection_service_projections_updated_total`: Projections updated

## Grafana Dashboards

### Available Dashboards

1. **Data Processing Overview**: High-level pipeline metrics
2. **Normalization Service**: Raw data processing metrics
3. **Enrichment Service**: Metadata lookup and caching metrics
4. **Aggregation Service**: OHLC bar generation metrics
5. **Projection Service**: Materialized view and cache metrics

### Dashboard Features

- **Real-time Metrics**: 30-second refresh rate
- **Historical Data**: 1-hour default time range
- **Alerting**: Visual indicators for threshold breaches
- **Drill-down**: Click through to detailed service views

### Custom Dashboards

To create custom dashboards:

1. Access Grafana at http://localhost:3000
2. Login with admin/admin
3. Go to Dashboards → New Dashboard
4. Add panels with Prometheus queries
5. Save and share with team

## Alerting

### Alert Rules

Alert rules are defined in `monitoring/alert_rules.yml`:

#### Service Health Alerts
- **ServiceDown**: Service unavailable for >1 minute
- **ServiceUnhealthy**: Health check failing for >2 minutes

#### Performance Alerts
- **HighRequestLatency**: 95th percentile latency >1 second
- **HighErrorRate**: Error rate >0.1 errors/second
- **HighMemoryUsage**: Memory usage >1GB

#### Business Logic Alerts
- **MessageProcessingLag**: Failure rate >5%
- **NoMessagesProcessed**: No messages for >10 minutes
- **HighDataQualityIssues**: Data quality issues detected

### Alert Configuration

Alerts are sent to Alert Manager (if configured) or can be integrated with:
- Slack notifications
- Email alerts
- PagerDuty integration
- Webhook endpoints

## Service Integration

### Adding Metrics to Services

Each service automatically exposes metrics at `/metrics` endpoint:

```python
from shared.framework.metrics import MetricsCollector

# Initialize metrics collector
metrics = MetricsCollector("service-name")

# Record custom metrics
metrics.record_request("GET", "/health", "200", 0.001)
metrics.record_message_processed("topic", "success", 0.05, "message_type")
metrics.record_error("validation_error", "parser")
```

### Custom Metrics

To add custom metrics:

```python
# Create custom counter
error_counter = metrics.create_counter(
    "custom_errors_total",
    "Custom error counter",
    ["error_type", "component"]
)

# Increment counter
error_counter.labels(error_type="validation", component="parser").inc()
```

## Troubleshooting

### Common Issues

#### Prometheus Not Scraping Services

1. Check service health: `curl http://localhost:8080/health`
2. Verify metrics endpoint: `curl http://localhost:8080/metrics`
3. Check Prometheus targets: http://localhost:9090/targets
4. Review service logs: `docker-compose logs service-name`

#### Grafana Dashboards Not Loading

1. Check Prometheus datasource: http://localhost:3000/datasources
2. Verify Prometheus connectivity from Grafana
3. Check dashboard JSON syntax
4. Review Grafana logs: `docker-compose logs grafana`

#### High Memory Usage

1. Check service memory metrics in Grafana
2. Review memory usage patterns
3. Adjust service resource limits
4. Consider scaling services

### Debug Commands

```bash
# Check service health
curl http://localhost:8080/health

# View service metrics
curl http://localhost:8080/metrics

# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# View Grafana health
curl http://localhost:3000/api/health

# Check container metrics
curl http://localhost:8080/metrics

# View system metrics
curl http://localhost:9100/metrics
```

### Log Analysis

```bash
# View service logs
docker-compose logs -f service-name

# View Prometheus logs
docker-compose logs -f prometheus

# View Grafana logs
docker-compose logs -f grafana

# View all monitoring logs
docker-compose logs -f prometheus grafana node-exporter cadvisor
```

## Performance Tuning

### Prometheus Configuration

- **Scrape Interval**: 15s (default)
- **Evaluation Interval**: 15s (default)
- **Retention Time**: 200h (default)
- **Storage**: Local TSDB

### Grafana Configuration

- **Refresh Rate**: 30s (default)
- **Time Range**: 1h (default)
- **Query Timeout**: 30s (default)

### Resource Requirements

- **Prometheus**: 2GB RAM, 10GB disk
- **Grafana**: 512MB RAM, 1GB disk
- **Node Exporter**: 64MB RAM
- **cAdvisor**: 128MB RAM

## Security Considerations

### Access Control

- Grafana admin password: `admin` (change in production)
- Prometheus: No authentication (add in production)
- Internal network: Services communicate over Docker network

### Production Recommendations

1. **Enable Authentication**: Set up LDAP/OAuth for Grafana
2. **TLS Encryption**: Use HTTPS for all endpoints
3. **Network Policies**: Restrict access to monitoring ports
4. **Secrets Management**: Store passwords in secure vault
5. **Audit Logging**: Enable audit logs for all services

## Backup and Recovery

### Prometheus Data

```bash
# Backup Prometheus data
docker run --rm -v prometheus-data:/data -v $(pwd):/backup alpine tar czf /backup/prometheus-backup.tar.gz -C /data .

# Restore Prometheus data
docker run --rm -v prometheus-data:/data -v $(pwd):/backup alpine tar xzf /backup/prometheus-backup.tar.gz -C /data
```

### Grafana Configuration

```bash
# Backup Grafana data
docker run --rm -v grafana-data:/data -v $(pwd):/backup alpine tar czf /backup/grafana-backup.tar.gz -C /data .

# Restore Grafana data
docker run --rm -v grafana-data:/data -v $(pwd):/backup alpine tar xzf /backup/grafana-backup.tar.gz -C /data
```

## Monitoring Best Practices

### Metrics Design

1. **Use Consistent Naming**: Follow Prometheus naming conventions
2. **Include Labels**: Add relevant dimensions for filtering
3. **Avoid High Cardinality**: Limit label values
4. **Document Metrics**: Provide clear descriptions

### Dashboard Design

1. **Start Simple**: Begin with basic metrics
2. **Add Context**: Include thresholds and baselines
3. **Use Appropriate Visualizations**: Choose right chart types
4. **Organize Logically**: Group related metrics

### Alerting Strategy

1. **Set Meaningful Thresholds**: Based on SLOs and baselines
2. **Avoid Alert Fatigue**: Limit number of alerts
3. **Provide Context**: Include relevant information in alerts
4. **Test Alerts**: Verify alert delivery and response

## Integration with CI/CD

### Monitoring in Deployment

1. **Health Checks**: Verify services are healthy after deployment
2. **Metrics Validation**: Ensure metrics are being collected
3. **Dashboard Updates**: Deploy dashboard changes
4. **Alert Testing**: Verify alerts are working

### Automated Testing

```bash
# Run monitoring tests in CI
python scripts/test_monitoring.py

# Check service health
python scripts/test_integration_simple.py
```

## Support and Maintenance

### Regular Tasks

1. **Review Metrics**: Check for new metrics and trends
2. **Update Dashboards**: Add new visualizations as needed
3. **Tune Alerts**: Adjust thresholds based on experience
4. **Clean Up**: Remove unused metrics and dashboards

### Monitoring the Monitors

- Monitor Prometheus itself for errors
- Check Grafana for dashboard load times
- Verify alert delivery mechanisms
- Review storage usage and retention

## References

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Prometheus Client Library](https://github.com/prometheus/client_python)
- [Docker Compose Documentation](https://docs.docker.com/compose/)

