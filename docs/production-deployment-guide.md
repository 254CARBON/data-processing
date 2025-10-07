# Production Deployment Guide

## Overview

This guide provides comprehensive instructions for deploying the 254Carbon Data Processing Pipeline to production with Phase 5 production hardening features.

## Prerequisites

- Kubernetes cluster (v1.26+)
- Helm 3.12+
- kubectl configured for your cluster
- Docker registry access
- 100GB+ storage for backups

## Architecture

The production deployment includes:

- **Service Mesh**: Linkerd for mTLS, observability, and traffic management
- **Progressive Delivery**: Flagger for automated canary and blue-green deployments
- **High Availability**: Redis Sentinel, pgBouncer, multi-replica services
- **Backup/DR**: Velero with MinIO storage backend
- **Monitoring**: Prometheus, Grafana, Pyroscope continuous profiling
- **Autoscaling**: HPA based on CPU, memory, and custom metrics

## Installation Steps

### 1. Prepare Cluster

```bash
# Create namespace
kubectl create namespace data-processing

# Label namespace for Linkerd injection
kubectl label namespace data-processing linkerd.io/inject=enabled
```

### 2. Install Service Mesh

```bash
# Install Linkerd
cd infrastructure/linkerd
chmod +x install.sh
./install.sh

# Verify installation
linkerd check

# Apply service profiles
kubectl apply -f service-profiles/
```

### 3. Deploy Infrastructure

```bash
# Install Redis cluster with Sentinel
kubectl apply -f infrastructure/redis/cluster-config.yaml

# Install pgBouncer for PostgreSQL connection pooling
kubectl apply -f infrastructure/postgres/pgbouncer-deployment.yaml

# Wait for infrastructure to be ready
kubectl wait --for=condition=available --timeout=300s \
    deployment/redis-sentinel -n data-processing
kubectl wait --for=condition=available --timeout=300s \
    deployment/pgbouncer -n data-processing
```

### 4. Run Database Migrations

```bash
# Run ClickHouse migrations
python migrations/migrate.py clickhouse

# Includes performance indexes and materialized views
# - 009_performance_indexes.sql
# - 010_materialized_views.sql

# Run PostgreSQL migrations
python migrations/migrate.py postgres

# Includes performance indexes
# - 004_performance_indexes.sql
```

### 5. Deploy Services with Helm

```bash
# Add Helm repository (if using)
helm repo add data-processing https://charts.254carbon.com
helm repo update

# Deploy with production values
helm upgrade --install data-processing-pipeline ./helm \
    --namespace data-processing \
    --create-namespace \
    --values helm/values-production.yaml \
    --set global.environment=production \
    --set services.normalization.autoscaling.enabled=true \
    --set services.enrichment.autoscaling.enabled=true \
    --set services.aggregation.autoscaling.enabled=true \
    --set services.projection.autoscaling.enabled=true \
    --wait \
    --timeout=15m
```

### 6. Install Progressive Delivery

```bash
# Install Flagger
cd infrastructure/flagger
chmod +x install.sh
./install.sh

# Apply metric templates
kubectl apply -f metric-templates.yaml

# Apply canary configurations
kubectl apply -f ../linkerd/traffic-splits/
```

### 7. Setup Backup and DR

```bash
# Install Velero with MinIO
cd infrastructure/velero
chmod +x install.sh
./install.sh

# Verify backup schedules
velero schedule get

# Test backup
velero backup create manual-test-backup \
    --include-namespaces data-processing
```

### 8. Install Monitoring

```bash
# Install Pyroscope for continuous profiling
kubectl apply -f infrastructure/pyroscope/deployment.yaml

# Update Prometheus with new scrape configs
kubectl apply -f monitoring/prometheus.yml

# Import Grafana dashboards
# - linkerd-mesh.json
# - performance-optimization.json
```

## Deployment Strategies

### Canary Deployment (Recommended)

```bash
# Deploy new version with canary strategy
kubectl set image deployment/normalization-service \
    normalization-service=gcr.io/project/normalization-service:v1.2.3 \
    -n data-processing

# Flagger automatically:
# 1. Creates canary deployment
# 2. Shifts traffic progressively (10% → 25% → 50% → 100%)
# 3. Monitors metrics (error rate, latency, success rate)
# 4. Rolls back automatically if metrics fail
# 5. Promotes to stable after success

# Monitor canary progress
kubectl get canary normalization-service -n data-processing -w
```

### Blue-Green Deployment

```bash
# Deploy using blue-green script
bash scripts/deploy_blue_green.sh normalization-service v1.2.3 data-processing

# Script automatically:
# 1. Deploys to inactive environment
# 2. Runs smoke tests
# 3. Switches traffic instantly
# 4. Keeps old version for rollback
```

### Automated CI/CD Deployment

```bash
# Trigger via GitHub Actions
# Navigate to: Actions → Deploy to Production
# Select:
#   - Deployment strategy: canary
#   - Image tag: v1.2.3
#   - Services: all
#   - Rollback on failure: true
```

## Performance Tuning

### Service Configuration

Each service includes performance optimizations:

```yaml
# Example environment variables
env:
  # Connection pooling
  - name: DATA_PROC_CLICKHOUSE_POOL_MIN
    value: "5"
  - name: DATA_PROC_CLICKHOUSE_POOL_MAX
    value: "20"
  - name: DATA_PROC_REDIS_POOL_MIN
    value: "10"
  - name: DATA_PROC_REDIS_POOL_MAX
    value: "50"
  
  # Batch processing
  - name: DATA_PROC_NORMALIZATION_BATCH_SIZE
    value: "1000"
  - name: DATA_PROC_NORMALIZATION_FLUSH_INTERVAL
    value: "5"
  
  # Kafka optimization
  - name: DATA_PROC_KAFKA_FETCH_MIN_BYTES
    value: "102400"
  - name: DATA_PROC_KAFKA_FETCH_MAX_WAIT_MS
    value: "500"
  
  # Caching
  - name: DATA_PROC_TAXONOMY_CACHE_TTL
    value: "3600"
  - name: DATA_PROC_INSTRUMENT_CACHE_TTL
    value: "1800"
```

### Resource Limits

```yaml
resources:
  requests:
    cpu: 500m
    memory: 512Mi
  limits:
    cpu: 2000m
    memory: 2Gi

autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 10
  targetCPUUtilizationPercentage: 70
  targetMemoryUtilizationPercentage: 80
```

## Monitoring and Observability

### Key Dashboards

- **Service Mesh**: http://grafana/d/linkerd-mesh-dataproc
- **Performance Optimization**: http://grafana/d/perf-opt-dataproc
- **Service Overview**: http://grafana/d/overview
- **Continuous Profiling**: http://pyroscope:4040

### Key Metrics

- Request success rate > 99%
- P95 latency < 200ms
- P99 latency < 500ms
- Error rate < 1%
- Pod availability > 99%

### Alerts

Configured in `monitoring/alert_rules.yml`:
- High error rate
- High latency
- Pod failures
- Backup failures
- Performance regression

## Disaster Recovery

### Backup Verification

```bash
# List backups
velero backup get

# Test restore (dry-run)
velero restore create test-restore \
    --from-backup daily-full-backup-latest \
    --dry-run
```

### Recovery Procedures

See `docs/runbooks/disaster-recovery-procedures.md` for:
- Pod failure recovery
- Service degradation recovery
- Database corruption recovery
- Complete cluster loss recovery

## Security

### mTLS

All service-to-service communication encrypted via Linkerd:

```bash
# Verify mTLS status
linkerd viz stat deploy -n data-processing

# Check TLS connection percentage (should be 100%)
linkerd viz edges deployment -n data-processing
```

### API Authentication

Services use API key authentication:

```bash
# Create API key
kubectl exec -it normalization-service-0 -n data-processing -- \
    python -c "from shared.framework.auth import APIKeyManager; \
    manager = APIKeyManager(); \
    print(manager.create_key('tenant-1', permissions=['read', 'write']))"
```

## Troubleshooting

### Common Issues

1. **Pod not starting**: Check `kubectl logs` and `kubectl describe pod`
2. **High latency**: Check service mesh metrics and connection pool utilization
3. **Backup failures**: Verify MinIO connectivity and storage capacity
4. **Canary stuck**: Check Flagger logs and metric queries

### Debug Commands

```bash
# Check service mesh status
linkerd check

# View real-time traffic
linkerd viz tap deploy/normalization-service -n data-processing

# Check canary status
kubectl describe canary normalization-service -n data-processing

# View backup logs
velero backup logs daily-full-backup-latest

# Check performance metrics
kubectl top pods -n data-processing
```

## Maintenance

### Regular Tasks

- **Daily**: Review monitoring dashboards and alerts
- **Weekly**: Verify backup success and test restore
- **Monthly**: Run chaos engineering tests
- **Quarterly**: DR drill and capacity planning review

### Updates

```bash
# Update services
helm upgrade data-processing-pipeline ./helm \
    --namespace data-processing \
    --reuse-values \
    --set services.normalization.image.tag=v1.2.4

# Update service mesh
linkerd upgrade | kubectl apply -f -
```

## Support

For issues:
1. Check monitoring dashboards
2. Review runbooks in `docs/runbooks/`
3. Contact platform team
4. Create incident in incident management system
