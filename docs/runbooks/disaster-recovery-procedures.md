# Disaster Recovery Procedures

## Overview

This document provides comprehensive disaster recovery procedures for the 254Carbon Data Processing Pipeline on on-premises Kubernetes with Velero backup/DR.

## RTO and RPO Targets

- **RTO (Recovery Time Objective)**: 15 minutes
- **RPO (Recovery Point Objective)**: 1 hour
- **Availability Target**: 99.95%

## Backup Strategy

### Backup Schedule

- **Hourly Incremental**: ConfigMaps, Secrets, PVCs (24h retention)
- **Daily Full**: All resources except events (7 days retention)
- **Weekly Full**: Complete namespace backup (30 days retention)
- **Monthly Full**: Long-term archive (180 days retention)

### Backup Verification

```bash
# List all backups
velero backup get

# Describe specific backup
velero backup describe daily-full-backup-20250107

# Check backup logs
velero backup logs daily-full-backup-20250107

# Verify backup completion
velero backup describe daily-full-backup-20250107 | grep "Phase:"
```

## Disaster Scenarios and Recovery Procedures

### Scenario 1: Single Pod Failure

**Detection**: Pod crash, health check failure

**Recovery**:
```bash
# Kubernetes automatically restarts the pod
# Verify recovery
kubectl get pods -n data-processing -w

# If pod doesn't recover, delete it
kubectl delete pod <pod-name> -n data-processing
```

**Expected RTO**: < 2 minutes

### Scenario 2: Service Degradation

**Detection**: High error rate, increased latency

**Recovery**:
```bash
# Check service health
kubectl describe service <service-name> -n data-processing

# Check endpoints
kubectl get endpoints <service-name> -n data-processing

# Restart deployment if needed
kubectl rollout restart deployment/<service-name> -n data-processing

# Monitor rollout
kubectl rollout status deployment/<service-name> -n data-processing
```

**Expected RTO**: < 5 minutes

### Scenario 3: Complete Service Failure

**Detection**: All replicas down, service unavailable

**Recovery**:
```bash
# Scale up deployment
kubectl scale deployment/<service-name> -n data-processing --replicas=3

# If scaling fails, restore from backup
velero restore create --from-backup daily-full-backup-latest \
    --include-namespaces data-processing \
    --selector app=<service-name>

# Wait for restore
velero restore describe <restore-name>

# Verify service
curl http://<service-name>:8080/health
```

**Expected RTO**: < 10 minutes

### Scenario 4: Database Corruption or Loss

**Detection**: Query failures, data inconsistency

**ClickHouse Recovery**:
```bash
# Stop affected service
kubectl scale deployment/normalization-service -n data-processing --replicas=0

# Restore ClickHouse from latest backup
kubectl exec -it clickhouse-0 -n data-processing -- \
    clickhouse-backup restore <backup-name>

# Verify data integrity
kubectl exec -it clickhouse-0 -n data-processing -- \
    clickhouse-client --query "SELECT count() FROM market_ticks"

# Restart services
kubectl scale deployment/normalization-service -n data-processing --replicas=3
```

**PostgreSQL Recovery**:
```bash
# Create new pod for restoration
kubectl run postgres-restore --image=postgres:14 \
    --namespace=data-processing \
    -- sleep infinity

# Copy backup to pod
kubectl cp backup.sql data-processing/postgres-restore:/tmp/

# Restore database
kubectl exec -it postgres-restore -n data-processing -- \
    psql -U postgres -d taxonomy_db < /tmp/backup.sql

# Verify restoration
kubectl exec -it postgres-restore -n data-processing -- \
    psql -U postgres -d taxonomy_db -c "SELECT count(*) FROM taxonomy"
```

**Expected RTO**: < 15 minutes

### Scenario 5: Complete Namespace Loss

**Detection**: Namespace deleted, all resources gone

**Recovery**:
```bash
# Recreate namespace
kubectl create namespace data-processing

# Restore from latest full backup
velero restore create disaster-recovery-$(date +%Y%m%d) \
    --from-backup daily-full-backup-latest \
    --include-namespaces data-processing

# Monitor restore progress
velero restore describe disaster-recovery-$(date +%Y%m%d) -w

# Verify all services
kubectl get all -n data-processing

# Check service health
for service in normalization enrichment aggregation projection; do
    kubectl run test-${service} --image=curlimages/curl --rm -i --restart=Never \
        --namespace=data-processing \
        -- curl -f http://${service}-service:8080/health
done
```

**Expected RTO**: < 15 minutes

### Scenario 6: Complete Cluster Loss

**Detection**: Cluster unreachable, all nodes down

**Recovery**:
```bash
# Provision new cluster
# Configure kubectl for new cluster

# Install Velero
bash infrastructure/velero/install.sh

# Configure backup location (use same MinIO)
velero backup-location create default \
    --provider aws \
    --bucket velero-backups \
    --config region=minio,s3ForcePathStyle="true",s3Url=http://minio-external:9000

# List available backups
velero backup get

# Restore latest backup
velero restore create cluster-restore-$(date +%Y%m%d) \
    --from-backup daily-full-backup-latest

# Install Linkerd service mesh
bash infrastructure/linkerd/install.sh

# Install Flagger
bash infrastructure/flagger/install.sh

# Verify complete restoration
kubectl get namespaces
kubectl get all -n data-processing
kubectl get all -n linkerd

# Run smoke tests
bash scripts/run_integration_tests.py
```

**Expected RTO**: < 30 minutes (including cluster provisioning)

## Rollback Procedures

### Rollback Canary Deployment

```bash
# Check canary status
kubectl get canary <service-name> -n data-processing

# Manual rollback
kubectl patch canary <service-name> -n data-processing \
    --type='json' -p='[{"op": "replace", "path": "/spec/analysis/threshold", "value": 0}]'

# Flagger will automatically rollback
# Verify rollback
kubectl describe canary <service-name> -n data-processing
```

### Rollback Blue-Green Deployment

```bash
# Switch back to previous version
kubectl patch service <service-name> -n data-processing \
    -p '{"spec":{"selector":{"version":"blue"}}}'

# Scale up previous deployment
kubectl scale deployment/<service-name>-blue -n data-processing --replicas=3

# Verify rollback
curl http://<service-name>:8080/health
```

### Rollback Database Migration

```bash
# ClickHouse rollback
kubectl exec -it clickhouse-0 -n data-processing -- \
    clickhouse-client --query "DROP TABLE IF EXISTS new_table"

# PostgreSQL rollback  
kubectl exec -it postgresql-0 -n data-processing -- \
    psql -U postgres -d taxonomy_db -c "DROP TABLE IF EXISTS new_table CASCADE"
```

## Automated DR Testing

Run quarterly automated DR tests:

```bash
# Execute DR automation
python tests/disaster_recovery/dr_automation.py --scenario full_cluster_loss

# Verify results
cat dr_test_results_$(date +%Y%m%d).json
```

## Monitoring and Alerting

### Backup Monitoring

Prometheus alerts configured in `monitoring/backup_alerts.yml`:

- Backup failure alert
- Backup age alert (> 25 hours)
- Restore failure alert
- Backup storage full alert

### DR Metrics

Key metrics to monitor:
- `velero_backup_success_total`
- `velero_backup_failure_total`
- `velero_backup_duration_seconds`
- `velero_restore_success_total`
- `velero_restore_failure_total`

## Communication Plan

### Incident Declaration

1. Declare incident in incident management system
2. Page on-call team
3. Open war room (Slack channel: #incident-<timestamp>)
4. Notify stakeholders

### Status Updates

Provide updates every 15 minutes during active incident:
- Current status
- Actions taken
- Next steps
- Estimated resolution time

### Post-Incident

1. Conduct post-mortem within 48 hours
2. Document lessons learned
3. Create action items for improvements
4. Update runbooks based on learnings

## Contact Information

- **On-Call Engineer**: PagerDuty rotation
- **Platform Team Lead**: [Contact Info]
- **Database Administrator**: [Contact Info]
- **Security Team**: [Contact Info]

## Useful Commands

```bash
# Quick health check
kubectl get pods -n data-processing | grep -v Running

# Check recent events
kubectl get events -n data-processing --sort-by='.lastTimestamp'

# View service logs
kubectl logs -n data-processing deployment/<service-name> --tail=100

# Execute backup now
velero backup create manual-backup-$(date +%Y%m%d-%H%M) \
    --include-namespaces data-processing

# Test restore (dry-run)
velero restore create test-restore --from-backup <backup-name> --dry-run

# Emergency scale down
kubectl scale deployment --all -n data-processing --replicas=0

# Emergency scale up
kubectl scale deployment --all -n data-processing --replicas=3
```
