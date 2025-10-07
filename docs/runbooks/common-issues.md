# Common Issues Runbook for 254Carbon Data Processing Pipeline

## Overview

This runbook provides step-by-step procedures for diagnosing and resolving common issues in the 254Carbon Data Processing Pipeline.

## Table of Contents

1. [Service Health Checks](#service-health-checks)
2. [Data Processing Issues](#data-processing-issues)
3. [Performance Issues](#performance-issues)
4. [Security Issues](#security-issues)
5. [Deployment Issues](#deployment-issues)
6. [Monitoring Issues](#monitoring-issues)
7. [Database Issues](#database-issues)
8. [Kafka Issues](#kafka-issues)
9. [Tracing Issues](#tracing-issues)
10. [Emergency Procedures](#emergency-procedures)

## Service Health Checks

### Check Service Status

```bash
# Check all services
kubectl get pods -n data-processing

# Check specific service
kubectl get pods -n data-processing -l app=normalization-service

# Check service logs
kubectl logs -f -n data-processing deployment/normalization-service

# Check service health endpoint
curl http://normalization-service:8080/health
```

### Service Not Starting

**Symptoms:**
- Pods stuck in `Pending` or `CrashLoopBackOff` state
- Services not responding to health checks

**Diagnosis:**
```bash
# Check pod events
kubectl describe pod <pod-name> -n data-processing

# Check resource usage
kubectl top pods -n data-processing

# Check node resources
kubectl top nodes
```

**Resolution:**
1. Check resource limits and requests
2. Verify image availability
3. Check configuration errors
4. Review security policies

## Data Processing Issues

### Data Not Processing

**Symptoms:**
- Messages accumulating in Kafka topics
- No data in ClickHouse tables
- Processing metrics showing zero

**Diagnosis:**
```bash
# Check Kafka topic lag
kubectl exec -it kafka-0 -n data-processing -- kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group normalization-group \
  --describe

# Check ClickHouse tables
kubectl exec -it clickhouse-0 -n data-processing -- clickhouse-client \
  --query "SELECT count() FROM market_ticks"

# Check service metrics
curl http://normalization-service:9090/metrics | grep processed
```

**Resolution:**
1. Restart affected services
2. Check Kafka connectivity
3. Verify database connections
4. Review error logs

### Data Quality Issues

**Symptoms:**
- High error rates in processing
- Invalid data in output tables
- Data quality alerts firing

**Diagnosis:**
```bash
# Check data quality metrics
curl http://enrichment-service:9090/metrics | grep quality

# Check error logs
kubectl logs -n data-processing deployment/enrichment-service | grep ERROR

# Check data validation
kubectl exec -it clickhouse-0 -n data-processing -- clickhouse-client \
  --query "SELECT * FROM market_ticks WHERE price <= 0 LIMIT 10"
```

**Resolution:**
1. Review data validation rules
2. Check input data format
3. Update enrichment logic
4. Implement data quality checks

## Performance Issues

### High Latency

**Symptoms:**
- Slow response times
- High P95/P99 latencies
- Timeout errors

**Diagnosis:**
```bash
# Check service performance
curl http://normalization-service:9090/metrics | grep duration

# Check resource usage
kubectl top pods -n data-processing

# Check database performance
kubectl exec -it clickhouse-0 -n data-processing -- clickhouse-client \
  --query "SELECT * FROM system.query_log WHERE event_date = today() ORDER BY query_duration_ms DESC LIMIT 10"
```

**Resolution:**
1. Scale up services
2. Optimize database queries
3. Check network connectivity
4. Review caching strategies

### High Memory Usage

**Symptoms:**
- Pods being OOMKilled
- High memory usage alerts
- Slow performance

**Diagnosis:**
```bash
# Check memory usage
kubectl top pods -n data-processing

# Check memory limits
kubectl describe pod <pod-name> -n data-processing

# Check heap dumps (if available)
kubectl exec -it <pod-name> -n data-processing -- jmap -histo 1
```

**Resolution:**
1. Increase memory limits
2. Optimize memory usage
3. Check for memory leaks
4. Review batch sizes

## Security Issues

### Authentication Failures

**Symptoms:**
- 401 Unauthorized errors
- API key validation failures
- mTLS connection errors

**Diagnosis:**
```bash
# Check API key status
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db \
  -c "SELECT * FROM api_keys WHERE is_active = true"

# Check certificate status
kubectl exec -it <pod-name> -n data-processing -- openssl x509 \
  -in /etc/certs/server-cert.pem -text -noout

# Check mTLS connections
kubectl logs -n data-processing deployment/normalization-service | grep TLS
```

**Resolution:**
1. Regenerate API keys
2. Update certificates
3. Check service identity
4. Review security policies

### Rate Limiting Issues

**Symptoms:**
- 429 Too Many Requests errors
- Rate limit alerts
- Service throttling

**Diagnosis:**
```bash
# Check rate limit metrics
curl http://normalization-service:9090/metrics | grep rate_limit

# Check Redis rate limit data
kubectl exec -it redis-0 -n data-processing -- redis-cli \
  --scan --pattern "rate_limit:*"

# Check service logs
kubectl logs -n data-processing deployment/normalization-service | grep rate_limit
```

**Resolution:**
1. Adjust rate limits
2. Check for abuse
3. Implement backoff strategies
4. Review traffic patterns

## Deployment Issues

### Deployment Failures

**Symptoms:**
- Helm deployment failures
- Pods not updating
- Rollback required

**Diagnosis:**
```bash
# Check deployment status
helm status data-processing-pipeline -n data-processing

# Check deployment history
helm history data-processing-pipeline -n data-processing

# Check pod events
kubectl get events -n data-processing --sort-by='.lastTimestamp'
```

**Resolution:**
1. Check configuration errors
2. Verify image availability
3. Review resource constraints
4. Perform rollback if needed

### Image Pull Failures

**Symptoms:**
- Pods stuck in `ImagePullBackOff`
- Image not found errors
- Registry connectivity issues

**Diagnosis:**
```bash
# Check pod status
kubectl describe pod <pod-name> -n data-processing

# Check image pull secrets
kubectl get secrets -n data-processing

# Test image pull
kubectl run test-pod --image=<image-name> --rm -it --restart=Never
```

**Resolution:**
1. Check image registry access
2. Verify image tags
3. Update pull secrets
4. Check network connectivity

## Monitoring Issues

### Prometheus Issues

**Symptoms:**
- Metrics not appearing
- Alerting not working
- Dashboard errors

**Diagnosis:**
```bash
# Check Prometheus status
kubectl get pods -n monitoring -l app=prometheus

# Check targets
curl http://prometheus:9090/api/v1/targets

# Check metrics
curl http://prometheus:9090/api/v1/query?query=up
```

**Resolution:**
1. Restart Prometheus
2. Check service discovery
3. Verify metric endpoints
4. Review configuration

### Grafana Issues

**Symptoms:**
- Dashboard not loading
- Data source errors
- Authentication issues

**Diagnosis:**
```bash
# Check Grafana status
kubectl get pods -n monitoring -l app=grafana

# Check Grafana logs
kubectl logs -n monitoring deployment/grafana

# Test data source
curl http://grafana:3000/api/datasources
```

**Resolution:**
1. Restart Grafana
2. Check data source configuration
3. Verify authentication
4. Review dashboard queries

## Database Issues

### ClickHouse Issues

**Symptoms:**
- Query timeouts
- Connection errors
- Data corruption

**Diagnosis:**
```bash
# Check ClickHouse status
kubectl exec -it clickhouse-0 -n data-processing -- clickhouse-client \
  --query "SELECT * FROM system.processes"

# Check table status
kubectl exec -it clickhouse-0 -n data-processing -- clickhouse-client \
  --query "SELECT * FROM system.tables WHERE database = 'default'"

# Check query log
kubectl exec -it clickhouse-0 -n data-processing -- clickhouse-client \
  --query "SELECT * FROM system.query_log WHERE event_date = today() ORDER BY event_time DESC LIMIT 10"
```

**Resolution:**
1. Restart ClickHouse
2. Check disk space
3. Optimize queries
4. Review table structure

### PostgreSQL Issues

**Symptoms:**
- Connection pool exhaustion
- Query timeouts
- Lock contention

**Diagnosis:**
```bash
# Check PostgreSQL status
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db \
  -c "SELECT * FROM pg_stat_activity"

# Check database size
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db \
  -c "SELECT pg_size_pretty(pg_database_size('db'))"

# Check slow queries
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db \
  -c "SELECT * FROM pg_stat_statements ORDER BY total_time DESC LIMIT 10"
```

**Resolution:**
1. Restart PostgreSQL
2. Check connection limits
3. Optimize queries
4. Review indexing

## Kafka Issues

### Consumer Lag

**Symptoms:**
- High consumer lag
- Processing delays
- Memory issues

**Diagnosis:**
```bash
# Check consumer groups
kubectl exec -it kafka-0 -n data-processing -- kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --list

# Check topic details
kubectl exec -it kafka-0 -n data-processing -- kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic market-data

# Check partition status
kubectl exec -it kafka-0 -n data-processing -- kafka-log-dirs.sh \
  --bootstrap-server localhost:9092 \
  --describe --json
```

**Resolution:**
1. Scale up consumers
2. Check partition distribution
3. Review consumer configuration
4. Monitor broker health

### Broker Issues

**Symptoms:**
- Broker failures
- Topic creation failures
- Replication issues

**Diagnosis:**
```bash
# Check broker status
kubectl exec -it kafka-0 -n data-processing -- kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092

# Check cluster health
kubectl exec -it kafka-0 -n data-processing -- kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --list

# Check broker logs
kubectl logs -n data-processing kafka-0
```

**Resolution:**
1. Restart brokers
2. Check disk space
3. Review replication settings
4. Monitor broker metrics

## Tracing Issues

### Jaeger Issues

**Symptoms:**
- Traces not appearing
- High memory usage
- Query timeouts

**Diagnosis:**
```bash
# Check Jaeger status
kubectl get pods -n data-processing -l component=jaeger-collector

# Check collector health
curl http://jaeger-collector:14268/

# Check storage status
kubectl exec -it elasticsearch-0 -n data-processing -- curl \
  http://localhost:9200/_cluster/health
```

**Resolution:**
1. Restart Jaeger components
2. Check storage connectivity
3. Review sampling rates
4. Monitor resource usage

### Trace Quality Issues

**Symptoms:**
- Incomplete traces
- Missing spans
- High trace volume

**Diagnosis:**
```bash
# Check trace metrics
curl http://jaeger-collector:9090/metrics | grep traces

# Check sampling configuration
kubectl exec -it <pod-name> -n data-processing -- env | grep TRACING

# Check span volume
kubectl exec -it elasticsearch-0 -n data-processing -- curl \
  http://localhost:9200/jaeger-span-*/_count
```

**Resolution:**
1. Adjust sampling rates
2. Check trace configuration
3. Review span creation
4. Monitor trace volume

## Emergency Procedures

### Service Outage

**Immediate Actions:**
1. Check service status
2. Review error logs
3. Check resource usage
4. Notify stakeholders

**Recovery Steps:**
1. Restart affected services
2. Check dependencies
3. Verify data integrity
4. Monitor recovery

### Data Loss

**Immediate Actions:**
1. Stop data processing
2. Assess data loss scope
3. Check backup availability
4. Notify stakeholders

**Recovery Steps:**
1. Restore from backup
2. Replay missing data
3. Verify data integrity
4. Resume processing

### Security Breach

**Immediate Actions:**
1. Isolate affected systems
2. Preserve evidence
3. Notify security team
4. Document incident

**Recovery Steps:**
1. Patch vulnerabilities
2. Rotate credentials
3. Review access logs
4. Update security policies

## Escalation Procedures

### Level 1 (On-call Engineer)
- Basic troubleshooting
- Service restarts
- Configuration changes
- Initial diagnosis

### Level 2 (Senior Engineer)
- Complex troubleshooting
- Performance optimization
- Security issues
- Architecture changes

### Level 3 (Principal Engineer)
- Critical system failures
- Data loss incidents
- Security breaches
- Architecture decisions

## Contact Information

- **On-call Engineer**: +1-XXX-XXX-XXXX
- **Senior Engineer**: +1-XXX-XXX-XXXX
- **Principal Engineer**: +1-XXX-XXX-XXXX
- **Security Team**: security@254carbon.com
- **Management**: management@254carbon.com

## Additional Resources

- [Service Documentation](../services/)
- [Monitoring Dashboards](../monitoring/)
- [Security Policies](../security/)
- [Deployment Guide](../deployment/)
- [API Documentation](../api/)
