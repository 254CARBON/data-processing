# Troubleshooting Guide

This guide provides comprehensive troubleshooting information for the 254Carbon data processing pipeline, including common issues, diagnostic procedures, and resolution steps.

## Quick Diagnostic Commands

### Service Health Checks

```bash
# Check all service health endpoints
curl http://localhost:8080/health  # Normalization
curl http://localhost:8081/health  # Enrichment
curl http://localhost:8082/health  # Aggregation
curl http://localhost:8083/health  # Projection

# Check infrastructure health
curl http://localhost:9090/-/healthy  # Prometheus
curl http://localhost:3000/api/health  # Grafana
curl http://localhost:9100/metrics  # Node Exporter
curl http://localhost:8080/metrics  # cAdvisor
```

### Docker Compose Status

```bash
# Check service status
docker-compose ps

# View service logs
docker-compose logs -f service-name

# Check resource usage
docker stats

# Restart specific service
docker-compose restart service-name
```

### Kubernetes Status

```bash
# Check pod status
kubectl get pods -n data-processing-prod

# Check service status
kubectl get services -n data-processing-prod

# Check deployment status
kubectl get deployments -n data-processing-prod

# Check ingress status
kubectl get ingress -n data-processing-prod
```

## Common Issues and Solutions

### 1. Service Startup Issues

#### Problem: Service fails to start

**Symptoms**:
- Service container exits immediately
- Health check fails
- No logs from service

**Diagnostic Steps**:
```bash
# Check container logs
docker-compose logs service-name

# Check container status
docker-compose ps

# Check resource usage
docker stats

# Check port conflicts
netstat -tulpn | grep :8080
```

**Common Causes and Solutions**:

1. **Port Conflict**:
   ```bash
   # Find process using port
   lsof -i :8080
   
   # Kill conflicting process
   kill -9 <PID>
   
   # Or change port in docker-compose.yml
   ports:
     - "8081:8080"  # Use different host port
   ```

2. **Missing Dependencies**:
   ```bash
   # Check if dependencies are running
   docker-compose ps
   
   # Start dependencies first
   docker-compose up -d kafka clickhouse postgres redis
   
   # Wait for dependencies to be ready
   sleep 30
   
   # Start service
   docker-compose up -d service-name
   ```

3. **Configuration Errors**:
   ```bash
   # Check configuration file
   cat service-name/app/config.py
   
   # Validate environment variables
   docker-compose config
   
   # Check for missing environment variables
   grep -r "os.getenv" service-name/
   ```

#### Problem: Service starts but health check fails

**Symptoms**:
- Service container is running
- Health endpoint returns 503
- Service logs show errors

**Diagnostic Steps**:
```bash
# Check health endpoint directly
curl -v http://localhost:8080/health

# Check service logs
docker-compose logs -f service-name

# Check service metrics
curl http://localhost:8080/metrics
```

**Common Causes and Solutions**:

1. **Database Connection Issues**:
   ```bash
   # Test database connectivity
   docker-compose exec clickhouse clickhouse-client --query "SELECT 1"
   docker-compose exec postgres psql -U user -d db -c "SELECT 1"
   
   # Check database logs
   docker-compose logs clickhouse
   docker-compose logs postgres
   
   # Restart database
   docker-compose restart clickhouse postgres
   ```

2. **Kafka Connection Issues**:
   ```bash
   # Test Kafka connectivity
   docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092
   
   # Check Kafka topics
   docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list
   
   # Check Kafka logs
   docker-compose logs kafka
   ```

3. **Redis Connection Issues**:
   ```bash
   # Test Redis connectivity
   docker-compose exec redis redis-cli ping
   
   # Check Redis logs
   docker-compose logs redis
   
   # Restart Redis
   docker-compose restart redis
   ```

### 2. Data Processing Issues

#### Problem: Messages not being processed

**Symptoms**:
- Messages accumulate in Kafka topics
- Processing rate drops to zero
- Service logs show no activity

**Diagnostic Steps**:
```bash
# Check Kafka topic offsets
docker-compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group normalization-local \
  --describe

# Check service metrics
curl http://localhost:8080/metrics | grep messages_processed

# Check service logs
docker-compose logs -f normalization-service
```

**Common Causes and Solutions**:

1. **Consumer Group Issues**:
   ```bash
   # Reset consumer group offset
   docker-compose exec kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group normalization-local \
     --reset-offsets \
     --to-earliest \
     --topic ingestion.market.raw.v1 \
     --execute
   
   # Restart service
   docker-compose restart normalization-service
   ```

2. **Message Format Issues**:
   ```bash
   # Check message format
   docker-compose exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic ingestion.market.raw.v1 \
     --from-beginning \
     --max-messages 5
   
   # Check for malformed messages
   docker-compose logs normalization-service | grep ERROR
   ```

3. **Service Overload**:
   ```bash
   # Check resource usage
   docker stats
   
   # Check service metrics
   curl http://localhost:8080/metrics | grep processing_duration
   
   # Scale service
   docker-compose up -d --scale normalization-service=2
   ```

#### Problem: High error rates

**Symptoms**:
- Error rate > 5%
- Dead letter queue filling up
- Service logs show frequent errors

**Diagnostic Steps**:
```bash
# Check error metrics
curl http://localhost:8080/metrics | grep errors_total

# Check dead letter queue
docker-compose exec kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic dlq \
  --from-beginning \
  --max-messages 10

# Check service logs for errors
docker-compose logs normalization-service | grep ERROR
```

**Common Causes and Solutions**:

1. **Data Quality Issues**:
   ```bash
   # Check data quality rules
   curl http://localhost:8080/quality-rules
   
   # Review quality flags
   curl http://localhost:8080/metrics | grep quality_flags
   
   # Adjust quality thresholds
   # Edit service-normalization/app/config.py
   ```

2. **Schema Mismatches**:
   ```bash
   # Check message schema
   docker-compose exec kafka kafka-console-consumer \
     --bootstrap-server localhost:9092 \
     --topic ingestion.market.raw.v1 \
     --from-beginning \
     --max-messages 1
   
   # Validate schema compliance
   python scripts/validate_schema.py
   ```

3. **Resource Exhaustion**:
   ```bash
   # Check memory usage
   docker stats
   
   # Check disk space
   df -h
   
   # Restart service
   docker-compose restart service-name
   ```

### 3. Performance Issues

#### Problem: High latency

**Symptoms**:
- End-to-end latency > 2 seconds
- Processing time increasing
- Queue buildup

**Diagnostic Steps**:
```bash
# Check latency metrics
curl http://localhost:8080/metrics | grep duration

# Check queue depths
docker-compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group normalization-local \
  --describe

# Check resource usage
docker stats
```

**Common Causes and Solutions**:

1. **Resource Constraints**:
   ```bash
   # Check CPU usage
   docker stats --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}"
   
   # Check memory usage
   free -h
   
   # Scale services
   docker-compose up -d --scale normalization-service=3
   ```

2. **Database Performance**:
   ```bash
   # Check database performance
   docker-compose exec clickhouse clickhouse-client --query "
     SELECT query, query_duration_ms 
     FROM system.query_log 
     WHERE query_duration_ms > 1000 
     ORDER BY query_duration_ms DESC 
     LIMIT 10"
   
   # Check database connections
   docker-compose exec clickhouse clickhouse-client --query "
     SELECT count() FROM system.processes"
   ```

3. **Network Issues**:
   ```bash
   # Check network latency
   ping kafka
   ping clickhouse
   
   # Check network usage
   iftop
   
   # Check Docker network
   docker network ls
   docker network inspect data-processing_data-processing
   ```

#### Problem: Low throughput

**Symptoms**:
- Processing rate < 1000 messages/second
- Services underutilized
- No errors but slow processing

**Diagnostic Steps**:
```bash
# Check throughput metrics
curl http://localhost:8080/metrics | grep messages_processed_total

# Check service utilization
docker stats

# Check Kafka producer performance
docker-compose exec kafka kafka-producer-perf-test \
  --topic test-topic \
  --num-records 1000 \
  --record-size 1000 \
  --throughput 1000 \
  --producer-props bootstrap.servers=localhost:9092
```

**Common Causes and Solutions**:

1. **Batch Size Issues**:
   ```bash
   # Check batch configuration
   grep -r "batch_size" service-normalization/
   
   # Increase batch size
   # Edit service-normalization/app/config.py
   DATA_PROC_MAX_BATCH_SIZE=10000
   ```

2. **Connection Pool Issues**:
   ```bash
   # Check connection pool settings
   grep -r "pool" service-normalization/
   
   # Increase connection pool size
   # Edit service-normalization/app/config.py
   DATA_PROC_DB_POOL_SIZE=20
   ```

3. **Serialization Bottlenecks**:
   ```bash
   # Check serialization performance
   python scripts/benchmark_serialization.py
   
   # Profile service performance
   python -m cProfile -o profile.stats service-normalization/app/main.py
   ```

### 4. Monitoring Issues

#### Problem: Metrics not appearing in Prometheus

**Symptoms**:
- Prometheus targets show as down
- Grafana dashboards show no data
- Service metrics endpoint returns 404

**Diagnostic Steps**:
```bash
# Check Prometheus targets
curl http://localhost:9090/api/v1/targets

# Check service metrics endpoint
curl http://localhost:8080/metrics

# Check Prometheus configuration
docker-compose exec prometheus cat /etc/prometheus/prometheus.yml
```

**Common Causes and Solutions**:

1. **Service Discovery Issues**:
   ```bash
   # Check service labels
   kubectl get pods -n data-processing-prod --show-labels
   
   # Add required labels
   kubectl label pod <pod-name> prometheus.io/scrape=true
   kubectl label pod <pod-name> prometheus.io/port=8080
   ```

2. **Network Connectivity**:
   ```bash
   # Test connectivity from Prometheus
   docker-compose exec prometheus wget -qO- http://normalization-service:8080/metrics
   
   # Check network policies
   kubectl get networkpolicies -n data-processing-prod
   ```

3. **Configuration Errors**:
   ```bash
   # Validate Prometheus configuration
   docker-compose exec prometheus promtool check config /etc/prometheus/prometheus.yml
   
   # Reload Prometheus configuration
   curl -X POST http://localhost:9090/-/reload
   ```

#### Problem: Grafana dashboards not loading

**Symptoms**:
- Dashboards show "No data"
- Datasource connection fails
- Dashboard panels are empty

**Diagnostic Steps**:
```bash
# Check Grafana datasources
curl http://admin:admin@localhost:3000/api/datasources

# Check Prometheus connectivity from Grafana
docker-compose exec grafana wget -qO- http://prometheus:9090/api/v1/query?query=up

# Check Grafana logs
docker-compose logs grafana
```

**Common Causes and Solutions**:

1. **Datasource Configuration**:
   ```bash
   # Check datasource configuration
   docker-compose exec grafana cat /etc/grafana/provisioning/datasources/datasources.yaml
   
   # Test datasource connection
   curl -X POST http://admin:admin@localhost:3000/api/datasources/1/test
   ```

2. **Dashboard Configuration**:
   ```bash
   # Check dashboard configuration
   docker-compose exec grafana cat /etc/grafana/provisioning/dashboards/dashboards.yaml
   
   # Import dashboard manually
   curl -X POST http://admin:admin@localhost:3000/api/dashboards/db \
     -H "Content-Type: application/json" \
     -d @monitoring/grafana/dashboards/data-processing-overview.json
   ```

3. **Permission Issues**:
   ```bash
   # Check Grafana permissions
   curl http://admin:admin@localhost:3000/api/org/users
   
   # Reset admin password
   docker-compose exec grafana grafana-cli admin reset-admin-password admin
   ```

### 5. Database Issues

#### Problem: ClickHouse connection failures

**Symptoms**:
- Service logs show connection errors
- ClickHouse queries fail
- Data not being written

**Diagnostic Steps**:
```bash
# Test ClickHouse connectivity
docker-compose exec clickhouse clickhouse-client --query "SELECT 1"

# Check ClickHouse logs
docker-compose logs clickhouse

# Check ClickHouse system tables
docker-compose exec clickhouse clickhouse-client --query "
  SELECT * FROM system.tables WHERE database = 'market_data'"
```

**Common Causes and Solutions**:

1. **Connection Pool Exhaustion**:
   ```bash
   # Check active connections
   docker-compose exec clickhouse clickhouse-client --query "
     SELECT count() FROM system.processes"
   
   # Check connection limits
   docker-compose exec clickhouse clickhouse-client --query "
     SELECT * FROM system.settings WHERE name LIKE '%connection%'"
   
   # Increase connection limits
   # Edit clickhouse configuration
   ```

2. **Disk Space Issues**:
   ```bash
   # Check disk usage
   docker-compose exec clickhouse df -h
   
   # Check ClickHouse data directory
   docker-compose exec clickhouse du -sh /var/lib/clickhouse/
   
   # Clean up old data
   docker-compose exec clickhouse clickhouse-client --query "
     DROP TABLE IF EXISTS old_table"
   ```

3. **Memory Issues**:
   ```bash
   # Check memory usage
   docker-compose exec clickhouse free -h
   
   # Check ClickHouse memory settings
   docker-compose exec clickhouse clickhouse-client --query "
     SELECT * FROM system.settings WHERE name LIKE '%memory%'"
   
   # Increase memory limits
   # Edit clickhouse configuration
   ```

#### Problem: PostgreSQL connection failures

**Symptoms**:
- Service logs show connection errors
- Metadata lookups fail
- Taxonomy classification errors

**Diagnostic Steps**:
```bash
# Test PostgreSQL connectivity
docker-compose exec postgres psql -U user -d db -c "SELECT 1"

# Check PostgreSQL logs
docker-compose logs postgres

# Check active connections
docker-compose exec postgres psql -U user -d db -c "
  SELECT count(*) FROM pg_stat_activity"
```

**Common Causes and Solutions**:

1. **Connection Limit Exceeded**:
   ```bash
   # Check connection limits
   docker-compose exec postgres psql -U user -d db -c "
     SHOW max_connections"
   
   # Check active connections
   docker-compose exec postgres psql -U user -d db -c "
     SELECT count(*) FROM pg_stat_activity"
   
   # Increase connection limits
   # Edit postgres configuration
   ```

2. **Lock Contention**:
   ```bash
   # Check for locks
   docker-compose exec postgres psql -U user -d db -c "
     SELECT * FROM pg_locks WHERE NOT granted"
   
   # Check for long-running queries
   docker-compose exec postgres psql -U user -d db -c "
     SELECT * FROM pg_stat_activity WHERE state = 'active'"
   ```

3. **Index Issues**:
   ```bash
   # Check index usage
   docker-compose exec postgres psql -U user -d db -c "
     SELECT * FROM pg_stat_user_indexes"
   
   # Rebuild indexes
   docker-compose exec postgres psql -U user -d db -c "
     REINDEX DATABASE db"
   ```

### 6. Kafka Issues

#### Problem: Kafka broker failures

**Symptoms**:
- Services can't connect to Kafka
- Messages not being produced/consumed
- Topic replication issues

**Diagnostic Steps**:
```bash
# Check Kafka broker status
docker-compose exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092

# Check Kafka logs
docker-compose logs kafka

# Check topic status
docker-compose exec kafka kafka-topics --bootstrap-server localhost:9092 --describe
```

**Common Causes and Solutions**:

1. **Disk Space Issues**:
   ```bash
   # Check disk usage
   docker-compose exec kafka df -h
   
   # Check Kafka data directory
   docker-compose exec kafka du -sh /var/lib/kafka/data/
   
   # Clean up old logs
   docker-compose exec kafka kafka-log-dirs \
     --bootstrap-server localhost:9092 \
     --describe
   ```

2. **Memory Issues**:
   ```bash
   # Check memory usage
   docker-compose exec kafka free -h
   
   # Check Kafka memory settings
   docker-compose exec kafka cat /etc/kafka/server.properties | grep -i memory
   
   # Increase memory limits
   # Edit kafka configuration
   ```

3. **Network Issues**:
   ```bash
   # Check network connectivity
   ping kafka
   
   # Check port accessibility
   telnet kafka 9092
   
   # Check Docker network
   docker network inspect data-processing_data-processing
   ```

#### Problem: Consumer lag

**Symptoms**:
- Messages accumulating in topics
- Processing delays
- Consumer groups not progressing

**Diagnostic Steps**:
```bash
# Check consumer group lag
docker-compose exec kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --group normalization-local \
  --describe

# Check topic offsets
docker-compose exec kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic ingestion.market.raw.v1
```

**Common Causes and Solutions**:

1. **Slow Processing**:
   ```bash
   # Check processing metrics
   curl http://localhost:8080/metrics | grep processing_duration
   
   # Check resource usage
   docker stats
   
   # Scale consumers
   docker-compose up -d --scale normalization-service=3
   ```

2. **Consumer Group Issues**:
   ```bash
   # Reset consumer group
   docker-compose exec kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group normalization-local \
     --reset-offsets \
     --to-earliest \
     --topic ingestion.market.raw.v1 \
     --execute
   
   # Restart consumers
   docker-compose restart normalization-service
   ```

3. **Partition Imbalance**:
   ```bash
   # Check partition distribution
   docker-compose exec kafka kafka-consumer-groups \
     --bootstrap-server localhost:9092 \
     --group normalization-local \
     --describe
   
   # Rebalance partitions
   docker-compose exec kafka kafka-reassign-partitions \
     --bootstrap-server localhost:9092 \
     --reassignment-json-file reassign.json \
     --execute
   ```

## Advanced Troubleshooting

### Performance Profiling

#### CPU Profiling

```bash
# Profile Python service
python -m cProfile -o profile.stats service-normalization/app/main.py

# Analyze profile
python -c "
import pstats
p = pstats.Stats('profile.stats')
p.sort_stats('cumulative').print_stats(10)
"

# Use py-spy for live profiling
pip install py-spy
py-spy record -o profile.svg --pid <pid>
```

#### Memory Profiling

```bash
# Use memory_profiler
pip install memory-profiler
python -m memory_profiler service-normalization/app/main.py

# Use pympler for memory analysis
pip install pympler
python -c "
from pympler import tracker
tr = tracker.SummaryTracker()
# ... run code ...
tr.print_diff()
"
```

#### Network Profiling

```bash
# Use tcpdump for network analysis
tcpdump -i any -w capture.pcap port 9092

# Use Wireshark to analyze capture
wireshark capture.pcap

# Use netstat for connection analysis
netstat -tulpn | grep :9092
```

### Log Analysis

#### Structured Log Analysis

```bash
# Parse structured logs
docker-compose logs normalization-service | jq 'select(.level == "ERROR")'

# Search for specific patterns
docker-compose logs normalization-service | grep -E "(ERROR|WARN|FATAL)"

# Analyze log patterns
docker-compose logs normalization-service | \
  awk '{print $4}' | sort | uniq -c | sort -nr
```

#### Log Aggregation

```bash
# Use ELK stack for log aggregation
docker-compose up -d elasticsearch logstash kibana

# Configure log forwarding
# Edit docker-compose.yml to add logging driver
logging:
  driver: "json-file"
  options:
    max-size: "10m"
    max-file: "3"
```

### Debugging Tools

#### Service Debugging

```bash
# Enable debug logging
export DATA_PROC_LOG_LEVEL=debug
docker-compose restart normalization-service

# Use Python debugger
python -m pdb service-normalization/app/main.py

# Use ipdb for enhanced debugging
pip install ipdb
python -c "import ipdb; ipdb.set_trace()"
```

#### Database Debugging

```bash
# Enable ClickHouse query logging
docker-compose exec clickhouse clickhouse-client --query "
  SET log_queries = 1"

# Check ClickHouse query log
docker-compose exec clickhouse clickhouse-client --query "
  SELECT * FROM system.query_log 
  WHERE query_duration_ms > 1000 
  ORDER BY event_time DESC 
  LIMIT 10"

# Enable PostgreSQL query logging
docker-compose exec postgres psql -U user -d db -c "
  SET log_statement = 'all'"
```

#### Kafka Debugging

```bash
# Enable Kafka debug logging
# Edit kafka configuration
log4j.logger.kafka=DEBUG

# Check Kafka metrics
docker-compose exec kafka kafka-run-class kafka.tools.JmxTool \
  --object-name kafka.server:type=BrokerTopicMetrics,name=MessagesInPerSec \
  --jmx-url service:jmx:rmi:///jndi/rmi://localhost:9999/jmxrmi
```

## Emergency Procedures

### Service Recovery

#### Complete Service Restart

```bash
# Stop all services
docker-compose down

# Clean up volumes (if needed)
docker-compose down -v

# Restart services
docker-compose up -d

# Verify service health
curl http://localhost:8080/health
```

#### Data Recovery

```bash
# Restore from backup
docker-compose exec clickhouse clickhouse-backup restore backup_name

# Restore PostgreSQL
docker-compose exec postgres psql -U user -d db < backup.sql

# Restore Redis
docker-compose exec redis redis-cli --rdb /backup/redis-backup.rdb
```

#### Failover Procedures

```bash
# Switch to backup services
docker-compose -f docker-compose.backup.yml up -d

# Update service discovery
kubectl patch service normalization-service \
  -p '{"spec":{"selector":{"version":"backup"}}}'

# Monitor failover
kubectl get pods -n data-processing-prod
```

### Incident Response

#### Service Outage

1. **Assess Impact**: Check service health and error rates
2. **Isolate Issue**: Identify affected components
3. **Implement Fix**: Apply appropriate solution
4. **Verify Recovery**: Confirm service restoration
5. **Post-Mortem**: Document incident and lessons learned

#### Data Loss

1. **Stop Processing**: Prevent further data loss
2. **Assess Damage**: Quantify lost data
3. **Restore Data**: Use backups and reprocessing
4. **Validate Recovery**: Verify data integrity
5. **Update Procedures**: Improve backup and recovery

#### Security Incident

1. **Contain Threat**: Isolate affected systems
2. **Assess Damage**: Determine scope of compromise
3. **Eradicate Threat**: Remove malicious code/access
4. **Recover Systems**: Restore from clean backups
5. **Lessons Learned**: Update security procedures

## Prevention and Best Practices

### Proactive Monitoring

```bash
# Set up alerting rules
# Edit monitoring/alert_rules.yml

# Monitor key metrics
curl http://localhost:9090/api/v1/query?query=up

# Check service dependencies
curl http://localhost:8080/health
```

### Regular Maintenance

```bash
# Update dependencies
pip install -r requirements.txt --upgrade

# Clean up old data
docker-compose exec clickhouse clickhouse-client --query "
  DELETE FROM market_ticks WHERE timestamp < now() - INTERVAL 30 DAY"

# Rotate logs
docker-compose exec kafka kafka-log-dirs \
  --bootstrap-server localhost:9092 \
  --describe
```

### Capacity Planning

```bash
# Monitor resource usage
docker stats

# Check growth trends
curl http://localhost:9090/api/v1/query?query=rate(messages_processed_total[1h])

# Plan scaling
kubectl get hpa -n data-processing-prod
```

## Support and Escalation

### Internal Support

- **Level 1**: Basic troubleshooting and restart procedures
- **Level 2**: Advanced diagnostics and configuration changes
- **Level 3**: Architecture changes and performance optimization

### External Support

- **Community**: GitHub Issues, Slack channel
- **Enterprise**: Direct support channel, SLA guarantees
- **Vendor**: Kafka, ClickHouse, PostgreSQL support

### Escalation Criteria

- **Critical**: Service down, data loss, security breach
- **High**: Performance degradation, error rate > 10%
- **Medium**: Minor issues, configuration changes
- **Low**: Documentation updates, feature requests

## Conclusion

This troubleshooting guide provides comprehensive procedures for diagnosing and resolving issues in the data processing pipeline. Regular monitoring, proactive maintenance, and following best practices can prevent most issues from occurring. When issues do arise, systematic diagnosis and appropriate solutions can minimize impact and restore service quickly.

For additional support and questions, refer to the [API documentation](api.md), [monitoring guide](monitoring.md), and [deployment guide](deployment.md).

