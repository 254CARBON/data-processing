# Performance Troubleshooting Runbook

## Overview

This runbook provides detailed procedures for diagnosing and resolving performance issues in the 254Carbon Data Processing Pipeline.

## Performance Metrics Baseline

### Expected Performance Targets

- **Throughput**: 10,000+ messages/second
- **Latency**: P95 < 100ms, P99 < 500ms
- **Error Rate**: < 0.1%
- **CPU Usage**: < 70% average
- **Memory Usage**: < 80% average
- **Database Query Time**: < 50ms average

## Performance Issue Categories

### 1. High Latency Issues

#### Symptoms
- API response times > 100ms
- Database queries taking > 50ms
- Message processing delays
- User complaints about slow performance

#### Diagnosis Steps

```bash
# Check service latency metrics
curl http://normalization-service:9090/metrics | grep duration

# Check database query performance
kubectl exec -it clickhouse-0 -n data-processing -- clickhouse-client \
  --query "SELECT query, query_duration_ms FROM system.query_log WHERE event_date = today() ORDER BY query_duration_ms DESC LIMIT 10"

# Check Kafka consumer lag
kubectl exec -it kafka-0 -n data-processing -- kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group normalization-group \
  --describe

# Check network latency
kubectl exec -it normalization-service-0 -n data-processing -- ping clickhouse
```

#### Resolution Steps

1. **Database Optimization**
   ```bash
   # Check slow queries
   kubectl exec -it clickhouse-0 -n data-processing -- clickhouse-client \
     --query "SELECT query, query_duration_ms, memory_usage FROM system.query_log WHERE query_duration_ms > 1000 ORDER BY query_duration_ms DESC LIMIT 10"
   
   # Add missing indexes
   kubectl exec -it clickhouse-0 -n data-processing -- clickhouse-client \
     --query "ALTER TABLE market_ticks ADD INDEX idx_instrument_timestamp (instrument_id, timestamp) TYPE minmax GRANULARITY 8192"
   ```

2. **Service Scaling**
   ```bash
   # Scale up services
   kubectl scale deployment normalization-service --replicas=4 -n data-processing
   
   # Check HPA status
   kubectl get hpa -n data-processing
   ```

3. **Caching Optimization**
   ```bash
   # Check cache hit rates
   curl http://enrichment-service:9090/metrics | grep cache_hit_rate
   
   # Restart Redis if needed
   kubectl restart deployment redis -n data-processing
   ```

### 2. High CPU Usage

#### Symptoms
- CPU usage > 70% sustained
- Pods being throttled
- Slow response times
- High energy consumption

#### Diagnosis Steps

```bash
# Check CPU usage by pod
kubectl top pods -n data-processing

# Check CPU usage by container
kubectl top pods -n data-processing --containers

# Check CPU limits
kubectl describe pod <pod-name> -n data-processing | grep -A 5 "Limits:"

# Check process CPU usage
kubectl exec -it <pod-name> -n data-processing -- top -p 1
```

#### Resolution Steps

1. **Increase CPU Limits**
   ```yaml
   # Update Helm values
   resources:
     limits:
       cpu: 2000m
     requests:
       cpu: 1000m
   ```

2. **Optimize Code**
   ```bash
   # Check for CPU-intensive operations
   kubectl exec -it <pod-name> -n data-processing -- python -m cProfile -s cumulative app/main.py
   ```

3. **Scale Horizontally**
   ```bash
   # Scale up services
   kubectl scale deployment normalization-service --replicas=6 -n data-processing
   ```

### 3. High Memory Usage

#### Symptoms
- Memory usage > 80% sustained
- Pods being OOMKilled
- Slow performance
- Memory allocation failures

#### Diagnosis Steps

```bash
# Check memory usage
kubectl top pods -n data-processing

# Check memory limits
kubectl describe pod <pod-name> -n data-processing | grep -A 5 "Limits:"

# Check memory allocation
kubectl exec -it <pod-name> -n data-processing -- cat /proc/meminfo

# Check heap usage (Python)
kubectl exec -it <pod-name> -n data-processing -- python -c "
import psutil
import os
process = psutil.Process(os.getpid())
print(f'Memory usage: {process.memory_info().rss / 1024 / 1024:.2f} MB')
"
```

#### Resolution Steps

1. **Increase Memory Limits**
   ```yaml
   # Update Helm values
   resources:
     limits:
       memory: 4Gi
     requests:
       memory: 2Gi
   ```

2. **Optimize Memory Usage**
   ```python
   # Implement memory-efficient processing
   def process_data_in_batches(data, batch_size=1000):
       for i in range(0, len(data), batch_size):
           batch = data[i:i + batch_size]
           yield process_batch(batch)
   ```

3. **Check for Memory Leaks**
   ```bash
   # Monitor memory usage over time
   kubectl exec -it <pod-name> -n data-processing -- watch -n 5 'ps aux | grep python'
   ```

### 4. Database Performance Issues

#### Symptoms
- Slow database queries
- High database CPU usage
- Connection pool exhaustion
- Query timeouts

#### Diagnosis Steps

```bash
# Check ClickHouse performance
kubectl exec -it clickhouse-0 -n data-processing -- clickhouse-client \
  --query "SELECT * FROM system.processes"

# Check PostgreSQL performance
kubectl exec -it postgresql-0 -n data-processing -- psql -U user -d db \
  -c "SELECT * FROM pg_stat_activity WHERE state = 'active'"

# Check connection pool usage
curl http://normalization-service:9090/metrics | grep connection_pool

# Check slow queries
kubectl exec -it clickhouse-0 -n data-processing -- clickhouse-client \
  --query "SELECT query, query_duration_ms, memory_usage FROM system.query_log WHERE event_date = today() AND query_duration_ms > 1000 ORDER BY query_duration_ms DESC LIMIT 10"
```

#### Resolution Steps

1. **Optimize Queries**
   ```sql
   -- Add indexes
   ALTER TABLE market_ticks ADD INDEX idx_instrument_timestamp (instrument_id, timestamp) TYPE minmax GRANULARITY 8192;
   
   -- Optimize materialized views
   CREATE MATERIALIZED VIEW latest_prices_mv
   ENGINE = ReplacingMergeTree(timestamp)
   ORDER BY (instrument_id)
   AS SELECT
       instrument_id,
       argMax(price, timestamp) AS latest_price,
       max(timestamp) AS timestamp
   FROM market_ticks
   GROUP BY instrument_id;
   ```

2. **Scale Database**
   ```bash
   # Scale ClickHouse
   kubectl scale deployment clickhouse --replicas=3 -n data-processing
   
   # Scale PostgreSQL
   kubectl scale deployment postgresql --replicas=2 -n data-processing
   ```

3. **Optimize Connection Pools**
   ```python
   # Update connection pool settings
   DATABASE_CONFIG = {
       'max_connections': 20,
       'min_connections': 5,
       'connection_timeout': 30,
       'idle_timeout': 300
   }
   ```

### 5. Kafka Performance Issues

#### Symptoms
- High consumer lag
- Slow message processing
- Broker performance issues
- Topic partition imbalance

#### Diagnosis Steps

```bash
# Check consumer lag
kubectl exec -it kafka-0 -n data-processing -- kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 \
  --group normalization-group \
  --describe

# Check topic details
kubectl exec -it kafka-0 -n data-processing -- kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic market-data

# Check broker performance
kubectl exec -it kafka-0 -n data-processing -- kafka-log-dirs.sh \
  --bootstrap-server localhost:9092 \
  --describe --json

# Check partition distribution
kubectl exec -it kafka-0 -n data-processing -- kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic market-data | grep Partition
```

#### Resolution Steps

1. **Scale Consumers**
   ```bash
   # Scale up consumer services
   kubectl scale deployment normalization-service --replicas=4 -n data-processing
   ```

2. **Optimize Topic Configuration**
   ```bash
   # Increase partition count
   kubectl exec -it kafka-0 -n data-processing -- kafka-topics.sh \
     --bootstrap-server localhost:9092 \
     --alter --topic market-data --partitions 12
   ```

3. **Optimize Consumer Configuration**
   ```python
   # Update consumer settings
   CONSUMER_CONFIG = {
       'bootstrap_servers': 'kafka:9092',
       'group_id': 'normalization-group',
       'auto_offset_reset': 'latest',
       'enable_auto_commit': True,
       'max_poll_records': 1000,
       'fetch_max_wait_ms': 500,
       'fetch_min_bytes': 1
   }
   ```

### 6. Network Performance Issues

#### Symptoms
- High network latency
- Packet loss
- Connection timeouts
- Slow inter-service communication

#### Diagnosis Steps

```bash
# Check network latency
kubectl exec -it normalization-service-0 -n data-processing -- ping -c 10 clickhouse

# Check network throughput
kubectl exec -it normalization-service-0 -n data-processing -- iperf3 -c clickhouse

# Check network errors
kubectl exec -it normalization-service-0 -n data-processing -- netstat -i

# Check DNS resolution
kubectl exec -it normalization-service-0 -n data-processing -- nslookup clickhouse
```

#### Resolution Steps

1. **Optimize Network Configuration**
   ```yaml
   # Update service configuration
   spec:
     ports:
     - port: 8080
       targetPort: 8080
       protocol: TCP
     sessionAffinity: None
     type: ClusterIP
   ```

2. **Check Network Policies**
   ```bash
   # Review network policies
   kubectl get networkpolicies -n data-processing
   
   # Check policy rules
   kubectl describe networkpolicy default-deny-all -n data-processing
   ```

3. **Optimize Service Discovery**
   ```bash
   # Check service endpoints
   kubectl get endpoints -n data-processing
   
   # Check DNS resolution
   kubectl exec -it normalization-service-0 -n data-processing -- dig clickhouse
   ```

## Performance Monitoring

### Key Metrics to Monitor

1. **Service Metrics**
   - Request duration (P50, P95, P99)
   - Request rate (requests/second)
   - Error rate (errors/second)
   - Active connections

2. **Resource Metrics**
   - CPU usage (percentage)
   - Memory usage (percentage)
   - Disk I/O (IOPS, throughput)
   - Network I/O (packets/second, bytes/second)

3. **Database Metrics**
   - Query duration (P50, P95, P99)
   - Connection pool usage
   - Cache hit rate
   - Lock wait time

4. **Kafka Metrics**
   - Consumer lag
   - Producer throughput
   - Broker CPU usage
   - Topic partition distribution

### Performance Alerts

```yaml
# Prometheus alert rules
groups:
- name: performance
  rules:
  - alert: HighLatency
    expr: histogram_quantile(0.95, rate(request_duration_seconds_bucket[5m])) > 0.1
    for: 2m
    labels:
      severity: warning
    annotations:
      summary: "High latency detected"
      description: "P95 latency is {{ $value }}s"

  - alert: HighCPUUsage
    expr: rate(container_cpu_usage_seconds_total[5m]) > 0.7
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High CPU usage"
      description: "CPU usage is {{ $value }}%"

  - alert: HighMemoryUsage
    expr: container_memory_usage_bytes / container_spec_memory_limit_bytes > 0.8
    for: 5m
    labels:
      severity: warning
    annotations:
      summary: "High memory usage"
      description: "Memory usage is {{ $value }}%"
```

## Performance Optimization Best Practices

### 1. Code Optimization

```python
# Use async/await for I/O operations
async def process_data(data):
    async with aiohttp.ClientSession() as session:
        async with session.post('http://enrichment-service:8081/enrich', json=data) as response:
            return await response.json()

# Use connection pooling
async def get_database_connection():
    return await asyncpg.create_pool(
        host='postgresql',
        port=5432,
        user='user',
        password='password',
        database='db',
        min_size=5,
        max_size=20
    )

# Use batch processing
async def process_batch(data_batch):
    tasks = [process_item(item) for item in data_batch]
    return await asyncio.gather(*tasks)
```

### 2. Database Optimization

```sql
-- Use appropriate indexes
CREATE INDEX idx_market_ticks_instrument_timestamp ON market_ticks (instrument_id, timestamp);

-- Use materialized views for aggregations
CREATE MATERIALIZED VIEW daily_ohlc_mv
ENGINE = SummingMergeTree(open, high, low, close, volume)
ORDER BY (instrument_id, toStartOfDay(timestamp))
AS SELECT
    instrument_id,
    toStartOfDay(timestamp) AS day_start,
    argMin(price, timestamp) AS open,
    max(price) AS high,
    min(price) AS low,
    argMax(price, timestamp) AS close,
    sum(volume) AS volume,
    count() AS trade_count
FROM market_ticks
GROUP BY instrument_id, day_start;

-- Use query hints
SELECT /*+ USE_INDEX(market_ticks, idx_instrument_timestamp) */
    instrument_id, price, timestamp
FROM market_ticks
WHERE instrument_id = 'AAPL' AND timestamp >= '2024-01-01';
```

### 3. Caching Strategies

```python
# Implement Redis caching
import redis
import json

redis_client = redis.Redis(host='redis', port=6379, db=0)

async def get_cached_data(key):
    cached = redis_client.get(key)
    if cached:
        return json.loads(cached)
    return None

async def set_cached_data(key, data, ttl=300):
    redis_client.setex(key, ttl, json.dumps(data))

# Implement in-memory caching
from functools import lru_cache

@lru_cache(maxsize=1000)
def get_instrument_metadata(instrument_id):
    # Expensive operation
    return fetch_from_database(instrument_id)
```

### 4. Resource Optimization

```yaml
# Optimize resource requests and limits
resources:
  requests:
    cpu: 1000m
    memory: 2Gi
  limits:
    cpu: 2000m
    memory: 4Gi

# Use node affinity for better performance
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
      - matchExpressions:
        - key: node-type
          operator: In
          values:
          - high-performance
```

## Performance Testing

### Load Testing

```bash
# Use k6 for load testing
k6 run --vus 100 --duration 5m load-test.js

# Use Locust for distributed load testing
locust -f locustfile.py --host=http://normalization-service:8080 --users=1000 --spawn-rate=100
```

### Performance Regression Testing

```python
# Implement performance regression tests
import pytest
import time

@pytest.mark.performance
def test_api_response_time():
    start_time = time.time()
    response = requests.get('http://normalization-service:8080/health')
    end_time = time.time()
    
    assert response.status_code == 200
    assert (end_time - start_time) < 0.1  # 100ms threshold
```

## Emergency Performance Procedures

### Critical Performance Degradation

1. **Immediate Actions**
   - Scale up all services
   - Check for resource constraints
   - Review error logs
   - Notify stakeholders

2. **Diagnosis**
   - Check system metrics
   - Review application logs
   - Analyze database performance
   - Check network connectivity

3. **Recovery**
   - Implement hot fixes
   - Optimize critical paths
   - Scale resources
   - Monitor recovery

### Performance Incident Response

1. **Detection**
   - Monitor alerts
   - Check dashboards
   - Review user reports
   - Analyze metrics

2. **Response**
   - Assess impact
   - Implement mitigations
   - Communicate status
   - Document actions

3. **Recovery**
   - Verify fixes
   - Monitor stability
   - Update documentation
   - Conduct post-mortem

## Performance Improvement Roadmap

### Short-term (1-2 weeks)
- Optimize database queries
- Implement caching strategies
- Scale critical services
- Fix performance bottlenecks

### Medium-term (1-2 months)
- Implement advanced caching
- Optimize data processing pipelines
- Improve monitoring and alerting
- Conduct performance testing

### Long-term (3-6 months)
- Implement microservices optimization
- Advanced performance monitoring
- Automated performance testing
- Performance optimization automation
