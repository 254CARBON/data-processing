# 254Carbon Data Processing Pipeline

A high-performance, scalable data processing pipeline for real-time market data ingestion, normalization, enrichment, aggregation, and projection.

## 🚀 Quick Start

### Prerequisites

- Python 3.9+
- Docker & Docker Compose
- Git

### Installation

```bash
# Clone the repository
git clone <repository-url>
cd data-processing

# Create virtual environment
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Start infrastructure
docker-compose up -d

# Run tests
python scripts/test_integration_simple.py
```

### First Run

```bash
# Start all services
make start

# Generate sample data
python scripts/generate_sample_data.py --mode continuous

# View monitoring dashboards
open http://localhost:3000  # Grafana (admin/admin)
open http://localhost:9090  # Prometheus
```

## 📋 Overview

This pipeline processes real-time market data through four main stages:

1. **Normalization**: Parse raw market data from various exchanges
2. **Enrichment**: Add metadata and taxonomy classification
3. **Aggregation**: Calculate OHLC bars and curves
4. **Projection**: Build materialized views and cache latest prices

### Production-Ready Features ✅

- **Service Mesh**: Linkerd with automatic mTLS encryption
- **Progressive Delivery**: Automated canary and blue-green deployments with Flagger
- **Performance Optimized**: Database indexes, connection pooling, batch processing, caching
- **High Availability**: Redis Sentinel, pgBouncer, multi-replica deployments with HPA
- **Backup & DR**: Velero with automated backups (hourly, daily, weekly, monthly)
- **Observability**: Prometheus, Grafana, Pyroscope continuous profiling, distributed tracing
- **Chaos Tested**: Validated resilience with automated chaos engineering experiments

### Architecture

```
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│ Raw Market  │───▶│Normalization│───▶│ Enrichment  │───▶│ Aggregation  │
│    Data     │    │   Service   │    │   Service   │    │   Service   │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
                                                              │
                                                              ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Client    │◀───│ Projection  │◀───│   Cache     │◀───│   ClickHouse│
│ Applications│    │   Service   │    │   (Redis)   │    │   Database  │
└─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

## 🏗️ Services

### Normalization Service

Processes raw market data from various exchanges:

- **Input**: Raw market ticks from Kafka topics
- **Processing**: Parse MISO, CAISO, ERCOT, PJM, NYISO formats
- **Output**: Normalized tick data with quality flags
- **Port**: 8080

### Enrichment Service

Adds metadata and taxonomy classification:

- **Input**: Normalized tick data
- **Processing**: Metadata lookup, taxonomy classification
- **Output**: Enriched tick data with commodity/region/product tier
- **Port**: 8081

### Aggregation Service

Calculates OHLC bars and curves:

- **Input**: Enriched tick data
- **Processing**: Time window management, OHLC calculations
- **Output**: Bar data and curve points
- **Port**: 8082

### Projection Service

Builds materialized views and manages cache:

- **Input**: Bar data and curve points
- **Processing**: Materialized view updates, cache invalidation
- **Output**: Latest prices and curve snapshots
- **Port**: 8083

## 🛠️ Development

### Local Development

```bash
# Start all services
make start

# Start specific service
make start-normalization

# View logs
make logs

# Stop all services
make stop

# Clean up
make clean
```

### Testing

```bash
# Run all tests
make test

# Run specific test suite
python scripts/test_integration_simple.py
python scripts/test_monitoring.py

# Run unit tests
pytest tests/unit/

# Run integration tests
pytest tests/integration/
```

### Sample Data Generation

```bash
# Generate continuous data stream
python scripts/generate_sample_data.py --mode continuous

# Generate historical data
python scripts/generate_sample_data.py --mode historical --days 7

# Generate high-volume test data
python scripts/generate_sample_data.py --mode high-volume --count 10000

# Generate malformed data for error testing
python scripts/generate_sample_data.py --mode malformed --count 100
```

## 📊 Monitoring

### Dashboards

- **Overview**: http://localhost:3000/d/overview
- **Normalization**: http://localhost:3000/d/normalization
- **Enrichment**: http://localhost:3000/d/enrichment
- **Aggregation**: http://localhost:3000/d/aggregation
- **Projection**: http://localhost:3000/d/projection

### Key Metrics

- **Throughput**: Messages processed per second
- **Latency**: End-to-end processing time
- **Error Rate**: Failed message percentage
- **Resource Usage**: CPU, memory, disk utilization

### Alerts

- Service health status
- High error rates
- Processing lag
- Resource exhaustion

## 🗄️ Data Storage

### ClickHouse (Analytical Store)

- **Purpose**: High-performance analytical queries
- **Tables**: `market_ticks`, `ohlc_bars`, `curve_points`
- **Features**: Partitioning, TTL, compression

### PostgreSQL (Reference Data)

- **Purpose**: Metadata and taxonomy storage
- **Tables**: `instruments`, `taxonomy`, `metadata`
- **Features**: ACID compliance, complex queries

### Redis (Cache)

- **Purpose**: High-speed data access
- **Keys**: Latest prices, curve snapshots
- **Features**: TTL, pub/sub, clustering

## 🔧 Configuration

### Environment Variables

```bash
# Service Configuration
DATA_PROC_ENV=local
DATA_PROC_LOG_LEVEL=info
DATA_PROC_TRACE_ENABLED=true

# Kafka Configuration
DATA_PROC_KAFKA_BOOTSTRAP=localhost:9092
DATA_PROC_CONSUMER_GROUP=local

# Database Configuration
DATA_PROC_CLICKHOUSE_URL=http://localhost:8123
DATA_PROC_POSTGRES_DSN=postgresql://user:pass@localhost:5432/db
DATA_PROC_REDIS_URL=redis://localhost:6379/0
```

### Service-Specific Configuration

Each service has its own configuration file:

- `service-normalization/app/config.py`
- `service-enrichment/app/config.py`
- `service-aggregation/app/config.py`
- `service-projection/app/config.py`

## 🚨 Troubleshooting

### Common Issues

#### Service Won't Start

```bash
# Check service logs
docker-compose logs service-name

# Check service health
curl http://localhost:8080/health

# Restart service
docker-compose restart service-name
```

#### High Memory Usage

```bash
# Check memory metrics
curl http://localhost:8080/metrics | grep memory

# Restart service
docker-compose restart service-name

# Scale service
docker-compose up -d --scale service-name=2
```

#### Data Processing Errors

```bash
# Check error logs
docker-compose logs service-name | grep ERROR

# Check dead letter queue
kafka-console-consumer --bootstrap-server localhost:9092 --topic dlq

# Reprocess failed messages
python scripts/reprocess_dlq.py
```

### Debug Commands

```bash
# Check service status
make status

# View service metrics
curl http://localhost:8080/metrics

# Check Kafka topics
kafka-topics --bootstrap-server localhost:9092 --list

# Check database connections
docker-compose exec clickhouse clickhouse-client --query "SELECT 1"
docker-compose exec postgres psql -U user -d db -c "SELECT 1"
```

## 📚 Documentation

- [Architecture Overview](docs/architecture.md)
- [API Reference](docs/api.md)
- [Monitoring Guide](docs/monitoring.md)
- [Deployment Guide](docs/deployment.md)
- [Troubleshooting Guide](docs/troubleshooting.md)

## 🤝 Contributing

### Development Workflow

1. Fork the repository
2. Create a feature branch
3. Make changes
4. Run tests
5. Submit a pull request

### Code Standards

- Follow PEP 8 style guidelines
- Write comprehensive tests
- Document public APIs
- Use type hints

### Testing Requirements

- Unit tests for all business logic
- Integration tests for service interactions
- Performance benchmarks
- Chaos testing for resilience

## 📈 Performance

### Benchmarks

- **Throughput**: 10,000+ messages/second per service
- **Latency**: <100ms end-to-end processing
- **Availability**: 99.9% uptime target
- **Scalability**: Horizontal scaling support

### Optimization Tips

- Use appropriate batch sizes
- Tune Kafka consumer settings
- Optimize database queries
- Implement caching strategies

## 🔒 Security

### Authentication

- Service-to-service authentication
- API key management
- Role-based access control

### Data Protection

- Encryption in transit (TLS)
- Encryption at rest
- Data anonymization
- Audit logging

## 📋 Roadmap

### Phase 1: Core Implementation ✅
- [x] Service implementations
- [x] Database schemas
- [x] Integration tests

### Phase 2: Integration & Testing ✅
- [x] End-to-end testing
- [x] Performance benchmarks
- [x] Sample data generation

### Phase 3: Observability & Operations ✅
- [x] Prometheus metrics
- [x] Grafana dashboards
- [x] Alerting rules

### Phase 4: Documentation 🔄
- [x] README and setup guides
- [x] API documentation
- [x] Troubleshooting guides

### Phase 5: Production Hardening ✅
- [x] Service mesh (Linkerd) with mTLS
- [x] Progressive delivery (Flagger) with canary/blue-green deployments
- [x] Performance optimizations (database indexes, connection pooling, batch processing)
- [x] Backup/DR with Velero
- [x] Continuous profiling with Pyroscope
- [x] Enhanced monitoring dashboards
- [x] Chaos engineering framework
- [x] Production deployment automation

## 📞 Support

### Getting Help

- Check the [troubleshooting guide](docs/troubleshooting.md)
- Review [monitoring dashboards](http://localhost:3000)
- Check service logs: `docker-compose logs service-name`

### Reporting Issues

- Use GitHub Issues for bug reports
- Include logs and configuration details
- Provide steps to reproduce

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Built with Python, Docker, Kafka, ClickHouse, PostgreSQL, Redis
- Monitoring with Prometheus and Grafana
- Testing with pytest and Docker Compose

---

**Ready to process market data at scale?** Start with the [Quick Start](#-quick-start) guide above!