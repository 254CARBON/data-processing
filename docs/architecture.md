# Architecture Overview

This document provides a comprehensive overview of the 254Carbon data processing pipeline architecture, including system design, data flow, and technical decisions.

## System Overview

The data processing pipeline is designed as a microservices architecture that processes real-time market data through four distinct stages: normalization, enrichment, aggregation, and projection. Each stage is implemented as an independent service that can be scaled and deployed separately.

## High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           Data Processing Pipeline                              │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │ Raw Market  │───▶│Normalization│───▶│ Enrichment  │───▶│ Aggregation  │     │
│  │    Data     │    │   Service   │    │   Service   │    │   Service   │     │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘     │
│         │                   │                   │                   │         │
│         │                   │                   │                   │         │
│         ▼                   ▼                   ▼                   ▼         │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │   Kafka     │    │   Kafka     │    │   Kafka     │    │   Kafka     │     │
│  │  Topics     │    │  Topics     │    │  Topics     │    │  Topics     │     │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘     │
│                                                                                 │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐     │
│  │   Client    │◀───│ Projection  │◀───│   Cache     │◀───│   ClickHouse│     │
│  │Applications │    │   Service   │    │   (Redis)   │    │   Database  │     │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘     │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

### 1. Ingestion Layer

**Purpose**: Receive raw market data from various exchanges

**Components**:
- Kafka topics for raw market data
- Message envelope with metadata
- Dead letter queue for failed messages

**Data Format**:
```json
{
  "envelope": {
    "event_type": "ingestion.market.raw.v1",
    "event_id": "uuid",
    "timestamp": "2024-01-01T00:00:00Z",
    "tenant_id": "tenant_1",
    "source": "ingestion.miso",
    "version": "1.0"
  },
  "payload": {
    "market": "MISO",
    "instrument_id": "NG_HH_BALMO",
    "raw_data": {
      "timestamp": "2024-01-01T00:00:00Z",
      "price": 120.50,
      "volume": 1000,
      "bid": 120.45,
      "ask": 120.55
    }
  }
}
```

### 2. Normalization Service

**Purpose**: Parse and normalize raw market data from different exchanges

**Key Features**:
- Multi-exchange support (MISO, CAISO, ERCOT, PJM, NYISO)
- Data validation and quality flagging
- Error handling and dead letter queue
- Metrics collection

**Processing Logic**:
1. Receive raw market data from Kafka
2. Parse exchange-specific format
3. Validate data quality
4. Add quality flags
5. Emit normalized tick data

**Output Format**:
```json
{
  "envelope": {
    "event_type": "normalized.market.ticks.v1",
    "event_id": "uuid",
    "timestamp": "2024-01-01T00:00:00Z",
    "tenant_id": "tenant_1",
    "source": "normalization-service"
  },
  "payload": {
    "instrument_id": "NG_HH_BALMO",
    "timestamp": "2024-01-01T00:00:00Z",
    "price": 120.50,
    "volume": 1000,
    "quality_flags": ["VALID"],
    "metadata": {
      "market": "miso",
      "node": "MISO_NODE_001",
      "zone": "MISO_ZONE_A"
    }
  }
}
```

### 3. Enrichment Service

**Purpose**: Add metadata and taxonomy classification to normalized data

**Key Features**:
- Metadata lookup from PostgreSQL
- Taxonomy classification (commodity, region, product tier)
- Redis caching for performance
- Confidence scoring

**Processing Logic**:
1. Receive normalized tick data
2. Lookup instrument metadata
3. Classify taxonomy
4. Add enrichment metadata
5. Emit enriched tick data

**Data Sources**:
- **PostgreSQL**: Instrument metadata, taxonomy rules
- **Redis**: Cached metadata for performance

### 4. Aggregation Service

**Purpose**: Calculate OHLC bars and curves from enriched tick data

**Key Features**:
- Time window management
- OHLC bar calculations
- Curve point generation
- Basis and spread calculations

**Processing Logic**:
1. Receive enriched tick data
2. Group ticks by time windows
3. Calculate OHLC values
4. Generate curve points
5. Store results in ClickHouse

**Time Windows**:
- 1-minute bars
- 5-minute bars
- 1-hour bars
- Daily bars

### 5. Projection Service

**Purpose**: Build materialized views and manage cache for fast access

**Key Features**:
- Materialized view updates
- Cache invalidation
- Latest price projections
- Curve snapshots

**Processing Logic**:
1. Receive bar data and curve points
2. Update materialized views
3. Invalidate relevant cache entries
4. Build latest price projections
5. Generate curve snapshots

## Technology Stack

### Core Technologies

- **Python 3.9+**: Primary programming language
- **aiohttp**: Asynchronous HTTP framework
- **asyncio**: Asynchronous programming support

### Message Streaming

- **Apache Kafka**: Message broker and event streaming
- **confluent-kafka**: Python Kafka client
- **Avro**: Schema serialization (planned)

### Data Storage

- **ClickHouse**: Analytical database for time-series data
- **PostgreSQL**: Relational database for reference data
- **Redis**: In-memory cache for high-speed access

### Infrastructure

- **Docker**: Containerization
- **Docker Compose**: Local development orchestration
- **Kubernetes**: Production orchestration (planned)

### Monitoring & Observability

- **Prometheus**: Metrics collection and storage
- **Grafana**: Visualization and dashboards
- **OpenTelemetry**: Distributed tracing (planned)
- **Node Exporter**: System metrics
- **cAdvisor**: Container metrics

## Service Architecture

### AsyncService Base Class

All services inherit from a common `AsyncService` base class that provides:

- HTTP server with health checks
- Metrics collection
- Graceful shutdown
- Kafka integration
- Configuration management

```python
class AsyncService(ABC):
    def __init__(self, config: ServiceConfig):
        self.config = config
        self.metrics = MetricsCollector(self.config.service_name)
        self.health_checker = HealthChecker(self.config)
        
    async def startup(self) -> None:
        # Service-specific startup logic
        
    async def shutdown(self) -> None:
        # Service-specific shutdown logic
```

### Configuration Management

Each service has its own configuration class:

```python
class ServiceConfig:
    def __init__(self):
        self.service_name = "service-name"
        self.environment = os.getenv("DATA_PROC_ENV", "local")
        self.log_level = os.getenv("DATA_PROC_LOG_LEVEL", "info")
        self.kafka_bootstrap_servers = os.getenv("DATA_PROC_KAFKA_BOOTSTRAP", "localhost:9092")
        # ... other configuration
```

### Error Handling

Consistent error handling across all services:

```python
class DataProcessingError(Exception):
    def __init__(self, message: str, stage: str, context: ErrorContext):
        self.message = message
        self.stage = stage
        self.context = context
        super().__init__(message)
```

## Data Models

### Core Data Types

#### TickData
```python
@dataclass
class TickData:
    instrument_id: str
    timestamp: datetime
    price: float
    volume: float
    quality_flags: List[QualityFlag]
    tenant_id: str
    source_id: str
    metadata: Dict[str, Any]
```

#### BarData
```python
@dataclass
class BarData:
    instrument_id: str
    interval: str
    interval_start: datetime
    open_price: float
    high_price: float
    low_price: float
    close_price: float
    volume: float
    trade_count: int
    tenant_id: str
    metadata: Dict[str, Any]
```

#### QualityFlag
```python
class QualityFlag(Enum):
    VALID = "valid"
    PRICE_NEGATIVE = "price_negative"
    OUT_OF_RANGE = "out_of_range"
    VOLUME_SPIKE = "volume_spike"
    LATE_ARRIVAL = "late_arrival"
    MISSING_METADATA = "missing_metadata"
```

## Scalability Design

### Horizontal Scaling

Each service can be scaled independently:

```yaml
# docker-compose.yml
services:
  normalization-service:
    deploy:
      replicas: 3
  enrichment-service:
    deploy:
      replicas: 2
  aggregation-service:
    deploy:
      replicas: 4
  projection-service:
    deploy:
      replicas: 2
```

### Load Balancing

- Kafka partitions for parallel processing
- Round-robin service instances
- Database connection pooling

### Performance Optimization

- Asynchronous processing
- Batch operations
- Connection pooling
- Caching strategies

## Security Architecture

### Network Security

- Service-to-service communication over internal network
- TLS encryption for external communication
- Network policies for access control

### Authentication & Authorization

- API key authentication
- Role-based access control
- Service mesh integration (planned)

### Data Protection

- Encryption at rest
- Encryption in transit
- Data anonymization
- Audit logging

## Deployment Architecture

### Local Development

```bash
# Start all services
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

### Production Deployment

```yaml
# kubernetes.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: normalization-service
spec:
  replicas: 3
  selector:
    matchLabels:
      app: normalization-service
  template:
    metadata:
      labels:
        app: normalization-service
    spec:
      containers:
      - name: normalization-service
        image: normalization-service:latest
        ports:
        - containerPort: 8080
        env:
        - name: DATA_PROC_ENV
          value: "production"
```

## Monitoring Architecture

### Metrics Collection

- **Prometheus**: Scrapes metrics from all services
- **Grafana**: Visualizes metrics and dashboards
- **Alert Manager**: Handles alert routing and notifications

### Key Metrics

- **Service Health**: Up/down status, health checks
- **Performance**: Latency, throughput, error rates
- **Business**: Messages processed, data quality
- **Infrastructure**: CPU, memory, disk usage

### Alerting Strategy

- **Critical**: Service down, data loss
- **Warning**: High latency, error rates
- **Info**: Performance degradation, capacity issues

## Data Quality & Governance

### Data Quality Checks

- Price validation (positive, reasonable ranges)
- Volume validation (non-negative, reasonable ranges)
- Timestamp validation (not in future, not too old)
- Completeness checks (required fields present)

### Data Lineage

- Track data flow from source to destination
- Audit trail for data transformations
- Version control for schema changes

### Compliance

- Data retention policies
- Privacy regulations (GDPR, CCPA)
- Financial regulations (SOX, MiFID II)

## Future Enhancements

### Planned Features

1. **Schema Registry**: Avro schema management
2. **Multi-tenancy**: Tenant isolation and resource quotas
3. **Advanced Analytics**: Machine learning, anomaly detection
4. **Real-time Dashboards**: WebSocket-based live updates
5. **API Gateway**: Centralized API management

### Performance Improvements

1. **Stream Processing**: Apache Flink integration
2. **GPU Acceleration**: CUDA support for calculations
3. **Edge Computing**: Distributed processing nodes
4. **Caching Layers**: Multi-level caching strategy

## Conclusion

The 254Carbon data processing pipeline is designed for high performance, scalability, and reliability. The microservices architecture allows for independent scaling and deployment, while the shared framework ensures consistency across services. The comprehensive monitoring and observability stack provides visibility into system performance and health.

The architecture supports both current requirements and future enhancements, making it a solid foundation for processing real-time market data at scale.
