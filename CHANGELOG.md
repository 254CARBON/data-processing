# Changelog

All notable changes to the 254Carbon Data Processing project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project structure and scaffolding
- Shared framework with AsyncService base class
- Kafka consumer and producer abstractions
- ClickHouse, PostgreSQL, and Redis client wrappers
- Structured logging and OpenTelemetry tracing setup
- Error handling with custom exception classes

### Services
- **Normalization Service**: Raw market data normalization with validation
- **Enrichment Service**: Metadata lookup and taxonomy classification
- **Aggregation Service**: OHLC bars and curve calculations (planned)
- **Projection Service**: Cache builders and invalidation logic (planned)

### Infrastructure
- Docker Compose setup for local development
- Database migration scripts for ClickHouse and PostgreSQL
- Service manifests with configuration and scaling guidance
- Makefile with common development tasks
- Pre-commit hooks for code quality
- Configuration templates and examples

### Documentation
- Comprehensive README with architecture overview
- Service-specific documentation
- Database schema documentation
- API documentation (planned)
- Deployment guides (planned)

## [0.1.0] - 2025-01-27

### Added
- Initial release with core data processing pipeline
- Bronze → Silver → Gold → Served data lifecycle
- Event-driven architecture with Kafka
- Multi-tenant support with soft isolation
- Comprehensive observability with metrics, logs, and traces
- Local development environment with all dependencies

### Services
- **Normalization Service (v0.1.0)**
  - Raw market data parsing (MISO, CAISO, ERCOT)
  - Data validation and quality flagging
  - ClickHouse integration for Silver layer
  - Kafka event production for downstream services

- **Enrichment Service (v0.1.0)**
  - Instrument metadata lookup with caching
  - Taxonomy classification with confidence scoring
  - PostgreSQL integration for reference data
  - Redis caching for performance optimization

### Infrastructure
- **ClickHouse**: Analytical data store with partitioning and TTL
- **PostgreSQL**: Reference data and taxonomy storage
- **Redis**: Caching and session storage
- **Kafka**: Event streaming with dead letter queues
- **Docker Compose**: Local development environment

### Data Models
- **Silver Layer**: Normalized tick data with quality flags
- **Gold Layer**: Enriched tick data with metadata and taxonomy
- **Served Layer**: Latest prices and curve snapshots (planned)

### Event Contracts
- Raw market data ingestion events
- Normalized tick events
- Enriched tick events
- Aggregated bar events (planned)
- Curve update events (planned)
- Projection invalidation events (planned)

### Configuration
- Environment-based configuration management
- Service-specific configuration classes
- Docker Compose environment variables
- Logging and metrics configuration

### Development Tools
- Makefile with common tasks
- Pre-commit hooks for code quality
- Docker Compose for local development
- Migration scripts for database setup
- Service manifests for deployment

### Testing
- Unit test framework setup
- Integration test harness (planned)
- Performance testing tools (planned)
- End-to-end testing (planned)

### Monitoring
- Prometheus metrics collection
- Structured logging with correlation IDs
- OpenTelemetry distributed tracing
- Health check endpoints
- Service status monitoring

### Security
- Non-root Docker containers
- Environment variable configuration
- Input validation and sanitization
- Error handling without information leakage

### Performance
- Async/await throughout the codebase
- Connection pooling for databases
- Batch processing for efficiency
- Caching strategies for hot data
- Horizontal scaling support

### Reliability
- Graceful shutdown handling
- Circuit breaker patterns
- Retry logic with exponential backoff
- Dead letter queues for failed messages
- Idempotent operations

### Maintainability
- Modular architecture with clear separation
- Comprehensive error handling
- Structured logging for debugging
- Configuration management
- Documentation and comments

### Scalability
- Horizontal scaling with Kafka partitioning
- Database partitioning strategies
- Caching for performance
- Async processing for throughput
- Resource management and limits

## [0.0.1] - 2025-01-27

### Added
- Initial project setup
- Repository structure
- Basic configuration
- Development environment setup
- Documentation framework

### Infrastructure
- Docker Compose for local development
- Database setup scripts
- Service configuration templates
- Development tools and scripts

### Documentation
- README with project overview
- Architecture documentation
- Service documentation
- Development guidelines
- Deployment instructions

### Development
- Code quality tools setup
- Pre-commit hooks configuration
- Testing framework setup
- CI/CD pipeline setup (planned)
- Monitoring and observability setup

## [0.0.0] - 2025-01-27

### Added
- Project initialization
- Repository creation
- Basic structure setup
- Initial documentation
- Development environment preparation

