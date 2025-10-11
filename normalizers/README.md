# Normalization and Enrichment Pipeline

Data processing pipeline for transforming Bronze → Silver → Gold layers with EDAM flags and other enrichments.

## Overview

The normalization pipeline processes raw market data through multiple stages:

1. **Bronze** → Raw data from connectors (unstructured, minimal validation)
2. **Silver** → Normalized data (structured, validated, typed)
3. **Gold** → Enriched data (business context, relationships, aggregations)

## Architecture

```
Bronze Topics → Normalization → Silver Topics → Enrichment → Gold Tables
```

### Pipeline Components

1. **Normalizers** - Transform Bronze to Silver format
2. **Enrichers** - Add business context to Silver records
3. **Aggregators** - Create summary tables from Gold data

## CAISO OASIS Normalization

### Supported Data Types

- **LMP (Locational Marginal Prices)**
  - `lmp_dam` - Day-Ahead Market LMP
  - `lmp_fmm` - Fifteen-Minute Market LMP
  - `lmp_rtm` - Real-Time Market LMP

- **Ancillary Services**
  - `as_dam` - Day-Ahead Market AS
  - `as_fmm` - Fifteen-Minute Market AS
  - `as_rtm` - Real-Time Market AS

- **Congestion Revenue Rights**
  - `crr` - CRR auction results

### Bronze → Silver Transformation

**LMP Normalization:**
```python
# Bronze record
{
    "timestamp": "2024-01-01T00:00:00Z",
    "node": "TH_NP15_GEN-APND",
    "lmp_usd_per_mwh": "45.67",
    "lmp_energy_usd_per_mwh": "42.10",
    "lmp_congestion_usd_per_mwh": "2.50",
    "lmp_marginal_loss_usd_per_mwh": "1.07",
    "market_run_id": "DAM"
}

# Silver record
{
    "timestamp": "2024-01-01T00:00:00Z",
    "market_run_id": "DAM",
    "node_id": "TH_NP15_GEN-APND",
    "ba_id": "CAISO",
    "lmp_total": 45.67,
    "lmp_energy": 42.10,
    "lmp_congestion": 2.50,
    "lmp_losses": 1.07,
    "data_type": "lmp_dam",
    "source": "OASIS_SingleZip"
}
```

**EDAM Enrichment:**
```python
# Enriched record
{
    "edam_eligible": true,
    "edam_participant": true,
    "edam_hurdle_rate_usd_per_mwh": 2.50,
    "edam_transfer_limit_mw": 1000,
    "edam_region": "NP15",
    "enriched_at": "2024-01-01T01:00:00Z"
}
```

## Gold Layer Schema

### Tables

**CAISO LMP Gold Table:**
- `gold.market.caiso.lmp` - Normalized LMP data with EDAM flags
- `gold.market.caiso.lmp_daily_summary` - Daily LMP aggregations

**CAISO AS Gold Table:**
- `gold.market.caiso.as` - Normalized AS data with EDAM flags
- `gold.market.caiso.as_hourly_summary` - Hourly AS aggregations

**CAISO CRR Gold Table:**
- `gold.market.caiso.crr` - Normalized CRR data
- `gold.market.caiso.crr_daily_summary` - Daily CRR aggregations

### Partitioning Strategy

- **Time-based partitioning** by month (`toYYYYMM(timestamp)`)
- **TTL** of 2 years for cost management
- **Optimized indexes** for common query patterns

## Configuration

### Environment Variables

```bash
# ClickHouse
CLICKHOUSE_URL=http://localhost:8123
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SCHEMA_REGISTRY_URL=http://localhost:8081

# Processing settings
NORMALIZATION_BATCH_SIZE=1000
ENRICHMENT_BATCH_SIZE=500
PROCESSING_TIMEOUT=300
```

### Pipeline Configuration

```python
from data_processing.normalizers.pipeline import NormalizationPipeline

# Initialize pipeline
pipeline = NormalizationPipeline()

# Process Bronze records
result = await pipeline.process_bronze_to_gold(
    bronze_records=bronze_data,
    data_type="lmp_dam"
)

# Check results
if result["status"] == "success":
    print(f"Processed {result['gold_records']} Gold records")
    print(f"Applied {result['enrichment_applied']} enrichments")
else:
    print(f"Pipeline failed: {result['error']}")
```

## Usage Examples

### Batch Processing

```python
# Process multiple data types in parallel
import asyncio

async def process_batch():
    data_types = ["lmp_dam", "as_dam", "crr"]
    tasks = []

    for data_type in data_types:
        # Get Bronze records for this data type
        bronze_records = await get_bronze_records(data_type)

        # Process through pipeline
        task = pipeline.process_bronze_to_gold(bronze_records, data_type)
        tasks.append(task)

    # Wait for all processing to complete
    results = await asyncio.gather(*tasks, return_exceptions=True)

    # Check results
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            print(f"Failed to process {data_types[i]}: {result}")
        elif result["status"] == "success":
            print(f"Successfully processed {data_types[i]}")
        else:
            print(f"Processing failed for {data_types[i]}: {result['error']}")

# Run batch processing
await process_batch()
```

### Real-time Processing

```python
# Process records as they arrive from Bronze topics
async def process_stream():
    # Subscribe to Bronze topics
    bronze_consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        topics=["ingestion.market.caiso.lmp.dam.raw.v1"]
    )

    async for message in bronze_consumer:
        bronze_records = [message.value]

        # Process through pipeline
        result = await pipeline.process_bronze_to_gold(
            bronze_records,
            data_type="lmp_dam"
        )

        if result["status"] == "success":
            # Publish to Gold topics
            await publish_to_gold(result["gold_records"])
        else:
            # Handle error (retry, alert, etc.)
            await handle_processing_error(result)
```

## Monitoring and Quality

### Metrics

- **Normalization Success Rate**: Records successfully normalized
- **Enrichment Coverage**: Records with EDAM flags applied
- **Processing Latency**: Time from Bronze to Gold
- **Data Quality Score**: Validation errors and completeness

### Validation

**Schema Validation:**
- Avro schema compliance
- Required field presence
- Data type validation

**Business Rules:**
- Price reasonableness checks
- EDAM flag consistency
- Temporal ordering validation

### Error Handling

**Retry Logic:**
- Exponential backoff for transient failures
- Dead letter queues for permanent failures
- Circuit breakers for sustained issues

**Alerting:**
- Processing failures
- Data quality violations
- Schema changes detected

## Integration

### Downstream Consumers

**IRP Core:**
- `gold.market.caiso.lmp` → Production-cost model inputs
- `gold.market.caiso.as` → Reserve requirement calculations
- `gold.market.caiso.crr` → Congestion risk analysis

**Analytics Services:**
- Daily summaries for trend analysis
- EDAM participation metrics
- Price volatility calculations

**Reporting:**
- Market price dashboards
- EDAM impact assessments
- Compliance reporting data

### Data Lineage

**Bronze → Silver → Gold:**
1. **Extraction**: Source API → Raw records
2. **Normalization**: Raw → Structured format
3. **Enrichment**: Structured → Business context
4. **Aggregation**: Business → Summary tables
5. **Serving**: Summary → ClickHouse analytics

## Troubleshooting

### Common Issues

1. **Schema Mismatches**: Check Avro schema versions
2. **EDAM Configuration**: Verify EDAM flag mappings
3. **Performance**: Monitor batch sizes and processing times
4. **Data Quality**: Review validation error logs

### Debugging

```python
# Enable debug logging
import structlog

structlog.configure(
    processors=[
        structlog.dev.ConsoleRenderer(),
        structlog.dev.LogRecord
    ],
    wrapper_class=structlog.make_filtering_bound_logger(10),  # DEBUG level
    logger_factory=structlog.PrintLoggerFactory(),
)

# Run single record through pipeline
test_record = {...}
result = await pipeline.process_bronze_to_gold([test_record], "lmp_dam")

# Inspect result
print(f"Normalization errors: {result['normalization_errors']}")
print(f"Enrichment applied: {result['enrichment_applied']}")
```

## Future Enhancements

- **Real-time Streaming**: WebSocket connections for live data
- **Machine Learning**: Automated EDAM flag predictions
- **Multi-region Support**: Expand beyond CAISO/WECC
- **Advanced Analytics**: Statistical outlier detection
- **Data Quality Scoring**: Automated quality assessment
