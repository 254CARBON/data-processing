# Program Metrics Loader

CLI utility for loading IRP/RA/RPS/GHG snapshot datasets into ClickHouse.

## Overview

The program metrics loader consumes CSV snapshots and loads them into ClickHouse `markets.*` tables for analytics services.

## CSV Contract

The loader expects these CSV files in the snapshot directory:

### load_demand.csv
```
timestamp,market,ba,zone,demand_mw,data_source
2024-01-01T00:00:00Z,wecc,CAISO,ZONE1,45000.0,oasis_singlezip
```

### generation_actual.csv
```
timestamp,market,ba,zone,resource_id,resource_type,fuel,output_mw,output_mwh,data_source
2024-01-01T00:00:00Z,wecc,CAISO,ZONE1,SOLAR_FARM_001,solar_pv,solar,1500.0,1500.0,oasis_singlezip
```

### generation_capacity.csv
```
effective_date,market,ba,zone,resource_id,resource_type,fuel,nameplate_mw,ucap_factor,ucap_mw,availability_factor,cost_curve,data_source
2024-01-01,wecc,CAISO,ZONE1,SOLAR_FARM_001,solar_pv,solar,2000.0,0.5,1000.0,0.95,"linear:1000,2000",static_mvp
```

### rec_ledger.csv
```
vintage_year,market,lse,certificate_id,resource_id,mwh,status,retired_year,data_source
2024,wecc,PACIFIC_GAS_ELECTRIC,REC_2024_001,SOLAR_FARM_001,1000.0,available,,synthetic_mvp
```

### emission_factors.csv
```
fuel,scope,kg_co2e_per_mwh,source,effective_date,expires_at
natural_gas,scope1,400.0,epa_egrid,2024-01-01,
```

## Usage

### Basic Usage
```bash
python loader.py --snapshot-dir /path/to/snapshots --clickhouse-url http://localhost:8123 --database markets
```

### Command Line Options
- `--snapshot-dir`: Directory containing CSV snapshots (required)
- `--clickhouse-url`: ClickHouse HTTP URL (default: http://localhost:8123)
- `--database`: Target database name (default: markets)
- `--batch-size`: Insert batch size (default: 5000)

### Example
```bash
python loader.py \
  --snapshot-dir /home/m/254/carbon/analytics/fixtures/program-mvp/wecc \
  --clickhouse-url http://localhost:8123 \
  --database markets
```

## Prerequisites

1. **ClickHouse**: Running and accessible
2. **Schemas**: `markets_program_schemas.sql` applied
3. **CSV Files**: Snapshot directory contains required CSVs

## Data Validation

The loader performs basic validation:
- Required columns present
- Data types compatible
- Non-null constraints respected
- Enum values valid

## Error Handling

- **Missing files**: Skipped with warning
- **Parse errors**: Logged and skipped
- **Insert failures**: Retried with backoff
- **Schema mismatches**: Detailed error messages

## Performance

- **Batch inserts**: Configurable batch size (default: 5000)
- **Parallel processing**: Multiple tables loaded concurrently
- **Memory efficient**: Streaming CSV processing
- **Idempotent**: Safe to re-run on same data

## Monitoring

- **Structured logging**: JSON format with context
- **Progress tracking**: Records loaded per table
- **Error reporting**: Detailed failure information
- **Timing metrics**: Load duration and throughput

## Integration

### Makefile Targets
```bash
make load-wecc    # Load WECC fixtures
make load-miso    # Load MISO fixtures  
make load-ercot   # Load ERCOT fixtures
make load-all     # Load all fixtures
```

### Airflow DAG
```python
from airflow import DAG
from airflow.operators.bash import BashOperator

dag = DAG('program_metrics_loader')

load_task = BashOperator(
    task_id='load_metrics',
    bash_command='python loader.py --snapshot-dir {{ params.snapshot_dir }}',
    dag=dag
)
```

## Troubleshooting

### Common Issues

1. **Connection refused**: Check ClickHouse is running
2. **Schema errors**: Verify DDL applied correctly
3. **CSV format**: Check column headers and data types
4. **Memory issues**: Reduce batch size

### Debug Mode
```bash
LOG_LEVEL=DEBUG python loader.py --snapshot-dir /path/to/snapshots
```

### Validation Queries
```sql
-- Check table counts
SELECT count() FROM markets.load_demand;
SELECT count() FROM markets.generation_actual;
SELECT count() FROM markets.generation_capacity;

-- Verify derived views
SELECT * FROM markets.ra_hourly LIMIT 5;
SELECT * FROM markets.rps_compliance_annual LIMIT 5;
SELECT * FROM markets.ghg_inventory_hourly LIMIT 5;
```

## Development

### Testing
```bash
# Test with fixtures
make test-fixtures

# Validate specific market
python loader.py --snapshot-dir fixtures/program-mvp/wecc
```

### Adding New Tables
1. Update `TABLE_SPECS` in `loader.py`
2. Add corresponding ClickHouse DDL
3. Update CSV contract documentation
4. Add test fixtures

## License

Internal use only.
