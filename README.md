````markdown name=README.md
# 254Carbon Data Processing (`254carbon-data-processing`)

> Domain pipeline services that transform raw inbound market data into enriched, aggregated, and query‑ready serving layers.  
> This repository hosts deployable microservices focused on the **Bronze → Silver → Gold → Served** lifecycle and cross‑cut event propagation.

---

## Table of Contents
1. Purpose & Scope  
2. Data Lifecycle Overview  
3. Services (Normalization, Enrichment, Aggregation, Projection)  
4. Architecture & Flow Diagrams  
5. Repository Structure  
6. Event Contracts & Topics  
7. Storage Model (ClickHouse / Postgres / Redis)  
8. Caching & Materialization Strategy  
9. Configuration & Environment Variables  
10. Multi‑Tenancy (Soft Isolation)  
11. Error Handling & Data Quality  
12. Observability (Metrics, Logs, Traces)  
13. Performance Objectives & SLO Drafts  
14. Deployment & Scaling Guidance  
15. Local Development Workflow  
16. Testing Strategy  
17. Security & Access Control  
18. Backfill & Reprocessing Strategy  
19. Schema Evolution & Compatibility  
20. AI Agent Integration  
21. Contribution Workflow  
22. Roadmap & Future Enhancements  
23. Troubleshooting Guide  
24. Glossary  
25. License / Ownership  

---

## 1. Purpose & Scope

This repo is responsible for transforming raw ingestion outputs (Avro/JSON events or staged raw tables) into high‑quality, query‑efficient datasets and downstream events:

| Layer | Description |
|-------|-------------|
| Bronze | Raw connector output (opaque; minimal parsing) – emitted upstream (not owned here) |
| Silver | Cleaned + normalized core schema (instrument, timestamp, price, quantity, quality flags) |
| Gold | Domain‑enriched + aggregated constructs (curves, OHLC bars, forward ladders, basis differentials) |
| Served | Denormalized / projection views optimized for the API Gateway & Streaming Service |

Non‑goals:
- Direct user access (done via 254carbon-access)
- ML model training orchestration (254carbon-ml)
- Ingestion scheduling (254carbon-ingestion)

---

## 2. Data Lifecycle Overview

```
        (Connectors)             (This Repo)                          (Consumers)
 Raw Events ->  normalized.market.ticks.v1  -> enrichment events -> aggregation events -> served read models
    |                  |                        |                         |               |
    |                  v                        v                         v               v
 Bronze Tables --> Silver Tables ---------> Gold Tables ----------> Served / Projections --> API / Streaming / Analytics / ML
```

Key Transform Stages:
1. Normalization Service: Validate & standardize tick records → emits `normalized.market.ticks.v1`.
2. Enrichment Service: Adds taxonomy, instrument metadata, region, commodity classification.
3. Aggregation Service: Computes bars (5m/1h/day), curve slices, rolling stats, forward ladders.
4. Projection Service: Materializes low‑latency views & caches (e.g., “latest price by instrument”, “active curve snapshot”).

---

## 3. Services

| Service | Port (Default) | Responsibility | Scaling | Statefulness |
|---------|----------------|----------------|---------|--------------|
| Normalization Service | 8200 | Consume raw topics / parse / validate / output normalized events & Silver tables | Horiz (Kafka partition scaling) | Stateless (idempotent) |
| Enrichment Service | 8201 | Lookup + attach metadata taxonomy; produce enriched ticks; maintain dimension caches | Horiz | Semi (metadata cache) |
| Aggregation Service | 8202 | Compute OHLC bars, forward curves, spreads, basis, rolling metrics | Horiz w/ partitioning by instrument groups | Mixed (batch windows) |
| Projection Service | 8203 | Build/refresh Served projections & hot caches (Redis + ClickHouse MV) | Horiz (with TTL + invalidation) | Materialization windows |

Each service defines a `service-manifest.yaml` with metadata consumed by the platform meta index.

---

## 4. Architecture & Flow Diagrams

### Event & Table Flow

```
         ingestion.miso.raw.v1 / ingestion.caiso.raw.v1 (Bronze Topics)
                                |
                                v
                       [Normalization Service]
                                |
                 normalized.market.ticks.v1 (Silver Event)
                                |
                                v
                       [Enrichment Service]
                                |
                 enriched.market.ticks.v1 (Gold Event)
                                |
                                +--> Gold Tables (enriched_ticks, taxonomy joins)
                                |
                                v
                      [Aggregation Service]
                                |
          aggregated.market.bars.5m.v1 / pricing.curve.updates.v1
                                |
                                v
                      [Projection Service]
                                |
                 served.market.latest_prices.v1 (Internal events)
                                |
                                v
                      API / Streaming / ML Consumers
```

### Control & Invalidations

```
 Entitlements Changes -----> (future) invalidation events (affect projections if access filters)
 Instrument Updates -------> enrichment cache refresh events
 Curve Recompute Requests -> aggregation jobs -> projection invalidations -> served refresh
```

---

## 5. Repository Structure

```
/
  service-normalization/
    app/
      consumers/
      processors/
      validation/
      output/
    tests/
    service-manifest.yaml
  service-enrichment/
    app/
      lookup/
      taxonomy/
      cache/
      processors/
    tests/
    service-manifest.yaml
  service-aggregation/
    app/
      bars/
      curves/
      calculators/
      schedulers/
      jobs/
    tests/
    service-manifest.yaml
  service-projection/
    app/
      builders/
      invalidation/
      refresh/
      apis/
    tests/
    service-manifest.yaml
  shared/
    schemas/
    lib/
  specs.lock.json
  scripts/
    run_local_consumer.py
    backfill_window.py
    rebuild_projection.py
    generate_manifest_index.py
  migrations/
    clickhouse/
      001_init_silver.sql
      002_enriched_indexes.sql
      003_bars_schema.sql
      004_curves_schema.sql
      005_served_views.sql
    postgres/
      001_taxonomy_tables.sql
      002_dimension_indexes.sql
  config/
    default.env.example
    logging.yaml
  .agent/
    context.yaml
  Makefile
  README.md
  CHANGELOG.md
  docs/
    DATA_LAYERS.md
    ENRICHMENT_MODEL.md
    AGGREGATION_METHODS.md
    PROJECTION_STRATEGY.md
    BACKFILL_GUIDE.md
    SCHEMA_EVOLUTION.md
```

---

## 6. Event Contracts & Topics

| Stage | Topic | Description |
|-------|-------|-------------|
| Raw (upstream) | ingestion.<market>.raw.v1 | Connector output (Bronze) |
| Normalized | normalized.market.ticks.v1 | Canonical tick schema |
| Enriched | enriched.market.ticks.v1 | Added taxonomy, region, commodity attributes |
| Aggregated | aggregated.market.bars.5m.v1 | OHLC 5m bars |
| Aggregated | aggregated.market.bars.1h.v1 | Hourly bars |
| Curves | pricing.curve.updates.v1 | Updated forward curve segments |
| Served (internal) | served.market.latest_prices.v1 | Snapshot updates for cache invalidation |
| Invalidations | projection.invalidate.instrument.v1 | Triggers served rebuild |
| Backfill Commands | processing.backfill.request.v1 | Reprocess historical window |
| Status / Telemetry | processing.job.status.v1 | Update on long-running batch jobs |

All schema files reside in `254carbon-specs` (pinned via `specs.lock.json` here).

---

## 7. Storage Model

### ClickHouse (Primary Analytical Store)

Tables (example naming):

| Table | Layer | Notes |
|-------|-------|------|
| bronze_ticks_miso (upstream) | Bronze | May exist outside repo scope |
| silver_ticks | Silver | Normalized schema (instrument_id, ts, price, volume, flags) |
| enriched_ticks | Gold | Joins taxonomy & metadata |
| bars_5m | Gold | Aggregated OHLC (5-min) |
| bars_1h | Gold | Aggregated OHLC (hour) |
| curves_base | Gold | Raw curve point sets |
| curves_computed | Gold | Derived / interpolated results |
| served_latest | Served | Last known price per instrument |
| served_curve_snapshots | Served | Last active curve snapshot per curve_id |
| metrics_processing | Internal | Processing latency & batch stats |

Partitioning & Order:
- Ticks partitioned by `toYYYYMMDD(ts)`; order by (instrument_id, ts).
- Bars partitioned by month; order by (instrument_id, interval_start).
- Curves partitioned by `as_of_date`.

TTL Examples (configurable):
- Bronze (if mirrored): 30–90 days.
- Silver: 365 days.
- Gold Aggregates: 3 years.
- Served: TTL doesn’t apply (stores only latest or small sets).

### PostgreSQL (Reference & Dimension)
- taxonomy_instruments
- taxonomy_regions
- instrument_aliases
- curve_metadata

### Redis
- Latest tick cache (instrument_id → value)
- Projection invalidation flags
- Short-lived enrichment lookups (instrument metadata TTL 60–300s)

---

## 8. Caching & Materialization Strategy

| Layer | Mechanism | Purpose |
|-------|-----------|---------|
| Normalization | In-memory schema cache | Avoid frequent disk/schema loads |
| Enrichment | Redis + local LRU | Accelerate instrument & taxonomy joins |
| Aggregation | Intermediate in-memory window store | Efficient rollups before flush |
| Projection | Redis + ClickHouse MV | Low-latency “last value” + pre-joined views |

Projection Refresh Triggers:
- New tick for instrument with stale served_latest entry
- Curve recompute event
- Backfill completion event (rebuild impacted segments)
- Explicit invalidation message

Consistency:
- Served caches are eventually consistent (< few seconds).
- Consumers needing strong consistency can force direct ClickHouse read.

---

## 9. Configuration & Environment Variables

Common prefix: `DATA_PROC_`

| Variable | Service(s) | Description | Example |
|----------|------------|-------------|---------|
| DATA_PROC_ENV | all | Environment name | local |
| DATA_PROC_KAFKA_BOOTSTRAP | all | Kafka broker addresses | kafka:9092 |
| DATA_PROC_CLICKHOUSE_URL | all | ClickHouse HTTP endpoint | http://clickhouse:8123 |
| DATA_PROC_POSTGRES_DSN | enrichment | Taxonomy reference DB | postgres://... |
| DATA_PROC_REDIS_URL | enrichment/projection | Cache | redis://redis:6379/0 |
| DATA_PROC_CONSUMER_GROUP | normalization | Kafka group ID | norm-svc-1 |
| DATA_PROC_MAX_BATCH_SIZE | aggregation | Batch size for rollups | 5000 |
| DATA_PROC_BAR_INTERVALS | aggregation | Enabled intervals | 5m,1h |
| DATA_PROC_CURVE_REBUILD_THREADS | aggregation | Parallelism | 4 |
| DATA_PROC_PROJECTION_REFRESH_INTERVAL | projection | Background sweep seconds | 10 |
| DATA_PROC_TRACE_ENABLED | all | OTel enable toggle | true |
| DATA_PROC_LOG_LEVEL | all | Log verbosity | info |

Service-specific `.env.example` lives under each service `config/`.

---

## 10. Multi‑Tenancy (Soft Isolation)

All persisted & emitted entities include `tenant_id` (string).
- If not provided upstream, default = "default".
- Projection Service segregates caches per tenant prefix: `tenant:{tenant_id}:latest:{instrument_id}`.
- Queries can be filtered on ClickHouse via `PREWHERE tenant_id = ?`.
- No physical partitioning initially; later: per-tenant partitions or separate clusters if needed.

---

## 11. Error Handling & Data Quality

| Aspect | Strategy |
|--------|----------|
| Schema Violations | Reject event, send to DLQ topic `processing.deadletter.normalization.v1`, increment metric |
| Out-of-Range Values | Flag via `quality_flags` (e.g., "PRICE_NEGATIVE", "VOLUME_SPIKE") |
| Duplicate Ticks | Dedup by composite (instrument_id, ts, source_id) using ClickHouse replacing merge tree or before insert LRU |
| Late Data | Accept with timestamp; bars recomputation triggered if window still “open” (sliding watermark) |
| Missing Instrument Metadata | Enrichment marks `quality_flags += MISSING_METADATA`; retains record |
| Aggregation Inconsistency | Recompute job scheduled; differences logged; optionally issue invalidation event |
| Projection Drift | Periodic reconciliation compares served_latest vs max(ts) in silver_ticks |

Data Quality Metrics:
- invalid_records_total
- enrichment_metadata_miss_total
- late_arrival_seconds_bucket
- recompute_trigger_count

---

## 12. Observability

Metrics (Prometheus format – examples):

| Metric | Labels | Description |
|--------|--------|-------------|
| normalization_records_processed_total | market, tenant_id | Count of processed raw records |
| normalization_processing_latency_ms_bucket | market | Time from raw ingest to normalized emit |
| enrichment_cache_hit_ratio | type | Metadata cache effectiveness |
| aggregation_bar_build_duration_ms_bucket | interval | Latency for bar computation |
| projection_refresh_duration_ms_bucket | projection | Time to rebuild projection |
| projection_invalidations_total | reason | Invalidation events count |
| processing_backfill_jobs_total | status | Backfill outcome counts |

Tracing:
- Span naming:
  - `normalization.consume`
  - `enrichment.lookup`
  - `aggregation.bar.compute`
  - `projection.rebuild`
- Trace attributes: instrument_id (hashed if large set), tenant_id, interval, curve_id.

Logging:
- Structured JSON:
```
{"ts":"...","service":"aggregation","interval":"5m","batch_size":1200,"duration_ms":84,"status":"success"}
```
- Error logs include `trace_id`.

---

## 13. Performance Objectives & SLO (Draft)

| Metric | Target |
|--------|--------|
| Normalization end-to-end latency (P95) | < 2s from raw event |
| Enrichment latency per record (P95) | < 15ms |
| Bar computation (5m window close) | < 30s after window end |
| Projection refresh lag (latest price) | < 3s |
| Curve recompute (standard size) | < 5s |
| Backfill throughput | 50k ticks / min (baseline local) |

Error Budget (initial conceptual):
- Normalization availability: 99.5%
- Projection correctness (drift > 5s): < 1% of minutes per day

---

## 14. Deployment & Scaling Guidance

| Service | Horizontal Driver | Vertical Driver | Notes |
|---------|-------------------|----------------|-------|
| Normalization | Kafka partition count | parse complexity | Start 2 replicas |
| Enrichment | Throughput; cache eviction | metadata join CPU | Scale read-heavy; ensure warm caches |
| Aggregation | #instruments × intervals | memory for holding partial windows | Partition by hash(instrument_id) |
| Projection | number of projections & invalidations | reactive rebuild CPU | Prioritize low-latency I/O path |

Kubernetes:
- Use resource requests to avoid CPU starvation.
- Potentially isolate Aggregation on nodes with more CPU.

---

## 15. Local Development Workflow

### Start Single Service (Normalization)
```bash
cd service-normalization
uvicorn app.main:app --reload --port 8200
```

### Simulate Raw Event Ingestion
```bash
python scripts/run_local_consumer.py \
  --topic ingestion.miso.raw.v1 \
  --sample samples/miso_raw.json
```

### Rebuild a Projection
```bash
python scripts/rebuild_projection.py --name latest_prices --since "2025-10-01T00:00:00Z"
```

### Backfill a Window
```bash
python scripts/backfill_window.py --from 2025-09-01 --to 2025-09-07 --instrument NG_HH_BALMO
```

---

## 16. Testing Strategy

| Layer | Tests | Tools |
|-------|-------|-------|
| Unit | Parsers, validators, calculators | pytest |
| Contract | Event schema compliance | Avro schema test harness |
| Integration | Fake Kafka + ClickHouse test container | docker-compose / testcontainers |
| Performance (select) | Micro-bench for bar build, enrichment cache hit ratio | pytest-benchmark |
| Backfill correctness | Diff expected vs recomputed aggregates | custom harness |
| Idempotency | Replay test streams | snapshot assertions |

Run:
```bash
make test
make coverage
```

---

## 17. Security & Access Control

- Services assume upstream JWT already verified; optionally re-validate if directly invoked.
- No PII; treat all data as non-sensitive market data.
- Least-privilege DB credentials (separate ClickHouse users: read vs write).
- Avoid dynamic code execution; whitelist enrichment metadata sources.
- (Future) mTLS between internal services.

---

## 18. Backfill & Reprocessing Strategy

Use events or CLI:
- `processing.backfill.request.v1` triggers targeted historical recompute.
- Backfill pipeline steps:
  1. Extract range from Silver (ticks)
  2. Re-run enrichment (optionally skip if metadata unchanged)
  3. Regenerate bars / curves
  4. Fire projection invalidations
- Throttle concurrency (avoid cluster overload).
- Track job status with `processing.job.status.v1`.

Idempotency:
- Aggregates written with (instrument_id, interval_start) primary key semantics (overwrite safe).

---

## 19. Schema Evolution & Compatibility

- Normalized tick schema: additive fields allowed (MINOR).
- Breaking changes → new topic with `.v2` suffix.
- Aggregation output: maintain stable dimension naming; new metrics additive.
- Migration path:
  1. Introduce new topic / table
  2. Dual-write if needed
  3. Consumer migration
  4. Deprecate old topic (announce via doc + event `processing.deprecate.notice.v1`)

References documented in `docs/SCHEMA_EVOLUTION.md`.

---

## 20. AI Agent Integration

`.agent/context.yaml` includes:
```yaml
repo: 254carbon-data-processing
services:
  - normalization
  - enrichment
  - aggregation
  - projection
contracts: specs.lock.json
rules:
  - Do not import code across services except via shared/ or events
  - Preserve envelope fields in events
  - Avoid adding new event fields without updating specs repo
quality:
  coverage_target: 70
  lint: ruff
```

Agents can:
- Optimize performance hotspots
- Add new aggregation intervals
- Enhance projection invalidation logic

Agents must not:
- Mutate schemas ad hoc
- Embed business licensing logic

---

## 21. Contribution Workflow

1. If contract change: update in `254carbon-specs` → PR → merge.
2. Sync `specs.lock.json`:
   ```bash
   make specs-sync
   ```
3. Implement changes (service-specific).
4. Add/Adjust tests.
5. Run validations:
   ```bash
   make validate
   ```
6. Commit with conventional prefix:
   - `feat(aggregation): add 15m bar support`
   - `fix(normalization): handle missing volume`
7. Open PR; CI runs:
   - Lint / Tests / Coverage
   - Schema compliance
   - Manifest validation
   - (Future) performance regression guard

---

## 22. Roadmap & Future Enhancements

| Milestone | Description | Status |
|-----------|-------------|--------|
| M1 | Baseline services & event flow | In progress |
| M2 | Adaptive aggregation windowing (late data) | Planned |
| M3 | Multi-tenancy filters in projections | Planned |
| M4 | Backfill orchestration via Temporal (or job orchestrator) | Future |
| M5 | Curve interpolation methods plugin system | Future |
| M6 | Real-time incremental forward curve updates (delta mode) | Future |
| M7 | Data lineage tracking per aggregate | Future |
| M8 | Query-time semantic enrichment (embedding hints) | Future |
| M9 | Automatic anomaly detection on tick streams | Future |

---

## 23. Troubleshooting Guide

| Issue | Symptom | Root Causes | Diagnostics | Resolution |
|-------|---------|-------------|-------------|------------|
| No normalized output | Silver tables empty | Kafka lag, schema parse failure | Check consumer logs; `kafka-consumer-groups` | Restart consumer; validate schema version |
| High enrichment latency | P95 > 50ms | Cache misses / DB slow | Redis metrics; Postgres EXPLAIN | Warm cache; add composite indexes |
| Bar gaps | Missing intervals | Late tick ordering / clock skew | Compare raw vs bars_5m gaps | Trigger targeted recompute |
| Projection stale | Latest not updating | Invalidation not fired | Check projection_invalidations_total | Rebuild: `rebuild_projection.py` |
| Curve drift mismatch | Curve different from expected | Input tick window mismatch | Query curves_base vs curves_computed | Force recompute + compare snapshots |
| Memory spikes aggregation | OOM in pods | Large batch buffer not flushed | Pod logs; memory profiler | Reduce batch size env var |
| Backfill stuck | Job never completes | Concurrency bottleneck | processing.job.status logs | Restart job with narrower window |

---

## 24. Glossary

| Term | Definition |
|------|------------|
| Bronze | Raw ingestion layer (untrusted, unnormalized) |
| Silver | Clean canonical core schema |
| Gold | Enriched & aggregated business-value data |
| Served | Final projection layer optimized for consumption |
| Invalidation | Trigger that causes a projection refresh |
| Backfill | Historical recomputation for accuracy or new logic |
| Forward Curve | Price values across forward time buckets (e.g., months) |
| OHLC Bar | Open/High/Low/Close aggregated interval snapshot |
| Taxonomy | Classification metadata (commodity, region, product tier) |

---

## 25. License / Ownership

Internal repository until public data APIs formalized.  
Ownership: Platform Data Processing domain (single developer + AI agents).  
License: To be defined (likely Apache 2.0 for generic components once externalized).

---

## Quick Commands

```bash
# Install dependencies (if unified Python toolchain)
make install

# Run normalization service locally
make run SERVICE=normalization

# Run tests for aggregation
make test SERVICE=aggregation

# Sync specifications
make specs-sync

# Backfill a week for instrument NG_HH_BALMO
python scripts/backfill_window.py --from 2025-09-01 --to 2025-09-07 --instrument NG_HH_BALMO

# Rebuild latest price projection
python scripts/rebuild_projection.py --name latest_prices
```

---

> “High-quality market intelligence emerges from disciplined data refinement—reliable layers, observable transformations, and reversible evolution.”

---
````
