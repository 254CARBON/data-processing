# Data Processing Pipeline (`254carbon-data-processing`)

> Normalization, enrichment, aggregation, and projection services that transform Bronze events into query-ready materializations consumed by analytics and the access layer.

Reference: [Platform Overview](../PLATFORM_OVERVIEW.md)

---

## Scope
- Consume Bronze topics (`ingestion.*`) and enforce canonical schemas before promoting to Silver/Gold tables.
- Maintain stateless workers for normalization, enrichment, aggregation, projection, and auxiliary batch jobs.
- Publish derived events (`normalized.market.*`, `aggregation.*`, `projection.*`) and hydrate Redis/ClickHouse stores.
- Provide operational tooling for replay, reconciliation, and materialization refresh.

Out of scope: upstream connector management (see `../ingestion`) and user-facing APIs (see `../access`).

---

## Architecture Overview
- Services live under `service-*/` directories with shared libraries in `shared/`.
- Infrastructure manifests (`helm/`, `k8s/`, `docker-compose.yml`) support local and cluster deployments.
- ClickHouse + Postgres + Redis hold materialized state; Kafka drives streaming inputs/outputs.
- Support tooling in `scripts/` for data generation, DLQ replays, and verification.

---

## Environments

| Environment | Bootstrap | Entry Points | Notes |
|-------------|-----------|--------------|-------|
| `local` | `make start` (full stack) or `make start-infra` + `make start-services` | Services on `http://localhost:8080-8083`, Prometheus `9090`, Grafana `3000` | Recommended for development and debugging pipelines. |
| `dev` | Deploy via `helm` chart (`make helm-install NAMESPACE=dp-dev`) on cluster from `../infra` | Namespace `data-processing-dev`; port-forward with `make helm-port-forward` | Shared integration cluster; uses smaller batch sizes. |
| `staging` | GitOps/Flux controlled (values in `helm/values-staging.yaml`) | Observability identical to prod | Used for load/SLO validation before promotion. |
| `production` | GitOps apply after approval; Helm chart pinned via `specs.lock.json` | TLS ingress for `/health`, `/metrics` endpoints | Strict SLOs (<100 ms p95) and data reconciliation schedules. |

Environment defaults live in `config/default.env.example`; copy to `.env` or provide through Helm values/secrets.

---

## Runbook

### Daily Checks
- `make status` – verify containers/pods running (local) or `kubectl get pods -n data-processing`.
- Review Grafana dashboard `observability/dashboards/data_processing/normalization_pipeline.json` for throughput, lag, and error counters.
- Inspect ClickHouse freshness: `clickhouse-client --query "SELECT max(event_time) FROM gold.curves"` (port-forward if remote).
- Confirm DLQ size: `kafka-consumer-groups --bootstrap-server <broker> --describe --group data-processing-dlq`.

### Deployments
1. Validate code: `make test` (unit/integration) and `make lint`.
2. Build/publish images via CI or `docker buildx bake`.
3. Update Helm values/manifests; run `make helm-lint` and `make helm-dry-run`.
4. Deploy: `make helm-upgrade NAMESPACE=<env>` (or GitOps merge).
5. Post-deploy verification: `kubectl rollout status deployment/<svc> -n <ns>` and review dashboard for new build tag.

### Backfill / Replay
- Use `scripts/test_integration_simple.py` for smoke validation before bulk backfill.
- Trigger targeted reprocess:
  ```bash
  python scripts/reprocess_dlq.py --topic normalized.market.ticks.v1 --partition 0 --start-offset 12345 --end-offset 12400
  ```
- For ClickHouse reconciliation run: `python scripts/run_reconciliation.py --window 24h` (ensures Gold tables match expectations).
- Coordinate with access/analytics before reprocessing to avoid double counting.

### Incident Response Playbook
- **High Latency / Backlog**: scale service `kubectl scale deployment/normalization-service --replicas=3 -n <env>`, monitor `processing_latency_ms`.
- **Data Quality Breach**: `python scripts/run_data_quality_checks.py --mode immediate` to reproduce; pause downstream consumers if failures persist.
- **Outage Rollback**: `helm rollback data-processing <revision> -n <env>`; verify backlog clears before resuming scheduled jobs.

---

## Configuration

| Variable | Description | Default | Notes |
|----------|-------------|---------|-------|
| `DATA_PROC_ENV` | Environment label for logging/tracing | `local` | Propagated into OTEL attributes. |
| `DATA_PROC_KAFKA_BOOTSTRAP` | Kafka bootstrap servers | `localhost:9092` | Required for all services. |
| `DATA_PROC_CLICKHOUSE_URL` | ClickHouse HTTP endpoint | `http://localhost:8123` | Include credentials for non-local envs. |
| `DATA_PROC_POSTGRES_DSN` | Reference metadata store | `postgresql://postgres:postgres@localhost:5432/data_proc` | Used by enrichment/taxonomy and audit. |
| `DATA_PROC_REDIS_URL` | Redis cache connection | `redis://localhost:6379/0` | Holds latest snapshots and idempotency keys. |
| `DATA_PROC_TRACE_ENABLED` | Enable OpenTelemetry tracing | `true` | Set exporter via `OTEL_EXPORTER_OTLP_ENDPOINT`. |
| `DATA_PROC_NORMALIZATION_MAX_BATCH_SIZE` | Batch size guardrail | `1000` | Tune per env for throughput vs memory. |
| `DATA_PROC_PROJECTION_REFRESH_INTERVAL` | Seconds between projection refresh | `10` | Increase in prod for stability. |

Full list in `config/default.env.example`; secrets (JWT keys, TLS certs) managed via Kubernetes secrets or `.env`.

---

## Observability
- Metrics exposed on `/metrics` for each service (port `9090` locally). Prometheus scrape jobs defined in `monitoring/prometheus.yml`.
- Grafana dashboards stored in `../observability/dashboards/data_processing/normalization_pipeline.json`.
- Alerts in `../observability/alerts/SLO/api_latency_slo.yaml` and `../observability/alerts/RED/gold_pipeline_red.yaml`.
- Tracing emits `service.name=254carbon-data-processing-<component>`. Tempo/Jaeger views highlight stage breakdown.
- Structured logs (JSON) streamed to Loki (future) or accessible via `docker-compose logs` / `kubectl logs -l app=data-processing`.

---

## Troubleshooting

### Service Fails Health Check
1. `make logs-<service>` (local) or `kubectl logs deployment/<svc> -n <env>`.
2. Validate dependencies (ClickHouse, Redis): `make status` or `kubectl get pods`.
3. Restart: `make restart` or `kubectl rollout restart deployment/<svc>`.

### Kafka Consumer Lag Growing
- Inspect metric `data_processing_consumer_lag` (Grafana).
- Scale horizontally: `kubectl scale deployment/<svc> --replicas=<n> -n <env>`.
- Confirm offset advancement with `kafka-consumer-groups --describe`.
- If backlog from malformed payloads, drain DLQ via `scripts/reprocess_dlq.py` after fix.

### ClickHouse Writes Failing
- Check error logs for table mismatch; run migrations: `python scripts/apply_clickhouse_migrations.py`.
- Validate schema exists: `clickhouse-client --query "SHOW TABLES FROM gold"`.
- Ensure user has write grants (Helm chart value `clickhouse.user`).

### Redis Cache Drift
- Flush specific keys: `redis-cli --scan --pattern 'projection:*' | xargs redis-cli DEL`.
- Run projection reconciliation: `python scripts/projection_reconcile.py --dry-run`.

---

## Reference & Tooling
- `Makefile` targets – `make help` for local automation, Helm, and Kubernetes helpers.
- `docs/runbooks/` – deep-dive operational playbooks (per service, DLQ handling, reconciliation).
- `monitoring/` – Prometheus rules and Grafana provisioning.
- `scripts/` – includes `generate_sample_data.py`, `reprocess_dlq.py`, `run_data_quality_checks.py`.
- `service-manifest.yaml` – metadata consumed by `../meta`.

Use the [Platform Overview](../PLATFORM_OVERVIEW.md) for cross-repo interactions, SLO targets, and deployment topology.
