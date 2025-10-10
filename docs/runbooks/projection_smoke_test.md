# Projection Service Kubernetes Smoke Test

This runbook captures the manual steps used to validate the projection service
against the shared `data-plane` Redis/Kafka/ClickHouse services in the
ephemeral Kubernetes cluster.

> **Heads-up:** The current projection service code assumes an older
> `KafkaConsumer` interface (keyword arguments such as `bootstrap_servers`).
> When the service boots it raises
> `TypeError: KafkaConsumer.__init__() got an unexpected keyword argument 'bootstrap_servers'`.
> The smoke test below gets as far as launching the service process, but the
> consumer mismatch prevents it from staying up until the codebase is updated.

## Prerequisites

- `kubectl` configured for the target cluster.
- Docker-less workflow (we copy the repository into a helper pod instead of
  baking an image).
- The `data-plane` namespace already provisioned with ClickHouse, Kafka, and
  Redis services (confirmed via `kubectl get services -n data-plane`).

## 1. Bootstrap a helper pod with source code

```bash
kubectl create namespace projection-test

cat <<'YAML' | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: projection-runner
  namespace: projection-test
spec:
  containers:
  - name: runner
    image: python:3.10-slim
    command: ["sleep", "infinity"]
    resources:
      requests:
        cpu: "250m"
        memory: "512Mi"
      limits:
        cpu: "500m"
        memory: "1Gi"
  restartPolicy: Never
YAML

kubectl wait -n projection-test --for=condition=Ready pod/projection-runner --timeout=120s

kubectl exec -n projection-test projection-runner -- mkdir -p /workspace
kubectl cp data-processing projection-test/projection-runner:/workspace/data-processing

kubectl exec -n projection-test projection-runner -- bash -c \
  "cd /workspace/data-processing && pip install --no-cache-dir -r requirements.txt"
```

## 2. Launch the projection service (current behaviour: startup failure)

Run the service inside the pod with environment variables pointing at the
cluster services. Because of inconsistent logging and Kafka consumer APIs,
we temporarily patch `logging.Logger._log` to swallow keyword arguments.

```bash
kubectl exec -n projection-test projection-runner -- bash -c '
cd /workspace/data-processing/service-projection
export DATA_PROC_ENV=dev
export DATA_PROC_LOG_LEVEL=info
export DATA_PROC_KAFKA_BOOTSTRAP=kafka.data-plane.svc.cluster.local:9092
export DATA_PROC_CLICKHOUSE_HOST=clickhouse.data-plane.svc.cluster.local
export DATA_PROC_CLICKHOUSE_PORT=9000
export DATA_PROC_CLICKHOUSE_DATABASE=data_processing
export DATA_PROC_CLICKHOUSE_USER=default
export DATA_PROC_REDIS_HOST=redis.data-plane.svc.cluster.local
export DATA_PROC_REDIS_PORT=6379
export DATA_PROC_REDIS_DB=0
export DATA_PROC_PROJECTION_PORT=8084
export DATA_PROC_INTERNAL_API_PORT=8085
export PYTHONPATH=/workspace/data-processing
nohup python - <<\"PY\" > /workspace/projection-service.log 2>&1 &
import logging
orig = logging.Logger._log
def _safe(self, level, msg, args, exc_info=None, extra=None, stack_info=False, stacklevel=1, **kw):
    if kw:
        extra = dict(extra or {})
        extra.update(kw)
    return orig(self, level, msg, args, exc_info=exc_info, extra=extra, stack_info=stack_info, stacklevel=stacklevel)
logging.Logger._log = _safe
import asyncio
from app.main import main
asyncio.run(main())
PY
'
```

Inspect logs:

```bash
kubectl exec -n projection-test projection-runner -- \
  bash -c 'tail -n 50 /workspace/projection-service.log'
```

Expected (current) output shows the Kafka consumer constructor mismatch and the
service shutting down. Once `AggregatedDataConsumer`/`InvalidationConsumer`
are updated to use `ConsumerConfig`/`KafkaConfig`, the service should remain
healthy and expose:

- `:8084` health endpoints (`/health`, `/metrics`)
- Internal API on `:8085` (`/projections/latest_price` etc.)

## 3. (Future) Smoke-test workflow once service boots cleanly

After fixing the Kafka consumer API:

1. Port-forward the internal API:  
   `kubectl port-forward -n projection-test pod/projection-runner 18085:8085`
2. Publish a bar event to Kafka (topic
   `${DATA_PROC_PROJECTION_AGGREGATED_TOPIC:-aggregation.bars.v1}`) using a
   small Python helper inside the pod with `confluent_kafka.Producer`.
3. Query Redis or call
   `http://127.0.0.1:18085/projections/latest_price?tenant_id=...&instrument_id=...`
   to confirm the event materialised in the served cache.

## 4. Cleanup

```bash
kubectl delete namespace projection-test
```

## Follow-ups

- Refactor the projection service consumers to instantiate
  `shared.framework.consumer.KafkaConsumer` with the new
  `ConsumerConfig + KafkaConfig` signature.
- Replace the ad-hoc logging monkey patch with `structlog.get_logger`
  or adapt logging helpers.
- Once fixed, automate the above steps (e.g. a `make projection-smoke`
  target that spawns the pod, runs the service, and emits a sample Kafka
  message for verification).
