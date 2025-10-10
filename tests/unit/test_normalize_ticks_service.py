import asyncio
import json
from datetime import datetime, timezone

import pytest

from services.normalize_ticks.app.config import NormalizeTicksConfig
from services.normalize_ticks.app.idempotency import IdempotencyGuard
from services.normalize_ticks.app.main import NormalizeTicksService
from services.normalize_ticks.app.processor import (
    ContractViolationError,
    NormalizedTickProcessor,
)


def _build_raw_event(
    *,
    price: float | None,
    volume: float | None = 10.0,
    event_id: str = "evt-123",
) -> dict:
    base_timestamp = 1_700_000_000_000_000
    payload = {
        "source_system": "miso-rest",
        "market": "MISO",
        "symbol": "MISO.NODAL",
        "sequence": 42,
        "received_at": base_timestamp + 300_000,
        "raw_payload": json.dumps(
            {
                **({"price": price} if price is not None else {}),
                **({"volume": volume} if volume is not None else {}),
                "currency": "USD",
            }
        ),
        "encoding": "json",
        "metadata": {"instrument_id": "INSTR-1"},
        "retry_count": 0,
    }

    return {
        "event_id": event_id,
        "trace_id": "trace-abc",
        "schema_version": "1.0.0",
        "tenant_id": "tenant-1",
        "producer": "miso-connector@1.2.3",
        "occurred_at": base_timestamp,
        "ingested_at": base_timestamp + 150_000,
        "payload": payload,
    }


class _StubBackfillRepository:
    def __init__(self, result: dict | None) -> None:
        self._result = result
        self.calls: list[str] = []

    async def get_latest_tick(self, instrument_id: str) -> dict | None:
        self.calls.append(instrument_id)
        return self._result


@pytest.fixture
def normalization_service(monkeypatch: pytest.MonkeyPatch) -> NormalizeTicksService:
    class _ProducerStub:
        def __init__(self, config, kafka_config, error_handler) -> None:  # noqa: ANN001
            self.config = config
            self.kafka_config = kafka_config
            self.error_handler = error_handler
            self.sent: list[dict] = []
            self.started = False

        async def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.started = False

        async def send_message(self, payload: dict, key: str | None = None) -> None:
            self.sent.append({"payload": payload, "key": key})

    class _ConsumerStub:
        def __init__(self, config, kafka_config, message_handler, error_handler) -> None:  # noqa: ANN001
            self.config = config
            self.kafka_config = kafka_config
            self.message_handler = message_handler
            self.error_handler = error_handler
            self.started = False

        async def start(self) -> None:
            self.started = True

        async def stop(self) -> None:
            self.started = False

    class _BackfillStub:
        def __init__(self, config) -> None:  # noqa: ANN001
            self.config = config
            self.enabled = False

        async def startup(self) -> None:
            return None

        async def shutdown(self) -> None:
            return None

        async def get_latest_tick(self, instrument_id: str) -> dict | None:
            return None

    monkeypatch.setattr("services.normalize_ticks.app.main.KafkaProducer", _ProducerStub)
    monkeypatch.setattr("services.normalize_ticks.app.main.KafkaConsumer", _ConsumerStub)
    monkeypatch.setattr("services.normalize_ticks.app.main.ClickHouseBackfillRepository", _BackfillStub)

    config = NormalizeTicksConfig()
    config.backfill_enabled = False
    return NormalizeTicksService(config=config)


@pytest.mark.asyncio
async def test_normalize_tick_happy_path() -> None:
    config = NormalizeTicksConfig()
    config.backfill_enabled = False
    processor = NormalizedTickProcessor(config=config)

    raw_event = _build_raw_event(price=49.25)
    result = await processor.normalize(raw_event)

    payload = result.tick.envelope.payload

    assert payload.price == pytest.approx(49.25)
    assert payload.volume == pytest.approx(10.0)
    assert payload.instrument_id == "INSTR-1"
    assert payload.market == "MISO"
    assert payload.quality.status == "canonical"
    assert "missing_volume" not in payload.quality.issues
    assert result.was_fallback is False
    assert payload.normalized_at >= datetime.fromtimestamp(0, tz=timezone.utc)


@pytest.mark.asyncio
async def test_normalize_tick_with_backfill_when_price_missing() -> None:
    config = NormalizeTicksConfig()
    config.backfill_enabled = True
    backfill_repo = _StubBackfillRepository({"price": 101.5})
    processor = NormalizedTickProcessor(config=config, backfill_repository=backfill_repo)

    raw_event = _build_raw_event(price=None)
    result = await processor.normalize(raw_event)

    payload = result.tick.envelope.payload

    assert result.was_fallback is True
    assert payload.price == pytest.approx(101.5)
    assert payload.quality.status == "fallback"
    assert "price_backfilled" in payload.quality.issues
    assert backfill_repo.calls == ["INSTR-1"]


@pytest.mark.asyncio
async def test_normalize_tick_raises_on_missing_price_without_backfill() -> None:
    config = NormalizeTicksConfig()
    config.backfill_enabled = False
    processor = NormalizedTickProcessor(config=config)

    raw_event = _build_raw_event(price=None)

    with pytest.raises(ContractViolationError):
        await processor.normalize(raw_event)


@pytest.mark.asyncio
async def test_normalize_service_emits_normalized_event_on_success(
    normalization_service: NormalizeTicksService,
) -> None:
    message = {
        "topic": "ingestion.market.ticks.raw.v1",
        "partition": 0,
        "offset": 15,
        "payload": _build_raw_event(price=51.5, event_id="evt-service-success"),
    }

    await normalization_service._handle_message(message)

    produced = normalization_service.normalized_producer.sent
    assert len(produced) == 1
    first_message = produced[0]
    assert first_message["key"] == "INSTR-1"

    normalized_payload = first_message["payload"]["payload"]
    assert normalized_payload["price"] == pytest.approx(51.5)
    assert normalized_payload["instrument_id"] == "INSTR-1"
    assert await normalization_service.idempotency.seen("evt-service-success") is True
    assert normalization_service.dlq_producer.sent == []


@pytest.mark.asyncio
async def test_normalize_service_routes_schema_error_to_dlq(
    normalization_service: NormalizeTicksService,
) -> None:
    raw_event = _build_raw_event(price=32.0, event_id="evt-schema-error")
    raw_event["payload"].pop("raw_payload")

    message = {
        "topic": "ingestion.market.ticks.raw.v1",
        "partition": 0,
        "offset": 22,
        "payload": raw_event,
    }

    await normalization_service._handle_message(message)

    assert normalization_service.normalized_producer.sent == []
    dlq_messages = normalization_service.dlq_producer.sent
    assert len(dlq_messages) == 1

    dlq_payload = dlq_messages[0]["payload"]
    assert dlq_payload["payload"]["failed_event_id"] == "evt-schema-error"
    assert dlq_payload["payload"]["error_code"] == "VALIDATION_ERROR"
    assert dlq_payload["payload"]["metadata"]["issues"] == "schema_validation_failed"


@pytest.mark.asyncio
async def test_normalize_service_routes_contract_violation_to_dlq(
    normalization_service: NormalizeTicksService,
) -> None:
    raw_event = _build_raw_event(price=77.7, event_id="evt-contract-error")
    raw_event["payload"]["metadata"].pop("instrument_id")
    raw_event["payload"]["symbol"] = ""
    raw_event["payload"]["raw_payload"] = json.dumps({"price": 77.7})

    message = {
        "topic": "ingestion.market.ticks.raw.v1",
        "partition": 1,
        "offset": 5,
        "payload": raw_event,
    }

    await normalization_service._handle_message(message)

    assert normalization_service.normalized_producer.sent == []
    dlq_messages = normalization_service.dlq_producer.sent
    assert len(dlq_messages) == 1

    dlq_payload = dlq_messages[0]["payload"]
    assert dlq_payload["payload"]["failed_event_id"] == "evt-contract-error"
    assert dlq_payload["payload"]["error_code"] == "CONTRACT_VIOLATION"
    assert dlq_payload["payload"]["metadata"]["issues"] == "missing_instrument_id"


@pytest.mark.asyncio
async def test_idempotency_guard_enforces_ttl() -> None:
    guard = IdempotencyGuard(ttl_seconds=0.01, max_entries=10)

    first_seen = await guard.seen("evt-1")
    assert first_seen is False

    await guard.mark("evt-1")
    assert await guard.seen("evt-1") is True

    await asyncio.sleep(0.02)
    assert await guard.seen("evt-1") is False
