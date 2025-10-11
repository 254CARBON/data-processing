"""Helpers for deterministic identifier generation across data-processing services."""

from __future__ import annotations

import uuid
from typing import Any

REFDATA_INSTRUMENT_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://254carbon.com/namespaces/refdata/instrument"
)
REFDATA_EVENT_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://254carbon.com/namespaces/refdata/event"
)
REFDATA_PROJECTION_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://254carbon.com/namespaces/refdata/projection"
)
AGGREGATION_BAR_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://254carbon.com/namespaces/aggregation/bar"
)
PROJECTION_NAMESPACE = uuid.uuid5(
    uuid.NAMESPACE_URL, "https://254carbon.com/namespaces/projection"
)


def _normalize_component(component: Any) -> str:
    """Convert a component to a normalized string suitable for UUID generation."""
    if component is None:
        return ""

    if isinstance(component, (bytes, bytearray)):
        try:
            component = component.decode("utf-8")
        except UnicodeDecodeError:
            component = component.hex()

    value = str(component).strip()
    return value


def deterministic_uuid(namespace: uuid.UUID, *components: Any) -> str:
    """
    Generate a deterministic UUIDv5 string for the provided components.

    Empty or None components are ignored. If all components are empty the
    function falls back to a sentinel value to keep behaviour deterministic.
    """
    normalized_parts = []
    for component in components:
        value = _normalize_component(component)
        if value:
            normalized_parts.append(value)

    if not normalized_parts:
        normalized_parts.append("__empty__")

    payload = "::".join(normalized_parts)
    return str(uuid.uuid5(namespace, payload))


__all__ = [
    "REFDATA_INSTRUMENT_NAMESPACE",
    "REFDATA_EVENT_NAMESPACE",
    "REFDATA_PROJECTION_NAMESPACE",
    "AGGREGATION_BAR_NAMESPACE",
    "PROJECTION_NAMESPACE",
    "deterministic_uuid",
]
