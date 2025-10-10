"""
Utilities for publishing topology graphs for supported markets.
"""

from __future__ import annotations

import argparse
import json
import sys
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping

if __package__ in (None, ""):
    CURRENT_DIR = Path(__file__).resolve().parent
    sys.path.insert(0, str(CURRENT_DIR))
    from ercot_topology import ERCOT_TOPOLOGY_DATA
    from miso_topology import MISO_TOPOLOGY_DATA
    from wecc_topology import WECC_TOPOLOGY_DATA
else:
    from .ercot_topology import ERCOT_TOPOLOGY_DATA
    from .miso_topology import MISO_TOPOLOGY_DATA
    from .wecc_topology import WECC_TOPOLOGY_DATA


MARKET_DATASETS: Mapping[str, Dict[str, Any]] = {
    "wecc": WECC_TOPOLOGY_DATA,
    "miso": MISO_TOPOLOGY_DATA,
    "ercot": ERCOT_TOPOLOGY_DATA,
}

PAYLOAD_SECTIONS = ("balancing_authorities", "zones", "nodes", "paths", "interties")


def _build_topology_payload(
    market: str,
    raw_data: Mapping[str, Any],
    *,
    generated_at: str | None = None,
) -> Dict[str, Any]:
    """Create a topology payload with metadata and computed counts."""
    payload = deepcopy(raw_data)

    balancing_authorities = list(payload.get("balancing_authorities", []))
    zones = list(payload.get("zones", []))
    nodes = list(payload.get("nodes", []))
    paths = list(payload.get("paths", []))
    interties = list(payload.get("interties", []))

    payload["metadata"] = {
        "version": "1.0.0",
        "generated_at": generated_at or datetime.now(timezone.utc).isoformat(),
        "market": market,
        "ba_count": len(balancing_authorities),
        "zone_count": len(zones),
        "node_count": len(nodes),
        "path_count": len(paths),
        "intertie_count": len(interties),
    }

    payload["balancing_authorities"] = balancing_authorities
    payload["zones"] = zones
    payload["nodes"] = nodes
    payload["paths"] = paths
    payload["interties"] = interties

    return payload


def _require_keys(section: str, records: Iterable[Mapping[str, Any]], required: Iterable[str]) -> None:
    """Validate that each dict in records contains the required keys."""
    for record in records:
        missing = [key for key in required if key not in record or record[key] in (None, "")]
        if missing:
            raise ValueError(f"{section} record {record!r} missing required keys: {missing}")


def _validate_payload(payload: Mapping[str, Any]) -> None:
    """Run lightweight structural validation on the topology payload."""
    required_sections = ["metadata", "balancing_authorities", "zones", "nodes", "paths", "interties"]
    for section in required_sections:
        if section not in payload:
            raise ValueError(f"Topology payload missing required section '{section}'")

    metadata = payload["metadata"]
    for key in ["version", "generated_at", "market"]:
        if not metadata.get(key):
            raise ValueError(f"Topology metadata missing '{key}'")

    _require_keys(
        "balancing_authorities",
        payload["balancing_authorities"],
        ["ba_id", "ba_name", "timezone"],
    )
    _require_keys(
        "zones",
        payload["zones"],
        ["zone_id", "zone_name", "ba_id", "zone_type"],
    )
    _require_keys(
        "nodes",
        payload["nodes"],
        ["node_id", "node_name", "zone_id", "node_type"],
    )
    _require_keys(
        "paths",
        payload["paths"],
        ["path_id", "path_name", "from_node", "to_node", "path_type", "capacity_mw", "losses_factor"],
    )
    _require_keys(
        "interties",
        payload["interties"],
        ["intertie_id", "intertie_name", "from_market", "to_market", "capacity_mw"],
    )

    for path in payload["paths"]:
        if path.get("losses_factor") is None:
            raise ValueError(f"Path {path['path_id']} missing losses_factor")
        if "hurdle_rate_usd_per_mwh" not in path:
            raise ValueError(f"Path {path['path_id']} missing hurdle_rate_usd_per_mwh field")


def publish_topology_graphs(output_dir: Path | str | None = None) -> Dict[str, Path]:
    """
    Publish topology graphs for all supported markets to JSON files.

    Returns:
        Mapping of market identifier to written file path.
    """
    if output_dir is None:
        output_dir = Path(__file__).resolve().parent.parent / "published"

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    written: Dict[str, Path] = {}
    for market, dataset in MARKET_DATASETS.items():
        file_path = output_path / f"{market}_topology.json"
        existing_payload: Dict[str, Any] | None = None

        if file_path.exists():
            with file_path.open("r", encoding="utf-8") as existing_file:
                try:
                    existing_payload = json.load(existing_file)
                except json.JSONDecodeError:
                    existing_payload = None

        payload = _build_topology_payload(
            market,
            dataset,
            generated_at=existing_payload.get("metadata", {}).get("generated_at") if existing_payload else None,
        )

        if existing_payload:
            if _extract_sections(existing_payload) != _extract_sections(payload):
                payload["metadata"]["generated_at"] = datetime.now(timezone.utc).isoformat()
            else:
                previous_generated_at = existing_payload.get("metadata", {}).get("generated_at")
                if previous_generated_at:
                    payload["metadata"]["generated_at"] = previous_generated_at

        _validate_payload(payload)

        with file_path.open("w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2)

        written[market] = file_path

    return written


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Publish topology graphs with losses and hurdles.")
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Directory to write topology JSON files. Defaults to ../published relative to this module.",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    paths = publish_topology_graphs(args.output_dir)
    for market, path in paths.items():
        print(f"Wrote {market.upper()} topology to {path}")


def _extract_sections(payload: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a mapping of the key sections to compare payload equality."""
    return {section: payload.get(section, []) for section in PAYLOAD_SECTIONS}


if __name__ == "__main__":
    main()
