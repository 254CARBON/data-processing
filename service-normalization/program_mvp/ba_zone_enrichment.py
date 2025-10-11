"""
Pre-normalization script for BA/zone enrichment.

Enriches connector outputs with BA/zone attribution when missing.
"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Dict, List, Optional

import structlog

logger = structlog.get_logger(__name__)


class BAZoneEnrichment:
    """Enriches data with BA/zone attribution using topology mappings."""

    def __init__(self, topology_mappings_dir: Path):
        self.topology_mappings_dir = topology_mappings_dir
        self.ba_zone_mappings = self._load_topology_mappings()

    def _load_topology_mappings(self) -> Dict[str, Dict[str, str]]:
        """Load BA/zone mappings from CSV files."""
        mappings = {}
        
        for csv_file in self.topology_mappings_dir.glob("*_ba_zones.csv"):
            market = csv_file.stem.replace("_ba_zones", "")
            mappings[market] = {}
            
            with open(csv_file, "r") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    zone_key = f"{row['ba']}:{row['zone']}"
                    mappings[market][zone_key] = {
                        "ba": row["ba"],
                        "zone": row["zone"],
                        "zone_name": row.get("zone_name", row["zone"]),
                        "zone_type": row.get("zone_type", "unknown"),
                    }
        
        return mappings

    def enrich_load_demand(
        self, 
        input_file: Path, 
        output_file: Path, 
        market: str
    ) -> None:
        """Enrich load demand CSV with BA/zone attribution."""
        logger.info("Enriching load demand", input_file=str(input_file), market=market)
        
        enriched_records = []
        with open(input_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                enriched_row = self._enrich_record(row, market, "load_demand")
                enriched_records.append(enriched_row)
        
        self._write_csv(output_file, enriched_records, [
            "timestamp", "market", "ba", "zone", "demand_mw", "data_source"
        ])

    def enrich_generation_actual(
        self, 
        input_file: Path, 
        output_file: Path, 
        market: str
    ) -> None:
        """Enrich generation actual CSV with BA/zone attribution."""
        logger.info("Enriching generation actual", input_file=str(input_file), market=market)
        
        enriched_records = []
        with open(input_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                enriched_row = self._enrich_record(row, market, "generation_actual")
                enriched_records.append(enriched_row)
        
        self._write_csv(output_file, enriched_records, [
            "timestamp", "market", "ba", "zone", "resource_id", "resource_type",
            "fuel", "output_mw", "output_mwh", "data_source"
        ])

    def enrich_generation_capacity(
        self, 
        input_file: Path, 
        output_file: Path, 
        market: str
    ) -> None:
        """Enrich generation capacity CSV with BA/zone attribution."""
        logger.info("Enriching generation capacity", input_file=str(input_file), market=market)
        
        enriched_records = []
        with open(input_file, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                enriched_row = self._enrich_record(row, market, "generation_capacity")
                enriched_records.append(enriched_row)
        
        self._write_csv(output_file, enriched_records, [
            "effective_date", "market", "ba", "zone", "resource_id", "resource_type",
            "fuel", "nameplate_mw", "ucap_factor", "ucap_mw", "availability_factor",
            "cost_curve", "data_source"
        ])

    def _enrich_record(self, row: Dict[str, Any], market: str, table_type: str) -> Dict[str, Any]:
        """Enrich a single record with BA/zone attribution."""
        enriched = row.copy()
        
        # If BA/zone already present, keep as-is
        if row.get("ba") and row.get("zone"):
            return enriched
        
        # Apply market-specific enrichment
        if market in self.ba_zone_mappings:
            market_mappings = self.ba_zone_mappings[market]
            
            # Try to infer BA/zone from existing fields
            ba_zone = self._infer_ba_zone(row, market, table_type)
            if ba_zone and ba_zone in market_mappings:
                mapping = market_mappings[ba_zone]
                enriched["ba"] = mapping["ba"]
                enriched["zone"] = mapping["zone"]
            else:
                # Fallback to market defaults
                enriched["ba"] = self._get_default_ba(market)
                enriched["zone"] = self._get_default_zone(market)
        else:
            # Unknown market, use generic defaults
            enriched["ba"] = market.upper()
            enriched["zone"] = market.upper()
        
        return enriched

    def _infer_ba_zone(self, row: Dict[str, Any], market: str, table_type: str) -> Optional[str]:
        """Infer BA/zone from record fields."""
        # Try different field combinations
        candidates = [
            f"{row.get('ba', '')}:{row.get('zone', '')}",
            f"{row.get('balancing_authority', '')}:{row.get('load_zone', '')}",
            f"{row.get('iso_rto', '')}:{row.get('price_zone', '')}",
        ]
        
        for candidate in candidates:
            if candidate and ":" in candidate and not candidate.endswith(":"):
                return candidate
        
        return None

    def _get_default_ba(self, market: str) -> str:
        """Get default BA for market."""
        defaults = {
            "wecc": "CAISO",
            "caiso": "CAISO",
            "miso": "MISO",
            "ercot": "ERCOT",
        }
        return defaults.get(market.lower(), market.upper())

    def _get_default_zone(self, market: str) -> str:
        """Get default zone for market."""
        defaults = {
            "wecc": "ZONE1",
            "caiso": "ZONE1",
            "miso": "MISO",
            "ercot": "HOUSTON",
        }
        return defaults.get(market.lower(), market.upper())

    def _write_csv(self, output_file: Path, records: List[Dict[str, Any]], fieldnames: List[str]) -> None:
        """Write enriched records to CSV."""
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(records)
        
        logger.info("Enriched CSV written", output_file=str(output_file), count=len(records))


def enrich_snapshot_directory(
    input_dir: Path,
    output_dir: Path,
    market: str,
    topology_mappings_dir: Path,
) -> None:
    """Enrich all CSV files in a snapshot directory."""
    enrichment = BAZoneEnrichment(topology_mappings_dir)
    
    # Define file mappings
    file_mappings = [
        ("load_demand.csv", enrichment.enrich_load_demand),
        ("generation_actual.csv", enrichment.enrich_generation_actual),
        ("generation_capacity.csv", enrichment.enrich_generation_capacity),
    ]
    
    for filename, enrich_func in file_mappings:
        input_file = input_dir / filename
        output_file = output_dir / filename
        
        if input_file.exists():
            enrich_func(input_file, output_file, market)
        else:
            logger.warning("Input file not found", file=str(input_file))
    
    # Copy other files as-is
    other_files = ["rec_ledger.csv", "emission_factors.csv"]
    for filename in other_files:
        input_file = input_dir / filename
        output_file = output_dir / filename
        
        if input_file.exists():
            output_file.parent.mkdir(parents=True, exist_ok=True)
            output_file.write_text(input_file.read_text())
            logger.info("Copied file", file=str(output_file))


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Enrich snapshot with BA/zone attribution")
    parser.add_argument("--input-dir", type=Path, required=True, help="Input snapshot directory")
    parser.add_argument("--output-dir", type=Path, required=True, help="Output snapshot directory")
    parser.add_argument("--market", type=str, required=True, help="Market identifier")
    parser.add_argument("--topology-mappings-dir", type=Path, required=True, help="Topology mappings directory")
    
    args = parser.parse_args()
    
    enrich_snapshot_directory(
        args.input_dir,
        args.output_dir,
        args.market,
        args.topology_mappings_dir,
    )
