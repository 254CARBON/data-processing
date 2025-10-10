"""
Topology data models for North American power markets.
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum


class NodeType(Enum):
    """Types of topology nodes."""
    LOAD = "load"
    GENERATION = "generation"
    HUB = "hub"
    INTERTIE = "intertie"
    REFERENCE = "reference"


class ZoneType(Enum):
    """Types of topology zones."""
    LOAD_ZONE = "load_zone"
    GENERATION_ZONE = "generation_zone"
    HUB_ZONE = "hub_zone"
    INTERFACE = "interface"


class PathType(Enum):
    """Types of transmission paths."""
    TRANSMISSION_LINE = "transmission_line"
    INTERTIE = "intertie"
    FLOWGATE = "flowgate"
    INTERFACE = "interface"


@dataclass
class TopologyBA:
    """Balancing Authority topology data."""
    ba_id: str
    ba_name: str
    iso_rto: Optional[str]
    timezone: str
    dst_rules: Dict[str, Any]
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TopologyNode:
    """Power system node topology data."""
    node_id: str
    node_name: str
    zone_id: str
    node_type: NodeType
    voltage_level_kv: Optional[float]
    latitude: Optional[float]
    longitude: Optional[float]
    capacity_mw: Optional[float]
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TopologyZone:
    """Power system zone topology data."""
    zone_id: str
    zone_name: str
    ba_id: str
    zone_type: ZoneType
    load_serving_entity: Optional[str]
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TopologyPath:
    """Transmission path topology data."""
    path_id: str
    path_name: str
    from_node: str
    to_node: str
    path_type: PathType
    capacity_mw: float
    losses_factor: float
    hurdle_rate_usd_per_mwh: Optional[float]
    wheel_rate_usd_per_mwh: Optional[float]
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TopologyIntertie:
    """Intertie topology data."""
    intertie_id: str
    intertie_name: str
    from_market: str
    to_market: str
    capacity_mw: float
    atc_ttc_ntc: Dict[str, float]
    deliverability_flags: List[str]
    metadata: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class TopologyGraph:
    """Complete topology graph for a market."""
    market: str
    version: str
    generated_at: datetime

    balancing_authorities: List[TopologyBA]
    zones: List[TopologyZone]
    nodes: List[TopologyNode]
    paths: List[TopologyPath]
    interties: List[TopologyIntertie]

    # Computed properties
    ba_count: int = field(init=False)
    zone_count: int = field(init=False)
    node_count: int = field(init=False)
    path_count: int = field(init=False)
    intertie_count: int = field(init=False)

    def __post_init__(self):
        self.ba_count = len(self.balancing_authorities)
        self.zone_count = len(self.zones)
        self.node_count = len(self.nodes)
        self.path_count = len(self.paths)
        self.intertie_count = len(self.interties)
