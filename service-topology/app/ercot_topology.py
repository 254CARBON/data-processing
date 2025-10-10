"""
ERCOT topology data for Electric Reliability Council of Texas.
"""

ERCOT_TOPOLOGY_DATA = {
    "balancing_authorities": [
        {
            "ba_id": "ERCOT",
            "ba_name": "Electric Reliability Council of Texas",
            "iso_rto": "ERCOT",
            "timezone": "America/Chicago",
            "dst_rules": {
                "observes_dst": True,
                "dst_start_rule": "second Sunday in March",
                "dst_end_rule": "first Sunday in November"
            }
        }
    ],
    "zones": [
        {
            "zone_id": "ERCOT_NORTH",
            "zone_name": "ERCOT North",
            "ba_id": "ERCOT",
            "zone_type": "load_zone",
            "load_serving_entity": "ERCOT"
        },
        {
            "zone_id": "ERCOT_SOUTH",
            "zone_name": "ERCOT South",
            "ba_id": "ERCOT",
            "zone_type": "load_zone",
            "load_serving_entity": "ERCOT"
        },
        {
            "zone_id": "ERCOT_WEST",
            "zone_name": "ERCOT West",
            "ba_id": "ERCOT",
            "zone_type": "load_zone",
            "load_serving_entity": "ERCOT"
        },
        {
            "zone_id": "ERCOT_HOUSTON",
            "zone_name": "ERCOT Houston",
            "ba_id": "ERCOT",
            "zone_type": "load_zone",
            "load_serving_entity": "ERCOT"
        }
    ],
    "nodes": [
        {
            "node_id": "HB_HOUSTON",
            "node_name": "HB_HOUSTON",
            "zone_id": "ERCOT_HOUSTON",
            "node_type": "hub",
            "voltage_level_kv": 345,
            "latitude": 29.7604,
            "longitude": -95.3698,
            "capacity_mw": None
        },
        {
            "node_id": "HB_NORTH",
            "node_name": "HB_NORTH",
            "zone_id": "ERCOT_NORTH",
            "node_type": "hub",
            "voltage_level_kv": 345,
            "latitude": 32.7767,
            "longitude": -96.7970,
            "capacity_mw": None
        },
        {
            "node_id": "HB_SOUTH",
            "node_name": "HB_SOUTH",
            "zone_id": "ERCOT_SOUTH",
            "node_type": "hub",
            "voltage_level_kv": 345,
            "latitude": 26.0765,
            "longitude": -97.2972,
            "capacity_mw": None
        },
        {
            "node_id": "HB_WEST",
            "node_name": "HB_WEST",
            "zone_id": "ERCOT_WEST",
            "node_type": "hub",
            "voltage_level_kv": 345,
            "latitude": 31.9686,
            "longitude": -99.9018,
            "capacity_mw": None
        },
        {
            "node_id": "COMANCHE_PEAK",
            "node_name": "Comanche Peak",
            "zone_id": "ERCOT_NORTH",
            "node_type": "generation",
            "voltage_level_kv": 345,
            "latitude": 32.2988,
            "longitude": -97.3956,
            "capacity_mw": 2400.0
        },
        {
            "node_id": "SOUTH_TEXAS_PROJECT",
            "node_name": "South Texas Project",
            "zone_id": "ERCOT_HOUSTON",
            "node_type": "generation",
            "voltage_level_kv": 345,
            "latitude": 28.7953,
            "longitude": -96.0481,
            "capacity_mw": 2500.0
        }
    ],
    "paths": [
        {
            "path_id": "NORTH_TO_HOUSTON",
            "path_name": "North to Houston",
            "from_node": "HB_NORTH",
            "to_node": "HB_HOUSTON",
            "path_type": "transmission_line",
            "capacity_mw": 8500.0,
            "losses_factor": 0.025,
            "hurdle_rate_usd_per_mwh": 4.25,
            "wheel_rate_usd_per_mwh": 1.75
        },
        {
            "path_id": "WEST_TO_NORTH",
            "path_name": "West to North",
            "from_node": "HB_WEST",
            "to_node": "HB_NORTH",
            "path_type": "transmission_line",
            "capacity_mw": 6200.0,
            "losses_factor": 0.022,
            "hurdle_rate_usd_per_mwh": 3.80,
            "wheel_rate_usd_per_mwh": 1.50
        },
        {
            "path_id": "SOUTH_TO_HOUSTON",
            "path_name": "South to Houston",
            "from_node": "HB_SOUTH",
            "to_node": "HB_HOUSTON",
            "path_type": "transmission_line",
            "capacity_mw": 4200.0,
            "losses_factor": 0.018,
            "hurdle_rate_usd_per_mwh": 2.90,
            "wheel_rate_usd_per_mwh": 1.25
        },
        {
            "path_id": "CREZ_LINES",
            "path_name": "Competitive Renewable Energy Zones",
            "from_node": "HB_WEST",
            "to_node": "HB_NORTH",
            "path_type": "interface",
            "capacity_mw": 18500.0,
            "losses_factor": 0.035,
            "hurdle_rate_usd_per_mwh": 6.75,
            "wheel_rate_usd_per_mwh": 3.00
        }
    ],
    "interties": [
        {
            "intertie_id": "ERCOT_SPP",
            "intertie_name": "ERCOT to SPP Intertie",
            "from_market": "ercot",
            "to_market": "spp",
            "capacity_mw": 820.0,
            "atc_ttc_ntc": {
                "atc_mw": 650.0,
                "ttc_mw": 820.0,
                "ntc_mw": 650.0
            },
            "deliverability_flags": ["firm", "non_firm", "recallable"]
        },
        {
            "intertie_id": "ERCOT_MEXICO",
            "intertie_name": "ERCOT to Mexico Intertie",
            "from_market": "ercot",
            "to_market": "cenace",
            "capacity_mw": 150.0,
            "atc_ttc_ntc": {
                "atc_mw": 100.0,
                "ttc_mw": 150.0,
                "ntc_mw": 100.0
            },
            "deliverability_flags": ["conditional", "recallable"]
        }
    ]
}
