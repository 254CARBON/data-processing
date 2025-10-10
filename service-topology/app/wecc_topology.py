"""
WECC topology data for Western Interconnection.
"""

WECC_TOPOLOGY_DATA = {
    "balancing_authorities": [
        {
            "ba_id": "CAISO",
            "ba_name": "California Independent System Operator",
            "iso_rto": "CAISO",
            "timezone": "America/Los_Angeles",
            "dst_rules": {
                "observes_dst": True,
                "dst_start_rule": "second Sunday in March",
                "dst_end_rule": "first Sunday in November"
            }
        },
        {
            "ba_id": "BPA",
            "ba_name": "Bonneville Power Administration",
            "iso_rto": None,
            "timezone": "America/Los_Angeles",
            "dst_rules": {
                "observes_dst": True,
                "dst_start_rule": "second Sunday in March",
                "dst_end_rule": "first Sunday in November"
            }
        },
        {
            "ba_id": "LADWP",
            "ba_name": "Los Angeles Department of Water and Power",
            "iso_rto": None,
            "timezone": "America/Los_Angeles",
            "dst_rules": {
                "observes_dst": True,
                "dst_start_rule": "second Sunday in March",
                "dst_end_rule": "first Sunday in November"
            }
        },
        {
            "ba_id": "SCE",
            "ba_name": "Southern California Edison",
            "iso_rto": "CAISO",
            "timezone": "America/Los_Angeles",
            "dst_rules": {
                "observes_dst": True,
                "dst_start_rule": "second Sunday in March",
                "dst_end_rule": "first Sunday in November"
            }
        },
        {
            "ba_id": "SDGE",
            "ba_name": "San Diego Gas & Electric",
            "iso_rto": "CAISO",
            "timezone": "America/Los_Angeles",
            "dst_rules": {
                "observes_dst": True,
                "dst_start_rule": "second Sunday in March",
                "dst_end_rule": "first Sunday in November"
            }
        }
    ],
    "zones": [
        {
            "zone_id": "CAISO_SP15",
            "zone_name": "CAISO SP15",
            "ba_id": "CAISO",
            "zone_type": "load_zone",
            "load_serving_entity": "CAISO"
        },
        {
            "zone_id": "CAISO_NP15",
            "zone_name": "CAISO NP15",
            "ba_id": "CAISO",
            "zone_type": "load_zone",
            "load_serving_entity": "CAISO"
        },
        {
            "zone_id": "BPA_NORTH",
            "zone_name": "BPA North",
            "ba_id": "BPA",
            "zone_type": "load_zone",
            "load_serving_entity": "BPA"
        },
        {
            "zone_id": "LADWP",
            "zone_name": "LADWP",
            "ba_id": "LADWP",
            "zone_type": "load_zone",
            "load_serving_entity": "LADWP"
        }
    ],
    "nodes": [
        {
            "node_id": "TH_NP15_GEN-APND",
            "node_name": "TH_NP15_GEN-APND",
            "zone_id": "CAISO_NP15",
            "node_type": "generation",
            "voltage_level_kv": 230,
            "latitude": 37.7749,
            "longitude": -122.4194,
            "capacity_mw": 500.0
        },
        {
            "node_id": "TH_SP15_GEN-APND",
            "node_name": "TH_SP15_GEN-APND",
            "zone_id": "CAISO_SP15",
            "node_type": "generation",
            "voltage_level_kv": 230,
            "latitude": 34.0522,
            "longitude": -118.2437,
            "capacity_mw": 750.0
        },
        {
            "node_id": "PALOVRDE_2_UNITS",
            "node_name": "Palo Verde 2 Units",
            "zone_id": "CAISO_SP15",
            "node_type": "generation",
            "voltage_level_kv": 500,
            "latitude": 33.3891,
            "longitude": -112.8647,
            "capacity_mw": 1300.0
        },
        {
            "node_id": "MIDWAY_500",
            "node_name": "Midway 500",
            "zone_id": "CAISO_SP15",
            "node_type": "hub",
            "voltage_level_kv": 500,
            "latitude": 35.1234,
            "longitude": -119.4567,
            "capacity_mw": None
        }
    ],
    "paths": [
        {
            "path_id": "PATH_15",
            "path_name": "Path 15",
            "from_node": "TH_NP15_GEN-APND",
            "to_node": "TH_SP15_GEN-APND",
            "path_type": "transmission_line",
            "capacity_mw": 5400.0,
            "losses_factor": 0.02,
            "hurdle_rate_usd_per_mwh": 5.50,
            "wheel_rate_usd_per_mwh": 2.25
        },
        {
            "path_id": "PATH_26",
            "path_name": "Path 26",
            "from_node": "TH_SP15_GEN-APND",
            "to_node": "PALOVRDE_2_UNITS",
            "path_type": "transmission_line",
            "capacity_mw": 2800.0,
            "losses_factor": 0.015,
            "hurdle_rate_usd_per_mwh": 3.25,
            "wheel_rate_usd_per_mwh": 1.50
        },
        {
            "path_id": "PDCI",
            "path_name": "Pacific DC Intertie",
            "from_node": "MIDWAY_500",
            "to_node": "TH_NP15_GEN-APND",
            "path_type": "intertie",
            "capacity_mw": 3100.0,
            "losses_factor": 0.04,
            "hurdle_rate_usd_per_mwh": 8.75,
            "wheel_rate_usd_per_mwh": None
        }
    ],
    "interties": [
        {
            "intertie_id": "CAISO_BPA",
            "intertie_name": "CAISO to BPA Intertie",
            "from_market": "wecc",
            "to_market": "wecc",
            "capacity_mw": 4800.0,
            "atc_ttc_ntc": {
                "atc_mw": 3200.0,
                "ttc_mw": 4800.0,
                "ntc_mw": 3200.0
            },
            "deliverability_flags": ["firm", "non_firm"]
        },
        {
            "intertie_id": "CAISO_AZ",
            "intertie_name": "CAISO to Arizona Intertie",
            "from_market": "wecc",
            "to_market": "wecc",
            "capacity_mw": 2800.0,
            "atc_ttc_ntc": {
                "atc_mw": 2400.0,
                "ttc_mw": 2800.0,
                "ntc_mw": 2400.0
            },
            "deliverability_flags": ["firm", "conditional"]
        }
    ]
}
