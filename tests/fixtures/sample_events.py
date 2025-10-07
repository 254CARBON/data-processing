"""Sample event generators for testing."""

import json
from datetime import datetime, timedelta
from typing import Dict, Any, List
from shared.schemas.events import RawMarketTick, NormalizedMarketTick, EnrichedMarketTick


class SampleEventGenerator:
    """Generator for sample events for testing."""
    
    @staticmethod
    def generate_raw_ticks(count: int = 100) -> List[Dict[str, Any]]:
        """Generate raw market ticks."""
        ticks = []
        base_time = datetime.now()
        
        for i in range(count):
            tick = {
                'instrument_id': f'TEST_INSTRUMENT_{i % 10}',
                'ts': (base_time + timedelta(seconds=i)).isoformat(),
                'price': 100.0 + (i * 0.1),
                'volume': 1000 + i,
                'source': 'test_source',
                'raw_data': f'raw_data_{i}',
                'tenant_id': 'test_tenant'
            }
            ticks.append(tick)
            
        return ticks
        
    @staticmethod
    def generate_normalized_ticks(count: int = 100) -> List[Dict[str, Any]]:
        """Generate normalized market ticks."""
        ticks = []
        base_time = datetime.now()
        
        for i in range(count):
            tick = {
                'instrument_id': f'TEST_INSTRUMENT_{i % 10}',
                'ts': (base_time + timedelta(seconds=i)).isoformat(),
                'price': 100.0 + (i * 0.1),
                'volume': 1000 + i,
                'quality_flags': 0,
                'tenant_id': 'test_tenant'
            }
            ticks.append(tick)
            
        return ticks
        
    @staticmethod
    def generate_enriched_ticks(count: int = 100) -> List[Dict[str, Any]]:
        """Generate enriched market ticks."""
        ticks = []
        base_time = datetime.now()
        
        for i in range(count):
            tick = {
                'instrument_id': f'TEST_INSTRUMENT_{i % 10}',
                'ts': (base_time + timedelta(seconds=i)).isoformat(),
                'price': 100.0 + (i * 0.1),
                'volume': 1000 + i,
                'quality_flags': 0,
                'tenant_id': 'test_tenant',
                'enrichment_data': {
                    'taxonomy': 'commodity',
                    'region': 'us',
                    'metadata': f'metadata_{i}'
                }
            }
            ticks.append(tick)
            
        return ticks
        
    @staticmethod
    def generate_ohlc_bars(count: int = 100) -> List[Dict[str, Any]]:
        """Generate OHLC bars."""
        bars = []
        base_time = datetime.now()
        
        for i in range(count):
            bar = {
                'instrument_id': f'TEST_INSTRUMENT_{i % 10}',
                'ts': (base_time + timedelta(minutes=i * 5)).isoformat(),
                'interval': '5m',
                'open_price': 100.0 + (i * 0.1),
                'high_price': 100.0 + (i * 0.1) + 0.5,
                'low_price': 100.0 + (i * 0.1) - 0.5,
                'close_price': 100.0 + (i * 0.1) + 0.2,
                'volume': 1000 + i,
                'trade_count': 10 + i,
                'quality_flags': 0,
                'tenant_id': 'test_tenant'
            }
            bars.append(bar)
            
        return bars
        
    @staticmethod
    def generate_forward_curves(count: int = 100) -> List[Dict[str, Any]]:
        """Generate forward curves."""
        curves = []
        base_time = datetime.now()
        
        for i in range(count):
            curve = {
                'instrument_id': f'TEST_INSTRUMENT_{i % 10}',
                'ts': (base_time + timedelta(minutes=i * 10)).isoformat(),
                'horizon': '1m',
                'curve_points': [
                    {
                        'time': (base_time + timedelta(days=30)).isoformat(),
                        'price': 100.0 + (i * 0.1),
                        'confidence': 0.9
                    },
                    {
                        'time': (base_time + timedelta(days=60)).isoformat(),
                        'price': 100.0 + (i * 0.1) + 0.5,
                        'confidence': 0.8
                    }
                ],
                'interpolation_method': 'linear',
                'quality_flags': 0,
                'tenant_id': 'test_tenant'
            }
            curves.append(curve)
            
        return curves
        
    @staticmethod
    def generate_spread_data(count: int = 100) -> List[Dict[str, Any]]:
        """Generate spread data."""
        spreads = []
        base_time = datetime.now()
        
        for i in range(count):
            spread = {
                'instrument_id': f'TEST_INSTRUMENT_{i % 10}',
                'ts': (base_time + timedelta(seconds=i)).isoformat(),
                'spread_type': 'bid_ask',
                'spread_value': 0.1 + (i * 0.01),
                'quality_flags': 0,
                'tenant_id': 'test_tenant'
            }
            spreads.append(spread)
            
        return spreads
        
    @staticmethod
    def generate_basis_data(count: int = 100) -> List[Dict[str, Any]]:
        """Generate basis data."""
        basis_data = []
        base_time = datetime.now()
        
        for i in range(count):
            basis = {
                'instrument_id': f'TEST_INSTRUMENT_{i % 10}',
                'ts': (base_time + timedelta(seconds=i)).isoformat(),
                'basis_type': 'spot_future',
                'basis_value': 0.5 + (i * 0.01),
                'quality_flags': 0,
                'tenant_id': 'test_tenant'
            }
            basis_data.append(basis)
            
        return basis_data
        
    @staticmethod
    def generate_rolling_stats(count: int = 100) -> List[Dict[str, Any]]:
        """Generate rolling statistics."""
        stats = []
        base_time = datetime.now()
        
        for i in range(count):
            stat = {
                'instrument_id': f'TEST_INSTRUMENT_{i % 10}',
                'ts': (base_time + timedelta(seconds=i)).isoformat(),
                'stat_type': 'mean',
                'stat_value': 100.0 + (i * 0.1),
                'window_size': 10,
                'quality_flags': 0,
                'tenant_id': 'test_tenant'
            }
            stats.append(stat)
            
        return stats
        
    @staticmethod
    def generate_projection_updates(count: int = 100) -> List[Dict[str, Any]]:
        """Generate projection updates."""
        updates = []
        base_time = datetime.now()
        
        for i in range(count):
            update = {
                'instrument_id': f'TEST_INSTRUMENT_{i % 10}',
                'ts': (base_time + timedelta(seconds=i)).isoformat(),
                'projection_type': 'latest_price',
                'data': {
                    'price': 100.0 + (i * 0.1),
                    'volume': 1000 + i
                },
                'quality_flags': 0,
                'tenant_id': 'test_tenant'
            }
            updates.append(update)
            
        return updates
        
    @staticmethod
    def generate_malformed_events(count: int = 10) -> List[Dict[str, Any]]:
        """Generate malformed events for error testing."""
        events = []
        
        # Missing required fields
        events.append({
            'instrument_id': 'TEST_INSTRUMENT',
            'ts': 'invalid_timestamp',
            'price': 'not_a_number'
        })
        
        # Invalid data types
        events.append({
            'instrument_id': 123,  # Should be string
            'ts': datetime.now().isoformat(),
            'price': 100.0,
            'volume': 'not_a_number'  # Should be number
        })
        
        # Missing fields
        events.append({
            'instrument_id': 'TEST_INSTRUMENT',
            'ts': datetime.now().isoformat()
            # Missing price and volume
        })
        
        return events

