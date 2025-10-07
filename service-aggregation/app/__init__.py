"""
Aggregation Service package.

Provides components for aggregating market data into higher-level
representations used downstream by projections and analytics.

Subpackages:
- bars: OHLC bar construction utilities
- curves: Forward curve calculation helpers
- calculators: Spread, basis, and rolling statistics calculators
- consumers: Kafka consumers for enriched tick streams
- output: Producers/Writers for publishing and persisting results
- schedulers: Batch scheduling and watermark management
- jobs: Background and maintenance job runners

The main service entrypoint is `app.main.AggregationService`.
"""

__all__ = [
    "__doc__",
]

