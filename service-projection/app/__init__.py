"""
Projection Service package.

Builds and serves derived, query-friendly views of market data (e.g.,
latest price, curve snapshots), and manages refresh/invalidation logic.

Subpackages:
- builders: Projection builders for different view types
- apis: Internal HTTP APIs for projection access/ops
- output: Writers and cache adapters
- refresh: Scheduling and executors for refresh workflows
- invalidation: Rules and triggers to keep views correct
- consumers: Kafka consumers for upstream updates and invalidations
"""

__all__ = [
    "__doc__",
]

