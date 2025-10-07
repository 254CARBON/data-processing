#!/usr/bin/env python3
"""
Rebuild projection script for refreshing served layer data.

Triggers projection rebuild for specific projections and date ranges.
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import argparse
import structlog

from confluent_kafka import Producer
import json
import uuid


logger = structlog.get_logger()


class ProjectionRebuilder:
    """Projection rebuild orchestration."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = structlog.get_logger("projection-rebuilder")
        
        # Create Kafka producer
        producer_config = {
            'bootstrap.servers': config['kafka']['bootstrap_servers'],
            'client.id': 'projection-rebuilder',
        }
        self.producer = Producer(producer_config)
        
        # Metrics
        self.rebuild_requests_sent = 0
        self.start_time = None
    
    async def start(self) -> None:
        """Start the projection rebuilder."""
        self.logger.info("Starting projection rebuilder")
        self.start_time = datetime.now()
    
    def send_rebuild_request(
        self,
        projection_name: str,
        since_date: Optional[datetime] = None,
        instrument_id: Optional[str] = None,
        tenant_id: str = "default"
    ) -> None:
        """Send projection rebuild request."""
        try:
            # Create rebuild request
            request = {
                "envelope": {
                    "event_type": "projection.invalidate.instrument.v1",
                    "event_id": str(uuid.uuid4()),
                    "timestamp": datetime.now().isoformat(),
                    "tenant_id": tenant_id,
                    "source": "projection-rebuilder",
                    "version": "1.0",
                    "correlation_id": str(uuid.uuid4()),
                    "causation_id": None,
                    "metadata": {}
                },
                "payload": {
                    "projection_type": projection_name,
                    "instrument_id": instrument_id,
                    "reason": "manual_rebuild",
                    "since_date": since_date.isoformat() if since_date else None,
                    "tenant_id": tenant_id,
                    "metadata": {
                        "requested_by": "projection-rebuilder",
                        "requested_at": datetime.now().isoformat()
                    }
                }
            }
            
            # Serialize request
            request_json = json.dumps(request)
            
            # Send request
            self.producer.produce(
                topic="projection.invalidate.instrument.v1",
                value=request_json,
                key=instrument_id or "all"
            )
            
            # Flush producer
            self.producer.flush()
            
            # Update metrics
            self.rebuild_requests_sent += 1
            
            self.logger.info(
                "Projection rebuild request sent",
                projection_name=projection_name,
                since_date=since_date.isoformat() if since_date else None,
                instrument_id=instrument_id,
                tenant_id=tenant_id
            )
            
        except Exception as e:
            self.logger.error(
                "Failed to send projection rebuild request",
                error=str(e),
                projection_name=projection_name,
                since_date=since_date.isoformat() if since_date else None,
                instrument_id=instrument_id
            )
            raise
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get rebuilder metrics."""
        runtime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        return {
            "rebuild_requests_sent": self.rebuild_requests_sent,
            "runtime_seconds": runtime,
        }


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Rebuild projection for served layer data")
    parser.add_argument("--name", required=True, help="Projection name to rebuild")
    parser.add_argument("--since", help="Rebuild since date (YYYY-MM-DD)")
    parser.add_argument("--instrument", help="Instrument ID to rebuild (optional)")
    parser.add_argument("--tenant", default="default", help="Tenant ID")
    parser.add_argument("--kafka-bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--dry-run", action="store_true", help="Dry run (don't send request)")
    
    args = parser.parse_args()
    
    # Setup logging
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.dev.ConsoleRenderer()
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
    
    try:
        # Parse since date if provided
        since_date = None
        if args.since:
            since_date = datetime.strptime(args.since, "%Y-%m-%d")
        
        # Configuration
        config = {
            'kafka': {
                'bootstrap_servers': args.kafka_bootstrap
            }
        }
        
        # Create rebuilder
        rebuilder = ProjectionRebuilder(config)
        await rebuilder.start()
        
        if args.dry_run:
            logger.info(
                "Dry run - would send projection rebuild request",
                projection_name=args.name,
                since_date=since_date.isoformat() if since_date else None,
                instrument_id=args.instrument,
                tenant_id=args.tenant
            )
        else:
            # Send rebuild request
            rebuilder.send_rebuild_request(
                projection_name=args.name,
                since_date=since_date,
                instrument_id=args.instrument,
                tenant_id=args.tenant
            )
            
            logger.info(
                "Projection rebuild request sent successfully",
                projection_name=args.name,
                since_date=since_date.isoformat() if since_date else None,
                instrument_id=args.instrument,
                tenant_id=args.tenant
            )
        
        # Print metrics
        metrics = rebuilder.get_metrics()
        print(f"\n--- Projection Rebuilder Metrics ---")
        print(f"Requests sent: {metrics['rebuild_requests_sent']}")
        print(f"Runtime: {metrics['runtime_seconds']:.2f} seconds")
        print("--- End Metrics ---\n")
        
    except ValueError as e:
        logger.error("Invalid date format", error=str(e))
        sys.exit(1)
    except Exception as e:
        logger.error("Projection rebuild failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

