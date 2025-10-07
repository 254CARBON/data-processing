#!/usr/bin/env python3
"""
Backfill window script for reprocessing historical data.

Triggers backfill processing for a specific date range and instrument.
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


class BackfillOrchestrator:
    """Backfill orchestration for historical data reprocessing."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = structlog.get_logger("backfill-orchestrator")
        
        # Create Kafka producer
        producer_config = {
            'bootstrap.servers': config['kafka']['bootstrap_servers'],
            'client.id': 'backfill-orchestrator',
        }
        self.producer = Producer(producer_config)
        
        # Metrics
        self.backfill_requests_sent = 0
        self.start_time = None
    
    async def start(self) -> None:
        """Start the backfill orchestrator."""
        self.logger.info("Starting backfill orchestrator")
        self.start_time = datetime.now()
    
    def send_backfill_request(
        self,
        from_date: datetime,
        to_date: datetime,
        instrument_id: Optional[str] = None,
        tenant_id: str = "default"
    ) -> None:
        """Send backfill request."""
        try:
            # Create backfill request
            request = {
                "envelope": {
                    "event_type": "processing.backfill.request.v1",
                    "event_id": str(uuid.uuid4()),
                    "timestamp": datetime.now().isoformat(),
                    "tenant_id": tenant_id,
                    "source": "backfill-orchestrator",
                    "version": "1.0",
                    "correlation_id": str(uuid.uuid4()),
                    "causation_id": None,
                    "metadata": {}
                },
                "payload": {
                    "from_date": from_date.isoformat(),
                    "to_date": to_date.isoformat(),
                    "instrument_id": instrument_id,
                    "tenant_id": tenant_id,
                    "priority": "normal",
                    "reason": "manual_backfill",
                    "metadata": {
                        "requested_by": "backfill-orchestrator",
                        "requested_at": datetime.now().isoformat()
                    }
                }
            }
            
            # Serialize request
            request_json = json.dumps(request)
            
            # Send request
            self.producer.produce(
                topic="processing.backfill.request.v1",
                value=request_json,
                key=instrument_id or "all"
            )
            
            # Flush producer
            self.producer.flush()
            
            # Update metrics
            self.backfill_requests_sent += 1
            
            self.logger.info(
                "Backfill request sent",
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat(),
                instrument_id=instrument_id,
                tenant_id=tenant_id
            )
            
        except Exception as e:
            self.logger.error(
                "Failed to send backfill request",
                error=str(e),
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat(),
                instrument_id=instrument_id
            )
            raise
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get orchestrator metrics."""
        runtime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        return {
            "backfill_requests_sent": self.backfill_requests_sent,
            "runtime_seconds": runtime,
        }


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Backfill window for historical data reprocessing")
    parser.add_argument("--from", dest="from_date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--instrument", help="Instrument ID to backfill (optional)")
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
        # Parse dates
        from_date = datetime.strptime(args.from_date, "%Y-%m-%d")
        to_date = datetime.strptime(args.to_date, "%Y-%m-%d")
        
        # Validate dates
        if from_date >= to_date:
            logger.error("From date must be before to date")
            sys.exit(1)
        
        if (to_date - from_date).days > 365:
            logger.error("Date range cannot exceed 365 days")
            sys.exit(1)
        
        # Configuration
        config = {
            'kafka': {
                'bootstrap_servers': args.kafka_bootstrap
            }
        }
        
        # Create orchestrator
        orchestrator = BackfillOrchestrator(config)
        await orchestrator.start()
        
        if args.dry_run:
            logger.info(
                "Dry run - would send backfill request",
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat(),
                instrument_id=args.instrument,
                tenant_id=args.tenant
            )
        else:
            # Send backfill request
            orchestrator.send_backfill_request(
                from_date=from_date,
                to_date=to_date,
                instrument_id=args.instrument,
                tenant_id=args.tenant
            )
            
            logger.info(
                "Backfill request sent successfully",
                from_date=from_date.isoformat(),
                to_date=to_date.isoformat(),
                instrument_id=args.instrument,
                tenant_id=args.tenant
            )
        
        # Print metrics
        metrics = orchestrator.get_metrics()
        print(f"\n--- Backfill Orchestrator Metrics ---")
        print(f"Requests sent: {metrics['backfill_requests_sent']}")
        print(f"Runtime: {metrics['runtime_seconds']:.2f} seconds")
        print("--- End Metrics ---\n")
        
    except ValueError as e:
        logger.error("Invalid date format", error=str(e))
        sys.exit(1)
    except Exception as e:
        logger.error("Backfill orchestration failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

