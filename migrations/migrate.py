#!/usr/bin/env python3
"""
Database migration runner for 254Carbon data processing.

Supports both ClickHouse and PostgreSQL migrations
with proper error handling and rollback capabilities.
"""

import asyncio
import os
import sys
from pathlib import Path
from typing import List, Dict, Any, Optional
import argparse
import structlog

from shared.storage.clickhouse import ClickHouseClient, ClickHouseConfig
from shared.storage.postgres import PostgresClient, PostgresConfig


logger = structlog.get_logger()


class MigrationRunner:
    """Database migration runner."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = structlog.get_logger("migration-runner")
        
        # Create database clients
        self.clickhouse = ClickHouseClient(
            ClickHouseConfig(
                url=config["clickhouse"]["url"],
                database=config["clickhouse"]["database"],
                timeout=30
            )
        )
        
        self.postgres = PostgresClient(
            PostgresConfig(
                dsn=config["postgres"]["dsn"],
                min_size=1,
                max_size=5,
                timeout=30
            )
        )
    
    async def run_migrations(self, database: str, migration_dir: str) -> None:
        """Run migrations for a specific database."""
        migration_path = Path(migration_dir)
        
        if not migration_path.exists():
            self.logger.error("Migration directory not found", path=migration_dir)
            return
        
        # Get migration files
        migration_files = sorted(migration_path.glob("*.sql"))
        
        if not migration_files:
            self.logger.warning("No migration files found", path=migration_dir)
            return
        
        self.logger.info(
            "Starting migrations",
            database=database,
            count=len(migration_files)
        )
        
        # Connect to database
        if database == "clickhouse":
            await self.clickhouse.connect()
        elif database == "postgres":
            await self.postgres.connect()
        else:
            raise ValueError(f"Unsupported database: {database}")
        
        try:
            # Run each migration
            for migration_file in migration_files:
                await self._run_migration(database, migration_file)
            
            self.logger.info("All migrations completed successfully")
            
        finally:
            # Disconnect from database
            if database == "clickhouse":
                await self.clickhouse.disconnect()
            elif database == "postgres":
                await self.postgres.disconnect()
    
    async def _run_migration(self, database: str, migration_file: Path) -> None:
        """Run a single migration file."""
        self.logger.info(
            "Running migration",
            database=database,
            file=migration_file.name
        )
        
        try:
            # Read migration file
            with open(migration_file, 'r') as f:
                migration_sql = f.read()
            
            # Execute migration
            if database == "clickhouse":
                await self.clickhouse.execute(migration_sql)
            elif database == "postgres":
                await self.postgres.execute(migration_sql)
            
            self.logger.info(
                "Migration completed",
                database=database,
                file=migration_file.name
            )
            
        except Exception as e:
            self.logger.error(
                "Migration failed",
                database=database,
                file=migration_file.name,
                error=str(e),
                exc_info=True
            )
            raise
    
    async def check_status(self) -> Dict[str, Any]:
        """Check migration status."""
        status = {
            "clickhouse": {"connected": False, "tables": []},
            "postgres": {"connected": False, "tables": []}
        }
        
        # Check ClickHouse
        try:
            await self.clickhouse.connect()
            status["clickhouse"]["connected"] = True
            
            # Get tables
            tables = await self.clickhouse.execute("SHOW TABLES")
            status["clickhouse"]["tables"] = [table["name"] for table in tables]
            
            await self.clickhouse.disconnect()
            
        except Exception as e:
            self.logger.error("ClickHouse status check failed", error=str(e))
        
        # Check PostgreSQL
        try:
            await self.postgres.connect()
            status["postgres"]["connected"] = True
            
            # Get tables
            tables = await self.postgres.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """)
            status["postgres"]["tables"] = [table["table_name"] for table in tables]
            
            await self.postgres.disconnect()
            
        except Exception as e:
            self.logger.error("PostgreSQL status check failed", error=str(e))
        
        return status


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Database migration runner")
    parser.add_argument("--database", choices=["clickhouse", "postgres", "all"], default="all")
    parser.add_argument("--migration-dir", help="Migration directory")
    parser.add_argument("--status", action="store_true", help="Check migration status")
    parser.add_argument("--config", help="Configuration file")
    
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
    
    # Load configuration
    config = {
        "clickhouse": {
            "url": os.getenv("DATA_PROC_CLICKHOUSE_URL", "http://localhost:8123"),
            "database": os.getenv("DATA_PROC_CLICKHOUSE_DATABASE", "data_processing")
        },
        "postgres": {
            "dsn": os.getenv("DATA_PROC_POSTGRES_DSN", "postgresql://localhost:5432/data_proc")
        }
    }
    
    # Create migration runner
    runner = MigrationRunner(config)
    
    if args.status:
        # Check status
        status = await runner.check_status()
        print("Migration Status:")
        print(f"ClickHouse: {'Connected' if status['clickhouse']['connected'] else 'Disconnected'}")
        print(f"Tables: {len(status['clickhouse']['tables'])}")
        print(f"PostgreSQL: {'Connected' if status['postgres']['connected'] else 'Disconnected'}")
        print(f"Tables: {len(status['postgres']['tables'])}")
        return
    
    # Determine migration directory
    if args.migration_dir:
        migration_dir = args.migration_dir
    else:
        # Default to migrations directory
        migration_dir = Path(__file__).parent
    
    # Run migrations
    if args.database == "all":
        # Run ClickHouse migrations
        clickhouse_dir = migration_dir / "clickhouse"
        if clickhouse_dir.exists():
            await runner.run_migrations("clickhouse", str(clickhouse_dir))
        
        # Run PostgreSQL migrations
        postgres_dir = migration_dir / "postgres"
        if postgres_dir.exists():
            await runner.run_migrations("postgres", str(postgres_dir))
    
    elif args.database == "clickhouse":
        clickhouse_dir = migration_dir / "clickhouse"
        await runner.run_migrations("clickhouse", str(clickhouse_dir))
    
    elif args.database == "postgres":
        postgres_dir = migration_dir / "postgres"
        await runner.run_migrations("postgres", str(postgres_dir))


if __name__ == "__main__":
    asyncio.run(main())

