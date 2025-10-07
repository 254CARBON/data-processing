#!/usr/bin/env python3
"""
Integration test runner for data processing services.

This script runs comprehensive integration tests to validate the end-to-end
data flow through all services in the data processing pipeline.
"""

import asyncio
import logging
import sys
import subprocess
import time
from pathlib import Path
from typing import List, Dict, Any
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class IntegrationTestRunner:
    """Runs integration tests for the data processing pipeline."""
    
    def __init__(self, test_timeout: int = 300):
        self.test_timeout = test_timeout
        self.project_root = Path(__file__).parent.parent
        self.test_results: List[Dict[str, Any]] = []
        
    async def run_all_tests(self) -> bool:
        """Run all integration tests."""
        logger.info("🚀 Starting integration test suite")
        
        # Check prerequisites
        if not await self._check_prerequisites():
            logger.error("❌ Prerequisites not met")
            return False
            
        # Start services
        if not await self._start_services():
            logger.error("❌ Failed to start services")
            return False
            
        try:
            # Wait for services to be ready
            await self._wait_for_services()
            
            # Run tests
            test_passed = await self._run_pytest_tests()
            
            return test_passed
            
        finally:
            # Cleanup
            await self._stop_services()
            
    async def _check_prerequisites(self) -> bool:
        """Check if all prerequisites are met."""
        logger.info("🔍 Checking prerequisites...")
        
        # Check if Docker is running
        try:
            result = subprocess.run(
                ["docker", "ps"], 
                capture_output=True, 
                text=True, 
                timeout=10
            )
            if result.returncode != 0:
                logger.error("Docker is not running")
                return False
        except subprocess.TimeoutExpired:
            logger.error("Docker command timed out")
            return False
        except FileNotFoundError:
            logger.error("Docker is not installed")
            return False
            
        # Check if docker-compose.yml exists
        compose_file = self.project_root / "docker-compose.yml"
        if not compose_file.exists():
            logger.error("docker-compose.yml not found")
            return False
            
        logger.info("✅ Prerequisites check passed")
        return True
        
    async def _start_services(self) -> bool:
        """Start all services using Docker Compose."""
        logger.info("🐳 Starting services with Docker Compose...")
        
        try:
            # Start services in detached mode
            result = subprocess.run(
                ["docker-compose", "up", "-d"],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode != 0:
                logger.error(f"Failed to start services: {result.stderr}")
                return False
                
            logger.info("✅ Services started successfully")
            return True
            
        except subprocess.TimeoutExpired:
            logger.error("Service startup timed out")
            return False
        except Exception as e:
            logger.error(f"Error starting services: {e}")
            return False
            
    async def _wait_for_services(self):
        """Wait for all services to be healthy."""
        logger.info("⏳ Waiting for services to be ready...")
        
        services = [
            "kafka",
            "clickhouse", 
            "postgres",
            "redis",
            "normalization-service",
            "enrichment-service",
            "aggregation-service",
            "projection-service"
        ]
        
        max_wait_time = 120  # 2 minutes
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            all_healthy = True
            
            for service in services:
                try:
                    result = subprocess.run(
                        ["docker-compose", "ps", "--services", "--filter", "status=running"],
                        cwd=self.project_root,
                        capture_output=True,
                        text=True,
                        timeout=10
                    )
                    
                    if service not in result.stdout:
                        all_healthy = False
                        break
                        
                except Exception:
                    all_healthy = False
                    break
                    
            if all_healthy:
                logger.info("✅ All services are healthy")
                return
                
            await asyncio.sleep(5)
            
        logger.warning("⚠️ Services may not be fully ready, proceeding with tests")
        
    async def _run_pytest_tests(self) -> bool:
        """Run pytest integration tests."""
        logger.info("🧪 Running integration tests...")
        
        try:
            # Run pytest with verbose output
            result = subprocess.run([
                sys.executable, "-m", "pytest",
                "tests/integration/test_flow_e2e.py",
                "-v",
                "--tb=short",
                "--timeout=300"
            ], cwd=self.project_root, capture_output=True, text=True, timeout=self.test_timeout)
            
            # Print test output
            if result.stdout:
                logger.info("Test output:")
                print(result.stdout)
                
            if result.stderr:
                logger.warning("Test warnings/errors:")
                print(result.stderr)
                
            if result.returncode == 0:
                logger.info("✅ All integration tests passed")
                return True
            else:
                logger.error("❌ Some integration tests failed")
                return False
                
        except subprocess.TimeoutExpired:
            logger.error("❌ Tests timed out")
            return False
        except Exception as e:
            logger.error(f"❌ Error running tests: {e}")
            return False
            
    async def _stop_services(self):
        """Stop all services."""
        logger.info("🛑 Stopping services...")
        
        try:
            result = subprocess.run(
                ["docker-compose", "down"],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=60
            )
            
            if result.returncode == 0:
                logger.info("✅ Services stopped successfully")
            else:
                logger.warning(f"⚠️ Warning stopping services: {result.stderr}")
                
        except Exception as e:
            logger.warning(f"⚠️ Error stopping services: {e}")
            
    def print_summary(self, success: bool):
        """Print test summary."""
        if success:
            logger.info("🎉 Integration test suite completed successfully!")
            logger.info("✅ All services are working correctly")
            logger.info("✅ End-to-end data flow is validated")
        else:
            logger.error("💥 Integration test suite failed!")
            logger.error("❌ Check logs above for details")
            logger.error("❌ Services may need debugging")


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Run integration tests for data processing services")
    parser.add_argument("--timeout", type=int, default=300, help="Test timeout in seconds")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        
    runner = IntegrationTestRunner(test_timeout=args.timeout)
    
    try:
        success = await runner.run_all_tests()
        runner.print_summary(success)
        
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("🛑 Tests interrupted by user")
        await runner._stop_services()
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        await runner._stop_services()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

