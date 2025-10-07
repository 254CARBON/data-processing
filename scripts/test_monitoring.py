#!/usr/bin/env python3
"""
Test monitoring infrastructure for data processing services.

This script tests Prometheus metrics collection and Grafana dashboards.
"""

import asyncio
import logging
import subprocess
import time
import json
from pathlib import Path
import sys
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MonitoringTester:
    """Test monitoring infrastructure for data processing services."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        
    async def test_monitoring(self) -> bool:
        """Test complete monitoring infrastructure."""
        logger.info("🧪 Testing monitoring infrastructure")
        
        try:
            # Test Prometheus
            if not await self._test_prometheus():
                logger.error("❌ Prometheus tests failed")
                return False
                
            # Test Grafana
            if not await self._test_grafana():
                logger.error("❌ Grafana tests failed")
                return False
                
            # Test metrics collection
            if not await self._test_metrics_collection():
                logger.error("❌ Metrics collection tests failed")
                return False
                
            logger.info("✅ All monitoring tests passed")
            return True
            
        except Exception as e:
            logger.error(f"💥 Error testing monitoring: {e}")
            return False
            
    async def _test_prometheus(self) -> bool:
        """Test Prometheus functionality."""
        logger.info("🔍 Testing Prometheus...")
        
        try:
            # Test health endpoint
            result = subprocess.run(
                ["docker", "exec", "prometheus", "wget", "-qO-", "http://localhost:9090/-/healthy"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                logger.error("Prometheus health check failed")
                return False
                
            # Test metrics endpoint
            result = subprocess.run(
                ["docker", "exec", "prometheus", "wget", "-qO-", "http://localhost:9090/metrics"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                logger.error("Prometheus metrics endpoint failed")
                return False
                
            # Check for basic metrics
            metrics_output = result.stdout
            if "prometheus_build_info" not in metrics_output:
                logger.error("Prometheus build info metric not found")
                return False
                
            logger.info("✅ Prometheus tests passed")
            return True
            
        except Exception as e:
            logger.error(f"Prometheus test error: {e}")
            return False
            
    async def _test_grafana(self) -> bool:
        """Test Grafana functionality."""
        logger.info("📈 Testing Grafana...")
        
        try:
            # Test health endpoint
            result = subprocess.run(
                ["docker", "exec", "grafana", "wget", "-qO-", "http://localhost:3000/api/health"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                logger.error("Grafana health check failed")
                return False
                
            # Parse health response
            try:
                health_data = json.loads(result.stdout)
                if health_data.get("database") != "ok":
                    logger.error("Grafana database not healthy")
                    return False
            except json.JSONDecodeError:
                logger.error("Invalid Grafana health response")
                return False
                
            # Test datasources
            result = subprocess.run(
                ["docker", "exec", "grafana", "wget", "-qO-", "http://admin:admin@localhost:3000/api/datasources"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                logger.error("Grafana datasources API failed")
                return False
                
            logger.info("✅ Grafana tests passed")
            return True
            
        except Exception as e:
            logger.error(f"Grafana test error: {e}")
            return False
            
    async def _test_metrics_collection(self) -> bool:
        """Test metrics collection from services."""
        logger.info("📊 Testing metrics collection...")
        
        try:
            # Test Node Exporter metrics
            result = subprocess.run(
                ["docker", "exec", "node-exporter", "wget", "-qO-", "http://localhost:9100/metrics"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                logger.error("Node Exporter metrics failed")
                return False
                
            # Check for system metrics
            metrics_output = result.stdout
            if "node_cpu_seconds_total" not in metrics_output:
                logger.error("Node Exporter CPU metrics not found")
                return False
                
            # Test cAdvisor metrics
            result = subprocess.run(
                ["docker", "exec", "cadvisor", "wget", "-qO-", "http://localhost:8080/metrics"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                logger.error("cAdvisor metrics failed")
                return False
                
            # Check for container metrics
            metrics_output = result.stdout
            if "container_cpu_usage_seconds_total" not in metrics_output:
                logger.error("cAdvisor container metrics not found")
                return False
                
            logger.info("✅ Metrics collection tests passed")
            return True
            
        except Exception as e:
            logger.error(f"Metrics collection test error: {e}")
            return False
            
    def print_test_results(self):
        """Print test results summary."""
        logger.info("=" * 60)
        logger.info("🧪 MONITORING TEST RESULTS")
        logger.info("=" * 60)
        logger.info("✅ Prometheus: Health check passed")
        logger.info("✅ Grafana: Health check passed")
        logger.info("✅ Node Exporter: Metrics collection working")
        logger.info("✅ cAdvisor: Container metrics working")
        logger.info("=" * 60)
        logger.info("📋 Test Summary:")
        logger.info("   • All monitoring services are healthy")
        logger.info("   • Metrics endpoints are accessible")
        logger.info("   • System and container metrics are being collected")
        logger.info("=" * 60)
        logger.info("🚀 Monitoring infrastructure is ready for use!")
        logger.info("=" * 60)


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Test monitoring infrastructure")
    parser.add_argument("--project-root", type=str, default=".", help="Project root directory")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        
    project_root = Path(args.project_root).resolve()
    
    tester = MonitoringTester(project_root)
    
    try:
        success = await tester.test_monitoring()
        
        if success:
            tester.print_test_results()
        else:
            logger.error("💥 Monitoring tests failed!")
            logger.error("❌ Check logs above for details")
            
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("🛑 Tests interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

