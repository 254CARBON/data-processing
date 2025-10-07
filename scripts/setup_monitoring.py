#!/usr/bin/env python3
"""
Setup monitoring infrastructure for data processing services.

This script sets up Prometheus, Grafana, and monitoring dashboards
for the data processing pipeline.
"""

import asyncio
import logging
import subprocess
import time
from pathlib import Path
import sys
import argparse

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class MonitoringSetup:
    """Setup monitoring infrastructure for data processing services."""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.monitoring_dir = project_root / "monitoring"
        
    async def setup_monitoring(self) -> bool:
        """Setup complete monitoring infrastructure."""
        logger.info("🚀 Setting up monitoring infrastructure")
        
        try:
            # Check prerequisites
            if not await self._check_prerequisites():
                logger.error("❌ Prerequisites not met")
                return False
                
            # Start monitoring services
            if not await self._start_monitoring_services():
                logger.error("❌ Failed to start monitoring services")
                return False
                
            # Wait for services to be ready
            await self._wait_for_services()
            
            # Verify monitoring setup
            if not await self._verify_monitoring():
                logger.error("❌ Monitoring verification failed")
                return False
                
            logger.info("✅ Monitoring infrastructure setup complete")
            return True
            
        except Exception as e:
            logger.error(f"💥 Error setting up monitoring: {e}")
            return False
            
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
            
        # Check if monitoring directory exists
        if not self.monitoring_dir.exists():
            logger.error("Monitoring directory not found")
            return False
            
        logger.info("✅ Prerequisites check passed")
        return True
        
    async def _start_monitoring_services(self) -> bool:
        """Start monitoring services using Docker Compose."""
        logger.info("🐳 Starting monitoring services...")
        
        try:
            # Start only monitoring services
            result = subprocess.run(
                ["docker-compose", "up", "-d", "prometheus", "grafana", "node-exporter", "cadvisor"],
                cwd=self.project_root,
                capture_output=True,
                text=True,
                timeout=120
            )
            
            if result.returncode != 0:
                logger.error(f"Failed to start monitoring services: {result.stderr}")
                return False
                
            logger.info("✅ Monitoring services started successfully")
            return True
            
        except subprocess.TimeoutExpired:
            logger.error("Service startup timed out")
            return False
        except Exception as e:
            logger.error(f"Error starting monitoring services: {e}")
            return False
            
    async def _wait_for_services(self):
        """Wait for monitoring services to be ready."""
        logger.info("⏳ Waiting for monitoring services to be ready...")
        
        services = [
            ("prometheus", "http://localhost:9090/-/healthy"),
            ("grafana", "http://localhost:3000/api/health"),
        ]
        
        max_wait_time = 120  # 2 minutes
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            all_ready = True
            
            for service_name, health_url in services:
                try:
                    # Try curl first
                    result = subprocess.run(
                        ["curl", "-f", health_url],
                        capture_output=True,
                        text=True,
                        timeout=5
                    )
                    
                    if result.returncode != 0:
                        # If curl fails, try docker exec
                        container_name = service_name.replace("-", "_")
                        if service_name == "prometheus":
                            docker_cmd = ["docker", "exec", "prometheus", "wget", "-qO-", "http://localhost:9090/-/healthy"]
                        elif service_name == "grafana":
                            docker_cmd = ["docker", "exec", "grafana", "wget", "-qO-", "http://localhost:3000/api/health"]
                        else:
                            all_ready = False
                            break
                            
                        result = subprocess.run(
                            docker_cmd,
                            capture_output=True,
                            text=True,
                            timeout=5
                        )
                        
                        if result.returncode != 0:
                            all_ready = False
                            break
                        
                except Exception:
                    all_ready = False
                    break
                    
            if all_ready:
                logger.info("✅ All monitoring services are ready")
                return
                
            await asyncio.sleep(5)
            
        logger.warning("⚠️ Some monitoring services may not be fully ready")
        
    async def _verify_monitoring(self) -> bool:
        """Verify monitoring setup is working."""
        logger.info("🔍 Verifying monitoring setup...")
        
        try:
            # Check Prometheus targets using docker exec
            result = subprocess.run(
                ["docker", "exec", "prometheus", "wget", "-qO-", "http://localhost:9090/api/v1/targets"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                logger.error("Failed to check Prometheus targets")
                return False
                
            # Check Grafana API using docker exec
            result = subprocess.run(
                ["docker", "exec", "grafana", "wget", "-qO-", "http://localhost:3000/api/health"],
                capture_output=True,
                text=True,
                timeout=10
            )
            
            if result.returncode != 0:
                logger.error("Failed to check Grafana API")
                return False
                
            logger.info("✅ Monitoring verification passed")
            return True
            
        except Exception as e:
            logger.error(f"Error verifying monitoring: {e}")
            return False
            
    def print_monitoring_info(self):
        """Print monitoring access information."""
        logger.info("=" * 60)
        logger.info("📊 MONITORING INFRASTRUCTURE READY")
        logger.info("=" * 60)
        logger.info("🔍 Prometheus: http://localhost:9090")
        logger.info("📈 Grafana: http://localhost:3000 (admin/admin)")
        logger.info("🖥️  Node Exporter: http://localhost:9100/metrics")
        logger.info("📦 cAdvisor: http://localhost:8080")
        logger.info("=" * 60)
        logger.info("📋 Available Dashboards:")
        logger.info("   • Data Processing Overview")
        logger.info("   • Normalization Service")
        logger.info("   • Enrichment Service")
        logger.info("   • Aggregation Service")
        logger.info("   • Projection Service")
        logger.info("=" * 60)
        logger.info("🚀 Next steps:")
        logger.info("   1. Start your data processing services")
        logger.info("   2. Visit Grafana to view dashboards")
        logger.info("   3. Check Prometheus for metrics")
        logger.info("   4. Set up alerts in Grafana")
        logger.info("=" * 60)


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Setup monitoring infrastructure")
    parser.add_argument("--project-root", type=str, default=".", help="Project root directory")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        
    project_root = Path(args.project_root).resolve()
    
    setup = MonitoringSetup(project_root)
    
    try:
        success = await setup.setup_monitoring()
        
        if success:
            setup.print_monitoring_info()
        else:
            logger.error("💥 Monitoring setup failed!")
            logger.error("❌ Check logs above for details")
            
        # Exit with appropriate code
        sys.exit(0 if success else 1)
        
    except KeyboardInterrupt:
        logger.info("🛑 Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"💥 Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
