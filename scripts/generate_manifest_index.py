#!/usr/bin/env python3
"""
Generate manifest index script for service discovery.

Creates an index of all service manifests for platform meta index.
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, Any, List
import argparse
import structlog
import yaml
import json
from datetime import datetime


logger = structlog.get_logger()


class ManifestIndexGenerator:
    """Service manifest index generator."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = structlog.get_logger("manifest-index-generator")
        
        # Service directories
        self.service_dirs = [
            "service-normalization",
            "service-enrichment", 
            "service-aggregation",
            "service-projection"
        ]
        
        # Metrics
        self.manifests_processed = 0
        self.start_time = None
    
    async def start(self) -> None:
        """Start the manifest index generator."""
        self.logger.info("Starting manifest index generator")
        self.start_time = datetime.now()
    
    def generate_index(self) -> Dict[str, Any]:
        """Generate service manifest index."""
        try:
            index = {
                "version": "1.0.0",
                "generated_at": datetime.now().isoformat(),
                "generator": "manifest-index-generator",
                "services": {}
            }
            
            # Process each service directory
            for service_dir in self.service_dirs:
                service_path = Path(service_dir)
                
                if not service_path.exists():
                    self.logger.warning("Service directory not found", service_dir=service_dir)
                    continue
                
                # Look for service manifest
                manifest_path = service_path / "service-manifest.yaml"
                
                if not manifest_path.exists():
                    self.logger.warning("Service manifest not found", service_dir=service_dir)
                    continue
                
                # Load manifest
                try:
                    with open(manifest_path, 'r') as f:
                        manifest = yaml.safe_load(f)
                    
                    # Extract service information
                    service_info = self._extract_service_info(manifest, service_dir)
                    index["services"][service_dir] = service_info
                    
                    self.manifests_processed += 1
                    
                    self.logger.info(
                        "Service manifest processed",
                        service_dir=service_dir,
                        service_name=service_info.get("name"),
                        version=service_info.get("version")
                    )
                    
                except Exception as e:
                    self.logger.error(
                        "Failed to process service manifest",
                        error=str(e),
                        service_dir=service_dir,
                        manifest_path=str(manifest_path)
                    )
                    continue
            
            return index
            
        except Exception as e:
            self.logger.error("Failed to generate manifest index", error=str(e), exc_info=True)
            raise
    
    def _extract_service_info(self, manifest: Dict[str, Any], service_dir: str) -> Dict[str, Any]:
        """Extract service information from manifest."""
        try:
            # Extract basic information
            service_info = {
                "name": manifest.get("metadata", {}).get("name", service_dir),
                "version": manifest.get("metadata", {}).get("version", "unknown"),
                "description": manifest.get("metadata", {}).get("description", ""),
                "owner": manifest.get("metadata", {}).get("owner", ""),
                "tags": manifest.get("metadata", {}).get("tags", []),
                "service": manifest.get("spec", {}).get("service", {}),
                "dependencies": manifest.get("spec", {}).get("dependencies", {}),
                "configuration": manifest.get("spec", {}).get("configuration", {}),
                "scaling": manifest.get("spec", {}).get("scaling", {}),
                "resources": manifest.get("spec", {}).get("resources", {}),
                "monitoring": manifest.get("spec", {}).get("monitoring", {}),
                "deployment": manifest.get("spec", {}).get("deployment", {}),
                "networking": manifest.get("spec", {}).get("networking", {}),
            }
            
            # Add service directory
            service_info["service_dir"] = service_dir
            
            # Add manifest path
            service_info["manifest_path"] = f"{service_dir}/service-manifest.yaml"
            
            # Add service type based on directory name
            if "normalization" in service_dir:
                service_info["service_type"] = "normalization"
            elif "enrichment" in service_dir:
                service_info["service_type"] = "enrichment"
            elif "aggregation" in service_dir:
                service_info["service_type"] = "aggregation"
            elif "projection" in service_dir:
                service_info["service_type"] = "projection"
            else:
                service_info["service_type"] = "unknown"
            
            return service_info
            
        except Exception as e:
            self.logger.error(
                "Failed to extract service info",
                error=str(e),
                service_dir=service_dir
            )
            return {
                "name": service_dir,
                "version": "unknown",
                "description": "",
                "service_type": "unknown",
                "service_dir": service_dir,
                "manifest_path": f"{service_dir}/service-manifest.yaml",
                "error": str(e)
            }
    
    def save_index(self, index: Dict[str, Any], output_path: Path) -> None:
        """Save manifest index to file."""
        try:
            # Create output directory if it doesn't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save as JSON
            with open(output_path, 'w') as f:
                json.dump(index, f, indent=2)
            
            self.logger.info("Manifest index saved", output_path=str(output_path))
            
        except Exception as e:
            self.logger.error(
                "Failed to save manifest index",
                error=str(e),
                output_path=str(output_path)
            )
            raise
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get generator metrics."""
        runtime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        return {
            "manifests_processed": self.manifests_processed,
            "runtime_seconds": runtime,
        }


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate service manifest index")
    parser.add_argument("--output", default="manifests/index.json", help="Output file path")
    parser.add_argument("--format", choices=["json", "yaml"], default="json", help="Output format")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
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
        # Configuration
        config = {}
        
        # Create generator
        generator = ManifestIndexGenerator(config)
        await generator.start()
        
        # Generate index
        index = generator.generate_index()
        
        # Save index
        output_path = Path(args.output)
        generator.save_index(index, output_path)
        
        # Print summary
        print(f"\n--- Manifest Index Generation Summary ---")
        print(f"Services processed: {generator.manifests_processed}")
        print(f"Output file: {output_path}")
        print(f"Format: {args.format}")
        
        # List services
        print(f"\nServices found:")
        for service_name, service_info in index["services"].items():
            print(f"  - {service_name}: {service_info.get('name')} v{service_info.get('version')}")
        
        print("--- End Summary ---\n")
        
        # Print metrics
        metrics = generator.get_metrics()
        print(f"--- Generator Metrics ---")
        print(f"Manifests processed: {metrics['manifests_processed']}")
        print(f"Runtime: {metrics['runtime_seconds']:.2f} seconds")
        print("--- End Metrics ---\n")
        
    except Exception as e:
        logger.error("Manifest index generation failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

