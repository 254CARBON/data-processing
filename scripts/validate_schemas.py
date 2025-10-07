#!/usr/bin/env python3
"""
Schema validation script for event contracts.

Validates event schemas against the schema registry.
"""

import asyncio
import sys
from pathlib import Path
from typing import Dict, Any, List
import argparse
import structlog
import json
from datetime import datetime


logger = structlog.get_logger()


class SchemaValidator:
    """Event schema validator."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = structlog.get_logger("schema-validator")
        
        # Schema files
        self.schema_files = [
            "shared/schemas/events.py",
            "shared/schemas/models.py",
            "shared/schemas/registry.py"
        ]
        
        # Metrics
        self.schemas_validated = 0
        self.validation_errors = 0
        self.start_time = None
    
    async def start(self) -> None:
        """Start the schema validator."""
        self.logger.info("Starting schema validator")
        self.start_time = datetime.now()
    
    def validate_schemas(self) -> Dict[str, Any]:
        """Validate all schemas."""
        try:
            results = {
                "version": "1.0.0",
                "validated_at": datetime.now().isoformat(),
                "validator": "schema-validator",
                "schemas": {},
                "summary": {
                    "total_schemas": 0,
                    "valid_schemas": 0,
                    "invalid_schemas": 0,
                    "validation_errors": 0
                }
            }
            
            # Validate each schema file
            for schema_file in self.schema_files:
                schema_path = Path(schema_file)
                
                if not schema_path.exists():
                    self.logger.warning("Schema file not found", schema_file=schema_file)
                    continue
                
                # Validate schema
                validation_result = self._validate_schema_file(schema_path)
                results["schemas"][schema_file] = validation_result
                
                # Update summary
                results["summary"]["total_schemas"] += 1
                if validation_result["valid"]:
                    results["summary"]["valid_schemas"] += 1
                else:
                    results["summary"]["invalid_schemas"] += 1
                    results["summary"]["validation_errors"] += len(validation_result["errors"])
                
                self.schemas_validated += 1
                
                self.logger.info(
                    "Schema validation completed",
                    schema_file=schema_file,
                    valid=validation_result["valid"],
                    error_count=len(validation_result["errors"])
                )
            
            return results
            
        except Exception as e:
            self.logger.error("Schema validation failed", error=str(e), exc_info=True)
            raise
    
    def _validate_schema_file(self, schema_path: Path) -> Dict[str, Any]:
        """Validate a single schema file."""
        try:
            validation_result = {
                "file_path": str(schema_path),
                "valid": True,
                "errors": [],
                "warnings": [],
                "validated_at": datetime.now().isoformat()
            }
            
            # Read schema file
            with open(schema_path, 'r') as f:
                content = f.read()
            
            # Basic validation checks
            self._validate_syntax(content, validation_result)
            self._validate_imports(content, validation_result)
            self._validate_classes(content, validation_result)
            self._validate_functions(content, validation_result)
            
            # Set valid flag
            validation_result["valid"] = len(validation_result["errors"]) == 0
            
            return validation_result
            
        except Exception as e:
            self.logger.error(
                "Schema file validation failed",
                error=str(e),
                schema_file=str(schema_path)
            )
            return {
                "file_path": str(schema_path),
                "valid": False,
                "errors": [str(e)],
                "warnings": [],
                "validated_at": datetime.now().isoformat()
            }
    
    def _validate_syntax(self, content: str, result: Dict[str, Any]) -> None:
        """Validate Python syntax."""
        try:
            compile(content, '<string>', 'exec')
        except SyntaxError as e:
            result["errors"].append(f"Syntax error: {e}")
            self.validation_errors += 1
    
    def _validate_imports(self, content: str, result: Dict[str, Any]) -> None:
        """Validate imports."""
        lines = content.split('\n')
        
        for i, line in enumerate(lines, 1):
            line = line.strip()
            
            # Check for relative imports
            if line.startswith('from .') or line.startswith('import .'):
                result["warnings"].append(f"Line {i}: Relative import detected: {line}")
            
            # Check for wildcard imports
            if 'import *' in line:
                result["warnings"].append(f"Line {i}: Wildcard import detected: {line}")
    
    def _validate_classes(self, content: str, result: Dict[str, Any]) -> None:
        """Validate class definitions."""
        lines = content.split('\n')
        
        for i, line in enumerate(lines, 1):
            line = line.strip()
            
            # Check for class definitions
            if line.startswith('class '):
                # Check for proper docstrings
                if i + 1 < len(lines) and not lines[i].strip().startswith('"""'):
                    result["warnings"].append(f"Line {i}: Class missing docstring: {line}")
                
                # Check for proper inheritance
                if 'object' in line and line.count('(') == 1:
                    result["warnings"].append(f"Line {i}: Explicit object inheritance: {line}")
    
    def _validate_functions(self, content: str, result: Dict[str, Any]) -> None:
        """Validate function definitions."""
        lines = content.split('\n')
        
        for i, line in enumerate(lines, 1):
            line = line.strip()
            
            # Check for function definitions
            if line.startswith('def '):
                # Check for proper docstrings
                if i + 1 < len(lines) and not lines[i].strip().startswith('"""'):
                    result["warnings"].append(f"Line {i}: Function missing docstring: {line}")
                
                # Check for type hints
                if '->' not in line and 'def ' in line:
                    result["warnings"].append(f"Line {i}: Function missing return type hint: {line}")
    
    def save_results(self, results: Dict[str, Any], output_path: Path) -> None:
        """Save validation results to file."""
        try:
            # Create output directory if it doesn't exist
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Save as JSON
            with open(output_path, 'w') as f:
                json.dump(results, f, indent=2)
            
            self.logger.info("Validation results saved", output_path=str(output_path))
            
        except Exception as e:
            self.logger.error(
                "Failed to save validation results",
                error=str(e),
                output_path=str(output_path)
            )
            raise
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get validator metrics."""
        runtime = (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        
        return {
            "schemas_validated": self.schemas_validated,
            "validation_errors": self.validation_errors,
            "runtime_seconds": runtime,
        }


async def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Validate event schemas")
    parser.add_argument("--output", default="validation/results.json", help="Output file path")
    parser.add_argument("--format", choices=["json", "yaml"], default="json", help="Output format")
    parser.add_argument("--verbose", action="store_true", help="Verbose output")
    parser.add_argument("--strict", action="store_true", help="Strict validation mode")
    
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
        config = {
            "strict_mode": args.strict
        }
        
        # Create validator
        validator = SchemaValidator(config)
        await validator.start()
        
        # Validate schemas
        results = validator.validate_schemas()
        
        # Save results
        output_path = Path(args.output)
        validator.save_results(results, output_path)
        
        # Print summary
        print(f"\n--- Schema Validation Summary ---")
        print(f"Total schemas: {results['summary']['total_schemas']}")
        print(f"Valid schemas: {results['summary']['valid_schemas']}")
        print(f"Invalid schemas: {results['summary']['invalid_schemas']}")
        print(f"Validation errors: {results['summary']['validation_errors']}")
        print(f"Output file: {output_path}")
        print(f"Format: {args.format}")
        
        # List validation results
        print(f"\nValidation results:")
        for schema_file, result in results["schemas"].items():
            status = "✓" if result["valid"] else "✗"
            print(f"  {status} {schema_file}: {len(result['errors'])} errors, {len(result['warnings'])} warnings")
        
        print("--- End Summary ---\n")
        
        # Print metrics
        metrics = validator.get_metrics()
        print(f"--- Validator Metrics ---")
        print(f"Schemas validated: {metrics['schemas_validated']}")
        print(f"Validation errors: {metrics['validation_errors']}")
        print(f"Runtime: {metrics['runtime_seconds']:.2f} seconds")
        print("--- End Metrics ---\n")
        
        # Exit with error code if validation failed
        if results['summary']['invalid_schemas'] > 0:
            sys.exit(1)
        
    except Exception as e:
        logger.error("Schema validation failed", error=str(e), exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())

