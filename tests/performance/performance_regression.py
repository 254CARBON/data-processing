"""
Performance Regression Tests for 254Carbon Data Processing Pipeline

This module implements performance regression testing to ensure that
performance does not degrade over time.
"""

import asyncio
import json
import time
import statistics
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, asdict
from pathlib import Path
import aiohttp
import pytest
import pandas as pd
import numpy as np
from scipy import stats


@dataclass
class PerformanceMetric:
    """Performance metric data structure."""
    name: str
    value: float
    unit: str
    timestamp: datetime
    test_case: str
    environment: str
    version: str
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class PerformanceBaseline:
    """Performance baseline data structure."""
    metric_name: str
    baseline_value: float
    threshold_percentage: float
    unit: str
    test_case: str
    environment: str
    version: str
    created_at: datetime
    metadata: Optional[Dict[str, Any]] = None


class PerformanceRegressionTester:
    """Performance regression testing framework."""
    
    def __init__(self, 
                 base_url: str = "http://localhost:8080",
                 api_key: str = "test-api-key",
                 baseline_file: str = "performance_baselines.json",
                 results_file: str = "performance_results.json"):
        
        self.base_url = base_url
        self.api_key = api_key
        self.baseline_file = Path(baseline_file)
        self.results_file = Path(results_file)
        self.baselines: Dict[str, PerformanceBaseline] = {}
        self.results: List[PerformanceMetric] = []
        
        # Load existing baselines
        self.load_baselines()
    
    def load_baselines(self):
        """Load performance baselines from file."""
        if self.baseline_file.exists():
            try:
                with open(self.baseline_file, 'r') as f:
                    data = json.load(f)
                
                for baseline_data in data:
                    baseline = PerformanceBaseline(
                        metric_name=baseline_data['metric_name'],
                        baseline_value=baseline_data['baseline_value'],
                        threshold_percentage=baseline_data['threshold_percentage'],
                        unit=baseline_data['unit'],
                        test_case=baseline_data['test_case'],
                        environment=baseline_data['environment'],
                        version=baseline_data['version'],
                        created_at=datetime.fromisoformat(baseline_data['created_at']),
                        metadata=baseline_data.get('metadata')
                    )
                    self.baselines[baseline.metric_name] = baseline
                
                print(f"Loaded {len(self.baselines)} performance baselines")
            except Exception as e:
                print(f"Error loading baselines: {e}")
    
    def save_baselines(self):
        """Save performance baselines to file."""
        try:
            data = []
            for baseline in self.baselines.values():
                baseline_data = asdict(baseline)
                baseline_data['created_at'] = baseline.created_at.isoformat()
                data.append(baseline_data)
            
            with open(self.baseline_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            print(f"Saved {len(self.baselines)} performance baselines")
        except Exception as e:
            print(f"Error saving baselines: {e}")
    
    def save_results(self):
        """Save performance results to file."""
        try:
            data = []
            for result in self.results:
                result_data = asdict(result)
                result_data['timestamp'] = result.timestamp.isoformat()
                data.append(result_data)
            
            with open(self.results_file, 'w') as f:
                json.dump(data, f, indent=2)
            
            print(f"Saved {len(self.results)} performance results")
        except Exception as e:
            print(f"Error saving results: {e}")
    
    def add_baseline(self, 
                    metric_name: str,
                    baseline_value: float,
                    threshold_percentage: float = 10.0,
                    unit: str = "ms",
                    test_case: str = "default",
                    environment: str = "test",
                    version: str = "1.0.0",
                    metadata: Optional[Dict[str, Any]] = None):
        """Add a new performance baseline."""
        baseline = PerformanceBaseline(
            metric_name=metric_name,
            baseline_value=baseline_value,
            threshold_percentage=threshold_percentage,
            unit=unit,
            test_case=test_case,
            environment=environment,
            version=version,
            created_at=datetime.now(timezone.utc),
            metadata=metadata
        )
        
        self.baselines[metric_name] = baseline
        print(f"Added baseline for {metric_name}: {baseline_value}{unit} (±{threshold_percentage}%)")
    
    def record_metric(self,
                     name: str,
                     value: float,
                     unit: str = "ms",
                     test_case: str = "default",
                     environment: str = "test",
                     version: str = "1.0.0",
                     metadata: Optional[Dict[str, Any]] = None):
        """Record a performance metric."""
        metric = PerformanceMetric(
            name=name,
            value=value,
            unit=unit,
            timestamp=datetime.now(timezone.utc),
            test_case=test_case,
            environment=environment,
            version=version,
            metadata=metadata
        )
        
        self.results.append(metric)
        print(f"Recorded metric {name}: {value}{unit}")
    
    def check_regression(self, metric_name: str, current_value: float) -> Tuple[bool, Dict[str, Any]]:
        """Check if performance has regressed."""
        if metric_name not in self.baselines:
            return False, {"error": f"No baseline found for {metric_name}"}
        
        baseline = self.baselines[metric_name]
        threshold_value = baseline.baseline_value * (1 + baseline.threshold_percentage / 100)
        
        regression_detected = current_value > threshold_value
        regression_percentage = ((current_value - baseline.baseline_value) / baseline.baseline_value) * 100
        
        result = {
            "regression_detected": regression_detected,
            "baseline_value": baseline.baseline_value,
            "current_value": current_value,
            "threshold_value": threshold_value,
            "regression_percentage": regression_percentage,
            "threshold_percentage": baseline.threshold_percentage,
            "unit": baseline.unit,
            "test_case": baseline.test_case,
            "environment": baseline.environment,
            "version": baseline.version
        }
        
        return regression_detected, result
    
    async def measure_response_time(self, 
                                  method: str,
                                  endpoint: str,
                                  data: Optional[Dict[str, Any]] = None,
                                  headers: Optional[Dict[str, str]] = None) -> float:
        """Measure response time for an HTTP request."""
        url = f"{self.base_url}{endpoint}"
        
        if headers is None:
            headers = {}
        
        if self.api_key:
            headers['X-API-Key'] = self.api_key
        
        start_time = time.time()
        
        async with aiohttp.ClientSession() as session:
            if method.upper() == 'GET':
                async with session.get(url, headers=headers) as response:
                    await response.text()
            elif method.upper() == 'POST':
                async with session.post(url, json=data, headers=headers) as response:
                    await response.text()
            elif method.upper() == 'PUT':
                async with session.put(url, json=data, headers=headers) as response:
                    await response.text()
            elif method.upper() == 'DELETE':
                async with session.delete(url, headers=headers) as response:
                    await response.text()
        
        end_time = time.time()
        return (end_time - start_time) * 1000  # Convert to milliseconds
    
    async def measure_throughput(self, 
                               endpoint: str,
                               data: Optional[Dict[str, Any]] = None,
                               duration: int = 10,
                               concurrency: int = 10) -> float:
        """Measure throughput (requests per second)."""
        url = f"{self.base_url}{endpoint}"
        headers = {'X-API-Key': self.api_key} if self.api_key else {}
        
        start_time = time.time()
        request_count = 0
        
        async def make_request():
            nonlocal request_count
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=data, headers=headers) as response:
                    await response.text()
                    request_count += 1
        
        # Run concurrent requests
        tasks = []
        for _ in range(concurrency):
            task = asyncio.create_task(self._run_requests_for_duration(make_request, duration))
            tasks.append(task)
        
        await asyncio.gather(*tasks)
        
        end_time = time.time()
        actual_duration = end_time - start_time
        throughput = request_count / actual_duration
        
        return throughput
    
    async def _run_requests_for_duration(self, request_func, duration: int):
        """Run requests for a specified duration."""
        end_time = time.time() + duration
        
        while time.time() < end_time:
            await request_func()
            await asyncio.sleep(0.001)  # Small delay to prevent overwhelming
    
    async def measure_latency_percentiles(self, 
                                        endpoint: str,
                                        data: Optional[Dict[str, Any]] = None,
                                        sample_count: int = 100) -> Dict[str, float]:
        """Measure latency percentiles."""
        latencies = []
        
        for _ in range(sample_count):
            latency = await self.measure_response_time('POST', endpoint, data)
            latencies.append(latency)
        
        latencies.sort()
        
        percentiles = {
            'p50': np.percentile(latencies, 50),
            'p90': np.percentile(latencies, 90),
            'p95': np.percentile(latencies, 95),
            'p99': np.percentile(latencies, 99),
            'p99.9': np.percentile(latencies, 99.9),
            'mean': statistics.mean(latencies),
            'median': statistics.median(latencies),
            'std_dev': statistics.stdev(latencies) if len(latencies) > 1 else 0,
            'min': min(latencies),
            'max': max(latencies)
        }
        
        return percentiles
    
    async def run_performance_tests(self) -> Dict[str, Any]:
        """Run comprehensive performance tests."""
        test_results = {}
        
        # Test 1: Single market data processing
        print("Running single market data processing test...")
        market_data = {
            'instrument_id': 'AAPL',
            'price': 150.25,
            'volume': 1000000,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'bid': 150.20,
            'ask': 150.30,
            'last_trade_price': 150.25
        }
        
        latency = await self.measure_response_time('POST', '/api/market-data', market_data)
        test_results['single_market_data_latency'] = latency
        self.record_metric('single_market_data_latency', latency, 'ms', 'single_market_data')
        
        # Test 2: Batch market data processing
        print("Running batch market data processing test...")
        batch_data = [market_data for _ in range(10)]
        
        batch_latency = await self.measure_response_time('POST', '/api/market-data/batch', batch_data)
        test_results['batch_market_data_latency'] = batch_latency
        self.record_metric('batch_market_data_latency', batch_latency, 'ms', 'batch_market_data')
        
        # Test 3: Throughput test
        print("Running throughput test...")
        throughput = await self.measure_throughput('/api/market-data', market_data, duration=10, concurrency=20)
        test_results['throughput'] = throughput
        self.record_metric('throughput', throughput, 'req/s', 'throughput')
        
        # Test 4: Latency percentiles
        print("Running latency percentiles test...")
        percentiles = await self.measure_latency_percentiles('/api/market-data', market_data, sample_count=100)
        test_results['latency_percentiles'] = percentiles
        
        for percentile, value in percentiles.items():
            self.record_metric(f'latency_{percentile}', value, 'ms', 'latency_percentiles')
        
        # Test 5: Health check performance
        print("Running health check performance test...")
        health_latency = await self.measure_response_time('GET', '/health')
        test_results['health_check_latency'] = health_latency
        self.record_metric('health_check_latency', health_latency, 'ms', 'health_check')
        
        # Test 6: Metrics endpoint performance
        print("Running metrics endpoint performance test...")
        metrics_latency = await self.measure_response_time('GET', '/metrics')
        test_results['metrics_latency'] = metrics_latency
        self.record_metric('metrics_latency', metrics_latency, 'ms', 'metrics')
        
        return test_results
    
    def analyze_results(self) -> Dict[str, Any]:
        """Analyze performance results and check for regressions."""
        analysis = {
            'total_tests': len(self.results),
            'regressions_detected': [],
            'performance_summary': {},
            'recommendations': []
        }
        
        # Group results by test case
        test_cases = {}
        for result in self.results:
            if result.test_case not in test_cases:
                test_cases[result.test_case] = []
            test_cases[result.test_case].append(result)
        
        # Analyze each test case
        for test_case, results in test_cases.items():
            analysis['performance_summary'][test_case] = {}
            
            for result in results:
                regression_detected, regression_info = self.check_regression(result.name, result.value)
                
                if regression_detected:
                    analysis['regressions_detected'].append({
                        'metric': result.name,
                        'regression_info': regression_info
                    })
                
                analysis['performance_summary'][test_case][result.name] = {
                    'value': result.value,
                    'unit': result.unit,
                    'regression_detected': regression_detected,
                    'regression_info': regression_info
                }
        
        # Generate recommendations
        if analysis['regressions_detected']:
            analysis['recommendations'].append("Performance regressions detected. Review recent changes.")
            analysis['recommendations'].append("Consider optimizing slow endpoints.")
            analysis['recommendations'].append("Monitor system resources during peak load.")
        else:
            analysis['recommendations'].append("No performance regressions detected.")
            analysis['recommendations'].append("Performance is within acceptable limits.")
        
        return analysis
    
    def generate_report(self) -> str:
        """Generate a performance regression report."""
        analysis = self.analyze_results()
        
        report = f"""
Performance Regression Test Report
==================================

Generated: {datetime.now(timezone.utc).isoformat()}
Total Tests: {analysis['total_tests']}
Regressions Detected: {len(analysis['regressions_detected'])}

Performance Summary:
-------------------
"""
        
        for test_case, metrics in analysis['performance_summary'].items():
            report += f"\n{test_case}:\n"
            for metric_name, metric_info in metrics.items():
                status = "❌ REGRESSION" if metric_info['regression_detected'] else "✅ OK"
                report += f"  {metric_name}: {metric_info['value']}{metric_info['unit']} {status}\n"
        
        if analysis['regressions_detected']:
            report += "\nRegressions Detected:\n"
            report += "-------------------\n"
            for regression in analysis['regressions_detected']:
                info = regression['regression_info']
                report += f"  {regression['metric']}: {info['regression_percentage']:.2f}% regression\n"
                report += f"    Baseline: {info['baseline_value']}{info['unit']}\n"
                report += f"    Current: {info['current_value']}{info['unit']}\n"
                report += f"    Threshold: {info['threshold_value']}{info['unit']}\n"
        
        report += "\nRecommendations:\n"
        report += "---------------\n"
        for recommendation in analysis['recommendations']:
            report += f"  • {recommendation}\n"
        
        return report


# Pytest integration
class TestPerformanceRegression:
    """Pytest test class for performance regression testing."""
    
    @pytest.fixture(scope="class")
    async def performance_tester(self):
        """Create performance tester instance."""
        tester = PerformanceRegressionTester()
        return tester
    
    @pytest.mark.asyncio
    async def test_single_market_data_performance(self, performance_tester):
        """Test single market data processing performance."""
        market_data = {
            'instrument_id': 'AAPL',
            'price': 150.25,
            'volume': 1000000,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'bid': 150.20,
            'ask': 150.30,
            'last_trade_price': 150.25
        }
        
        latency = await performance_tester.measure_response_time('POST', '/api/market-data', market_data)
        performance_tester.record_metric('single_market_data_latency', latency, 'ms', 'single_market_data')
        
        # Check for regression
        regression_detected, regression_info = performance_tester.check_regression('single_market_data_latency', latency)
        
        if regression_detected:
            pytest.fail(f"Performance regression detected: {regression_info['regression_percentage']:.2f}% regression")
        
        assert latency < 100, f"Latency too high: {latency}ms"
    
    @pytest.mark.asyncio
    async def test_batch_market_data_performance(self, performance_tester):
        """Test batch market data processing performance."""
        market_data = {
            'instrument_id': 'AAPL',
            'price': 150.25,
            'volume': 1000000,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'bid': 150.20,
            'ask': 150.30,
            'last_trade_price': 150.25
        }
        
        batch_data = [market_data for _ in range(10)]
        latency = await performance_tester.measure_response_time('POST', '/api/market-data/batch', batch_data)
        performance_tester.record_metric('batch_market_data_latency', latency, 'ms', 'batch_market_data')
        
        # Check for regression
        regression_detected, regression_info = performance_tester.check_regression('batch_market_data_latency', latency)
        
        if regression_detected:
            pytest.fail(f"Performance regression detected: {regression_info['regression_percentage']:.2f}% regression")
        
        assert latency < 200, f"Batch latency too high: {latency}ms"
    
    @pytest.mark.asyncio
    async def test_throughput_performance(self, performance_tester):
        """Test throughput performance."""
        market_data = {
            'instrument_id': 'AAPL',
            'price': 150.25,
            'volume': 1000000,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'bid': 150.20,
            'ask': 150.30,
            'last_trade_price': 150.25
        }
        
        throughput = await performance_tester.measure_throughput('/api/market-data', market_data, duration=5, concurrency=10)
        performance_tester.record_metric('throughput', throughput, 'req/s', 'throughput')
        
        # Check for regression
        regression_detected, regression_info = performance_tester.check_regression('throughput', throughput)
        
        if regression_detected:
            pytest.fail(f"Throughput regression detected: {regression_info['regression_percentage']:.2f}% regression")
        
        assert throughput > 100, f"Throughput too low: {throughput} req/s"
    
    @pytest.mark.asyncio
    async def test_latency_percentiles(self, performance_tester):
        """Test latency percentiles."""
        market_data = {
            'instrument_id': 'AAPL',
            'price': 150.25,
            'volume': 1000000,
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'bid': 150.20,
            'ask': 150.30,
            'last_trade_price': 150.25
        }
        
        percentiles = await performance_tester.measure_latency_percentiles('/api/market-data', market_data, sample_count=50)
        
        # Record percentiles
        for percentile, value in percentiles.items():
            performance_tester.record_metric(f'latency_{percentile}', value, 'ms', 'latency_percentiles')
        
        # Check P95 latency
        p95_latency = percentiles['p95']
        regression_detected, regression_info = performance_tester.check_regression('latency_p95', p95_latency)
        
        if regression_detected:
            pytest.fail(f"P95 latency regression detected: {regression_info['regression_percentage']:.2f}% regression")
        
        assert p95_latency < 200, f"P95 latency too high: {p95_latency}ms"


# Command-line interface
async def main():
    """Main function for running performance regression tests."""
    import argparse
    
    parser = argparse.ArgumentParser(description='254Carbon Data Processing Pipeline Performance Regression Tests')
    parser.add_argument('--base-url', default='http://localhost:8080', help='Base URL for the API')
    parser.add_argument('--api-key', default='test-api-key', help='API key for authentication')
    parser.add_argument('--baseline-file', default='performance_baselines.json', help='Baseline file path')
    parser.add_argument('--results-file', default='performance_results.json', help='Results file path')
    parser.add_argument('--update-baselines', action='store_true', help='Update baselines with current results')
    parser.add_argument('--generate-report', action='store_true', help='Generate performance report')
    
    args = parser.parse_args()
    
    # Create performance tester
    tester = PerformanceRegressionTester(
        base_url=args.base_url,
        api_key=args.api_key,
        baseline_file=args.baseline_file,
        results_file=args.results_file
    )
    
    print("Running performance regression tests...")
    
    # Run performance tests
    test_results = await tester.run_performance_tests()
    
    # Save results
    tester.save_results()
    
    # Generate report
    if args.generate_report:
        report = tester.generate_report()
        print(report)
        
        # Save report to file
        with open('performance_regression_report.txt', 'w') as f:
            f.write(report)
    
    # Update baselines if requested
    if args.update_baselines:
        print("Updating baselines with current results...")
        for result in tester.results:
            tester.add_baseline(
                metric_name=result.name,
                baseline_value=result.value,
                threshold_percentage=10.0,
                unit=result.unit,
                test_case=result.test_case,
                environment=result.environment,
                version=result.version
            )
        tester.save_baselines()
    
    print("Performance regression tests completed!")


if __name__ == "__main__":
    asyncio.run(main())
