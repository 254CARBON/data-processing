"""
Scalability Tests for 254Carbon Data Processing Pipeline

This module implements comprehensive scalability testing to ensure the system
can handle increasing load and scale horizontally.
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
from concurrent.futures import ThreadPoolExecutor
import subprocess
import kubernetes
from kubernetes import client, config


@dataclass
class ScalabilityTestResult:
    """Scalability test result data structure."""
    test_name: str
    test_type: str
    load_level: int
    duration: float
    success_rate: float
    average_latency: float
    p95_latency: float
    p99_latency: float
    throughput: float
    error_count: int
    total_requests: int
    timestamp: datetime
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class ScalabilityTestConfig:
    """Scalability test configuration."""
    base_url: str = "http://localhost:8080"
    api_key: str = "test-api-key"
    test_duration: int = 300  # 5 minutes
    ramp_up_time: int = 60    # 1 minute
    ramp_down_time: int = 60  # 1 minute
    max_load: int = 1000      # Maximum concurrent users
    step_size: int = 100      # Load increment step
    min_load: int = 10        # Minimum load
    target_success_rate: float = 0.95
    target_latency_p95: float = 200.0  # milliseconds
    target_throughput: float = 1000.0   # requests per second


class ScalabilityTester:
    """Scalability testing framework."""
    
    def __init__(self, config: Optional[ScalabilityTestConfig] = None):
        self.config = config or ScalabilityTestConfig()
        self.results: List[ScalabilityTestResult] = []
        self.k8s_client = None
        
        # Initialize Kubernetes client if available
        try:
            config.load_incluster_config()
            self.k8s_client = client.ApiClient()
            print("Kubernetes client initialized")
        except:
            try:
                config.load_kube_config()
                self.k8s_client = client.ApiClient()
                print("Kubernetes client initialized with local config")
            except:
                print("Kubernetes client not available")
    
    async def generate_market_data(self) -> Dict[str, Any]:
        """Generate random market data."""
        instruments = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META', 'NVDA', 'NFLX']
        instrument = np.random.choice(instruments)
        base_price = np.random.uniform(100, 500)
        price_variation = np.random.uniform(-10, 10)
        price = base_price + price_variation
        
        return {
            'instrument_id': instrument,
            'price': round(price, 2),
            'volume': np.random.randint(1000, 100000),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'bid': round(price - np.random.uniform(1, 5), 2),
            'ask': round(price + np.random.uniform(1, 5), 2),
            'last_trade_price': round(price, 2),
            'market': 'NASDAQ',
            'currency': 'USD'
        }
    
    async def make_request(self, session: aiohttp.ClientSession, url: str, data: Dict[str, Any]) -> Tuple[bool, float, int]:
        """Make a single request and return success, latency, and status code."""
        start_time = time.time()
        
        try:
            async with session.post(url, json=data) as response:
                await response.text()
                end_time = time.time()
                latency = (end_time - start_time) * 1000  # Convert to milliseconds
                return response.status == 200, latency, response.status
        except Exception as e:
            end_time = time.time()
            latency = (end_time - start_time) * 1000
            return False, latency, 0
    
    async def run_load_test(self, 
                           load_level: int, 
                           duration: int,
                           test_name: str = "load_test") -> ScalabilityTestResult:
        """Run a load test with specified parameters."""
        print(f"Running {test_name} with load level: {load_level}, duration: {duration}s")
        
        url = f"{self.config.base_url}/api/market-data"
        headers = {'X-API-Key': self.config.api_key}
        
        results = []
        error_count = 0
        total_requests = 0
        
        start_time = time.time()
        
        async with aiohttp.ClientSession(headers=headers) as session:
            # Create semaphore to limit concurrent requests
            semaphore = asyncio.Semaphore(load_level)
            
            async def make_request_with_semaphore():
                nonlocal error_count, total_requests
                async with semaphore:
                    data = await self.generate_market_data()
                    success, latency, status_code = await self.make_request(session, url, data)
                    
                    if not success:
                        error_count += 1
                    
                    total_requests += 1
                    results.append((success, latency, status_code))
            
            # Run requests for the specified duration
            end_time = start_time + duration
            tasks = []
            
            while time.time() < end_time:
                # Create batch of requests
                batch_size = min(load_level, 50)  # Limit batch size
                for _ in range(batch_size):
                    task = asyncio.create_task(make_request_with_semaphore())
                    tasks.append(task)
                
                # Wait a bit before creating next batch
                await asyncio.sleep(0.1)
            
            # Wait for all tasks to complete
            await asyncio.gather(*tasks, return_exceptions=True)
        
        end_time = time.time()
        actual_duration = end_time - start_time
        
        # Calculate metrics
        if results:
            latencies = [r[1] for r in results]
            success_count = sum(1 for r in results if r[0])
            success_rate = success_count / len(results)
            average_latency = statistics.mean(latencies)
            p95_latency = np.percentile(latencies, 95)
            p99_latency = np.percentile(latencies, 99)
            throughput = len(results) / actual_duration
        else:
            success_rate = 0.0
            average_latency = 0.0
            p95_latency = 0.0
            p99_latency = 0.0
            throughput = 0.0
        
        result = ScalabilityTestResult(
            test_name=test_name,
            test_type="load_test",
            load_level=load_level,
            duration=actual_duration,
            success_rate=success_rate,
            average_latency=average_latency,
            p95_latency=p95_latency,
            p99_latency=p99_latency,
            throughput=throughput,
            error_count=error_count,
            total_requests=total_requests,
            timestamp=datetime.now(timezone.utc),
            metadata={
                'target_duration': duration,
                'actual_duration': actual_duration,
                'load_level': load_level
            }
        )
        
        self.results.append(result)
        return result
    
    async def run_ramp_test(self, 
                           max_load: int,
                           step_size: int,
                           step_duration: int,
                           test_name: str = "ramp_test") -> List[ScalabilityTestResult]:
        """Run a ramp test with increasing load."""
        print(f"Running {test_name} with max load: {max_load}, step size: {step_size}")
        
        results = []
        current_load = self.config.min_load
        
        while current_load <= max_load:
            print(f"Testing load level: {current_load}")
            
            result = await self.run_load_test(
                load_level=current_load,
                duration=step_duration,
                test_name=f"{test_name}_load_{current_load}"
            )
            
            results.append(result)
            
            # Check if we should stop due to poor performance
            if result.success_rate < self.config.target_success_rate:
                print(f"Stopping ramp test due to low success rate: {result.success_rate:.2%}")
                break
            
            if result.p95_latency > self.config.target_latency_p95:
                print(f"Stopping ramp test due to high latency: {result.p95_latency:.2f}ms")
                break
            
            current_load += step_size
            
            # Wait between steps
            await asyncio.sleep(10)
        
        return results
    
    async def run_stress_test(self, 
                             max_load: int,
                             duration: int,
                             test_name: str = "stress_test") -> ScalabilityTestResult:
        """Run a stress test to find breaking point."""
        print(f"Running {test_name} with max load: {max_load}, duration: {duration}s")
        
        return await self.run_load_test(
            load_level=max_load,
            duration=duration,
            test_name=test_name
        )
    
    async def run_endurance_test(self, 
                                load_level: int,
                                duration: int,
                                test_name: str = "endurance_test") -> ScalabilityTestResult:
        """Run an endurance test to check for memory leaks and performance degradation."""
        print(f"Running {test_name} with load level: {load_level}, duration: {duration}s")
        
        return await self.run_load_test(
            load_level=load_level,
            duration=duration,
            test_name=test_name
        )
    
    async def run_spike_test(self, 
                            base_load: int,
                            spike_load: int,
                            spike_duration: int,
                            test_name: str = "spike_test") -> List[ScalabilityTestResult]:
        """Run a spike test to check system behavior under sudden load increases."""
        print(f"Running {test_name} with base load: {base_load}, spike load: {spike_load}")
        
        results = []
        
        # Baseline load
        print("Running baseline load...")
        baseline_result = await self.run_load_test(
            load_level=base_load,
            duration=60,
            test_name=f"{test_name}_baseline"
        )
        results.append(baseline_result)
        
        # Spike load
        print("Running spike load...")
        spike_result = await self.run_load_test(
            load_level=spike_load,
            duration=spike_duration,
            test_name=f"{test_name}_spike"
        )
        results.append(spike_result)
        
        # Recovery load
        print("Running recovery load...")
        recovery_result = await self.run_load_test(
            load_level=base_load,
            duration=60,
            test_name=f"{test_name}_recovery"
        )
        results.append(recovery_result)
        
        return results
    
    def get_k8s_metrics(self) -> Dict[str, Any]:
        """Get Kubernetes metrics for scalability analysis."""
        if not self.k8s_client:
            return {}
        
        try:
            v1 = client.CoreV1Api(self.k8s_client)
            apps_v1 = client.AppsV1Api(self.k8s_client)
            
            metrics = {
                'pods': {},
                'deployments': {},
                'services': {},
                'nodes': {}
            }
            
            # Get pod metrics
            pods = v1.list_pod_for_all_namespaces()
            for pod in pods.items:
                if pod.metadata.namespace == 'data-processing':
                    metrics['pods'][pod.metadata.name] = {
                        'status': pod.status.phase,
                        'ready': pod.status.conditions[0].status if pod.status.conditions else 'Unknown',
                        'restart_count': sum(container.restart_count for container in pod.status.container_statuses or []),
                        'cpu_requests': pod.spec.containers[0].resources.requests.get('cpu', '0') if pod.spec.containers else '0',
                        'memory_requests': pod.spec.containers[0].resources.requests.get('memory', '0') if pod.spec.containers else '0',
                        'cpu_limits': pod.spec.containers[0].resources.limits.get('cpu', '0') if pod.spec.containers else '0',
                        'memory_limits': pod.spec.containers[0].resources.limits.get('memory', '0') if pod.spec.containers else '0'
                    }
            
            # Get deployment metrics
            deployments = apps_v1.list_deployment_for_all_namespaces()
            for deployment in deployments.items:
                if deployment.metadata.namespace == 'data-processing':
                    metrics['deployments'][deployment.metadata.name] = {
                        'replicas': deployment.spec.replicas,
                        'ready_replicas': deployment.status.ready_replicas or 0,
                        'available_replicas': deployment.status.available_replicas or 0,
                        'updated_replicas': deployment.status.updated_replicas or 0
                    }
            
            return metrics
            
        except Exception as e:
            print(f"Error getting Kubernetes metrics: {e}")
            return {}
    
    def analyze_scalability(self) -> Dict[str, Any]:
        """Analyze scalability test results."""
        if not self.results:
            return {}
        
        analysis = {
            'total_tests': len(self.results),
            'load_levels_tested': sorted(set(r.load_level for r in self.results)),
            'performance_summary': {},
            'scalability_metrics': {},
            'recommendations': []
        }
        
        # Group results by load level
        load_levels = {}
        for result in self.results:
            if result.load_level not in load_levels:
                load_levels[result.load_level] = []
            load_levels[result.load_level].append(result)
        
        # Calculate metrics for each load level
        for load_level, results in load_levels.items():
            if not results:
                continue
            
            # Calculate averages
            avg_success_rate = statistics.mean(r.success_rate for r in results)
            avg_latency = statistics.mean(r.average_latency for r in results)
            avg_p95_latency = statistics.mean(r.p95_latency for r in results)
            avg_throughput = statistics.mean(r.throughput for r in results)
            total_errors = sum(r.error_count for r in results)
            total_requests = sum(r.total_requests for r in results)
            
            analysis['performance_summary'][load_level] = {
                'success_rate': avg_success_rate,
                'average_latency': avg_latency,
                'p95_latency': avg_p95_latency,
                'throughput': avg_throughput,
                'error_rate': total_errors / total_requests if total_requests > 0 else 0,
                'total_requests': total_requests,
                'total_errors': total_errors
            }
        
        # Calculate scalability metrics
        load_levels_sorted = sorted(load_levels.keys())
        if len(load_levels_sorted) >= 2:
            # Calculate throughput scaling
            throughputs = [analysis['performance_summary'][ll]['throughput'] for ll in load_levels_sorted]
            throughput_scaling = throughputs[-1] / throughputs[0] if throughputs[0] > 0 else 0
            
            # Calculate latency scaling
            latencies = [analysis['performance_summary'][ll]['p95_latency'] for ll in load_levels_sorted]
            latency_scaling = latencies[-1] / latencies[0] if latencies[0] > 0 else 0
            
            analysis['scalability_metrics'] = {
                'throughput_scaling': throughput_scaling,
                'latency_scaling': latency_scaling,
                'max_sustainable_load': self._find_max_sustainable_load(analysis['performance_summary']),
                'scalability_efficiency': self._calculate_scalability_efficiency(analysis['performance_summary'])
            }
        
        # Generate recommendations
        if analysis['scalability_metrics']:
            max_load = analysis['scalability_metrics']['max_sustainable_load']
            if max_load < 500:
                analysis['recommendations'].append("Consider horizontal scaling to increase capacity")
            
            if analysis['scalability_metrics']['latency_scaling'] > 2:
                analysis['recommendations'].append("Latency scaling is poor, consider optimization")
            
            if analysis['scalability_metrics']['throughput_scaling'] < 0.8:
                analysis['recommendations'].append("Throughput scaling is poor, investigate bottlenecks")
        
        return analysis
    
    def _find_max_sustainable_load(self, performance_summary: Dict[int, Dict[str, Any]]) -> int:
        """Find the maximum sustainable load level."""
        for load_level in sorted(performance_summary.keys(), reverse=True):
            metrics = performance_summary[load_level]
            if (metrics['success_rate'] >= self.config.target_success_rate and 
                metrics['p95_latency'] <= self.config.target_latency_p95):
                return load_level
        return 0
    
    def _calculate_scalability_efficiency(self, performance_summary: Dict[int, Dict[str, Any]]) -> float:
        """Calculate scalability efficiency score."""
        if len(performance_summary) < 2:
            return 0.0
        
        load_levels = sorted(performance_summary.keys())
        base_load = load_levels[0]
        max_load = load_levels[-1]
        
        base_throughput = performance_summary[base_load]['throughput']
        max_throughput = performance_summary[max_load]['throughput']
        
        if base_throughput == 0:
            return 0.0
        
        # Calculate efficiency as ratio of throughput scaling to load scaling
        load_scaling = max_load / base_load
        throughput_scaling = max_throughput / base_throughput
        
        efficiency = throughput_scaling / load_scaling if load_scaling > 0 else 0.0
        return min(efficiency, 1.0)  # Cap at 100%
    
    def generate_report(self) -> str:
        """Generate scalability test report."""
        analysis = self.analyze_scalability()
        
        report = f"""
Scalability Test Report
======================

Generated: {datetime.now(timezone.utc).isoformat()}
Total Tests: {analysis['total_tests']}
Load Levels Tested: {analysis['load_levels_tested']}

Performance Summary:
-------------------
"""
        
        for load_level, metrics in analysis['performance_summary'].items():
            report += f"\nLoad Level {load_level}:\n"
            report += f"  Success Rate: {metrics['success_rate']:.2%}\n"
            report += f"  Average Latency: {metrics['average_latency']:.2f}ms\n"
            report += f"  P95 Latency: {metrics['p95_latency']:.2f}ms\n"
            report += f"  Throughput: {metrics['throughput']:.2f} req/s\n"
            report += f"  Error Rate: {metrics['error_rate']:.2%}\n"
            report += f"  Total Requests: {metrics['total_requests']}\n"
        
        if analysis['scalability_metrics']:
            report += "\nScalability Metrics:\n"
            report += "-------------------\n"
            metrics = analysis['scalability_metrics']
            report += f"  Throughput Scaling: {metrics['throughput_scaling']:.2f}x\n"
            report += f"  Latency Scaling: {metrics['latency_scaling']:.2f}x\n"
            report += f"  Max Sustainable Load: {metrics['max_sustainable_load']}\n"
            report += f"  Scalability Efficiency: {metrics['scalability_efficiency']:.2%}\n"
        
        report += "\nRecommendations:\n"
        report += "---------------\n"
        for recommendation in analysis['recommendations']:
            report += f"  • {recommendation}\n"
        
        return report
    
    def save_results(self, filename: str = "scalability_results.json"):
        """Save test results to file."""
        data = [asdict(result) for result in self.results]
        
        # Convert datetime objects to ISO strings
        for item in data:
            item['timestamp'] = item['timestamp'].isoformat()
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Saved {len(self.results)} scalability test results to {filename}")


# Pytest integration
class TestScalability:
    """Pytest test class for scalability testing."""
    
    @pytest.fixture(scope="class")
    async def scalability_tester(self):
        """Create scalability tester instance."""
        config = ScalabilityTestConfig(
            base_url="http://localhost:8080",
            api_key="test-api-key",
            test_duration=60,
            max_load=100,
            step_size=20
        )
        tester = ScalabilityTester(config)
        return tester
    
    @pytest.mark.asyncio
    async def test_basic_scalability(self, scalability_tester):
        """Test basic scalability."""
        result = await scalability_tester.run_load_test(
            load_level=50,
            duration=30,
            test_name="basic_scalability"
        )
        
        assert result.success_rate >= 0.95, f"Success rate too low: {result.success_rate:.2%}"
        assert result.p95_latency <= 200, f"P95 latency too high: {result.p95_latency:.2f}ms"
        assert result.throughput >= 100, f"Throughput too low: {result.throughput:.2f} req/s"
    
    @pytest.mark.asyncio
    async def test_ramp_scalability(self, scalability_tester):
        """Test ramp scalability."""
        results = await scalability_tester.run_ramp_test(
            max_load=100,
            step_size=20,
            step_duration=30,
            test_name="ramp_scalability"
        )
        
        assert len(results) > 0, "No ramp test results"
        
        # Check that performance degrades gracefully
        for result in results:
            assert result.success_rate >= 0.90, f"Success rate too low at load {result.load_level}: {result.success_rate:.2%}"
    
    @pytest.mark.asyncio
    async def test_spike_scalability(self, scalability_tester):
        """Test spike scalability."""
        results = await scalability_tester.run_spike_test(
            base_load=20,
            spike_load=100,
            spike_duration=30,
            test_name="spike_scalability"
        )
        
        assert len(results) == 3, "Spike test should have 3 phases"
        
        # Check recovery
        recovery_result = results[2]
        assert recovery_result.success_rate >= 0.95, f"Recovery success rate too low: {recovery_result.success_rate:.2%}"


# Command-line interface
async def main():
    """Main function for running scalability tests."""
    import argparse
    
    parser = argparse.ArgumentParser(description='254Carbon Data Processing Pipeline Scalability Tests')
    parser.add_argument('--base-url', default='http://localhost:8080', help='Base URL for the API')
    parser.add_argument('--api-key', default='test-api-key', help='API key for authentication')
    parser.add_argument('--test-type', choices=['load', 'ramp', 'stress', 'endurance', 'spike'], 
                       default='ramp', help='Type of scalability test')
    parser.add_argument('--max-load', type=int, default=200, help='Maximum load level')
    parser.add_argument('--step-size', type=int, default=50, help='Load increment step')
    parser.add_argument('--duration', type=int, default=60, help='Test duration in seconds')
    parser.add_argument('--generate-report', action='store_true', help='Generate scalability report')
    
    args = parser.parse_args()
    
    # Create scalability tester
    config = ScalabilityTestConfig(
        base_url=args.base_url,
        api_key=args.api_key,
        max_load=args.max_load,
        step_size=args.step_size,
        test_duration=args.duration
    )
    
    tester = ScalabilityTester(config)
    
    print(f"Running {args.test_type} scalability test...")
    
    # Run appropriate test
    if args.test_type == 'load':
        result = await tester.run_load_test(
            load_level=args.max_load,
            duration=args.duration,
            test_name="load_test"
        )
        print(f"Load test completed: {result.success_rate:.2%} success rate, {result.throughput:.2f} req/s")
    
    elif args.test_type == 'ramp':
        results = await tester.run_ramp_test(
            max_load=args.max_load,
            step_size=args.step_size,
            step_duration=args.duration // 5,  # Divide duration across steps
            test_name="ramp_test"
        )
        print(f"Ramp test completed: {len(results)} load levels tested")
    
    elif args.test_type == 'stress':
        result = await tester.run_stress_test(
            max_load=args.max_load,
            duration=args.duration,
            test_name="stress_test"
        )
        print(f"Stress test completed: {result.success_rate:.2%} success rate")
    
    elif args.test_type == 'endurance':
        result = await tester.run_endurance_test(
            load_level=args.max_load // 2,  # Use half load for endurance
            duration=args.duration,
            test_name="endurance_test"
        )
        print(f"Endurance test completed: {result.success_rate:.2%} success rate")
    
    elif args.test_type == 'spike':
        results = await tester.run_spike_test(
            base_load=args.max_load // 4,
            spike_load=args.max_load,
            spike_duration=args.duration // 3,
            test_name="spike_test"
        )
        print(f"Spike test completed: {len(results)} phases tested")
    
    # Save results
    tester.save_results()
    
    # Generate report
    if args.generate_report:
        report = tester.generate_report()
        print(report)
        
        # Save report to file
        with open('scalability_report.txt', 'w') as f:
            f.write(report)
    
    print("Scalability tests completed!")


if __name__ == "__main__":
    asyncio.run(main())
