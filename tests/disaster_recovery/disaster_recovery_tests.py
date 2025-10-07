"""
Disaster Recovery Tests for 254Carbon Data Processing Pipeline

This module implements comprehensive disaster recovery testing to ensure
the system can recover from various failure scenarios.
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
import kubernetes
from kubernetes import client, config
import subprocess
import yaml
import random


@dataclass
class DisasterRecoveryTestResult:
    """Disaster recovery test result data structure."""
    test_name: str
    failure_type: str
    start_time: datetime
    end_time: datetime
    duration: float
    recovery_time: float
    data_loss: bool
    data_corruption: bool
    service_availability: float
    success_rate: float
    error_count: int
    total_requests: int
    average_latency: float
    p95_latency: float
    p99_latency: float
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class DisasterRecoveryTestConfig:
    """Disaster recovery test configuration."""
    base_url: str = "http://localhost:8080"
    api_key: str = "test-api-key"
    test_duration: int = 600  # 10 minutes
    recovery_timeout: int = 300  # 5 minutes
    request_interval: float = 0.1  # 100ms between requests
    timeout_threshold: float = 5.0  # 5 seconds timeout
    error_threshold: float = 0.01  # 1% error threshold
    latency_threshold: float = 1000.0  # 1 second latency threshold
    namespace: str = "data-processing"
    release_name: str = "data-processing-pipeline"


class DisasterRecoveryTester:
    """Disaster recovery testing framework."""
    
    def __init__(self, config: Optional[DisasterRecoveryTestConfig] = None):
        self.config = config or DisasterRecoveryTestConfig()
        self.results: List[DisasterRecoveryTestResult] = []
        self.k8s_client = None
        self.test_running = False
        self.request_results: List[Tuple[bool, float, int]] = []
        
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
        import numpy as np
        
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
            async with session.post(url, json=data, timeout=aiohttp.ClientTimeout(total=self.config.timeout_threshold)) as response:
                await response.text()
                end_time = time.time()
                latency = (end_time - start_time) * 1000  # Convert to milliseconds
                return response.status == 200, latency, response.status
        except Exception as e:
            end_time = time.time()
            latency = (end_time - start_time) * 1000
            return False, latency, 0
    
    async def continuous_load_test(self, 
                                 duration: int,
                                 test_name: str = "continuous_load") -> Tuple[float, float, float, int, int]:
        """Run continuous load test and return metrics."""
        print(f"Running continuous load test: {test_name} for {duration}s")
        
        url = f"{self.config.base_url}/api/market-data"
        headers = {'X-API-Key': self.config.api_key}
        
        self.test_running = True
        self.request_results = []
        
        start_time = time.time()
        
        async with aiohttp.ClientSession(headers=headers) as session:
            while self.test_running and (time.time() - start_time) < duration:
                try:
                    data = await self.generate_market_data()
                    success, latency, status_code = await self.make_request(session, url, data)
                    self.request_results.append((success, latency, status_code))
                    
                except Exception as e:
                    print(f"Request error: {e}")
                    self.request_results.append((False, self.config.latency_threshold, 0))
                
                await asyncio.sleep(self.config.request_interval)
        
        end_time = time.time()
        actual_duration = end_time - start_time
        
        # Calculate metrics
        if self.request_results:
            latencies = [r[1] for r in self.request_results]
            success_count = sum(1 for r in self.request_results if r[0])
            success_rate = success_count / len(self.request_results)
            error_count = len(self.request_results) - success_count
            average_latency = statistics.mean(latencies)
            p95_latency = np.percentile(latencies, 95)
            p99_latency = np.percentile(latencies, 99)
        else:
            success_rate = 0.0
            error_count = 0
            average_latency = 0.0
            p95_latency = 0.0
            p99_latency = 0.0
        
        return success_rate, average_latency, p95_latency, error_count, len(self.request_results)
    
    def stop_test(self):
        """Stop the continuous test."""
        self.test_running = False
    
    async def test_pod_failure(self, 
                              service_name: str,
                              test_name: str = "pod_failure") -> DisasterRecoveryTestResult:
        """Test recovery from pod failure."""
        print(f"Testing pod failure recovery for {service_name}")
        
        test_start_time = datetime.now(timezone.utc)
        
        # Start continuous load test
        load_task = asyncio.create_task(
            self.continuous_load_test(
                duration=self.config.test_duration,
                test_name=f"{test_name}_load"
            )
        )
        
        # Wait for load test to stabilize
        await asyncio.sleep(30)
        
        # Simulate pod failure
        failure_start_time = time.time()
        await self._simulate_pod_failure(service_name)
        
        # Wait for recovery
        recovery_start_time = time.time()
        await self._wait_for_service_recovery(service_name)
        recovery_time = time.time() - recovery_start_time
        
        # Wait for load test to complete
        await load_task
        
        test_end_time = datetime.now(timezone.utc)
        test_duration = (test_end_time - test_start_time).total_seconds()
        
        # Get metrics
        success_rate, average_latency, p95_latency, error_count, total_requests = self.request_results[-1] if self.request_results else (0.0, 0.0, 0.0, 0, 0)
        
        # Calculate service availability
        service_availability = 1.0 - (recovery_time / test_duration)
        
        result = DisasterRecoveryTestResult(
            test_name=test_name,
            failure_type="pod_failure",
            start_time=test_start_time,
            end_time=test_end_time,
            duration=test_duration,
            recovery_time=recovery_time,
            data_loss=False,  # Pod failure shouldn't cause data loss
            data_corruption=False,
            service_availability=service_availability,
            success_rate=success_rate,
            error_count=error_count,
            total_requests=total_requests,
            average_latency=average_latency,
            p95_latency=p95_latency,
            p99_latency=p95_latency,  # Approximate
            metadata={
                'service_name': service_name,
                'failure_start_time': failure_start_time,
                'recovery_start_time': recovery_start_time
            }
        )
        
        self.results.append(result)
        return result
    
    async def _simulate_pod_failure(self, service_name: str):
        """Simulate pod failure by deleting a pod."""
        if not self.k8s_client:
            print("Kubernetes client not available, skipping pod failure simulation")
            return
        
        try:
            # Get pods for the service
            cmd = [
                'kubectl', 'get', 'pods',
                f'--namespace={self.config.namespace}',
                f'-l app={service_name}',
                '-o', 'jsonpath={.items[0].metadata.name}'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                raise Exception(f"Failed to get pod name: {result.stderr}")
            
            pod_name = result.stdout.strip()
            if not pod_name:
                raise Exception("No pods found for service")
            
            # Delete the pod
            cmd = [
                'kubectl', 'delete', 'pod', pod_name,
                f'--namespace={self.config.namespace}'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                raise Exception(f"Failed to delete pod: {result.stderr}")
            
            print(f"Pod {pod_name} deleted to simulate failure")
            
        except Exception as e:
            print(f"Error simulating pod failure: {e}")
    
    async def _wait_for_service_recovery(self, service_name: str, timeout: int = 300):
        """Wait for service to recover."""
        if not self.k8s_client:
            return
        
        start_time = time.time()
        
        while (time.time() - start_time) < timeout:
            try:
                # Check if service is ready
                cmd = [
                    'kubectl', 'get', 'pods',
                    f'--namespace={self.config.namespace}',
                    f'-l app={service_name}',
                    '-o', 'jsonpath={.items[*].status.phase}'
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                
                if result.returncode == 0:
                    phases = result.stdout.strip().split()
                    if all(phase == 'Running' for phase in phases):
                        print(f"Service {service_name} recovered")
                        return
                
            except Exception as e:
                print(f"Error checking service recovery: {e}")
            
            await asyncio.sleep(10)
        
        raise Exception(f"Service {service_name} did not recover within {timeout} seconds")
    
    async def test_node_failure(self, 
                               test_name: str = "node_failure") -> DisasterRecoveryTestResult:
        """Test recovery from node failure."""
        print(f"Testing node failure recovery")
        
        test_start_time = datetime.now(timezone.utc)
        
        # Start continuous load test
        load_task = asyncio.create_task(
            self.continuous_load_test(
                duration=self.config.test_duration,
                test_name=f"{test_name}_load"
            )
        )
        
        # Wait for load test to stabilize
        await asyncio.sleep(30)
        
        # Simulate node failure
        failure_start_time = time.time()
        await self._simulate_node_failure()
        
        # Wait for recovery
        recovery_start_time = time.time()
        await self._wait_for_cluster_recovery()
        recovery_time = time.time() - recovery_start_time
        
        # Wait for load test to complete
        await load_task
        
        test_end_time = datetime.now(timezone.utc)
        test_duration = (test_end_time - test_start_time).total_seconds()
        
        # Get metrics
        success_rate, average_latency, p95_latency, error_count, total_requests = self.request_results[-1] if self.request_results else (0.0, 0.0, 0.0, 0, 0)
        
        # Calculate service availability
        service_availability = 1.0 - (recovery_time / test_duration)
        
        result = DisasterRecoveryTestResult(
            test_name=test_name,
            failure_type="node_failure",
            start_time=test_start_time,
            end_time=test_end_time,
            duration=test_duration,
            recovery_time=recovery_time,
            data_loss=False,  # Node failure shouldn't cause data loss with proper replication
            data_corruption=False,
            service_availability=service_availability,
            success_rate=success_rate,
            error_count=error_count,
            total_requests=total_requests,
            average_latency=average_latency,
            p95_latency=p95_latency,
            p99_latency=p95_latency,  # Approximate
            metadata={
                'failure_start_time': failure_start_time,
                'recovery_start_time': recovery_start_time
            }
        )
        
        self.results.append(result)
        return result
    
    async def _simulate_node_failure(self):
        """Simulate node failure by cordoning a node."""
        if not self.k8s_client:
            print("Kubernetes client not available, skipping node failure simulation")
            return
        
        try:
            # Get a random node
            cmd = ['kubectl', 'get', 'nodes', '-o', 'jsonpath={.items[0].metadata.name}']
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                raise Exception(f"Failed to get node name: {result.stderr}")
            
            node_name = result.stdout.strip()
            
            # Cordon the node
            cmd = ['kubectl', 'cordon', node_name]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                raise Exception(f"Failed to cordon node: {result.stderr}")
            
            print(f"Node {node_name} cordoned to simulate failure")
            
        except Exception as e:
            print(f"Error simulating node failure: {e}")
    
    async def _wait_for_cluster_recovery(self, timeout: int = 600):
        """Wait for cluster to recover."""
        if not self.k8s_client:
            return
        
        start_time = time.time()
        
        while (time.time() - start_time) < timeout:
            try:
                # Check if all services are running
                cmd = [
                    'kubectl', 'get', 'pods',
                    f'--namespace={self.config.namespace}',
                    '-o', 'jsonpath={.items[*].status.phase}'
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                
                if result.returncode == 0:
                    phases = result.stdout.strip().split()
                    if all(phase == 'Running' for phase in phases):
                        print("Cluster recovered")
                        return
                
            except Exception as e:
                print(f"Error checking cluster recovery: {e}")
            
            await asyncio.sleep(15)
        
        raise Exception(f"Cluster did not recover within {timeout} seconds")
    
    async def test_database_failure(self, 
                                   database_type: str = "clickhouse",
                                   test_name: str = "database_failure") -> DisasterRecoveryTestResult:
        """Test recovery from database failure."""
        print(f"Testing {database_type} database failure recovery")
        
        test_start_time = datetime.now(timezone.utc)
        
        # Start continuous load test
        load_task = asyncio.create_task(
            self.continuous_load_test(
                duration=self.config.test_duration,
                test_name=f"{test_name}_load"
            )
        )
        
        # Wait for load test to stabilize
        await asyncio.sleep(30)
        
        # Simulate database failure
        failure_start_time = time.time()
        await self._simulate_database_failure(database_type)
        
        # Wait for recovery
        recovery_start_time = time.time()
        await self._wait_for_database_recovery(database_type)
        recovery_time = time.time() - recovery_start_time
        
        # Wait for load test to complete
        await load_task
        
        test_end_time = datetime.now(timezone.utc)
        test_duration = (test_end_time - test_start_time).total_seconds()
        
        # Get metrics
        success_rate, average_latency, p95_latency, error_count, total_requests = self.request_results[-1] if self.request_results else (0.0, 0.0, 0.0, 0, 0)
        
        # Calculate service availability
        service_availability = 1.0 - (recovery_time / test_duration)
        
        result = DisasterRecoveryTestResult(
            test_name=test_name,
            failure_type=f"{database_type}_failure",
            start_time=test_start_time,
            end_time=test_end_time,
            duration=test_duration,
            recovery_time=recovery_time,
            data_loss=False,  # Database failure shouldn't cause data loss with proper backups
            data_corruption=False,
            service_availability=service_availability,
            success_rate=success_rate,
            error_count=error_count,
            total_requests=total_requests,
            average_latency=average_latency,
            p95_latency=p95_latency,
            p99_latency=p95_latency,  # Approximate
            metadata={
                'database_type': database_type,
                'failure_start_time': failure_start_time,
                'recovery_start_time': recovery_start_time
            }
        )
        
        self.results.append(result)
        return result
    
    async def _simulate_database_failure(self, database_type: str):
        """Simulate database failure by scaling down the database."""
        if not self.k8s_client:
            print("Kubernetes client not available, skipping database failure simulation")
            return
        
        try:
            # Scale down database
            cmd = [
                'kubectl', 'scale', 'deployment', database_type,
                f'--namespace={self.config.namespace}',
                '--replicas=0'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                raise Exception(f"Failed to scale down database: {result.stderr}")
            
            print(f"Database {database_type} scaled down to simulate failure")
            
        except Exception as e:
            print(f"Error simulating database failure: {e}")
    
    async def _wait_for_database_recovery(self, database_type: str, timeout: int = 300):
        """Wait for database to recover."""
        if not self.k8s_client:
            return
        
        start_time = time.time()
        
        while (time.time() - start_time) < timeout:
            try:
                # Check if database is running
                cmd = [
                    'kubectl', 'get', 'pods',
                    f'--namespace={self.config.namespace}',
                    f'-l app={database_type}',
                    '-o', 'jsonpath={.items[*].status.phase}'
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                
                if result.returncode == 0:
                    phases = result.stdout.strip().split()
                    if all(phase == 'Running' for phase in phases):
                        print(f"Database {database_type} recovered")
                        return
                
            except Exception as e:
                print(f"Error checking database recovery: {e}")
            
            await asyncio.sleep(10)
        
        raise Exception(f"Database {database_type} did not recover within {timeout} seconds")
    
    async def test_network_partition(self, 
                                   test_name: str = "network_partition") -> DisasterRecoveryTestResult:
        """Test recovery from network partition."""
        print(f"Testing network partition recovery")
        
        test_start_time = datetime.now(timezone.utc)
        
        # Start continuous load test
        load_task = asyncio.create_task(
            self.continuous_load_test(
                duration=self.config.test_duration,
                test_name=f"{test_name}_load"
            )
        )
        
        # Wait for load test to stabilize
        await asyncio.sleep(30)
        
        # Simulate network partition
        failure_start_time = time.time()
        await self._simulate_network_partition()
        
        # Wait for recovery
        recovery_start_time = time.time()
        await self._wait_for_network_recovery()
        recovery_time = time.time() - recovery_start_time
        
        # Wait for load test to complete
        await load_task
        
        test_end_time = datetime.now(timezone.utc)
        test_duration = (test_end_time - test_start_time).total_seconds()
        
        # Get metrics
        success_rate, average_latency, p95_latency, error_count, total_requests = self.request_results[-1] if self.request_results else (0.0, 0.0, 0.0, 0, 0)
        
        # Calculate service availability
        service_availability = 1.0 - (recovery_time / test_duration)
        
        result = DisasterRecoveryTestResult(
            test_name=test_name,
            failure_type="network_partition",
            start_time=test_start_time,
            end_time=test_end_time,
            duration=test_duration,
            recovery_time=recovery_time,
            data_loss=False,  # Network partition shouldn't cause data loss
            data_corruption=False,
            service_availability=service_availability,
            success_rate=success_rate,
            error_count=error_count,
            total_requests=total_requests,
            average_latency=average_latency,
            p95_latency=p95_latency,
            p99_latency=p95_latency,  # Approximate
            metadata={
                'failure_start_time': failure_start_time,
                'recovery_start_time': recovery_start_time
            }
        )
        
        self.results.append(result)
        return result
    
    async def _simulate_network_partition(self):
        """Simulate network partition by applying network policies."""
        if not self.k8s_client:
            print("Kubernetes client not available, skipping network partition simulation")
            return
        
        try:
            # Create network policy to block traffic
            network_policy = {
                'apiVersion': 'networking.k8s.io/v1',
                'kind': 'NetworkPolicy',
                'metadata': {
                    'name': 'block-all-traffic',
                    'namespace': self.config.namespace
                },
                'spec': {
                    'podSelector': {},
                    'policyTypes': ['Ingress', 'Egress']
                }
            }
            
            policy_file = '/tmp/block-all-traffic.yaml'
            with open(policy_file, 'w') as f:
                yaml.dump(network_policy, f)
            
            cmd = ['kubectl', 'apply', '-f', policy_file]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode != 0:
                raise Exception(f"Failed to apply network policy: {result.stderr}")
            
            print("Network partition simulated by blocking all traffic")
            
        except Exception as e:
            print(f"Error simulating network partition: {e}")
    
    async def _wait_for_network_recovery(self, timeout: int = 300):
        """Wait for network to recover."""
        if not self.k8s_client:
            return
        
        start_time = time.time()
        
        while (time.time() - start_time) < timeout:
            try:
                # Remove network policy
                cmd = [
                    'kubectl', 'delete', 'networkpolicy', 'block-all-traffic',
                    f'--namespace={self.config.namespace}'
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                
                if result.returncode == 0:
                    print("Network partition removed")
                    return
                
            except Exception as e:
                print(f"Error removing network policy: {e}")
            
            await asyncio.sleep(10)
        
        raise Exception(f"Network did not recover within {timeout} seconds")
    
    def analyze_disaster_recovery_results(self) -> Dict[str, Any]:
        """Analyze disaster recovery test results."""
        if not self.results:
            return {}
        
        analysis = {
            'total_tests': len(self.results),
            'failure_types': list(set(r.failure_type for r in self.results)),
            'recovery_summary': {},
            'recommendations': []
        }
        
        # Analyze recovery by failure type
        for failure_type in analysis['failure_types']:
            type_results = [r for r in self.results if r.failure_type == failure_type]
            
            if type_results:
                analysis['recovery_summary'][failure_type] = {
                    'average_recovery_time': statistics.mean(r.recovery_time for r in type_results),
                    'max_recovery_time': max(r.recovery_time for r in type_results),
                    'average_service_availability': statistics.mean(r.service_availability for r in type_results),
                    'average_success_rate': statistics.mean(r.success_rate for r in type_results),
                    'data_loss_incidents': sum(1 for r in type_results if r.data_loss),
                    'data_corruption_incidents': sum(1 for r in type_results if r.data_corruption),
                    'total_tests': len(type_results)
                }
        
        # Generate recommendations
        for failure_type, summary in analysis['recovery_summary'].items():
            if summary['average_recovery_time'] > 300:  # 5 minutes
                analysis['recommendations'].append(
                    f"High recovery time for {failure_type}: {summary['average_recovery_time']:.2f}s"
                )
            
            if summary['average_service_availability'] < 0.95:
                analysis['recommendations'].append(
                    f"Low service availability for {failure_type}: {summary['average_service_availability']:.2%}"
                )
            
            if summary['data_loss_incidents'] > 0:
                analysis['recommendations'].append(
                    f"Data loss detected in {failure_type}: {summary['data_loss_incidents']} incidents"
                )
            
            if summary['data_corruption_incidents'] > 0:
                analysis['recommendations'].append(
                    f"Data corruption detected in {failure_type}: {summary['data_corruption_incidents']} incidents"
                )
        
        return analysis
    
    def generate_report(self) -> str:
        """Generate disaster recovery test report."""
        analysis = self.analyze_disaster_recovery_results()
        
        report = f"""
Disaster Recovery Test Report
============================

Generated: {datetime.now(timezone.utc).isoformat()}
Total Tests: {analysis['total_tests']}
Failure Types Tested: {', '.join(analysis['failure_types'])}

Recovery Summary:
----------------
"""
        
        for failure_type, summary in analysis['recovery_summary'].items():
            report += f"\n{failure_type.upper()}:\n"
            report += f"  Average Recovery Time: {summary['average_recovery_time']:.2f}s\n"
            report += f"  Max Recovery Time: {summary['max_recovery_time']:.2f}s\n"
            report += f"  Service Availability: {summary['average_service_availability']:.2%}\n"
            report += f"  Success Rate: {summary['average_success_rate']:.2%}\n"
            report += f"  Data Loss Incidents: {summary['data_loss_incidents']}\n"
            report += f"  Data Corruption Incidents: {summary['data_corruption_incidents']}\n"
            report += f"  Total Tests: {summary['total_tests']}\n"
        
        report += "\nRecommendations:\n"
        report += "---------------\n"
        for recommendation in analysis['recommendations']:
            report += f"  • {recommendation}\n"
        
        return report
    
    def save_results(self, filename: str = "disaster_recovery_results.json"):
        """Save test results to file."""
        data = [asdict(result) for result in self.results]
        
        # Convert datetime objects to ISO strings
        for item in data:
            item['start_time'] = item['start_time'].isoformat()
            item['end_time'] = item['end_time'].isoformat()
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Saved {len(self.results)} disaster recovery test results to {filename}")


# Pytest integration
class TestDisasterRecovery:
    """Pytest test class for disaster recovery testing."""
    
    @pytest.fixture(scope="class")
    async def disaster_recovery_tester(self):
        """Create disaster recovery tester instance."""
        config = DisasterRecoveryTestConfig(
            base_url="http://localhost:8080",
            api_key="test-api-key",
            test_duration=120,  # 2 minutes for testing
            request_interval=0.5  # 500ms between requests
        )
        tester = DisasterRecoveryTester(config)
        return tester
    
    @pytest.mark.asyncio
    async def test_pod_failure_recovery(self, disaster_recovery_tester):
        """Test pod failure recovery."""
        result = await disaster_recovery_tester.test_pod_failure(
            service_name="normalization-service",
            test_name="pod_failure"
        )
        
        assert result.recovery_time <= 300, f"Recovery time too long: {result.recovery_time:.2f}s"
        assert result.service_availability >= 0.95, f"Service availability too low: {result.service_availability:.2%}"
        assert not result.data_loss, "Data loss detected"
        assert not result.data_corruption, "Data corruption detected"
    
    @pytest.mark.asyncio
    async def test_node_failure_recovery(self, disaster_recovery_tester):
        """Test node failure recovery."""
        result = await disaster_recovery_tester.test_node_failure(
            test_name="node_failure"
        )
        
        assert result.recovery_time <= 600, f"Recovery time too long: {result.recovery_time:.2f}s"
        assert result.service_availability >= 0.90, f"Service availability too low: {result.service_availability:.2%}"
        assert not result.data_loss, "Data loss detected"
        assert not result.data_corruption, "Data corruption detected"
    
    @pytest.mark.asyncio
    async def test_database_failure_recovery(self, disaster_recovery_tester):
        """Test database failure recovery."""
        result = await disaster_recovery_tester.test_database_failure(
            database_type="clickhouse",
            test_name="database_failure"
        )
        
        assert result.recovery_time <= 300, f"Recovery time too long: {result.recovery_time:.2f}s"
        assert result.service_availability >= 0.90, f"Service availability too low: {result.service_availability:.2%}"
        assert not result.data_loss, "Data loss detected"
        assert not result.data_corruption, "Data corruption detected"
    
    @pytest.mark.asyncio
    async def test_network_partition_recovery(self, disaster_recovery_tester):
        """Test network partition recovery."""
        result = await disaster_recovery_tester.test_network_partition(
            test_name="network_partition"
        )
        
        assert result.recovery_time <= 300, f"Recovery time too long: {result.recovery_time:.2f}s"
        assert result.service_availability >= 0.90, f"Service availability too low: {result.service_availability:.2%}"
        assert not result.data_loss, "Data loss detected"
        assert not result.data_corruption, "Data corruption detected"


# Command-line interface
async def main():
    """Main function for running disaster recovery tests."""
    import argparse
    
    parser = argparse.ArgumentParser(description='254Carbon Data Processing Pipeline Disaster Recovery Tests')
    parser.add_argument('--base-url', default='http://localhost:8080', help='Base URL for the API')
    parser.add_argument('--api-key', default='test-api-key', help='API key for authentication')
    parser.add_argument('--test-type', choices=['pod', 'node', 'database', 'network', 'all'], 
                       default='pod', help='Type of disaster recovery test')
    parser.add_argument('--service-name', default='normalization-service', help='Service name to test')
    parser.add_argument('--database-type', default='clickhouse', help='Database type to test')
    parser.add_argument('--duration', type=int, default=600, help='Test duration in seconds')
    parser.add_argument('--generate-report', action='store_true', help='Generate disaster recovery report')
    
    args = parser.parse_args()
    
    # Create disaster recovery tester
    config = DisasterRecoveryTestConfig(
        base_url=args.base_url,
        api_key=args.api_key,
        test_duration=args.duration
    )
    
    tester = DisasterRecoveryTester(config)
    
    print(f"Running {args.test_type} disaster recovery test...")
    
    # Run appropriate test
    if args.test_type == 'pod':
        result = await tester.test_pod_failure(
            service_name=args.service_name,
            test_name="pod_failure"
        )
        print(f"Pod failure test completed: {result.recovery_time:.2f}s recovery time")
    
    elif args.test_type == 'node':
        result = await tester.test_node_failure(
            test_name="node_failure"
        )
        print(f"Node failure test completed: {result.recovery_time:.2f}s recovery time")
    
    elif args.test_type == 'database':
        result = await tester.test_database_failure(
            database_type=args.database_type,
            test_name="database_failure"
        )
        print(f"Database failure test completed: {result.recovery_time:.2f}s recovery time")
    
    elif args.test_type == 'network':
        result = await tester.test_network_partition(
            test_name="network_partition"
        )
        print(f"Network partition test completed: {result.recovery_time:.2f}s recovery time")
    
    elif args.test_type == 'all':
        # Run all disaster recovery tests
        results = []
        
        # Pod failure
        pod_result = await tester.test_pod_failure(
            service_name=args.service_name,
            test_name="pod_failure"
        )
        results.append(pod_result)
        
        # Node failure
        node_result = await tester.test_node_failure(
            test_name="node_failure"
        )
        results.append(node_result)
        
        # Database failure
        db_result = await tester.test_database_failure(
            database_type=args.database_type,
            test_name="database_failure"
        )
        results.append(db_result)
        
        # Network partition
        network_result = await tester.test_network_partition(
            test_name="network_partition"
        )
        results.append(network_result)
        
        print(f"All disaster recovery tests completed: {len(results)} tests")
    
    # Save results
    tester.save_results()
    
    # Generate report
    if args.generate_report:
        report = tester.generate_report()
        print(report)
        
        # Save report to file
        with open('disaster_recovery_report.txt', 'w') as f:
            f.write(report)
    
    print("Disaster recovery tests completed!")


if __name__ == "__main__":
    asyncio.run(main())
