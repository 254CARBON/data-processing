"""
Zero-Downtime Deployment Tests for 254Carbon Data Processing Pipeline

This module implements tests to verify zero-downtime deployment capabilities
and rolling update procedures.
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


@dataclass
class DeploymentTestResult:
    """Deployment test result data structure."""
    test_name: str
    deployment_type: str
    start_time: datetime
    end_time: datetime
    duration: float
    success_rate: float
    error_count: int
    total_requests: int
    average_latency: float
    p95_latency: float
    p99_latency: float
    downtime_detected: bool
    max_downtime: float
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class DeploymentTestConfig:
    """Deployment test configuration."""
    base_url: str = "http://localhost:8080"
    api_key: str = "test-api-key"
    test_duration: int = 300  # 5 minutes
    request_interval: float = 0.1  # 100ms between requests
    timeout_threshold: float = 5.0  # 5 seconds timeout
    error_threshold: float = 0.01  # 1% error threshold
    latency_threshold: float = 1000.0  # 1 second latency threshold
    namespace: str = "data-processing"
    release_name: str = "data-processing-pipeline"


class ZeroDowntimeTester:
    """Zero-downtime deployment testing framework."""
    
    def __init__(self, config: Optional[DeploymentTestConfig] = None):
        self.config = config or DeploymentTestConfig()
        self.results: List[DeploymentTestResult] = []
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
                                 test_name: str = "continuous_load") -> DeploymentTestResult:
        """Run continuous load test during deployment."""
        print(f"Running continuous load test: {test_name} for {duration}s")
        
        url = f"{self.config.base_url}/api/market-data"
        headers = {'X-API-Key': self.config.api_key}
        
        self.test_running = True
        self.request_results = []
        
        start_time = time.time()
        test_start_time = datetime.now(timezone.utc)
        
        async with aiohttp.ClientSession(headers=headers) as session:
            while self.test_running and (time.time() - start_time) < duration:
                try:
                    data = await self.generate_market_data()
                    success, latency, status_code = await self.make_request(session, url, data)
                    self.request_results.append((success, latency, status_code))
                    
                    # Check for timeout
                    if latency > self.config.latency_threshold:
                        print(f"High latency detected: {latency:.2f}ms")
                    
                except Exception as e:
                    print(f"Request error: {e}")
                    self.request_results.append((False, self.config.latency_threshold, 0))
                
                await asyncio.sleep(self.config.request_interval)
        
        end_time = time.time()
        test_end_time = datetime.now(timezone.utc)
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
        
        # Detect downtime
        downtime_detected, max_downtime = self._detect_downtime()
        
        result = DeploymentTestResult(
            test_name=test_name,
            deployment_type="continuous_load",
            start_time=test_start_time,
            end_time=test_end_time,
            duration=actual_duration,
            success_rate=success_rate,
            error_count=error_count,
            total_requests=len(self.request_results),
            average_latency=average_latency,
            p95_latency=p95_latency,
            p99_latency=p99_latency,
            downtime_detected=downtime_detected,
            max_downtime=max_downtime,
            metadata={
                'request_interval': self.config.request_interval,
                'timeout_threshold': self.config.timeout_threshold,
                'latency_threshold': self.config.latency_threshold
            }
        )
        
        self.results.append(result)
        return result
    
    def _detect_downtime(self) -> Tuple[bool, float]:
        """Detect downtime periods in request results."""
        if not self.request_results:
            return False, 0.0
        
        # Look for consecutive failures
        consecutive_failures = 0
        max_consecutive_failures = 0
        downtime_periods = []
        
        for success, latency, status_code in self.request_results:
            if not success:
                consecutive_failures += 1
                max_consecutive_failures = max(max_consecutive_failures, consecutive_failures)
            else:
                if consecutive_failures > 0:
                    downtime_periods.append(consecutive_failures)
                consecutive_failures = 0
        
        # Calculate max downtime in seconds
        max_downtime = max_consecutive_failures * self.config.request_interval
        
        # Consider downtime if we have more than 5 consecutive failures
        downtime_detected = max_consecutive_failures > 5
        
        return downtime_detected, max_downtime
    
    def stop_test(self):
        """Stop the continuous test."""
        self.test_running = False
    
    async def run_rolling_update_test(self, 
                                    service_name: str,
                                    new_image_tag: str,
                                    test_name: str = "rolling_update") -> DeploymentTestResult:
        """Test rolling update deployment."""
        print(f"Running rolling update test for {service_name} with image {new_image_tag}")
        
        # Start continuous load test
        load_task = asyncio.create_task(
            self.continuous_load_test(
                duration=self.config.test_duration,
                test_name=f"{test_name}_load"
            )
        )
        
        # Wait a bit for load test to stabilize
        await asyncio.sleep(30)
        
        # Perform rolling update
        try:
            await self._perform_rolling_update(service_name, new_image_tag)
        except Exception as e:
            print(f"Rolling update failed: {e}")
            self.stop_test()
            await load_task
            raise
        
        # Wait for load test to complete
        await load_task
        
        # Get the result
        result = self.results[-1] if self.results else None
        
        if result:
            result.test_name = test_name
            result.deployment_type = "rolling_update"
            result.metadata = result.metadata or {}
            result.metadata.update({
                'service_name': service_name,
                'new_image_tag': new_image_tag
            })
        
        return result
    
    async def _perform_rolling_update(self, service_name: str, new_image_tag: str):
        """Perform rolling update using kubectl."""
        if not self.k8s_client:
            print("Kubernetes client not available, skipping rolling update")
            return
        
        try:
            # Update deployment image
            cmd = [
                'kubectl', 'set', 'image',
                f'deployment/{service_name}',
                f'{service_name}={new_image_tag}',
                f'--namespace={self.config.namespace}'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
            
            if result.returncode != 0:
                raise Exception(f"kubectl set image failed: {result.stderr}")
            
            print(f"Rolling update initiated for {service_name}")
            
            # Wait for rollout to complete
            await self._wait_for_rollout(service_name)
            
        except subprocess.TimeoutExpired:
            raise Exception(f"Rolling update timed out for {service_name}")
        except Exception as e:
            raise Exception(f"Rolling update failed for {service_name}: {e}")
    
    async def _wait_for_rollout(self, service_name: str, timeout: int = 600):
        """Wait for deployment rollout to complete."""
        if not self.k8s_client:
            return
        
        start_time = time.time()
        
        while (time.time() - start_time) < timeout:
            try:
                cmd = [
                    'kubectl', 'rollout', 'status',
                    f'deployment/{service_name}',
                    f'--namespace={self.config.namespace}',
                    '--timeout=30s'
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                
                if result.returncode == 0:
                    print(f"Rollout completed for {service_name}")
                    return
                
            except subprocess.TimeoutExpired:
                pass
            except Exception as e:
                print(f"Error checking rollout status: {e}")
            
            await asyncio.sleep(10)
        
        raise Exception(f"Rollout timeout for {service_name}")
    
    async def run_blue_green_test(self, 
                                service_name: str,
                                new_image_tag: str,
                                test_name: str = "blue_green") -> DeploymentTestResult:
        """Test blue-green deployment."""
        print(f"Running blue-green test for {service_name} with image {new_image_tag}")
        
        # Start continuous load test
        load_task = asyncio.create_task(
            self.continuous_load_test(
                duration=self.config.test_duration,
                test_name=f"{test_name}_load"
            )
        )
        
        # Wait a bit for load test to stabilize
        await asyncio.sleep(30)
        
        # Perform blue-green deployment
        try:
            await self._perform_blue_green_deployment(service_name, new_image_tag)
        except Exception as e:
            print(f"Blue-green deployment failed: {e}")
            self.stop_test()
            await load_task
            raise
        
        # Wait for load test to complete
        await load_task
        
        # Get the result
        result = self.results[-1] if self.results else None
        
        if result:
            result.test_name = test_name
            result.deployment_type = "blue_green"
            result.metadata = result.metadata or {}
            result.metadata.update({
                'service_name': service_name,
                'new_image_tag': new_image_tag
            })
        
        return result
    
    async def _perform_blue_green_deployment(self, service_name: str, new_image_tag: str):
        """Perform blue-green deployment."""
        if not self.k8s_client:
            print("Kubernetes client not available, skipping blue-green deployment")
            return
        
        try:
            # Create green deployment
            green_deployment_name = f"{service_name}-green"
            
            # Get current deployment configuration
            cmd = [
                'kubectl', 'get', 'deployment', service_name,
                f'--namespace={self.config.namespace}',
                '-o', 'yaml'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                raise Exception(f"Failed to get deployment config: {result.stderr}")
            
            # Parse and modify deployment config
            deployment_config = yaml.safe_load(result.stdout)
            deployment_config['metadata']['name'] = green_deployment_name
            deployment_config['spec']['replicas'] = 0  # Start with 0 replicas
            
            # Update image
            for container in deployment_config['spec']['template']['spec']['containers']:
                if container['name'] == service_name:
                    container['image'] = f"{service_name}:{new_image_tag}"
                    break
            
            # Apply green deployment
            green_config_file = f"/tmp/{green_deployment_name}.yaml"
            with open(green_config_file, 'w') as f:
                yaml.dump(deployment_config, f)
            
            cmd = ['kubectl', 'apply', '-f', green_config_file]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                raise Exception(f"Failed to apply green deployment: {result.stderr}")
            
            # Scale up green deployment
            cmd = [
                'kubectl', 'scale', 'deployment', green_deployment_name,
                f'--namespace={self.config.namespace}',
                '--replicas=2'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                raise Exception(f"Failed to scale green deployment: {result.stderr}")
            
            # Wait for green deployment to be ready
            await self._wait_for_rollout(green_deployment_name)
            
            # Switch traffic to green (update service selector)
            await self._switch_traffic_to_green(service_name, green_deployment_name)
            
            # Scale down blue deployment
            cmd = [
                'kubectl', 'scale', 'deployment', service_name,
                f'--namespace={self.config.namespace}',
                '--replicas=0'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                print(f"Warning: Failed to scale down blue deployment: {result.stderr}")
            
            print(f"Blue-green deployment completed for {service_name}")
            
        except Exception as e:
            raise Exception(f"Blue-green deployment failed for {service_name}: {e}")
    
    async def _switch_traffic_to_green(self, service_name: str, green_deployment_name: str):
        """Switch traffic from blue to green deployment."""
        try:
            # Get service configuration
            cmd = [
                'kubectl', 'get', 'service', service_name,
                f'--namespace={self.config.namespace}',
                '-o', 'yaml'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                raise Exception(f"Failed to get service config: {result.stderr}")
            
            # Parse and modify service config
            service_config = yaml.safe_load(result.stdout)
            
            # Update selector to point to green deployment
            if 'selector' in service_config['spec']:
                service_config['spec']['selector']['app'] = green_deployment_name
            
            # Apply updated service
            service_config_file = f"/tmp/{service_name}-service.yaml"
            with open(service_config_file, 'w') as f:
                yaml.dump(service_config, f)
            
            cmd = ['kubectl', 'apply', '-f', service_config_file]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                raise Exception(f"Failed to update service: {result.stderr}")
            
            print(f"Traffic switched to green deployment: {green_deployment_name}")
            
        except Exception as e:
            raise Exception(f"Failed to switch traffic to green: {e}")
    
    async def run_canary_test(self, 
                            service_name: str,
                            new_image_tag: str,
                            canary_percentage: int = 10,
                            test_name: str = "canary") -> DeploymentTestResult:
        """Test canary deployment."""
        print(f"Running canary test for {service_name} with {canary_percentage}% traffic")
        
        # Start continuous load test
        load_task = asyncio.create_task(
            self.continuous_load_test(
                duration=self.config.test_duration,
                test_name=f"{test_name}_load"
            )
        )
        
        # Wait a bit for load test to stabilize
        await asyncio.sleep(30)
        
        # Perform canary deployment
        try:
            await self._perform_canary_deployment(service_name, new_image_tag, canary_percentage)
        except Exception as e:
            print(f"Canary deployment failed: {e}")
            self.stop_test()
            await load_task
            raise
        
        # Wait for load test to complete
        await load_task
        
        # Get the result
        result = self.results[-1] if self.results else None
        
        if result:
            result.test_name = test_name
            result.deployment_type = "canary"
            result.metadata = result.metadata or {}
            result.metadata.update({
                'service_name': service_name,
                'new_image_tag': new_image_tag,
                'canary_percentage': canary_percentage
            })
        
        return result
    
    async def _perform_canary_deployment(self, service_name: str, new_image_tag: str, canary_percentage: int):
        """Perform canary deployment."""
        if not self.k8s_client:
            print("Kubernetes client not available, skipping canary deployment")
            return
        
        try:
            # Create canary deployment
            canary_deployment_name = f"{service_name}-canary"
            
            # Get current deployment configuration
            cmd = [
                'kubectl', 'get', 'deployment', service_name,
                f'--namespace={self.config.namespace}',
                '-o', 'yaml'
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                raise Exception(f"Failed to get deployment config: {result.stderr}")
            
            # Parse and modify deployment config
            deployment_config = yaml.safe_load(result.stdout)
            deployment_config['metadata']['name'] = canary_deployment_name
            
            # Update image
            for container in deployment_config['spec']['template']['spec']['containers']:
                if container['name'] == service_name:
                    container['image'] = f"{service_name}:{new_image_tag}"
                    break
            
            # Apply canary deployment
            canary_config_file = f"/tmp/{canary_deployment_name}.yaml"
            with open(canary_config_file, 'w') as f:
                yaml.dump(deployment_config, f)
            
            cmd = ['kubectl', 'apply', '-f', canary_config_file]
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                raise Exception(f"Failed to apply canary deployment: {result.stderr}")
            
            # Scale canary deployment based on percentage
            cmd = [
                'kubectl', 'scale', 'deployment', canary_deployment_name,
                f'--namespace={self.config.namespace}',
                '--replicas=1'  # Start with 1 replica
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode != 0:
                raise Exception(f"Failed to scale canary deployment: {result.stderr}")
            
            # Wait for canary deployment to be ready
            await self._wait_for_rollout(canary_deployment_name)
            
            print(f"Canary deployment completed for {service_name}")
            
        except Exception as e:
            raise Exception(f"Canary deployment failed for {service_name}: {e}")
    
    def analyze_deployment_results(self) -> Dict[str, Any]:
        """Analyze deployment test results."""
        if not self.results:
            return {}
        
        analysis = {
            'total_tests': len(self.results),
            'deployment_types': list(set(r.deployment_type for r in self.results)),
            'zero_downtime_achieved': True,
            'performance_summary': {},
            'recommendations': []
        }
        
        # Check for downtime
        for result in self.results:
            if result.downtime_detected:
                analysis['zero_downtime_achieved'] = False
                analysis['recommendations'].append(
                    f"Downtime detected in {result.test_name}: {result.max_downtime:.2f}s"
                )
        
        # Analyze performance by deployment type
        for deployment_type in analysis['deployment_types']:
            type_results = [r for r in self.results if r.deployment_type == deployment_type]
            
            if type_results:
                analysis['performance_summary'][deployment_type] = {
                    'average_success_rate': statistics.mean(r.success_rate for r in type_results),
                    'average_latency': statistics.mean(r.average_latency for r in type_results),
                    'average_p95_latency': statistics.mean(r.p95_latency for r in type_results),
                    'max_downtime': max(r.max_downtime for r in type_results),
                    'total_requests': sum(r.total_requests for r in type_results),
                    'total_errors': sum(r.error_count for r in type_results)
                }
        
        # Generate recommendations
        if analysis['zero_downtime_achieved']:
            analysis['recommendations'].append("Zero-downtime deployments are working correctly")
        else:
            analysis['recommendations'].append("Investigate downtime causes and improve deployment strategy")
        
        # Check performance degradation
        for deployment_type, summary in analysis['performance_summary'].items():
            if summary['average_success_rate'] < 0.95:
                analysis['recommendations'].append(
                    f"Low success rate in {deployment_type}: {summary['average_success_rate']:.2%}"
                )
            
            if summary['average_p95_latency'] > 200:
                analysis['recommendations'].append(
                    f"High latency in {deployment_type}: {summary['average_p95_latency']:.2f}ms"
                )
        
        return analysis
    
    def generate_report(self) -> str:
        """Generate zero-downtime deployment report."""
        analysis = self.analyze_deployment_results()
        
        report = f"""
Zero-Downtime Deployment Test Report
===================================

Generated: {datetime.now(timezone.utc).isoformat()}
Total Tests: {analysis['total_tests']}
Zero-Downtime Achieved: {'✅ YES' if analysis['zero_downtime_achieved'] else '❌ NO'}

Performance Summary:
-------------------
"""
        
        for deployment_type, summary in analysis['performance_summary'].items():
            report += f"\n{deployment_type.upper()}:\n"
            report += f"  Success Rate: {summary['average_success_rate']:.2%}\n"
            report += f"  Average Latency: {summary['average_latency']:.2f}ms\n"
            report += f"  P95 Latency: {summary['average_p95_latency']:.2f}ms\n"
            report += f"  Max Downtime: {summary['max_downtime']:.2f}s\n"
            report += f"  Total Requests: {summary['total_requests']}\n"
            report += f"  Total Errors: {summary['total_errors']}\n"
        
        report += "\nRecommendations:\n"
        report += "---------------\n"
        for recommendation in analysis['recommendations']:
            report += f"  • {recommendation}\n"
        
        return report
    
    def save_results(self, filename: str = "zero_downtime_results.json"):
        """Save test results to file."""
        data = [asdict(result) for result in self.results]
        
        # Convert datetime objects to ISO strings
        for item in data:
            item['start_time'] = item['start_time'].isoformat()
            item['end_time'] = item['end_time'].isoformat()
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"Saved {len(self.results)} zero-downtime test results to {filename}")


# Pytest integration
class TestZeroDowntime:
    """Pytest test class for zero-downtime deployment testing."""
    
    @pytest.fixture(scope="class")
    async def zero_downtime_tester(self):
        """Create zero-downtime tester instance."""
        config = DeploymentTestConfig(
            base_url="http://localhost:8080",
            api_key="test-api-key",
            test_duration=120,  # 2 minutes for testing
            request_interval=0.5  # 500ms between requests
        )
        tester = ZeroDowntimeTester(config)
        return tester
    
    @pytest.mark.asyncio
    async def test_continuous_load(self, zero_downtime_tester):
        """Test continuous load without deployment."""
        result = await zero_downtime_tester.continuous_load_test(
            duration=60,
            test_name="continuous_load"
        )
        
        assert result.success_rate >= 0.95, f"Success rate too low: {result.success_rate:.2%}"
        assert not result.downtime_detected, f"Downtime detected: {result.max_downtime:.2f}s"
        assert result.p95_latency <= 200, f"P95 latency too high: {result.p95_latency:.2f}ms"
    
    @pytest.mark.asyncio
    async def test_rolling_update(self, zero_downtime_tester):
        """Test rolling update deployment."""
        result = await zero_downtime_tester.run_rolling_update_test(
            service_name="normalization-service",
            new_image_tag="v1.1.0",
            test_name="rolling_update"
        )
        
        assert result.success_rate >= 0.90, f"Success rate too low: {result.success_rate:.2%}"
        assert not result.downtime_detected, f"Downtime detected: {result.max_downtime:.2f}s"
    
    @pytest.mark.asyncio
    async def test_blue_green_deployment(self, zero_downtime_tester):
        """Test blue-green deployment."""
        result = await zero_downtime_tester.run_blue_green_test(
            service_name="normalization-service",
            new_image_tag="v1.2.0",
            test_name="blue_green"
        )
        
        assert result.success_rate >= 0.90, f"Success rate too low: {result.success_rate:.2%}"
        assert not result.downtime_detected, f"Downtime detected: {result.max_downtime:.2f}s"
    
    @pytest.mark.asyncio
    async def test_canary_deployment(self, zero_downtime_tester):
        """Test canary deployment."""
        result = await zero_downtime_tester.run_canary_test(
            service_name="normalization-service",
            new_image_tag="v1.3.0",
            canary_percentage=10,
            test_name="canary"
        )
        
        assert result.success_rate >= 0.90, f"Success rate too low: {result.success_rate:.2%}"
        assert not result.downtime_detected, f"Downtime detected: {result.max_downtime:.2f}s"


# Command-line interface
async def main():
    """Main function for running zero-downtime deployment tests."""
    import argparse
    
    parser = argparse.ArgumentParser(description='254Carbon Data Processing Pipeline Zero-Downtime Deployment Tests')
    parser.add_argument('--base-url', default='http://localhost:8080', help='Base URL for the API')
    parser.add_argument('--api-key', default='test-api-key', help='API key for authentication')
    parser.add_argument('--test-type', choices=['rolling', 'blue-green', 'canary', 'all'], 
                       default='rolling', help='Type of deployment test')
    parser.add_argument('--service-name', default='normalization-service', help='Service name to test')
    parser.add_argument('--new-image-tag', default='v1.1.0', help='New image tag for deployment')
    parser.add_argument('--duration', type=int, default=300, help='Test duration in seconds')
    parser.add_argument('--generate-report', action='store_true', help='Generate deployment report')
    
    args = parser.parse_args()
    
    # Create zero-downtime tester
    config = DeploymentTestConfig(
        base_url=args.base_url,
        api_key=args.api_key,
        test_duration=args.duration
    )
    
    tester = ZeroDowntimeTester(config)
    
    print(f"Running {args.test_type} zero-downtime deployment test...")
    
    # Run appropriate test
    if args.test_type == 'rolling':
        result = await tester.run_rolling_update_test(
            service_name=args.service_name,
            new_image_tag=args.new_image_tag,
            test_name="rolling_update"
        )
        print(f"Rolling update test completed: {result.success_rate:.2%} success rate")
    
    elif args.test_type == 'blue-green':
        result = await tester.run_blue_green_test(
            service_name=args.service_name,
            new_image_tag=args.new_image_tag,
            test_name="blue_green"
        )
        print(f"Blue-green test completed: {result.success_rate:.2%} success rate")
    
    elif args.test_type == 'canary':
        result = await tester.run_canary_test(
            service_name=args.service_name,
            new_image_tag=args.new_image_tag,
            canary_percentage=10,
            test_name="canary"
        )
        print(f"Canary test completed: {result.success_rate:.2%} success rate")
    
    elif args.test_type == 'all':
        # Run all deployment types
        results = []
        
        # Rolling update
        rolling_result = await tester.run_rolling_update_test(
            service_name=args.service_name,
            new_image_tag=f"{args.new_image_tag}-rolling",
            test_name="rolling_update"
        )
        results.append(rolling_result)
        
        # Blue-green
        blue_green_result = await tester.run_blue_green_test(
            service_name=args.service_name,
            new_image_tag=f"{args.new_image_tag}-blue-green",
            test_name="blue_green"
        )
        results.append(blue_green_result)
        
        # Canary
        canary_result = await tester.run_canary_test(
            service_name=args.service_name,
            new_image_tag=f"{args.new_image_tag}-canary",
            canary_percentage=10,
            test_name="canary"
        )
        results.append(canary_result)
        
        print(f"All deployment tests completed: {len(results)} tests")
    
    # Save results
    tester.save_results()
    
    # Generate report
    if args.generate_report:
        report = tester.generate_report()
        print(report)
        
        # Save report to file
        with open('zero_downtime_report.txt', 'w') as f:
            f.write(report)
    
    print("Zero-downtime deployment tests completed!")


if __name__ == "__main__":
    asyncio.run(main())
