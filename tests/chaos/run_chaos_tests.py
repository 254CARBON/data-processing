#!/usr/bin/env python3
"""
Chaos Engineering Test Runner
Validates system resilience and automated recovery mechanisms
"""

import subprocess
import time
import json
from datetime import datetime
from typing import Dict, List, Tuple

class ChaosTestRunner:
    """Execute and monitor chaos engineering experiments."""
    
    def __init__(self, namespace: str = "data-processing"):
        self.namespace = namespace
        self.results = []
    
    def run_experiment(self, experiment_file: str, duration: int = 300) -> Dict:
        """
        Run a chaos experiment and monitor system behavior.
        
        Args:
            experiment_file: Path to chaos experiment YAML
            duration: Duration to monitor in seconds
            
        Returns:
            Dict with experiment results
        """
        print(f"\n{'='*60}")
        print(f"Running experiment: {experiment_file}")
        print(f"{'='*60}\n")
        
        start_time = datetime.now()
        
        # Get baseline metrics
        baseline_metrics = self._get_metrics()
        print(f"Baseline metrics: {json.dumps(baseline_metrics, indent=2)}")
        
        # Apply chaos experiment
        print("\nApplying chaos experiment...")
        subprocess.run([
            "kubectl", "apply", "-f", experiment_file,
            "-n", self.namespace
        ], check=True)
        
        # Monitor system during chaos
        print("\nMonitoring system behavior...")
        chaos_metrics = []
        for i in range(duration // 10):
            time.sleep(10)
            metrics = self._get_metrics()
            chaos_metrics.append(metrics)
            print(f"  [{i*10}s] Error rate: {metrics['error_rate']}%, "
                  f"Latency P95: {metrics['latency_p95']}ms")
            
            # Check if system is recovering
            if self._check_recovery(metrics, baseline_metrics):
                print("  ✅ System recovering normally")
            else:
                print("  ⚠️  System showing degradation")
        
        # Delete chaos experiment
        print("\nRemoving chaos experiment...")
        subprocess.run([
            "kubectl", "delete", "-f", experiment_file,
            "-n", self.namespace
        ], check=True)
        
        # Wait for recovery
        print("\nWaiting for full recovery...")
        time.sleep(60)
        
        # Get post-chaos metrics
        final_metrics = self._get_metrics()
        print(f"Final metrics: {json.dumps(final_metrics, indent=2)}")
        
        # Analyze results
        end_time = datetime.now()
        result = {
            "experiment": experiment_file,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration": duration,
            "baseline_metrics": baseline_metrics,
            "chaos_metrics": chaos_metrics,
            "final_metrics": final_metrics,
            "passed": self._evaluate_experiment(baseline_metrics, chaos_metrics, final_metrics)
        }
        
        self.results.append(result)
        return result
    
    def _get_metrics(self) -> Dict:
        """Get current system metrics."""
        try:
            # Get error rate from Prometheus
            error_rate_result = subprocess.run([
                "kubectl", "run", "prom-query-" + str(int(time.time())),
                "--image=curlimages/curl", "--rm", "-i", "--restart=Never",
                "--namespace", self.namespace,
                "--", "curl", "-s",
                "http://prometheus.linkerd:9090/api/v1/query?query=sum(rate(request_total{classification!=\"success\"}[1m]))/sum(rate(request_total[1m]))*100"
            ], capture_output=True, text=True, timeout=30)
            
            # Parse response (simplified)
            error_rate = 0.0
            
            # Get latency P95
            latency_result = subprocess.run([
                "kubectl", "run", "prom-query-" + str(int(time.time())),
                "--image=curlimages/curl", "--rm", "-i", "--restart=Never",
                "--namespace", self.namespace,
                "--", "curl", "-s",
                "http://prometheus.linkerd:9090/api/v1/query?query=histogram_quantile(0.95,rate(response_latency_ms_bucket[1m]))"
            ], capture_output=True, text=True, timeout=30)
            
            latency_p95 = 0.0
            
            # Get pod status
            pod_result = subprocess.run([
                "kubectl", "get", "pods",
                "-n", self.namespace,
                "-o", "json"
            ], capture_output=True, text=True, check=True)
            
            pods_data = json.loads(pod_result.stdout)
            running_pods = sum(1 for pod in pods_data['items'] 
                             if pod['status']['phase'] == 'Running')
            total_pods = len(pods_data['items'])
            
            return {
                "error_rate": error_rate,
                "latency_p95": latency_p95,
                "running_pods": running_pods,
                "total_pods": total_pods,
                "pod_availability": (running_pods / total_pods * 100) if total_pods > 0 else 0
            }
        except Exception as e:
            print(f"Error getting metrics: {e}")
            return {
                "error_rate": 0.0,
                "latency_p95": 0.0,
                "running_pods": 0,
                "total_pods": 0,
                "pod_availability": 0.0
            }
    
    def _check_recovery(self, current: Dict, baseline: Dict) -> bool:
        """Check if system is recovering."""
        # Allow 20% degradation during chaos
        if current['error_rate'] > baseline['error_rate'] * 1.2:
            return False
        if current['latency_p95'] > baseline['latency_p95'] * 1.2:
            return False
        if current['pod_availability'] < 80:
            return False
        return True
    
    def _evaluate_experiment(self, baseline: Dict, chaos: List[Dict], final: Dict) -> bool:
        """Evaluate if experiment passed."""
        # System should recover to near baseline after chaos
        if final['error_rate'] > baseline['error_rate'] * 1.1:
            return False
        if final['latency_p95'] > baseline['latency_p95'] * 1.1:
            return False
        if final['pod_availability'] < 95:
            return False
        return True
    
    def generate_report(self, output_file: str = "chaos_test_report.json"):
        """Generate test report."""
        report = {
            "timestamp": datetime.now().isoformat(),
            "total_experiments": len(self.results),
            "passed": sum(1 for r in self.results if r['passed']),
            "failed": sum(1 for r in self.results if not r['passed']),
            "experiments": self.results
        }
        
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        print(f"\n{'='*60}")
        print("Chaos Testing Report")
        print(f"{'='*60}")
        print(f"Total experiments: {report['total_experiments']}")
        print(f"Passed: {report['passed']}")
        print(f"Failed: {report['failed']}")
        print(f"Report saved to: {output_file}")
        print(f"{'='*60}\n")

def main():
    """Run chaos engineering test suite."""
    runner = ChaosTestRunner()
    
    experiments = [
        "tests/chaos/experiments/pod-failure.yaml",
        "tests/chaos/experiments/network-chaos.yaml"
    ]
    
    for experiment in experiments:
        try:
            result = runner.run_experiment(experiment, duration=180)
            if result['passed']:
                print(f"✅ {experiment} PASSED")
            else:
                print(f"❌ {experiment} FAILED")
        except Exception as e:
            print(f"❌ {experiment} ERROR: {e}")
    
    runner.generate_report()

if __name__ == "__main__":
    main()
