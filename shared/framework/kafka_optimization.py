"""
Kafka optimization utilities for the 254Carbon Data Processing Pipeline.

This module provides optimized Kafka consumer configurations and performance
tuning utilities.
"""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Callable
from dataclasses import dataclass
from enum import Enum
import json
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaError
import numpy as np

logger = logging.getLogger(__name__)


class ConsumerStrategy(Enum):
    """Kafka consumer strategies."""
    BALANCED = "balanced"      # Balanced partition assignment
    STICKY = "sticky"          # Sticky partition assignment
    ROUND_ROBIN = "round_robin" # Round-robin partition assignment


@dataclass
class KafkaConsumerConfig:
    """Optimized Kafka consumer configuration."""
    bootstrap_servers: str = "localhost:9092"
    group_id: str = "data-processing-group"
    auto_offset_reset: str = "latest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 1000
    max_poll_records: int = 500
    fetch_min_bytes: int = 1
    fetch_max_wait_ms: int = 500
    max_partition_fetch_bytes: int = 1048576  # 1MB
    session_timeout_ms: int = 30000
    heartbeat_interval_ms: int = 3000
    max_poll_interval_ms: int = 300000
    request_timeout_ms: int = 40000
    retry_backoff_ms: int = 100
    reconnect_backoff_ms: int = 50
    reconnect_backoff_max_ms: int = 1000
    consumer_strategy: ConsumerStrategy = ConsumerStrategy.BALANCED
    enable_metrics: bool = True
    batch_size: int = 100
    batch_timeout_ms: int = 100
    compression_type: str = "gzip"
    acks: str = "all"
    retries: int = 3
    linger_ms: int = 5
    buffer_memory: int = 33554432  # 32MB


@dataclass
class KafkaProducerConfig:
    """Optimized Kafka producer configuration."""
    bootstrap_servers: str = "localhost:9092"
    compression_type: str = "gzip"
    acks: str = "all"
    retries: int = 3
    batch_size: int = 16384
    linger_ms: int = 5
    buffer_memory: int = 33554432  # 32MB
    max_request_size: int = 1048576  # 1MB
    request_timeout_ms: int = 30000
    delivery_timeout_ms: int = 120000
    enable_idempotence: bool = True
    max_in_flight_requests_per_connection: int = 5
    retry_backoff_ms: int = 100
    reconnect_backoff_ms: int = 50
    reconnect_backoff_max_ms: int = 1000


class KafkaOptimizer:
    """Kafka optimization utilities."""
    
    def __init__(self, consumer_config: Optional[KafkaConsumerConfig] = None,
                 producer_config: Optional[KafkaProducerConfig] = None):
        self.consumer_config = consumer_config or KafkaConsumerConfig()
        self.producer_config = producer_config or KafkaProducerConfig()
        self.metrics: Dict[str, Any] = {}
        self.consumer: Optional[AIOKafkaConsumer] = None
        self.producer: Optional[AIOKafkaProducer] = None
    
    def get_optimized_consumer_config(self, 
                                    topic: str,
                                    service_name: str,
                                    load_level: str = "medium") -> Dict[str, Any]:
        """Get optimized consumer configuration based on service and load."""
        base_config = {
            'bootstrap_servers': self.consumer_config.bootstrap_servers,
            'group_id': f"{service_name}-{topic}-group",
            'auto_offset_reset': self.consumer_config.auto_offset_reset,
            'enable_auto_commit': self.consumer_config.enable_auto_commit,
            'auto_commit_interval_ms': self.consumer_config.auto_commit_interval_ms,
            'session_timeout_ms': self.consumer_config.session_timeout_ms,
            'heartbeat_interval_ms': self.consumer_config.heartbeat_interval_ms,
            'max_poll_interval_ms': self.consumer_config.max_poll_interval_ms,
            'request_timeout_ms': self.consumer_config.request_timeout_ms,
            'retry_backoff_ms': self.consumer_config.retry_backoff_ms,
            'reconnect_backoff_ms': self.consumer_config.reconnect_backoff_ms,
            'reconnect_backoff_max_ms': self.consumer_config.reconnect_backoff_max_ms,
        }
        
        # Optimize based on load level
        if load_level == "low":
            base_config.update({
                'max_poll_records': 100,
                'fetch_min_bytes': 1,
                'fetch_max_wait_ms': 1000,
                'max_partition_fetch_bytes': 524288,  # 512KB
            })
        elif load_level == "medium":
            base_config.update({
                'max_poll_records': 500,
                'fetch_min_bytes': 1,
                'fetch_max_wait_ms': 500,
                'max_partition_fetch_bytes': 1048576,  # 1MB
            })
        elif load_level == "high":
            base_config.update({
                'max_poll_records': 1000,
                'fetch_min_bytes': 1024,
                'fetch_max_wait_ms': 100,
                'max_partition_fetch_bytes': 2097152,  # 2MB
            })
        elif load_level == "extreme":
            base_config.update({
                'max_poll_records': 2000,
                'fetch_min_bytes': 2048,
                'fetch_max_wait_ms': 50,
                'max_partition_fetch_bytes': 4194304,  # 4MB
            })
        
        return base_config
    
    def get_optimized_producer_config(self, 
                                    service_name: str,
                                    load_level: str = "medium") -> Dict[str, Any]:
        """Get optimized producer configuration based on service and load."""
        base_config = {
            'bootstrap_servers': self.producer_config.bootstrap_servers,
            'compression_type': self.producer_config.compression_type,
            'acks': self.producer_config.acks,
            'retries': self.producer_config.retries,
            'request_timeout_ms': self.producer_config.request_timeout_ms,
            'delivery_timeout_ms': self.producer_config.delivery_timeout_ms,
            'enable_idempotence': self.producer_config.enable_idempotence,
            'max_in_flight_requests_per_connection': self.producer_config.max_in_flight_requests_per_connection,
            'retry_backoff_ms': self.producer_config.retry_backoff_ms,
            'reconnect_backoff_ms': self.producer_config.reconnect_backoff_ms,
            'reconnect_backoff_max_ms': self.producer_config.reconnect_backoff_max_ms,
        }
        
        # Optimize based on load level
        if load_level == "low":
            base_config.update({
                'batch_size': 8192,
                'linger_ms': 10,
                'buffer_memory': 16777216,  # 16MB
                'max_request_size': 524288,  # 512KB
            })
        elif load_level == "medium":
            base_config.update({
                'batch_size': 16384,
                'linger_ms': 5,
                'buffer_memory': 33554432,  # 32MB
                'max_request_size': 1048576,  # 1MB
            })
        elif load_level == "high":
            base_config.update({
                'batch_size': 32768,
                'linger_ms': 2,
                'buffer_memory': 67108864,  # 64MB
                'max_request_size': 2097152,  # 2MB
            })
        elif load_level == "extreme":
            base_config.update({
                'batch_size': 65536,
                'linger_ms': 1,
                'buffer_memory': 134217728,  # 128MB
                'max_request_size': 4194304,  # 4MB
            })
        
        return base_config
    
    def calculate_optimal_partitions(self, 
                                   expected_throughput: int,
                                   target_latency_ms: int,
                                   message_size_bytes: int = 1024) -> int:
        """Calculate optimal number of partitions for a topic."""
        # Estimate based on throughput and latency requirements
        messages_per_second = expected_throughput
        messages_per_partition = messages_per_second / 10  # Assume 10 partitions initially
        
        # Adjust based on message size
        if message_size_bytes > 1024:
            messages_per_partition *= 0.8  # Reduce for larger messages
        elif message_size_bytes < 512:
            messages_per_partition *= 1.2  # Increase for smaller messages
        
        # Calculate optimal partitions
        optimal_partitions = max(1, int(messages_per_second / messages_per_partition))
        
        # Ensure reasonable bounds
        optimal_partitions = max(1, min(optimal_partitions, 100))
        
        logger.info(f"Calculated optimal partitions: {optimal_partitions} for throughput: {expected_throughput} msg/s")
        return optimal_partitions
    
    def calculate_optimal_replication_factor(self, 
                                           cluster_size: int,
                                           durability_requirement: str = "medium") -> int:
        """Calculate optimal replication factor."""
        if durability_requirement == "low":
            return min(2, cluster_size)
        elif durability_requirement == "medium":
            return min(3, cluster_size)
        elif durability_requirement == "high":
            return min(5, cluster_size)
        else:
            return min(3, cluster_size)
    
    def optimize_consumer_group(self, 
                               topic: str,
                               consumer_count: int,
                               partition_count: int) -> Dict[str, Any]:
        """Optimize consumer group configuration."""
        # Calculate optimal consumer count
        optimal_consumers = min(consumer_count, partition_count)
        
        # Calculate partition assignment
        partitions_per_consumer = partition_count // optimal_consumers
        remaining_partitions = partition_count % optimal_consumers
        
        assignment = {}
        partition_index = 0
        
        for i in range(optimal_consumers):
            consumer_id = f"consumer-{i}"
            partitions = []
            
            # Assign base partitions
            for _ in range(partitions_per_consumer):
                partitions.append(partition_index)
                partition_index += 1
            
            # Assign remaining partitions
            if i < remaining_partitions:
                partitions.append(partition_index)
                partition_index += 1
            
            assignment[consumer_id] = partitions
        
        return {
            'optimal_consumers': optimal_consumers,
            'partitions_per_consumer': partitions_per_consumer,
            'assignment': assignment,
            'load_balance': self._calculate_load_balance(assignment)
        }
    
    def _calculate_load_balance(self, assignment: Dict[str, List[int]]) -> float:
        """Calculate load balance score."""
        if not assignment:
            return 0.0
        
        partition_counts = [len(partitions) for partitions in assignment.values()]
        if not partition_counts:
            return 0.0
        
        mean_count = np.mean(partition_counts)
        if mean_count == 0:
            return 0.0
        
        variance = np.var(partition_counts)
        balance_score = 1.0 - (variance / (mean_count ** 2))
        return max(0.0, balance_score)
    
    def analyze_consumer_performance(self, 
                                  consumer_metrics: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze consumer performance and provide optimization recommendations."""
        analysis = {
            'current_performance': consumer_metrics,
            'optimization_recommendations': [],
            'performance_score': 0.0
        }
        
        # Analyze throughput
        current_throughput = consumer_metrics.get('throughput', 0)
        target_throughput = consumer_metrics.get('target_throughput', 1000)
        
        if current_throughput < target_throughput * 0.8:
            analysis['optimization_recommendations'].append(
                "Throughput is below target. Consider increasing max_poll_records or fetch_min_bytes"
            )
        
        # Analyze latency
        current_latency = consumer_metrics.get('average_latency', 0)
        target_latency = consumer_metrics.get('target_latency', 100)
        
        if current_latency > target_latency * 1.2:
            analysis['optimization_recommendations'].append(
                "Latency is above target. Consider reducing fetch_max_wait_ms or batch_size"
            )
        
        # Analyze error rate
        error_rate = consumer_metrics.get('error_rate', 0)
        if error_rate > 0.01:  # 1%
            analysis['optimization_recommendations'].append(
                "Error rate is high. Consider increasing retry_backoff_ms or session_timeout_ms"
            )
        
        # Calculate performance score
        throughput_score = min(1.0, current_throughput / target_throughput)
        latency_score = max(0.0, 1.0 - (current_latency / target_latency))
        error_score = max(0.0, 1.0 - error_rate * 100)
        
        analysis['performance_score'] = (throughput_score + latency_score + error_score) / 3
        
        return analysis
    
    def get_topic_optimization_recommendations(self, 
                                             topic: str,
                                             current_config: Dict[str, Any],
                                             usage_patterns: Dict[str, Any]) -> Dict[str, Any]:
        """Get topic-specific optimization recommendations."""
        recommendations = {
            'topic': topic,
            'current_config': current_config,
            'recommendations': [],
            'priority': 'medium'
        }
        
        # Analyze usage patterns
        message_rate = usage_patterns.get('message_rate', 0)
        message_size = usage_patterns.get('message_size', 1024)
        retention_hours = usage_patterns.get('retention_hours', 168)  # 7 days
        
        # Partition recommendations
        current_partitions = current_config.get('partitions', 1)
        optimal_partitions = self.calculate_optimal_partitions(
            message_rate, 
            usage_patterns.get('target_latency', 100),
            message_size
        )
        
        if current_partitions < optimal_partitions:
            recommendations['recommendations'].append({
                'type': 'partitions',
                'current': current_partitions,
                'recommended': optimal_partitions,
                'reason': f'Increase partitions to handle {message_rate} msg/s throughput',
                'priority': 'high'
            })
        
        # Retention recommendations
        current_retention = current_config.get('retention_ms', retention_hours * 3600 * 1000)
        if current_retention > retention_hours * 3600 * 1000 * 1.5:
            recommendations['recommendations'].append({
                'type': 'retention',
                'current': current_retention,
                'recommended': retention_hours * 3600 * 1000,
                'reason': 'Reduce retention to save disk space',
                'priority': 'medium'
            })
        
        # Compression recommendations
        current_compression = current_config.get('compression_type', 'none')
        if message_size > 1024 and current_compression == 'none':
            recommendations['recommendations'].append({
                'type': 'compression',
                'current': current_compression,
                'recommended': 'gzip',
                'reason': 'Enable compression for large messages',
                'priority': 'medium'
            })
        
        # Set overall priority
        high_priority_count = sum(1 for rec in recommendations['recommendations'] if rec['priority'] == 'high')
        if high_priority_count > 0:
            recommendations['priority'] = 'high'
        elif len(recommendations['recommendations']) > 2:
            recommendations['priority'] = 'medium'
        else:
            recommendations['priority'] = 'low'
        
        return recommendations
    
    def monitor_consumer_health(self, 
                               consumer: AIOKafkaConsumer,
                               topic: str) -> Dict[str, Any]:
        """Monitor consumer health and performance."""
        health_metrics = {
            'topic': topic,
            'timestamp': time.time(),
            'consumer_id': consumer._client_id,
            'group_id': consumer._group_id,
            'subscription': list(consumer._subscription.subscription),
            'assignment': list(consumer._subscription.assignment),
            'metrics': {}
        }
        
        try:
            # Get consumer metrics
            metrics = consumer._client._metrics
            if metrics:
                health_metrics['metrics'] = {
                    'records_consumed': metrics.get('records-consumed-total', 0),
                    'bytes_consumed': metrics.get('bytes-consumed-total', 0),
                    'fetch_latency_avg': metrics.get('fetch-latency-avg', 0),
                    'fetch_latency_max': metrics.get('fetch-latency-max', 0),
                    'fetch_rate': metrics.get('fetch-rate', 0),
                    'fetch_size_avg': metrics.get('fetch-size-avg', 0),
                    'fetch_size_max': metrics.get('fetch-size-max', 0),
                    'fetch_throttle_time_avg': metrics.get('fetch-throttle-time-avg', 0),
                    'fetch_wait_time_avg': metrics.get('fetch-wait-time-avg', 0),
                    'records_lag': metrics.get('records-lag-max', 0),
                    'records_lag_avg': metrics.get('records-lag-avg', 0),
                }
        except Exception as e:
            logger.warning(f"Error getting consumer metrics: {e}")
            health_metrics['metrics'] = {}
        
        return health_metrics
    
    def get_optimization_summary(self) -> Dict[str, Any]:
        """Get summary of optimization recommendations."""
        return {
            'consumer_config': self.consumer_config,
            'producer_config': self.producer_config,
            'optimization_tips': [
                "Use appropriate batch sizes based on load level",
                "Monitor consumer lag and adjust max_poll_records accordingly",
                "Enable compression for topics with large messages",
                "Use sticky partition assignment for better load balancing",
                "Set appropriate retention policies to manage disk usage",
                "Monitor fetch latency and adjust fetch_max_wait_ms",
                "Use idempotent producers for critical data",
                "Implement circuit breakers for consumer error handling"
            ],
            'performance_targets': {
                'throughput': '1000+ msg/s',
                'latency': '<100ms P95',
                'error_rate': '<0.1%',
                'consumer_lag': '<1000 messages'
            }
        }


# Global Kafka optimizer instance
_kafka_optimizer = None

def get_kafka_optimizer() -> KafkaOptimizer:
    """Get global Kafka optimizer instance."""
    global _kafka_optimizer
    if _kafka_optimizer is None:
        _kafka_optimizer = KafkaOptimizer()
    return _kafka_optimizer


# Example usage and testing
if __name__ == "__main__":
    # Create Kafka optimizer
    optimizer = KafkaOptimizer()
    
    # Test consumer configuration optimization
    print("Testing consumer configuration optimization...")
    
    # Low load configuration
    low_load_config = optimizer.get_optimized_consumer_config(
        topic="market-data",
        service_name="normalization-service",
        load_level="low"
    )
    print(f"Low load config: {low_load_config}")
    
    # High load configuration
    high_load_config = optimizer.get_optimized_consumer_config(
        topic="market-data",
        service_name="normalization-service",
        load_level="high"
    )
    print(f"High load config: {high_load_config}")
    
    # Test partition optimization
    print("\nTesting partition optimization...")
    optimal_partitions = optimizer.calculate_optimal_partitions(
        expected_throughput=1000,
        target_latency_ms=100,
        message_size_bytes=1024
    )
    print(f"Optimal partitions: {optimal_partitions}")
    
    # Test consumer group optimization
    print("\nTesting consumer group optimization...")
    group_optimization = optimizer.optimize_consumer_group(
        topic="market-data",
        consumer_count=4,
        partition_count=12
    )
    print(f"Consumer group optimization: {group_optimization}")
    
    # Test performance analysis
    print("\nTesting performance analysis...")
    consumer_metrics = {
        'throughput': 800,
        'target_throughput': 1000,
        'average_latency': 120,
        'target_latency': 100,
        'error_rate': 0.005
    }
    
    performance_analysis = optimizer.analyze_consumer_performance(consumer_metrics)
    print(f"Performance analysis: {performance_analysis}")
    
    # Test topic optimization recommendations
    print("\nTesting topic optimization recommendations...")
    current_config = {
        'partitions': 3,
        'retention_ms': 604800000,  # 7 days
        'compression_type': 'none'
    }
    
    usage_patterns = {
        'message_rate': 1500,
        'message_size': 2048,
        'retention_hours': 168,
        'target_latency': 100
    }
    
    topic_recommendations = optimizer.get_topic_optimization_recommendations(
        topic="market-data",
        current_config=current_config,
        usage_patterns=usage_patterns
    )
    print(f"Topic recommendations: {topic_recommendations}")
    
    # Get optimization summary
    print("\nOptimization summary:")
    summary = optimizer.get_optimization_summary()
    print(f"Summary: {summary}")
    
    print("\nKafka optimization testing completed!")
