"""
Locust Load Testing Suite for 254Carbon Data Processing Pipeline

This script tests the performance and scalability of the data processing pipeline
under various load conditions using Locust.
"""

import json
import random
import time
from datetime import datetime, timezone
from typing import Dict, Any, List

from locust import HttpUser, task, between, events
from locust.exception import StopUser


class MarketDataUser(HttpUser):
    """Locust user class for market data processing load testing."""
    
    wait_time = between(1, 3)
    weight = 3
    
    def on_start(self):
        """Setup for each user."""
        self.api_key = self.environment.parsed_options.api_key or "test-api-key"
        self.instruments = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META', 'NVDA', 'NFLX']
        self.headers = {
            'Content-Type': 'application/json',
            'X-API-Key': self.api_key
        }
        
        # Verify system health
        response = self.client.get("/health")
        if response.status_code != 200:
            raise StopUser("Health check failed")
    
    @task(10)
    def send_market_data(self):
        """Send single market data entry."""
        market_data = self.generate_market_data()
        
        with self.client.post(
            "/api/market-data",
            json=market_data,
            headers=self.headers,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
    
    @task(5)
    def send_batch_market_data(self):
        """Send batch of market data entries."""
        batch_size = random.randint(5, 15)
        market_data_batch = [self.generate_market_data() for _ in range(batch_size)]
        
        with self.client.post(
            "/api/market-data/batch",
            json=market_data_batch,
            headers=self.headers,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
    
    @task(2)
    def get_market_data(self):
        """Retrieve market data."""
        instrument = random.choice(self.instruments)
        
        with self.client.get(
            f"/api/market-data/{instrument}",
            headers=self.headers,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
    
    @task(1)
    def get_health_status(self):
        """Check health status."""
        with self.client.get("/health", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
    
    def generate_market_data(self) -> Dict[str, Any]:
        """Generate random market data."""
        instrument = random.choice(self.instruments)
        base_price = random.randint(100, 500)
        price_variation = random.randint(-10, 10)
        price = base_price + price_variation
        
        return {
            'instrument_id': instrument,
            'price': price,
            'volume': random.randint(1000, 100000),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'bid': price - random.randint(1, 5),
            'ask': price + random.randint(1, 5),
            'last_trade_price': price,
            'market': 'NASDAQ',
            'currency': 'USD'
        }


class HighFrequencyUser(HttpUser):
    """Locust user class for high-frequency trading simulation."""
    
    wait_time = between(0.1, 0.5)
    weight = 1
    
    def on_start(self):
        """Setup for each user."""
        self.api_key = self.environment.parsed_options.api_key or "test-api-key"
        self.instruments = ['AAPL', 'GOOGL', 'MSFT', 'TSLA']
        self.headers = {
            'Content-Type': 'application/json',
            'X-API-Key': self.api_key
        }
    
    @task(20)
    def send_high_frequency_data(self):
        """Send high-frequency market data."""
        market_data = self.generate_market_data()
        
        with self.client.post(
            "/api/market-data",
            json=market_data,
            headers=self.headers,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
    
    @task(5)
    def send_high_frequency_batch(self):
        """Send high-frequency batch data."""
        batch_size = random.randint(10, 25)
        market_data_batch = [self.generate_market_data() for _ in range(batch_size)]
        
        with self.client.post(
            "/api/market-data/batch",
            json=market_data_batch,
            headers=self.headers,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
    
    def generate_market_data(self) -> Dict[str, Any]:
        """Generate high-frequency market data."""
        instrument = random.choice(self.instruments)
        base_price = random.randint(100, 500)
        price_variation = random.randint(-5, 5)
        price = base_price + price_variation
        
        return {
            'instrument_id': instrument,
            'price': price,
            'volume': random.randint(100, 1000),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'bid': price - random.randint(1, 3),
            'ask': price + random.randint(1, 3),
            'last_trade_price': price,
            'market': 'NASDAQ',
            'currency': 'USD'
        }


class VolumeUser(HttpUser):
    """Locust user class for high-volume data processing."""
    
    wait_time = between(2, 5)
    weight = 1
    
    def on_start(self):
        """Setup for each user."""
        self.api_key = self.environment.parsed_options.api_key or "test-api-key"
        self.instruments = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META', 'NVDA', 'NFLX']
        self.headers = {
            'Content-Type': 'application/json',
            'X-API-Key': self.api_key
        }
    
    @task(10)
    def send_large_batch(self):
        """Send large batch of market data."""
        batch_size = random.randint(50, 100)
        market_data_batch = [self.generate_market_data() for _ in range(batch_size)]
        
        with self.client.post(
            "/api/market-data/batch",
            json=market_data_batch,
            headers=self.headers,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
    
    @task(3)
    def send_very_large_batch(self):
        """Send very large batch of market data."""
        batch_size = random.randint(200, 500)
        market_data_batch = [self.generate_market_data() for _ in range(batch_size)]
        
        with self.client.post(
            "/api/market-data/batch",
            json=market_data_batch,
            headers=self.headers,
            catch_response=True
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
    
    def generate_market_data(self) -> Dict[str, Any]:
        """Generate market data for volume testing."""
        instrument = random.choice(self.instruments)
        base_price = random.randint(100, 500)
        price_variation = random.randint(-10, 10)
        price = base_price + price_variation
        
        return {
            'instrument_id': instrument,
            'price': price,
            'volume': random.randint(1000, 100000),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'bid': price - random.randint(1, 5),
            'ask': price + random.randint(1, 5),
            'last_trade_price': price,
            'market': 'NASDAQ',
            'currency': 'USD'
        }


class AdminUser(HttpUser):
    """Locust user class for administrative operations."""
    
    wait_time = between(5, 10)
    weight = 1
    
    def on_start(self):
        """Setup for each user."""
        self.api_key = self.environment.parsed_options.api_key or "test-api-key"
        self.headers = {
            'Content-Type': 'application/json',
            'X-API-Key': self.api_key
        }
    
    @task(5)
    def get_system_status(self):
        """Get system status."""
        with self.client.get("/api/system/status", headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
    
    @task(3)
    def get_metrics(self):
        """Get system metrics."""
        with self.client.get("/metrics", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
    
    @task(2)
    def get_health_detailed(self):
        """Get detailed health information."""
        with self.client.get("/health/detailed", catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
    
    @task(1)
    def get_api_keys(self):
        """Get API keys (admin operation)."""
        with self.client.get("/api/admin/keys", headers=self.headers, catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")


# Custom event handlers
@events.request.add_listener
def on_request(request_type, name, response_time, response_length, exception, context, **kwargs):
    """Custom request event handler."""
    if exception:
        print(f"Request failed: {name} - {exception}")
    elif response_time > 1000:  # Log slow requests
        print(f"Slow request: {name} - {response_time}ms")


@events.user_error.add_listener
def on_user_error(user_instance, exception, tb, **kwargs):
    """Custom user error event handler."""
    print(f"User error: {exception}")


# Test configuration
class TestConfig:
    """Configuration for load tests."""
    
    def __init__(self):
        self.base_url = "http://localhost:8080"
        self.api_key = "test-api-key"
        self.test_duration = "5m"
        self.users = 50
        self.spawn_rate = 10
        
    def get_locust_config(self) -> Dict[str, Any]:
        """Get Locust configuration."""
        return {
            'host': self.base_url,
            'users': self.users,
            'spawn_rate': self.spawn_rate,
            'run_time': self.test_duration,
        }


# Main execution
if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='254Carbon Data Processing Pipeline Load Test')
    parser.add_argument('--host', default='http://localhost:8080', help='Target host')
    parser.add_argument('--users', type=int, default=50, help='Number of users')
    parser.add_argument('--spawn-rate', type=int, default=10, help='Spawn rate')
    parser.add_argument('--run-time', default='5m', help='Test duration')
    parser.add_argument('--api-key', default='test-api-key', help='API key for authentication')
    parser.add_argument('--headless', action='store_true', help='Run in headless mode')
    parser.add_argument('--html', help='Generate HTML report')
    parser.add_argument('--csv', help='Generate CSV report')
    
    args = parser.parse_args()
    
    print("Starting Locust load test for 254Carbon Data Processing Pipeline")
    print(f"Target host: {args.host}")
    print(f"Users: {args.users}")
    print(f"Spawn rate: {args.spawn_rate}")
    print(f"Duration: {args.run_time}")
    print(f"API key: {args.api_key}")
    
    # Set environment variables for Locust
    import os
    os.environ['LOCUST_HOST'] = args.host
    os.environ['LOCUST_USERS'] = str(args.users)
    os.environ['LOCUST_SPAWN_RATE'] = str(args.spawn_rate)
    os.environ['LOCUST_RUN_TIME'] = args.run_time
    os.environ['LOCUST_API_KEY'] = args.api_key
    
    if args.headless:
        print("Running in headless mode...")
        # Locust will be started with command line arguments
    else:
        print("Starting Locust web interface...")
        # Locust web interface will be available at http://localhost:8089
