/**
 * k6 Load Testing Suite for 254Carbon Data Processing Pipeline
 * 
 * This script tests the performance and scalability of the data processing pipeline
 * under various load conditions.
 */

import http from 'k6/http';
import { check, sleep } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';
import { randomString, randomIntBetween } from 'https://jslib.k6.io/k6-utils/1.2.0/index.js';

// Custom metrics
const errorRate = new Rate('error_rate');
const responseTime = new Trend('response_time');
const throughput = new Counter('throughput');
const dataProcessingLatency = new Trend('data_processing_latency');

// Test configuration
const BASE_URL = __ENV.BASE_URL || 'http://localhost:8080';
const API_KEY = __ENV.API_KEY || 'test-api-key';
const TEST_DURATION = __ENV.TEST_DURATION || '5m';
const VIRTUAL_USERS = __ENV.VIRTUAL_USERS || 50;

// Test data
const INSTRUMENTS = ['AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'META', 'NVDA', 'NFLX'];
const MARKET_DATA_TEMPLATE = {
    instrument_id: '',
    price: 0,
    volume: 0,
    timestamp: '',
    bid: 0,
    ask: 0,
    last_trade_price: 0
};

// Test scenarios
export const scenarios = {
    // Smoke test - light load to verify basic functionality
    smoke_test: {
        executor: 'constant-vus',
        vus: 5,
        duration: '1m',
        tags: { test_type: 'smoke' },
    },
    
    // Load test - normal expected load
    load_test: {
        executor: 'constant-vus',
        vus: parseInt(VIRTUAL_USERS),
        duration: TEST_DURATION,
        tags: { test_type: 'load' },
    },
    
    // Stress test - beyond normal capacity
    stress_test: {
        executor: 'ramping-vus',
        startVUs: 0,
        stages: [
            { duration: '2m', target: 50 },
            { duration: '5m', target: 100 },
            { duration: '3m', target: 200 },
            { duration: '2m', target: 0 },
        ],
        tags: { test_type: 'stress' },
    },
    
    // Spike test - sudden load increases
    spike_test: {
        executor: 'ramping-vus',
        startVUs: 0,
        stages: [
            { duration: '1m', target: 10 },
            { duration: '30s', target: 200 },
            { duration: '1m', target: 10 },
            { duration: '30s', target: 200 },
            { duration: '1m', target: 10 },
        ],
        tags: { test_type: 'spike' },
    },
    
    // Volume test - high data volume
    volume_test: {
        executor: 'constant-vus',
        vus: 20,
        duration: '10m',
        tags: { test_type: 'volume' },
    },
    
    // Endurance test - extended duration
    endurance_test: {
        executor: 'constant-vus',
        vus: 30,
        duration: '30m',
        tags: { test_type: 'endurance' },
    },
};

// Test options
export const options = {
    scenarios: scenarios,
    thresholds: {
        http_req_duration: ['p(95)<100', 'p(99)<200'],
        http_req_failed: ['rate<0.01'],
        error_rate: ['rate<0.01'],
        response_time: ['p(95)<100', 'p(99)<200'],
        data_processing_latency: ['p(95)<500', 'p(99)<1000'],
    },
    tags: {
        environment: __ENV.ENVIRONMENT || 'test',
        version: __ENV.VERSION || '1.0.0',
    },
};

// Setup function - runs once before the test
export function setup() {
    console.log('Starting k6 load test for 254Carbon Data Processing Pipeline');
    console.log(`Base URL: ${BASE_URL}`);
    console.log(`Test Duration: ${TEST_DURATION}`);
    console.log(`Virtual Users: ${VIRTUAL_USERS}`);
    
    // Verify system health
    const healthCheck = http.get(`${BASE_URL}/health`);
    if (healthCheck.status !== 200) {
        throw new Error(`Health check failed: ${healthCheck.status}`);
    }
    
    // Verify API key
    const authCheck = http.get(`${BASE_URL}/api/market-data`, {
        headers: { 'X-API-Key': API_KEY },
    });
    if (authCheck.status === 401) {
        throw new Error('API key authentication failed');
    }
    
    return {
        startTime: new Date().toISOString(),
        baseUrl: BASE_URL,
        apiKey: API_KEY,
    };
}

// Main test function - runs for each virtual user
export default function(data) {
    const testType = __ENV.TEST_TYPE || 'load';
    
    switch (testType) {
        case 'smoke':
            runSmokeTest(data);
            break;
        case 'load':
            runLoadTest(data);
            break;
        case 'stress':
            runStressTest(data);
            break;
        case 'spike':
            runSpikeTest(data);
            break;
        case 'volume':
            runVolumeTest(data);
            break;
        case 'endurance':
            runEnduranceTest(data);
            break;
        default:
            runLoadTest(data);
    }
}

// Smoke test - basic functionality verification
function runSmokeTest(data) {
    // Test health endpoint
    const healthResponse = http.get(`${data.baseUrl}/health`);
    check(healthResponse, {
        'health check status is 200': (r) => r.status === 200,
        'health check response time < 100ms': (r) => r.timings.duration < 100,
    });
    
    // Test API endpoint
    const apiResponse = http.get(`${data.baseUrl}/api/market-data`, {
        headers: { 'X-API-Key': data.apiKey },
    });
    check(apiResponse, {
        'API status is 200': (r) => r.status === 200,
        'API response time < 200ms': (r) => r.timings.duration < 200,
    });
    
    sleep(1);
}

// Load test - normal expected load
function runLoadTest(data) {
    const startTime = Date.now();
    
    // Generate market data
    const marketData = generateMarketData();
    
    // Send market data
    const response = http.post(`${data.baseUrl}/api/market-data`, JSON.stringify(marketData), {
        headers: {
            'Content-Type': 'application/json',
            'X-API-Key': data.apiKey,
        },
    });
    
    const endTime = Date.now();
    const processingLatency = endTime - startTime;
    
    // Record metrics
    errorRate.add(response.status !== 200);
    responseTime.add(response.timings.duration);
    throughput.add(1);
    dataProcessingLatency.add(processingLatency);
    
    // Assertions
    check(response, {
        'status is 200': (r) => r.status === 200,
        'response time < 100ms': (r) => r.timings.duration < 100,
        'processing latency < 500ms': () => processingLatency < 500,
        'response has data': (r) => r.json('status') === 'success',
    });
    
    sleep(randomIntBetween(1, 3));
}

// Stress test - beyond normal capacity
function runStressTest(data) {
    const startTime = Date.now();
    
    // Generate multiple market data entries
    const marketDataBatch = [];
    for (let i = 0; i < randomIntBetween(5, 15); i++) {
        marketDataBatch.push(generateMarketData());
    }
    
    // Send batch
    const response = http.post(`${data.baseUrl}/api/market-data/batch`, JSON.stringify(marketDataBatch), {
        headers: {
            'Content-Type': 'application/json',
            'X-API-Key': data.apiKey,
        },
    });
    
    const endTime = Date.now();
    const processingLatency = endTime - startTime;
    
    // Record metrics
    errorRate.add(response.status !== 200);
    responseTime.add(response.timings.duration);
    throughput.add(marketDataBatch.length);
    dataProcessingLatency.add(processingLatency);
    
    // Assertions
    check(response, {
        'status is 200': (r) => r.status === 200,
        'response time < 200ms': (r) => r.timings.duration < 200,
        'processing latency < 1000ms': () => processingLatency < 1000,
        'batch processed successfully': (r) => r.json('processed_count') === marketDataBatch.length,
    });
    
    sleep(randomIntBetween(0.5, 2));
}

// Spike test - sudden load increases
function runSpikeTest(data) {
    const startTime = Date.now();
    
    // Generate high-frequency market data
    const marketData = generateMarketData();
    
    // Send data with high frequency
    const response = http.post(`${data.baseUrl}/api/market-data`, JSON.stringify(marketData), {
        headers: {
            'Content-Type': 'application/json',
            'X-API-Key': data.apiKey,
        },
    });
    
    const endTime = Date.now();
    const processingLatency = endTime - startTime;
    
    // Record metrics
    errorRate.add(response.status !== 200);
    responseTime.add(response.timings.duration);
    throughput.add(1);
    dataProcessingLatency.add(processingLatency);
    
    // Assertions
    check(response, {
        'status is 200': (r) => r.status === 200,
        'response time < 300ms': (r) => r.timings.duration < 300,
        'processing latency < 1500ms': () => processingLatency < 1500,
    });
    
    sleep(randomIntBetween(0.1, 0.5));
}

// Volume test - high data volume
function runVolumeTest(data) {
    const startTime = Date.now();
    
    // Generate large batch of market data
    const marketDataBatch = [];
    for (let i = 0; i < randomIntBetween(50, 100); i++) {
        marketDataBatch.push(generateMarketData());
    }
    
    // Send large batch
    const response = http.post(`${data.baseUrl}/api/market-data/batch`, JSON.stringify(marketDataBatch), {
        headers: {
            'Content-Type': 'application/json',
            'X-API-Key': data.apiKey,
        },
    });
    
    const endTime = Date.now();
    const processingLatency = endTime - startTime;
    
    // Record metrics
    errorRate.add(response.status !== 200);
    responseTime.add(response.timings.duration);
    throughput.add(marketDataBatch.length);
    dataProcessingLatency.add(processingLatency);
    
    // Assertions
    check(response, {
        'status is 200': (r) => r.status === 200,
        'response time < 500ms': (r) => r.timings.duration < 500,
        'processing latency < 2000ms': () => processingLatency < 2000,
        'large batch processed': (r) => r.json('processed_count') === marketDataBatch.length,
    });
    
    sleep(randomIntBetween(2, 5));
}

// Endurance test - extended duration
function runEnduranceTest(data) {
    const startTime = Date.now();
    
    // Generate market data
    const marketData = generateMarketData();
    
    // Send data
    const response = http.post(`${data.baseUrl}/api/market-data`, JSON.stringify(marketData), {
        headers: {
            'Content-Type': 'application/json',
            'X-API-Key': data.apiKey,
        },
    });
    
    const endTime = Date.now();
    const processingLatency = endTime - startTime;
    
    // Record metrics
    errorRate.add(response.status !== 200);
    responseTime.add(response.timings.duration);
    throughput.add(1);
    dataProcessingLatency.add(processingLatency);
    
    // Assertions
    check(response, {
        'status is 200': (r) => r.status === 200,
        'response time < 150ms': (r) => r.timings.duration < 150,
        'processing latency < 800ms': () => processingLatency < 800,
    });
    
    sleep(randomIntBetween(1, 4));
}

// Helper function to generate market data
function generateMarketData() {
    const instrument = INSTRUMENTS[randomIntBetween(0, INSTRUMENTS.length - 1)];
    const basePrice = randomIntBetween(100, 500);
    const priceVariation = randomIntBetween(-10, 10);
    const price = basePrice + priceVariation;
    
    return {
        ...MARKET_DATA_TEMPLATE,
        instrument_id: instrument,
        price: price,
        volume: randomIntBetween(1000, 100000),
        timestamp: new Date().toISOString(),
        bid: price - randomIntBetween(1, 5),
        ask: price + randomIntBetween(1, 5),
        last_trade_price: price,
    };
}

// Teardown function - runs once after the test
export function teardown(data) {
    console.log('Load test completed');
    console.log(`Test started at: ${data.startTime}`);
    console.log(`Test ended at: ${new Date().toISOString()}`);
    
    // Generate summary report
    const summary = {
        testType: __ENV.TEST_TYPE || 'load',
        duration: TEST_DURATION,
        virtualUsers: VIRTUAL_USERS,
        baseUrl: BASE_URL,
        startTime: data.startTime,
        endTime: new Date().toISOString(),
    };
    
    console.log('Test Summary:', JSON.stringify(summary, null, 2));
}
