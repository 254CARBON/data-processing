#!/bin/bash
# Performance Testing Script for 254Carbon Data Processing Pipeline
# 
# This script runs comprehensive performance tests including load testing,
# performance regression tests, scalability tests, and disaster recovery tests.

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
BASE_URL=${BASE_URL:-"http://localhost:8080"}
API_KEY=${API_KEY:-"test-api-key"}
TEST_DURATION=${TEST_DURATION:-300}
MAX_LOAD=${MAX_LOAD:-200}
STEP_SIZE=${STEP_SIZE:-50}
NAMESPACE=${NAMESPACE:-"data-processing"}
SERVICE_NAME=${SERVICE_NAME:-"normalization-service"}
NEW_IMAGE_TAG=${NEW_IMAGE_TAG:-"v1.1.0"}

# Test results directory
RESULTS_DIR="test-results/$(date +%Y%m%d_%H%M%S)"
mkdir -p "$RESULTS_DIR"

echo -e "${BLUE}254Carbon Data Processing Pipeline - Performance Testing${NC}"
echo -e "${BLUE}======================================================${NC}"
echo "Base URL: $BASE_URL"
echo "API Key: $API_KEY"
echo "Test Duration: $TEST_DURATION seconds"
echo "Max Load: $MAX_LOAD users"
echo "Results Directory: $RESULTS_DIR"
echo ""

# Function to check if service is available
check_service() {
    echo -e "${YELLOW}Checking service availability...${NC}"
    
    if curl -s -f "$BASE_URL/health" > /dev/null; then
        echo -e "${GREEN}✅ Service is available${NC}"
        return 0
    else
        echo -e "${RED}❌ Service is not available${NC}"
        return 1
    fi
}

# Function to run k6 load tests
run_k6_tests() {
    echo -e "${YELLOW}Running k6 load tests...${NC}"
    
    # Check if k6 is installed
    if ! command -v k6 &> /dev/null; then
        echo -e "${RED}k6 is not installed. Please install k6 first.${NC}"
        return 1
    fi
    
    # Run different types of k6 tests
    local test_types=("smoke" "load" "stress" "spike" "volume" "endurance")
    
    for test_type in "${test_types[@]}"; do
        echo -e "${BLUE}Running $test_type test...${NC}"
        
        k6 run \
            --env BASE_URL="$BASE_URL" \
            --env API_KEY="$API_KEY" \
            --env TEST_TYPE="$test_type" \
            --env TEST_DURATION="$TEST_DURATION" \
            --env VIRTUAL_USERS="$MAX_LOAD" \
            tests/load/k6_load_test.js \
            --out json="$RESULTS_DIR/k6_${test_type}_results.json" \
            --out csv="$RESULTS_DIR/k6_${test_type}_results.csv" \
            --summary-export="$RESULTS_DIR/k6_${test_type}_summary.json" || {
            echo -e "${RED}❌ k6 $test_type test failed${NC}"
            continue
        }
        
        echo -e "${GREEN}✅ k6 $test_type test completed${NC}"
    done
}

# Function to run Locust load tests
run_locust_tests() {
    echo -e "${YELLOW}Running Locust load tests...${NC}"
    
    # Check if Locust is installed
    if ! command -v locust &> /dev/null; then
        echo -e "${RED}Locust is not installed. Please install Locust first.${NC}"
        return 1
    fi
    
    # Run Locust tests
    echo -e "${BLUE}Running Locust load test...${NC}"
    
    locust \
        --host="$BASE_URL" \
        --users="$MAX_LOAD" \
        --spawn-rate=10 \
        --run-time="$TEST_DURATION" \
        --headless \
        --html="$RESULTS_DIR/locust_results.html" \
        --csv="$RESULTS_DIR/locust_results" \
        --logfile="$RESULTS_DIR/locust.log" \
        tests/load/locust_load_test.py || {
        echo -e "${RED}❌ Locust test failed${NC}"
        return 1
    }
    
    echo -e "${GREEN}✅ Locust test completed${NC}"
}

# Function to run performance regression tests
run_performance_regression_tests() {
    echo -e "${YELLOW}Running performance regression tests...${NC}"
    
    # Check if Python is available
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}Python3 is not installed. Please install Python3 first.${NC}"
        return 1
    fi
    
    # Run performance regression tests
    echo -e "${BLUE}Running performance regression tests...${NC}"
    
    python3 tests/performance/performance_regression.py \
        --base-url="$BASE_URL" \
        --api-key="$API_KEY" \
        --baseline-file="performance_baselines.json" \
        --results-file="$RESULTS_DIR/performance_regression_results.json" \
        --generate-report || {
        echo -e "${RED}❌ Performance regression tests failed${NC}"
        return 1
    }
    
    echo -e "${GREEN}✅ Performance regression tests completed${NC}"
}

# Function to run scalability tests
run_scalability_tests() {
    echo -e "${YELLOW}Running scalability tests...${NC}"
    
    # Run scalability tests
    echo -e "${BLUE}Running scalability tests...${NC}"
    
    python3 tests/scalability/scalability_tests.py \
        --base-url="$BASE_URL" \
        --api-key="$API_KEY" \
        --test-type="ramp" \
        --max-load="$MAX_LOAD" \
        --step-size="$STEP_SIZE" \
        --duration="$TEST_DURATION" \
        --generate-report || {
        echo -e "${RED}❌ Scalability tests failed${NC}"
        return 1
    }
    
    echo -e "${GREEN}✅ Scalability tests completed${NC}"
}

# Function to run zero-downtime deployment tests
run_zero_downtime_tests() {
    echo -e "${YELLOW}Running zero-downtime deployment tests...${NC}"
    
    # Check if kubectl is available
    if ! command -v kubectl &> /dev/null; then
        echo -e "${YELLOW}kubectl is not available. Skipping zero-downtime tests.${NC}"
        return 0
    fi
    
    # Run zero-downtime tests
    echo -e "${BLUE}Running zero-downtime deployment tests...${NC}"
    
    python3 tests/deployment/zero_downtime_tests.py \
        --base-url="$BASE_URL" \
        --api-key="$API_KEY" \
        --test-type="rolling" \
        --service-name="$SERVICE_NAME" \
        --new-image-tag="$NEW_IMAGE_TAG" \
        --duration="$TEST_DURATION" \
        --generate-report || {
        echo -e "${RED}❌ Zero-downtime tests failed${NC}"
        return 1
    }
    
    echo -e "${GREEN}✅ Zero-downtime tests completed${NC}"
}

# Function to run disaster recovery tests
run_disaster_recovery_tests() {
    echo -e "${YELLOW}Running disaster recovery tests...${NC}"
    
    # Check if kubectl is available
    if ! command -v kubectl &> /dev/null; then
        echo -e "${YELLOW}kubectl is not available. Skipping disaster recovery tests.${NC}"
        return 0
    fi
    
    # Run disaster recovery tests
    echo -e "${BLUE}Running disaster recovery tests...${NC}"
    
    python3 tests/disaster_recovery/disaster_recovery_tests.py \
        --base-url="$BASE_URL" \
        --api-key="$API_KEY" \
        --test-type="pod" \
        --service-name="$SERVICE_NAME" \
        --duration="$TEST_DURATION" \
        --generate-report || {
        echo -e "${RED}❌ Disaster recovery tests failed${NC}"
        return 1
    }
    
    echo -e "${GREEN}✅ Disaster recovery tests completed${NC}"
}

# Function to generate comprehensive report
generate_comprehensive_report() {
    echo -e "${YELLOW}Generating comprehensive performance report...${NC}"
    
    local report_file="$RESULTS_DIR/comprehensive_performance_report.md"
    
    cat > "$report_file" << EOF
# Comprehensive Performance Test Report

**Generated**: $(date)
**Base URL**: $BASE_URL
**Test Duration**: $TEST_DURATION seconds
**Max Load**: $MAX_LOAD users

## Test Summary

This report summarizes the results of comprehensive performance testing for the 254Carbon Data Processing Pipeline.

## Test Results

### 1. Load Testing Results

#### k6 Load Tests
EOF

    # Add k6 results if available
    if [ -d "$RESULTS_DIR" ]; then
        for file in "$RESULTS_DIR"/k6_*_summary.json; do
            if [ -f "$file" ]; then
                local test_name=$(basename "$file" _summary.json | sed 's/k6_//')
                echo "#### $test_name Test" >> "$report_file"
                echo "\`\`\`json" >> "$report_file"
                cat "$file" >> "$report_file"
                echo "\`\`\`" >> "$report_file"
                echo "" >> "$report_file"
            fi
        done
    fi

    cat >> "$report_file" << EOF

#### Locust Load Tests
EOF

    # Add Locust results if available
    if [ -f "$RESULTS_DIR/locust_results.html" ]; then
        echo "Locust test results are available in: \`$RESULTS_DIR/locust_results.html\`" >> "$report_file"
    fi

    cat >> "$report_file" << EOF

### 2. Performance Regression Tests

Performance regression test results are available in: \`$RESULTS_DIR/performance_regression_results.json\`

### 3. Scalability Tests

Scalability test results are available in: \`scalability_results.json\`

### 4. Zero-Downtime Deployment Tests

Zero-downtime deployment test results are available in: \`zero_downtime_results.json\`

### 5. Disaster Recovery Tests

Disaster recovery test results are available in: \`disaster_recovery_results.json\`

## Recommendations

Based on the test results, the following recommendations are made:

1. **Performance Optimization**: Review and optimize slow endpoints
2. **Capacity Planning**: Ensure adequate resources for peak load
3. **Monitoring**: Implement comprehensive monitoring and alerting
4. **Testing**: Regular performance testing in CI/CD pipeline
5. **Documentation**: Keep performance baselines updated

## Next Steps

1. Review test results and identify performance bottlenecks
2. Implement performance optimizations
3. Update performance baselines
4. Schedule regular performance testing
5. Monitor production performance metrics

## Files Generated

- \`$RESULTS_DIR/\` - All test results and reports
- \`performance_baselines.json\` - Performance baselines
- \`scalability_results.json\` - Scalability test results
- \`zero_downtime_results.json\` - Zero-downtime test results
- \`disaster_recovery_results.json\` - Disaster recovery test results

EOF

    echo -e "${GREEN}✅ Comprehensive report generated: $report_file${NC}"
}

# Function to cleanup test results
cleanup_test_results() {
    echo -e "${YELLOW}Cleaning up test results...${NC}"
    
    # Move results to results directory
    if [ -f "performance_regression_results.json" ]; then
        mv "performance_regression_results.json" "$RESULTS_DIR/"
    fi
    
    if [ -f "scalability_results.json" ]; then
        mv "scalability_results.json" "$RESULTS_DIR/"
    fi
    
    if [ -f "zero_downtime_results.json" ]; then
        mv "zero_downtime_results.json" "$RESULTS_DIR/"
    fi
    
    if [ -f "disaster_recovery_results.json" ]; then
        mv "disaster_recovery_results.json" "$RESULTS_DIR/"
    fi
    
    if [ -f "performance_regression_report.txt" ]; then
        mv "performance_regression_report.txt" "$RESULTS_DIR/"
    fi
    
    if [ -f "scalability_report.txt" ]; then
        mv "scalability_report.txt" "$RESULTS_DIR/"
    fi
    
    if [ -f "zero_downtime_report.txt" ]; then
        mv "zero_downtime_report.txt" "$RESULTS_DIR/"
    fi
    
    if [ -f "disaster_recovery_report.txt" ]; then
        mv "disaster_recovery_report.txt" "$RESULTS_DIR/"
    fi
    
    echo -e "${GREEN}✅ Test results cleaned up${NC}"
}

# Main execution
main() {
    echo -e "${BLUE}Starting comprehensive performance testing...${NC}"
    
    # Check service availability
    if ! check_service; then
        echo -e "${RED}❌ Service is not available. Exiting.${NC}"
        exit 1
    fi
    
    # Run all tests
    local tests_passed=0
    local tests_total=0
    
    # k6 Load Tests
    tests_total=$((tests_total + 1))
    if run_k6_tests; then
        tests_passed=$((tests_passed + 1))
    fi
    
    # Locust Load Tests
    tests_total=$((tests_total + 1))
    if run_locust_tests; then
        tests_passed=$((tests_passed + 1))
    fi
    
    # Performance Regression Tests
    tests_total=$((tests_total + 1))
    if run_performance_regression_tests; then
        tests_passed=$((tests_passed + 1))
    fi
    
    # Scalability Tests
    tests_total=$((tests_total + 1))
    if run_scalability_tests; then
        tests_passed=$((tests_passed + 1))
    fi
    
    # Zero-Downtime Tests
    tests_total=$((tests_total + 1))
    if run_zero_downtime_tests; then
        tests_passed=$((tests_passed + 1))
    fi
    
    # Disaster Recovery Tests
    tests_total=$((tests_total + 1))
    if run_disaster_recovery_tests; then
        tests_passed=$((tests_passed + 1))
    fi
    
    # Cleanup test results
    cleanup_test_results
    
    # Generate comprehensive report
    generate_comprehensive_report
    
    # Summary
    echo ""
    echo -e "${BLUE}Performance Testing Summary${NC}"
    echo -e "${BLUE}=========================${NC}"
    echo "Tests Passed: $tests_passed/$tests_total"
    echo "Results Directory: $RESULTS_DIR"
    echo ""
    
    if [ $tests_passed -eq $tests_total ]; then
        echo -e "${GREEN}✅ All performance tests passed!${NC}"
        exit 0
    else
        echo -e "${YELLOW}⚠️  Some performance tests failed. Check results for details.${NC}"
        exit 1
    fi
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --base-url)
            BASE_URL="$2"
            shift 2
            ;;
        --api-key)
            API_KEY="$2"
            shift 2
            ;;
        --duration)
            TEST_DURATION="$2"
            shift 2
            ;;
        --max-load)
            MAX_LOAD="$2"
            shift 2
            ;;
        --step-size)
            STEP_SIZE="$2"
            shift 2
            ;;
        --namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        --service-name)
            SERVICE_NAME="$2"
            shift 2
            ;;
        --new-image-tag)
            NEW_IMAGE_TAG="$2"
            shift 2
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --base-url URL        Base URL for the API (default: http://localhost:8080)"
            echo "  --api-key KEY         API key for authentication (default: test-api-key)"
            echo "  --duration SECONDS    Test duration in seconds (default: 300)"
            echo "  --max-load USERS      Maximum load level (default: 200)"
            echo "  --step-size SIZE      Load increment step (default: 50)"
            echo "  --namespace NS        Kubernetes namespace (default: data-processing)"
            echo "  --service-name NAME   Service name to test (default: normalization-service)"
            echo "  --new-image-tag TAG   New image tag for deployment tests (default: v1.1.0)"
            echo "  --help                Show this help message"
            echo ""
            echo "Environment Variables:"
            echo "  BASE_URL, API_KEY, TEST_DURATION, MAX_LOAD, STEP_SIZE,"
            echo "  NAMESPACE, SERVICE_NAME, NEW_IMAGE_TAG"
            echo ""
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Run main function
main
