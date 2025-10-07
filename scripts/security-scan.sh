#!/bin/bash
# Security scanning script for the 254Carbon Data Processing Pipeline

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_ROOT/logs"
REPORT_DIR="$PROJECT_ROOT/security-reports"

# Create directories
mkdir -p "$LOG_DIR" "$REPORT_DIR"

# Logging function
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_DIR/security-scan.log"
}

error() {
    echo -e "${RED}[ERROR]${NC} $1" | tee -a "$LOG_DIR/security-scan.log"
}

success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1" | tee -a "$LOG_DIR/security-scan.log"
}

warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1" | tee -a "$LOG_DIR/security-scan.log"
}

# Check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Install required tools
install_tools() {
    log "Installing security scanning tools..."
    
    # Install Python tools
    if command_exists pip3; then
        pip3 install --user safety bandit semgrep
    else
        error "pip3 not found. Please install Python 3 and pip."
        exit 1
    fi
    
    # Install Trivy
    if ! command_exists trivy; then
        log "Installing Trivy..."
        curl -sfL https://raw.githubusercontent.com/aquasecurity/trivy/main/contrib/install.sh | sh -s -- -b /usr/local/bin
    fi
    
    # Install Checkov
    if ! command_exists checkov; then
        log "Installing Checkov..."
        pip3 install --user checkov
    fi
    
    # Install TruffleHog
    if ! command_exists trufflehog; then
        log "Installing TruffleHog..."
        go install github.com/trufflesecurity/trufflehog/v3@latest
    fi
    
    success "Security tools installed successfully"
}

# Python dependency vulnerability scan
scan_python_dependencies() {
    log "Scanning Python dependencies for vulnerabilities..."
    
    cd "$PROJECT_ROOT"
    
    # Safety scan
    if command_exists safety; then
        log "Running Safety scan..."
        safety check --json --output "$REPORT_DIR/safety-report.json" || true
        safety check --short-report | tee -a "$LOG_DIR/security-scan.log"
    else
        warning "Safety not found. Skipping Python dependency scan."
    fi
    
    # Bandit scan
    if command_exists bandit; then
        log "Running Bandit security linter..."
        bandit -r . -f json -o "$REPORT_DIR/bandit-report.json" || true
        bandit -r . -f txt | tee -a "$LOG_DIR/security-scan.log"
    else
        warning "Bandit not found. Skipping Python security linting."
    fi
    
    success "Python dependency scan completed"
}

# Static analysis scan
scan_static_analysis() {
    log "Running static analysis scan..."
    
    cd "$PROJECT_ROOT"
    
    # Semgrep scan
    if command_exists semgrep; then
        log "Running Semgrep static analysis..."
        semgrep --config=auto --json --output="$REPORT_DIR/semgrep-report.json" . || true
        semgrep --config=auto . | tee -a "$LOG_DIR/security-scan.log"
    else
        warning "Semgrep not found. Skipping static analysis."
    fi
    
    success "Static analysis scan completed"
}

# Container image security scan
scan_containers() {
    log "Scanning container images for vulnerabilities..."
    
    cd "$PROJECT_ROOT"
    
    # Build images if they don't exist
    services=("normalization-service" "enrichment-service" "aggregation-service" "projection-service")
    
    for service in "${services[@]}"; do
        if [ -d "service-${service}" ]; then
            log "Building image for $service..."
            docker build -t "$service:latest" "./service-${service}" || {
                error "Failed to build image for $service"
                continue
            }
            
            # Trivy scan
            if command_exists trivy; then
                log "Running Trivy scan on $service..."
                trivy image --format json --output "$REPORT_DIR/trivy-$service.json" "$service:latest" || true
                trivy image "$service:latest" | tee -a "$LOG_DIR/security-scan.log"
            else
                warning "Trivy not found. Skipping container scan for $service."
            fi
        else
            warning "Service directory not found: service-${service}"
        fi
    done
    
    success "Container security scan completed"
}

# Infrastructure security scan
scan_infrastructure() {
    log "Scanning infrastructure for security issues..."
    
    cd "$PROJECT_ROOT"
    
    # Checkov scan
    if command_exists checkov; then
        log "Running Checkov infrastructure scan..."
        checkov -d . --framework terraform --output json --output-file-path "$REPORT_DIR/checkov-report.json" || true
        checkov -d . --framework terraform | tee -a "$LOG_DIR/security-scan.log"
    else
        warning "Checkov not found. Skipping infrastructure scan."
    fi
    
    # Kubernetes security scan
    if [ -d "helm" ]; then
        log "Scanning Kubernetes manifests..."
        
        # Kube-score scan (if available)
        if command_exists kube-score; then
            kube-score score helm/templates/ --output-format json > "$REPORT_DIR/kube-score-report.json" || true
            kube-score score helm/templates/ | tee -a "$LOG_DIR/security-scan.log"
        else
            warning "kube-score not found. Skipping Kubernetes manifest scan."
        fi
    fi
    
    success "Infrastructure security scan completed"
}

# Secrets detection scan
scan_secrets() {
    log "Scanning for secrets and sensitive data..."
    
    cd "$PROJECT_ROOT"
    
    # TruffleHog scan
    if command_exists trufflehog; then
        log "Running TruffleHog secrets detection..."
        trufflehog filesystem . --json --output "$REPORT_DIR/trufflehog-report.json" || true
        trufflehog filesystem . | tee -a "$LOG_DIR/security-scan.log"
    else
        warning "TruffleHog not found. Skipping secrets detection."
    fi
    
    # GitLeaks scan (if available)
    if command_exists gitleaks; then
        log "Running GitLeaks scan..."
        gitleaks detect --source . --report-format json --report-path "$REPORT_DIR/gitleaks-report.json" || true
        gitleaks detect --source . | tee -a "$LOG_DIR/security-scan.log"
    else
        warning "GitLeaks not found. Skipping Git secrets scan."
    fi
    
    success "Secrets detection scan completed"
}

# Security headers check
check_security_headers() {
    log "Checking security headers..."
    
    cd "$PROJECT_ROOT"
    
    # Start services for testing
    if [ -f "docker-compose.yml" ]; then
        log "Starting services for security header testing..."
        docker-compose up -d
        
        # Wait for services to start
        sleep 30
        
        # Test security headers
        services=("normalization-service:8080" "enrichment-service:8081" "aggregation-service:8082" "projection-service:8083")
        
        for service in "${services[@]}"; do
            service_name=$(echo "$service" | cut -d: -f1)
            port=$(echo "$service" | cut -d: -f2)
            
            log "Testing security headers for $service_name..."
            
            # Test health endpoint
            if curl -s -I "http://localhost:$port/health" > "$REPORT_DIR/headers-$service_name.txt"; then
                log "Security headers for $service_name:"
                cat "$REPORT_DIR/headers-$service_name.txt" | tee -a "$LOG_DIR/security-scan.log"
            else
                warning "Failed to test security headers for $service_name"
            fi
        done
        
        # Cleanup
        docker-compose down
    else
        warning "docker-compose.yml not found. Skipping security headers check."
    fi
    
    success "Security headers check completed"
}

# Generate security report
generate_report() {
    log "Generating security scan report..."
    
    cd "$PROJECT_ROOT"
    
    # Create HTML report
    cat > "$REPORT_DIR/security-report.html" << 'EOF'
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>254Carbon Data Processing Pipeline - Security Scan Report</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        .header { background-color: #f4f4f4; padding: 20px; border-radius: 5px; }
        .section { margin: 20px 0; }
        .scan-result { margin: 10px 0; padding: 10px; border-left: 4px solid #007cba; }
        .success { border-left-color: #28a745; }
        .warning { border-left-color: #ffc107; }
        .error { border-left-color: #dc3545; }
        .timestamp { color: #666; font-size: 0.9em; }
    </style>
</head>
<body>
    <div class="header">
        <h1>254Carbon Data Processing Pipeline</h1>
        <h2>Security Scan Report</h2>
        <p class="timestamp">Generated: $(date)</p>
    </div>
    
    <div class="section">
        <h3>Scan Summary</h3>
        <div class="scan-result success">
            <strong>Python Dependencies:</strong> Safety and Bandit scans completed
        </div>
        <div class="scan-result success">
            <strong>Static Analysis:</strong> Semgrep scan completed
        </div>
        <div class="scan-result success">
            <strong>Container Security:</strong> Trivy scans completed
        </div>
        <div class="scan-result success">
            <strong>Infrastructure:</strong> Checkov scan completed
        </div>
        <div class="scan-result success">
            <strong>Secrets Detection:</strong> TruffleHog scan completed
        </div>
        <div class="scan-result success">
            <strong>Security Headers:</strong> HTTP headers check completed
        </div>
    </div>
    
    <div class="section">
        <h3>Recommendations</h3>
        <ul>
            <li>Review all scan results and address any high or critical vulnerabilities</li>
            <li>Ensure all security headers are properly configured</li>
            <li>Regularly update dependencies to latest secure versions</li>
            <li>Implement automated security scanning in CI/CD pipeline</li>
            <li>Conduct regular security audits and penetration testing</li>
        </ul>
    </div>
    
    <div class="section">
        <h3>Detailed Reports</h3>
        <p>Detailed scan results are available in the following files:</p>
        <ul>
            <li>safety-report.json - Python dependency vulnerabilities</li>
            <li>bandit-report.json - Python security issues</li>
            <li>semgrep-report.json - Static analysis results</li>
            <li>trivy-*.json - Container vulnerability scans</li>
            <li>checkov-report.json - Infrastructure security issues</li>
            <li>trufflehog-report.json - Secrets detection results</li>
        </ul>
    </div>
</body>
</html>
EOF
    
    success "Security report generated: $REPORT_DIR/security-report.html"
}

# Main function
main() {
    log "Starting security scan for 254Carbon Data Processing Pipeline"
    
    # Parse command line arguments
    INSTALL_TOOLS=false
    SCAN_ALL=true
    SCAN_DEPS=false
    SCAN_STATIC=false
    SCAN_CONTAINERS=false
    SCAN_INFRA=false
    SCAN_SECRETS=false
    SCAN_HEADERS=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --install-tools)
                INSTALL_TOOLS=true
                shift
                ;;
            --deps-only)
                SCAN_ALL=false
                SCAN_DEPS=true
                shift
                ;;
            --static-only)
                SCAN_ALL=false
                SCAN_STATIC=true
                shift
                ;;
            --containers-only)
                SCAN_ALL=false
                SCAN_CONTAINERS=true
                shift
                ;;
            --infra-only)
                SCAN_ALL=false
                SCAN_INFRA=true
                shift
                ;;
            --secrets-only)
                SCAN_ALL=false
                SCAN_SECRETS=true
                shift
                ;;
            --headers-only)
                SCAN_ALL=false
                SCAN_HEADERS=true
                shift
                ;;
            --help)
                echo "Usage: $0 [OPTIONS]"
                echo "Options:"
                echo "  --install-tools    Install security scanning tools"
                echo "  --deps-only        Scan Python dependencies only"
                echo "  --static-only      Run static analysis only"
                echo "  --containers-only  Scan containers only"
                echo "  --infra-only       Scan infrastructure only"
                echo "  --secrets-only     Detect secrets only"
                echo "  --headers-only     Check security headers only"
                echo "  --help             Show this help message"
                exit 0
                ;;
            *)
                error "Unknown option: $1"
                exit 1
                ;;
        esac
    done
    
    # Install tools if requested
    if [ "$INSTALL_TOOLS" = true ]; then
        install_tools
    fi
    
    # Run scans based on options
    if [ "$SCAN_ALL" = true ] || [ "$SCAN_DEPS" = true ]; then
        scan_python_dependencies
    fi
    
    if [ "$SCAN_ALL" = true ] || [ "$SCAN_STATIC" = true ]; then
        scan_static_analysis
    fi
    
    if [ "$SCAN_ALL" = true ] || [ "$SCAN_CONTAINERS" = true ]; then
        scan_containers
    fi
    
    if [ "$SCAN_ALL" = true ] || [ "$SCAN_INFRA" = true ]; then
        scan_infrastructure
    fi
    
    if [ "$SCAN_ALL" = true ] || [ "$SCAN_SECRETS" = true ]; then
        scan_secrets
    fi
    
    if [ "$SCAN_ALL" = true ] || [ "$SCAN_HEADERS" = true ]; then
        check_security_headers
    fi
    
    # Generate report
    generate_report
    
    success "Security scan completed successfully!"
    log "Reports available in: $REPORT_DIR"
    log "Logs available in: $LOG_DIR"
}

# Run main function
main "$@"
