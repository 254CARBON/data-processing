#!/bin/bash
# Blue-Green Deployment Script
# Zero-downtime deployment with instant rollback capability

set -euo pipefail

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
SERVICE=${1:-""}
VERSION=${2:-""}
NAMESPACE=${3:-"data-processing"}

usage() {
    echo "Usage: $0 <service> <version> [namespace]"
    echo "Example: $0 normalization-service v1.2.3 data-processing"
    exit 1
}

if [ -z "$SERVICE" ] || [ -z "$VERSION" ]; then
    usage
fi

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}Blue-Green Deployment${NC}"
echo -e "${BLUE}Service: $SERVICE${NC}"
echo -e "${BLUE}Version: $VERSION${NC}"
echo -e "${BLUE}Namespace: $NAMESPACE${NC}"
echo -e "${BLUE}============================================${NC}"

# Determine current active deployment (blue or green)
echo -e "\n${YELLOW}Determining current active deployment...${NC}"
CURRENT_VERSION=$(kubectl get service $SERVICE -n $NAMESPACE -o jsonpath='{.spec.selector.version}' 2>/dev/null || echo "blue")

if [ "$CURRENT_VERSION" = "blue" ]; then
    INACTIVE_VERSION="green"
else
    INACTIVE_VERSION="blue"
fi

echo -e "${BLUE}Current active: $CURRENT_VERSION${NC}"
echo -e "${BLUE}Deploying to: $INACTIVE_VERSION${NC}"

# Deploy to inactive environment
echo -e "\n${YELLOW}Deploying version $VERSION to $INACTIVE_VERSION...${NC}"

kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: $SERVICE-$INACTIVE_VERSION
  namespace: $NAMESPACE
  labels:
    app: $SERVICE
    version: $INACTIVE_VERSION
spec:
  replicas: 3
  selector:
    matchLabels:
      app: $SERVICE
      version: $INACTIVE_VERSION
  template:
    metadata:
      labels:
        app: $SERVICE
        version: $INACTIVE_VERSION
      annotations:
        linkerd.io/inject: enabled
    spec:
      containers:
      - name: $SERVICE
        image: gcr.io/project/$SERVICE:$VERSION
        ports:
        - containerPort: 8080
        env:
        - name: VERSION
          value: "$VERSION"
        - name: DEPLOYMENT_TYPE
          value: "$INACTIVE_VERSION"
        livenessProbe:
          httpGet:
            path: /health/live
            port: 8080
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 8080
          initialDelaySeconds: 5
          periodSeconds: 5
        resources:
          requests:
            cpu: 500m
            memory: 512Mi
          limits:
            cpu: 2000m
            memory: 2Gi
EOF

# Wait for deployment to be ready
echo -e "\n${YELLOW}Waiting for deployment to be ready...${NC}"
kubectl rollout status deployment/$SERVICE-$INACTIVE_VERSION -n $NAMESPACE --timeout=10m

echo -e "${GREEN}✅ Deployment ready${NC}"

# Run smoke tests
echo -e "\n${YELLOW}Running smoke tests...${NC}"

# Test health endpoint
if ! kubectl run smoke-test-$(date +%s) \
    --image=curlimages/curl:latest \
    --rm -i --restart=Never \
    --namespace=$NAMESPACE \
    -- curl -f http://$SERVICE-$INACTIVE_VERSION:8080/health; then
    echo -e "${RED}❌ Smoke tests failed${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Smoke tests passed${NC}"

# Switch traffic
echo -e "\n${YELLOW}Switching traffic to $INACTIVE_VERSION...${NC}"

kubectl patch service $SERVICE -n $NAMESPACE -p "{\"spec\":{\"selector\":{\"version\":\"$INACTIVE_VERSION\"}}}"

echo -e "${GREEN}✅ Traffic switched to $INACTIVE_VERSION${NC}"

# Monitor for issues
echo -e "\n${YELLOW}Monitoring for 2 minutes...${NC}"
sleep 120

# Check error rates
ERROR_RATE=$(kubectl exec -n $NAMESPACE deployment/$SERVICE-$INACTIVE_VERSION -- \
    curl -s http://localhost:9090/metrics | grep error_rate || echo "0")

echo -e "${BLUE}Error rate: $ERROR_RATE${NC}"

# Cleanup old deployment
echo -e "\n${YELLOW}Cleaning up old deployment...${NC}"
kubectl scale deployment/$SERVICE-$CURRENT_VERSION -n $NAMESPACE --replicas=0

echo -e "${BLUE}============================================${NC}"
echo -e "${GREEN}✅ Blue-Green deployment completed!${NC}"
echo -e "${BLUE}Active version: $INACTIVE_VERSION${NC}"
echo -e "${BLUE}============================================${NC}"
echo -e "\n${YELLOW}To rollback:${NC}"
echo -e "kubectl patch service $SERVICE -n $NAMESPACE -p '{\"spec\":{\"selector\":{\"version\":\"$CURRENT_VERSION\"}}}'"
echo -e "kubectl scale deployment/$SERVICE-$CURRENT_VERSION -n $NAMESPACE --replicas=3"
