#!/bin/bash
# Automated Canary Deployment Script
# Progressive traffic shifting: 10% → 25% → 50% → 100%

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
echo -e "${BLUE}Canary Deployment${NC}"
echo -e "${BLUE}Service: $SERVICE${NC}"
echo -e "${BLUE}Version: $VERSION${NC}"
echo -e "${BLUE}Namespace: $NAMESPACE${NC}"
echo -e "${BLUE}============================================${NC}"

# Check prerequisites
if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}❌ kubectl not found${NC}"
    exit 1
fi

# Update deployment with new version
echo -e "\n${YELLOW}Updating deployment with version $VERSION...${NC}"
kubectl set image deployment/$SERVICE \
    $SERVICE=gcr.io/project/$SERVICE:$VERSION \
    -n $NAMESPACE

# Wait for canary to be created
echo -e "\n${YELLOW}Waiting for canary to be created...${NC}"
sleep 5

# Monitor canary deployment
echo -e "\n${YELLOW}Monitoring canary deployment...${NC}"
kubectl describe canary $SERVICE -n $NAMESPACE

# Watch canary progress
echo -e "\n${YELLOW}Watching canary progress (Ctrl+C to stop watching)...${NC}"
kubectl get canary $SERVICE -n $NAMESPACE -w &
WATCH_PID=$!

# Wait for canary to complete or fail
while true; do
    STATUS=$(kubectl get canary $SERVICE -n $NAMESPACE -o jsonpath='{.status.phase}')
    
    case $STATUS in
        "Succeeded")
            echo -e "\n${GREEN}✅ Canary deployment succeeded!${NC}"
            kill $WATCH_PID 2>/dev/null || true
            break
            ;;
        "Failed")
            echo -e "\n${RED}❌ Canary deployment failed!${NC}"
            kubectl describe canary $SERVICE -n $NAMESPACE
            kill $WATCH_PID 2>/dev/null || true
            exit 1
            ;;
        "Progressing")
            echo -e "${BLUE}⏳ Canary is progressing...${NC}"
            ;;
        *)
            echo -e "${YELLOW}⚠️  Unknown status: $STATUS${NC}"
            ;;
    esac
    
    sleep 30
done

echo -e "${BLUE}============================================${NC}"
echo -e "${GREEN}✅ Deployment completed!${NC}"
echo -e "${BLUE}============================================${NC}"
