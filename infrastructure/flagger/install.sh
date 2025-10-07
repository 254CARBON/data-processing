#!/bin/bash
# Flagger Progressive Delivery Operator Installation Script
# Integrates with Linkerd for canary and blue-green deployments

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
FLAGGER_VERSION="1.35.0"
NAMESPACE="data-processing"
LINKERD_NAMESPACE="linkerd"

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}Flagger Progressive Delivery Installation${NC}"
echo -e "${BLUE}============================================${NC}"

# Check prerequisites
echo -e "\n${YELLOW}Checking prerequisites...${NC}"

if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}❌ kubectl not found${NC}"
    exit 1
fi

if ! command -v helm &> /dev/null; then
    echo -e "${RED}❌ helm not found${NC}"
    exit 1
fi

if ! kubectl cluster-info &> /dev/null; then
    echo -e "${RED}❌ Cannot connect to Kubernetes cluster${NC}"
    exit 1
fi

# Check if Linkerd is installed
if ! kubectl get namespace $LINKERD_NAMESPACE &> /dev/null; then
    echo -e "${RED}❌ Linkerd is not installed. Please install Linkerd first.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Prerequisites met${NC}"

# Add Flagger Helm repository
echo -e "\n${YELLOW}Adding Flagger Helm repository...${NC}"
helm repo add flagger https://flagger.app
helm repo update
echo -e "${GREEN}✅ Repository added${NC}"

# Install Flagger
echo -e "\n${YELLOW}Installing Flagger...${NC}"

if ! helm list -n $LINKERD_NAMESPACE | grep -q flagger; then
    helm upgrade --install flagger flagger/flagger \
        --namespace $LINKERD_NAMESPACE \
        --set meshProvider=linkerd \
        --set metricsServer=http://prometheus.$LINKERD_NAMESPACE:9090 \
        --set slack.url="" \
        --set slack.channel="" \
        --set slack.user="flagger" \
        --wait
    
    echo -e "${GREEN}✅ Flagger installed${NC}"
else
    echo -e "${YELLOW}⚠️  Flagger already installed${NC}"
fi

# Install Flagger Grafana dashboards
echo -e "\n${YELLOW}Installing Flagger Grafana integration...${NC}"

helm upgrade --install flagger-grafana flagger/grafana \
    --namespace $LINKERD_NAMESPACE \
    --set url=http://prometheus.$LINKERD_NAMESPACE:9090 \
    --wait || echo -e "${YELLOW}⚠️  Grafana integration skipped${NC}"

# Install Flagger Loadtester (for automated testing)
echo -e "\n${YELLOW}Installing Flagger Loadtester...${NC}"

helm upgrade --install flagger-loadtester flagger/loadtester \
    --namespace $NAMESPACE \
    --set fullnameOverride=loadtester \
    --wait

echo -e "${GREEN}✅ Loadtester installed${NC}"

# Wait for Flagger to be ready
echo -e "\n${YELLOW}Waiting for Flagger to be ready...${NC}"
kubectl wait --for=condition=available --timeout=300s deployment/flagger -n $LINKERD_NAMESPACE
echo -e "${GREEN}✅ Flagger is ready${NC}"

# Install metric templates
echo -e "\n${YELLOW}Installing metric templates...${NC}"
kubectl apply -f ./metric-templates.yaml -n $NAMESPACE
echo -e "${GREEN}✅ Metric templates installed${NC}"

# Verify installation
echo -e "\n${YELLOW}Verifying Flagger installation...${NC}"

if kubectl get deployment flagger -n $LINKERD_NAMESPACE &> /dev/null; then
    echo -e "${GREEN}✅ Flagger controller is running${NC}"
else
    echo -e "${RED}❌ Flagger controller not found${NC}"
    exit 1
fi

# Display status
echo -e "\n${BLUE}============================================${NC}"
echo -e "${GREEN}✅ Flagger installation completed!${NC}"
echo -e "${BLUE}============================================${NC}"
echo -e "\nFlagger components:"
kubectl get pods -n $LINKERD_NAMESPACE -l app.kubernetes.io/name=flagger
echo -e "\nLoadtester:"
kubectl get pods -n $NAMESPACE -l app=loadtester
echo -e "\n${YELLOW}Next steps:${NC}"
echo -e "1. Apply canary resources: kubectl apply -f infrastructure/deployments/canary/"
echo -e "2. Monitor canary deployment: kubectl describe canary -n $NAMESPACE"
echo -e "3. View Flagger logs: kubectl logs -n $LINKERD_NAMESPACE -l app.kubernetes.io/name=flagger"
echo -e "${BLUE}============================================${NC}"
