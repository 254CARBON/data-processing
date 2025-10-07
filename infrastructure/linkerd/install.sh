#!/bin/bash
# Linkerd Service Mesh Installation Script
# For on-premises Kubernetes deployment

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
LINKERD_VERSION="stable-2.14.10"
NAMESPACE="data-processing"

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}Linkerd Service Mesh Installation${NC}"
echo -e "${BLUE}============================================${NC}"

# Check prerequisites
echo -e "\n${YELLOW}Checking prerequisites...${NC}"

if ! command -v kubectl &> /dev/null; then
    echo -e "${RED}❌ kubectl not found. Please install kubectl first.${NC}"
    exit 1
fi

if ! kubectl cluster-info &> /dev/null; then
    echo -e "${RED}❌ Cannot connect to Kubernetes cluster.${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Prerequisites met${NC}"

# Download and install Linkerd CLI
echo -e "\n${YELLOW}Installing Linkerd CLI...${NC}"

if ! command -v linkerd &> /dev/null; then
    curl --proto '=https' --tlsv1.2 -sSfL https://run.linkerd.io/install | sh
    export PATH=$PATH:$HOME/.linkerd2/bin
    
    # Add to bash profile
    if ! grep -q "linkerd2/bin" ~/.bashrc; then
        echo 'export PATH=$PATH:$HOME/.linkerd2/bin' >> ~/.bashrc
    fi
    
    echo -e "${GREEN}✅ Linkerd CLI installed${NC}"
else
    echo -e "${GREEN}✅ Linkerd CLI already installed${NC}"
fi

# Verify Linkerd CLI version
INSTALLED_VERSION=$(linkerd version --client --short)
echo -e "${BLUE}Installed Linkerd version: ${INSTALLED_VERSION}${NC}"

# Pre-check cluster compatibility
echo -e "\n${YELLOW}Running pre-installation checks...${NC}"
if ! linkerd check --pre; then
    echo -e "${RED}❌ Pre-installation checks failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Pre-installation checks passed${NC}"

# Generate certificates for mTLS
echo -e "\n${YELLOW}Generating certificates for mTLS...${NC}"

CERT_DIR="./certificates"
mkdir -p $CERT_DIR

if [ ! -f "$CERT_DIR/ca.crt" ]; then
    # Generate trust anchor certificate
    step certificate create root.linkerd.cluster.local \
        $CERT_DIR/ca.crt $CERT_DIR/ca.key \
        --profile root-ca \
        --no-password \
        --insecure \
        --not-after 87600h
    
    # Generate issuer certificate
    step certificate create identity.linkerd.cluster.local \
        $CERT_DIR/issuer.crt $CERT_DIR/issuer.key \
        --profile intermediate-ca \
        --not-after 8760h \
        --no-password \
        --insecure \
        --ca $CERT_DIR/ca.crt \
        --ca-key $CERT_DIR/ca.key
    
    echo -e "${GREEN}✅ Certificates generated${NC}"
else
    echo -e "${YELLOW}⚠️  Using existing certificates${NC}"
fi

# Install Linkerd control plane
echo -e "\n${YELLOW}Installing Linkerd control plane...${NC}"

if ! kubectl get namespace linkerd &> /dev/null; then
    linkerd install \
        --identity-trust-anchors-file $CERT_DIR/ca.crt \
        --identity-issuer-certificate-file $CERT_DIR/issuer.crt \
        --identity-issuer-key-file $CERT_DIR/issuer.key \
        --set proxyInit.runAsRoot=false \
        --set proxy.cores=1 \
        --set proxy.cpu.request=100m \
        --set proxy.cpu.limit=1 \
        --set proxy.memory.request=128Mi \
        --set proxy.memory.limit=512Mi \
        | kubectl apply -f -
    
    echo -e "${GREEN}✅ Linkerd control plane installed${NC}"
else
    echo -e "${YELLOW}⚠️  Linkerd already installed, skipping...${NC}"
fi

# Wait for Linkerd to be ready
echo -e "\n${YELLOW}Waiting for Linkerd control plane to be ready...${NC}"
kubectl wait --for=condition=available --timeout=300s deployment -n linkerd --all

# Verify installation
echo -e "\n${YELLOW}Verifying Linkerd installation...${NC}"
if ! linkerd check; then
    echo -e "${RED}❌ Linkerd installation verification failed${NC}"
    exit 1
fi
echo -e "${GREEN}✅ Linkerd installation verified${NC}"

# Install Linkerd Viz extension for observability
echo -e "\n${YELLOW}Installing Linkerd Viz extension...${NC}"

if ! kubectl get namespace linkerd-viz &> /dev/null; then
    linkerd viz install | kubectl apply -f -
    kubectl wait --for=condition=available --timeout=300s deployment -n linkerd-viz --all
    echo -e "${GREEN}✅ Linkerd Viz extension installed${NC}"
else
    echo -e "${YELLOW}⚠️  Linkerd Viz already installed${NC}"
fi

# Enable automatic proxy injection for data-processing namespace
echo -e "\n${YELLOW}Configuring automatic proxy injection...${NC}"

if ! kubectl get namespace $NAMESPACE &> /dev/null; then
    kubectl create namespace $NAMESPACE
fi

kubectl annotate namespace $NAMESPACE linkerd.io/inject=enabled --overwrite
echo -e "${GREEN}✅ Automatic proxy injection enabled for $NAMESPACE namespace${NC}"

# Apply Linkerd configuration
echo -e "\n${YELLOW}Applying Linkerd configuration...${NC}"
kubectl apply -f ./linkerd-config.yaml
echo -e "${GREEN}✅ Linkerd configuration applied${NC}"

# Display access information
echo -e "\n${BLUE}============================================${NC}"
echo -e "${GREEN}✅ Linkerd installation completed!${NC}"
echo -e "${BLUE}============================================${NC}"
echo -e "\nAccess Linkerd dashboard:"
echo -e "  ${YELLOW}linkerd viz dashboard${NC}"
echo -e "\nCheck Linkerd status:"
echo -e "  ${YELLOW}linkerd check${NC}"
echo -e "\nView service mesh topology:"
echo -e "  ${YELLOW}linkerd viz stat deploy -n $NAMESPACE${NC}"
echo -e "\nTap live traffic:"
echo -e "  ${YELLOW}linkerd viz tap deploy/normalization-service -n $NAMESPACE${NC}"
echo -e "${BLUE}============================================${NC}"
