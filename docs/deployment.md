# Deployment Guide

This guide covers deployment strategies, configurations, and best practices for the 254Carbon data processing pipeline.

## Overview

The data processing pipeline can be deployed in various environments:

- **Local Development**: Docker Compose
- **Staging**: Kubernetes with Helm charts
- **Production**: Kubernetes with advanced configurations
- **Cloud**: AWS, GCP, Azure with managed services

## Prerequisites

### System Requirements

- **CPU**: 8+ cores recommended
- **Memory**: 16GB+ RAM recommended
- **Storage**: 100GB+ SSD recommended
- **Network**: 1Gbps+ bandwidth recommended

### Software Requirements

- **Docker**: 20.10+ with Compose 2.0+
- **Kubernetes**: 1.24+ (for production)
- **Helm**: 3.8+ (for Kubernetes deployments)
- **kubectl**: Latest version

### External Dependencies

- **Kafka Cluster**: 3+ brokers recommended
- **ClickHouse**: Single node or cluster
- **PostgreSQL**: Primary-replica setup
- **Redis**: Cluster mode recommended

## Local Development Deployment

### Quick Start

```bash
# Clone repository
git clone <repository-url>
cd data-processing

# Start all services
docker-compose up -d

# Verify deployment
docker-compose ps
```

### Service Configuration

```yaml
# docker-compose.yml
version: '3.8'
services:
  normalization-service:
    build: ./service-normalization
    ports:
      - "8080:8080"
    environment:
      DATA_PROC_ENV: local
      DATA_PROC_KAFKA_BOOTSTRAP: kafka:29092
      DATA_PROC_CLICKHOUSE_URL: http://clickhouse:8123
    depends_on:
      - kafka
      - clickhouse
```

### Health Checks

```bash
# Check all services
curl http://localhost:8080/health
curl http://localhost:8081/health
curl http://localhost:8082/health
curl http://localhost:8083/health

# Check infrastructure
curl http://localhost:9090/-/healthy  # Prometheus
curl http://localhost:3000/api/health  # Grafana
```

## Kubernetes Deployment

### Helm Chart Structure

```
helm/
├── Chart.yaml
├── values.yaml
├── values-dev.yaml
├── values-staging.yaml
├── values-prod.yaml
└── templates/
    ├── deployment.yaml
    ├── service.yaml
    ├── configmap.yaml
    ├── secret.yaml
    └── ingress.yaml
```

### Chart.yaml

```yaml
apiVersion: v2
name: data-processing
description: 254Carbon Data Processing Pipeline
version: 1.0.0
appVersion: "1.0.0"
keywords:
  - data-processing
  - market-data
  - real-time
home: https://github.com/254carbon/data-processing
sources:
  - https://github.com/254carbon/data-processing
maintainers:
  - name: 254Carbon Team
    email: team@254carbon.com
```

### values.yaml

```yaml
# Global configuration
global:
  imageRegistry: ""
  imagePullSecrets: []
  storageClass: ""

# Service configurations
services:
  normalization:
    replicaCount: 3
    image:
      repository: normalization-service
      tag: "latest"
      pullPolicy: IfNotPresent
    service:
      type: ClusterIP
      port: 8080
    resources:
      limits:
        cpu: 1000m
        memory: 2Gi
      requests:
        cpu: 500m
        memory: 1Gi
    autoscaling:
      enabled: true
      minReplicas: 3
      maxReplicas: 10
      targetCPUUtilizationPercentage: 70
      targetMemoryUtilizationPercentage: 80

  enrichment:
    replicaCount: 2
    image:
      repository: enrichment-service
      tag: "latest"
      pullPolicy: IfNotPresent
    service:
      type: ClusterIP
      port: 8081
    resources:
      limits:
        cpu: 1000m
        memory: 2Gi
      requests:
        cpu: 500m
        memory: 1Gi

  aggregation:
    replicaCount: 4
    image:
      repository: aggregation-service
      tag: "latest"
      pullPolicy: IfNotPresent
    service:
      type: ClusterIP
      port: 8082
    resources:
      limits:
        cpu: 2000m
        memory: 4Gi
      requests:
        cpu: 1000m
        memory: 2Gi

  projection:
    replicaCount: 2
    image:
      repository: projection-service
      tag: "latest"
      pullPolicy: IfNotPresent
    service:
      type: ClusterIP
      port: 8083
    resources:
      limits:
        cpu: 1000m
        memory: 2Gi
      requests:
        cpu: 500m
        memory: 1Gi

# Infrastructure
infrastructure:
  kafka:
    bootstrapServers: "kafka-cluster:9092"
    topics:
      - name: "ingestion.market.raw.v1"
        partitions: 12
        replicationFactor: 3
      - name: "normalized.market.ticks.v1"
        partitions: 12
        replicationFactor: 3
  
  clickhouse:
    url: "http://clickhouse-cluster:8123"
    database: "market_data"
    username: "default"
    password: ""
  
  postgresql:
    host: "postgresql-cluster"
    port: 5432
    database: "reference_data"
    username: "postgres"
    password: ""
  
  redis:
    url: "redis://redis-cluster:6379"
    database: 0
    password: ""

# Monitoring
monitoring:
  enabled: true
  prometheus:
    enabled: true
    scrapeInterval: 15s
  grafana:
    enabled: true
    adminPassword: "admin"
    dashboards:
      - data-processing-overview
      - normalization-service
      - enrichment-service
      - aggregation-service
      - projection-service

# Ingress
ingress:
  enabled: true
  className: "nginx"
  annotations:
    nginx.ingress.kubernetes.io/rewrite-target: /
    nginx.ingress.kubernetes.io/ssl-redirect: "true"
  hosts:
    - host: data-processing.example.com
      paths:
        - path: /
          pathType: Prefix
          service: normalization-service
          port: 8080
  tls:
    - secretName: data-processing-tls
      hosts:
        - data-processing.example.com
```

### Deployment Commands

```bash
# Add Helm repository
helm repo add 254carbon https://charts.254carbon.com
helm repo update

# Deploy to development
helm install data-processing-dev 254carbon/data-processing \
  --namespace data-processing-dev \
  --create-namespace \
  --values values-dev.yaml

# Deploy to staging
helm install data-processing-staging 254carbon/data-processing \
  --namespace data-processing-staging \
  --create-namespace \
  --values values-staging.yaml

# Deploy to production
helm install data-processing-prod 254carbon/data-processing \
  --namespace data-processing-prod \
  --create-namespace \
  --values values-prod.yaml
```

### Production values-prod.yaml

```yaml
# Production-specific overrides
services:
  normalization:
    replicaCount: 10
    resources:
      limits:
        cpu: 2000m
        memory: 4Gi
      requests:
        cpu: 1000m
        memory: 2Gi
    autoscaling:
      enabled: true
      minReplicas: 10
      maxReplicas: 50
      targetCPUUtilizationPercentage: 60
      targetMemoryUtilizationPercentage: 70

  enrichment:
    replicaCount: 8
    resources:
      limits:
        cpu: 2000m
        memory: 4Gi
      requests:
        cpu: 1000m
        memory: 2Gi

  aggregation:
    replicaCount: 15
    resources:
      limits:
        cpu: 4000m
        memory: 8Gi
      requests:
        cpu: 2000m
        memory: 4Gi

  projection:
    replicaCount: 8
    resources:
      limits:
        cpu: 2000m
        memory: 4Gi
      requests:
        cpu: 1000m
        memory: 2Gi

# Production infrastructure
infrastructure:
  kafka:
    bootstrapServers: "kafka-prod-cluster:9092"
    topics:
      - name: "ingestion.market.raw.v1"
        partitions: 50
        replicationFactor: 3
      - name: "normalized.market.ticks.v1"
        partitions: 50
        replicationFactor: 3
  
  clickhouse:
    url: "http://clickhouse-prod-cluster:8123"
    database: "market_data_prod"
  
  postgresql:
    host: "postgresql-prod-cluster"
    database: "reference_data_prod"
  
  redis:
    url: "redis://redis-prod-cluster:6379"

# Production monitoring
monitoring:
  prometheus:
    retention: "30d"
    storage: "50Gi"
  grafana:
    persistence:
      enabled: true
      size: "10Gi"

# Production security
security:
  enabled: true
  networkPolicies:
    enabled: true
  podSecurityPolicy:
    enabled: true
  serviceAccount:
    create: true
    annotations: {}
```

## Cloud Deployment

### AWS Deployment

#### EKS Cluster Setup

```bash
# Create EKS cluster
eksctl create cluster \
  --name data-processing-prod \
  --version 1.24 \
  --region us-west-2 \
  --nodegroup-name workers \
  --node-type m5.2xlarge \
  --nodes 3 \
  --nodes-min 3 \
  --nodes-max 10 \
  --managed

# Install AWS Load Balancer Controller
helm repo add eks https://aws.github.io/eks-charts
helm install aws-load-balancer-controller eks/aws-load-balancer-controller \
  --set clusterName=data-processing-prod \
  --set serviceAccount.create=false \
  --set region=us-west-2 \
  --set vpcId=vpc-12345678 \
  --namespace kube-system
```

#### Managed Services

```yaml
# AWS managed services configuration
aws:
  kafka:
    clusterName: "data-processing-kafka"
    instanceType: "kafka.m5.large"
    numberOfBrokerNodes: 3
    storageMode: "EBS"
    volumeSize: 100
    encryptionAtRest: true
    encryptionInTransit: true
  
  clickhouse:
    clusterName: "data-processing-clickhouse"
    nodeType: "clickhouse.x1.2xlarge"
    numberOfNodes: 3
    storageType: "GP3"
    storageSize: 1000
    encryptionAtRest: true
  
  postgresql:
    engine: "postgres"
    engineVersion: "14.7"
    instanceClass: "db.r5.2xlarge"
    allocatedStorage: 1000
    multiAZ: true
    encryptionAtRest: true
  
  redis:
    clusterMode: true
    nodeType: "cache.r5.large"
    numberOfNodes: 6
    encryptionAtRest: true
    encryptionInTransit: true
```

### GCP Deployment

#### GKE Cluster Setup

```bash
# Create GKE cluster
gcloud container clusters create data-processing-prod \
  --zone us-central1-a \
  --machine-type e2-standard-8 \
  --num-nodes 3 \
  --min-nodes 3 \
  --max-nodes 10 \
  --enable-autoscaling \
  --enable-autorepair \
  --enable-autoupgrade

# Install GKE Ingress Controller
kubectl apply -f https://raw.githubusercontent.com/kubernetes/ingress-nginx/controller-v1.5.1/deploy/static/provider/cloud/deploy.yaml
```

#### Managed Services

```yaml
# GCP managed services configuration
gcp:
  kafka:
    clusterName: "data-processing-kafka"
    machineType: "e2-standard-4"
    diskSize: "100GB"
    diskType: "pd-ssd"
    numberOfNodes: 3
    encryptionAtRest: true
  
  clickhouse:
    clusterName: "data-processing-clickhouse"
    machineType: "n1-highmem-8"
    diskSize: "1000GB"
    diskType: "pd-ssd"
    numberOfNodes: 3
    encryptionAtRest: true
  
  postgresql:
    instanceName: "data-processing-postgres"
    databaseVersion: "POSTGRES_14"
    machineType: "db-standard-4"
    diskSize: "1000GB"
    diskType: "PD_SSD"
    highAvailability: true
    encryptionAtRest: true
  
  redis:
    instanceName: "data-processing-redis"
    memorySizeGb: 8
    redisVersion: "REDIS_6_X"
    networkMode: "VPC_PEERING"
    encryptionAtRest: true
```

### Azure Deployment

#### AKS Cluster Setup

```bash
# Create AKS cluster
az aks create \
  --resource-group data-processing-rg \
  --name data-processing-prod \
  --node-count 3 \
  --node-vm-size Standard_D8s_v3 \
  --enable-cluster-autoscaler \
  --min-count 3 \
  --max-count 10 \
  --enable-addons monitoring \
  --generate-ssh-keys

# Install NGINX Ingress Controller
helm repo add ingress-nginx https://kubernetes.github.io/ingress-nginx
helm install ingress-nginx ingress-nginx/ingress-nginx \
  --namespace ingress-nginx \
  --create-namespace
```

## CI/CD Pipeline

### GitHub Actions Workflow

```yaml
# .github/workflows/deploy.yml
name: Deploy to Kubernetes

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.9'
      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install -r requirements.txt
      - name: Run tests
        run: |
          python scripts/test_integration_simple.py
          python scripts/test_monitoring.py

  build:
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Build Docker images
        run: |
          docker build -t normalization-service:latest ./service-normalization
          docker build -t enrichment-service:latest ./service-enrichment
          docker build -t aggregation-service:latest ./service-aggregation
          docker build -t projection-service:latest ./service-projection

  deploy-staging:
    needs: build
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    environment: staging
    steps:
      - uses: actions/checkout@v3
      - name: Deploy to staging
        run: |
          helm upgrade --install data-processing-staging ./helm \
            --namespace data-processing-staging \
            --create-namespace \
            --values ./helm/values-staging.yaml

  deploy-production:
    needs: deploy-staging
    runs-on: ubuntu-latest
    if: github.ref == 'refs/heads/main'
    environment: production
    steps:
      - uses: actions/checkout@v3
      - name: Deploy to production
        run: |
          helm upgrade --install data-processing-prod ./helm \
            --namespace data-processing-prod \
            --create-namespace \
            --values ./helm/values-prod.yaml
```

### GitLab CI Pipeline

```yaml
# .gitlab-ci.yml
stages:
  - test
  - build
  - deploy-staging
  - deploy-production

variables:
  DOCKER_DRIVER: overlay2
  DOCKER_TLS_CERTDIR: "/certs"

test:
  stage: test
  image: python:3.9
  script:
    - pip install -r requirements.txt
    - python scripts/test_integration_simple.py
    - python scripts/test_monitoring.py

build:
  stage: build
  image: docker:latest
  services:
    - docker:dind
  script:
    - docker build -t normalization-service:latest ./service-normalization
    - docker build -t enrichment-service:latest ./service-enrichment
    - docker build -t aggregation-service:latest ./service-aggregation
    - docker build -t projection-service:latest ./service-projection

deploy-staging:
  stage: deploy-staging
  image: alpine/helm:latest
  script:
    - helm upgrade --install data-processing-staging ./helm \
        --namespace data-processing-staging \
        --create-namespace \
        --values ./helm/values-staging.yaml
  only:
    - main

deploy-production:
  stage: deploy-production
  image: alpine/helm:latest
  script:
    - helm upgrade --install data-processing-prod ./helm \
        --namespace data-processing-prod \
        --create-namespace \
        --values ./helm/values-prod.yaml
  only:
    - main
  when: manual
```

## Security Configuration

### Network Policies

```yaml
# network-policy.yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: data-processing-network-policy
  namespace: data-processing-prod
spec:
  podSelector: {}
  policyTypes:
  - Ingress
  - Egress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          name: ingress-nginx
    ports:
    - protocol: TCP
      port: 8080
  - from:
    - namespaceSelector:
        matchLabels:
          name: monitoring
    ports:
    - protocol: TCP
      port: 8080
  egress:
  - to:
    - namespaceSelector:
        matchLabels:
          name: kafka
    ports:
    - protocol: TCP
      port: 9092
  - to:
    - namespaceSelector:
        matchLabels:
          name: databases
    ports:
    - protocol: TCP
      port: 5432
```

### Audit Logging

- Ensure `DATA_PROC_AUDIT_ENABLED=true` for environments requiring trail
- Configure ClickHouse retention via `migrations/clickhouse/006_audit_events.sql`
- Set per-service toggles (`DATA_PROC_<SERVICE>_AUDIT_ENABLED`) for fine control
- Optional Kafka topic (`DATA_PROC_AUDIT_KAFKA_TOPIC`) mirrors events to SIEM queue
- Monitor queue back pressure with `DATA_PROC_AUDIT_QUEUE_SIZE` and flush interval

### Pod Security Policy

```yaml
# pod-security-policy.yaml
apiVersion: policy/v1beta1
kind: PodSecurityPolicy
metadata:
  name: data-processing-psp
spec:
  privileged: false
  allowPrivilegeEscalation: false
  requiredDropCapabilities:
    - ALL
  volumes:
    - 'configMap'
    - 'emptyDir'
    - 'projected'
    - 'secret'
    - 'downwardAPI'
    - 'persistentVolumeClaim'
  runAsUser:
    rule: 'MustRunAsNonRoot'
  seLinux:
    rule: 'RunAsAny'
  fsGroup:
    rule: 'RunAsAny'
```

### RBAC Configuration

```yaml
# rbac.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: data-processing-sa
  namespace: data-processing-prod
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  namespace: data-processing-prod
  name: data-processing-role
rules:
- apiGroups: [""]
  resources: ["pods", "services", "endpoints", "persistentvolumeclaims"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: ["apps"]
  resources: ["deployments", "replicasets"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: data-processing-rolebinding
  namespace: data-processing-prod
subjects:
- kind: ServiceAccount
  name: data-processing-sa
  namespace: data-processing-prod
roleRef:
  kind: Role
  name: data-processing-role
  apiGroup: rbac.authorization.k8s.io
```

## Monitoring and Observability

### Prometheus Configuration

```yaml
# prometheus-config.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: prometheus-config
  namespace: monitoring
data:
  prometheus.yml: |
    global:
      scrape_interval: 15s
      evaluation_interval: 15s
    
    scrape_configs:
    - job_name: 'data-processing-services'
      kubernetes_sd_configs:
      - role: endpoints
        namespaces:
          names:
          - data-processing-prod
      relabel_configs:
      - source_labels: [__meta_kubernetes_service_annotation_prometheus_io_scrape]
        action: keep
        regex: true
      - source_labels: [__meta_kubernetes_service_annotation_prometheus_io_path]
        action: replace
        target_label: __metrics_path__
        regex: (.+)
```

### Grafana Configuration

```yaml
# grafana-config.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: grafana-datasources
  namespace: monitoring
data:
  datasources.yaml: |
    apiVersion: 1
    datasources:
    - name: Prometheus
      type: prometheus
      url: http://prometheus:9090
      access: proxy
      isDefault: true
```

## Backup and Recovery

### Database Backups

```bash
# ClickHouse backup
clickhouse-backup create --config=/etc/clickhouse-backup/config.yml backup_name

# PostgreSQL backup
pg_dump -h postgresql-cluster -U postgres -d reference_data > backup.sql

# Redis backup
redis-cli --rdb /backup/redis-backup.rdb
```

### Application Data Backups

```bash
# Kafka topic backup
kafka-console-consumer --bootstrap-server kafka-cluster:9092 \
  --topic ingestion.market.raw.v1 \
  --from-beginning > kafka-backup.json

# Configuration backup
kubectl get configmaps -n data-processing-prod -o yaml > config-backup.yaml
kubectl get secrets -n data-processing-prod -o yaml > secrets-backup.yaml
```