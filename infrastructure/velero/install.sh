#!/bin/bash
# Velero Installation Script for On-Premises Kubernetes
# Backup and Disaster Recovery with MinIO storage backend

set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Configuration
VELERO_VERSION="v1.12.0"
MINIO_NAMESPACE="velero"
BACKUP_LOCATION="s3:http://minio.${MINIO_NAMESPACE}:9000/velero-backups"
VELERO_NAMESPACE="velero"

echo -e "${BLUE}============================================${NC}"
echo -e "${BLUE}Velero Backup & DR Installation${NC}"
echo -e "${BLUE}============================================${NC}"

# Install MinIO for backup storage
echo -e "\n${YELLOW}Installing MinIO storage backend...${NC}"

kubectl create namespace $MINIO_NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: minio
  namespace: $MINIO_NAMESPACE
spec:
  selector:
    matchLabels:
      app: minio
  template:
    metadata:
      labels:
        app: minio
    spec:
      containers:
      - name: minio
        image: minio/minio:latest
        args:
        - server
        - /data
        - --console-address
        - ":9001"
        env:
        - name: MINIO_ACCESS_KEY
          value: "velero"
        - name: MINIO_SECRET_KEY
          value: "velero123"
        ports:
        - containerPort: 9000
        - containerPort: 9001
        volumeMounts:
        - name: storage
          mountPath: /data
      volumes:
      - name: storage
        persistentVolumeClaim:
          claimName: minio-storage
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: minio-storage
  namespace: $MINIO_NAMESPACE
spec:
  accessModes:
  - ReadWriteOnce
  resources:
    requests:
      storage: 100Gi
---
apiVersion: v1
kind: Service
metadata:
  name: minio
  namespace: $MINIO_NAMESPACE
spec:
  ports:
  - port: 9000
    targetPort: 9000
    name: s3
  - port: 9001
    targetPort: 9001
    name: console
  selector:
    app: minio
EOF

echo -e "${GREEN}✅ MinIO installed${NC}"

# Create Velero credentials
echo -e "\n${YELLOW}Creating Velero credentials...${NC}"

cat > /tmp/credentials-velero <<EOF
[default]
aws_access_key_id = velero
aws_secret_access_key = velero123
EOF

kubectl create namespace $VELERO_NAMESPACE --dry-run=client -o yaml | kubectl apply -f -

kubectl create secret generic cloud-credentials \
    --namespace $VELERO_NAMESPACE \
    --from-file cloud=/tmp/credentials-velero \
    --dry-run=client -o yaml | kubectl apply -f -

rm /tmp/credentials-velero

# Install Velero CLI
echo -e "\n${YELLOW}Installing Velero CLI...${NC}"

if ! command -v velero &> /dev/null; then
    wget https://github.com/vmware-tanzu/velero/releases/download/$VELERO_VERSION/velero-$VELERO_VERSION-linux-amd64.tar.gz
    tar -xvf velero-$VELERO_VERSION-linux-amd64.tar.gz
    sudo mv velero-$VELERO_VERSION-linux-amd64/velero /usr/local/bin/
    rm -rf velero-$VELERO_VERSION-linux-amd64*
fi

# Install Velero
echo -e "\n${YELLOW}Installing Velero...${NC}"

velero install \
    --provider aws \
    --plugins velero/velero-plugin-for-aws:v1.8.0 \
    --bucket velero-backups \
    --secret-file /dev/null \
    --use-volume-snapshots=false \
    --backup-location-config region=minio,s3ForcePathStyle="true",s3Url=http://minio.${MINIO_NAMESPACE}:9000 \
    --namespace $VELERO_NAMESPACE

# Wait for Velero to be ready
kubectl wait --for=condition=available --timeout=300s deployment/velero -n $VELERO_NAMESPACE

echo -e "${GREEN}✅ Velero installed${NC}"

# Create backup bucket in MinIO
echo -e "\n${YELLOW}Creating backup bucket...${NC}"
kubectl run -it --rm minio-mc --image=minio/mc --restart=Never -- \
    mc alias set minio http://minio.${MINIO_NAMESPACE}:9000 velero velero123 && \
    mc mb minio/velero-backups || true

# Apply backup schedules
echo -e "\n${YELLOW}Creating backup schedules...${NC}"
kubectl apply -f ./backup-schedules.yaml

echo -e "\n${BLUE}============================================${NC}"
echo -e "${GREEN}✅ Velero installation completed!${NC}"
echo -e "${BLUE}============================================${NC}"
echo -e "\nUseful commands:"
echo -e "  velero backup-location get"
echo -e "  velero schedule get"
echo -e "  velero backup create manual-backup --include-namespaces data-processing"
echo -e "  velero restore create --from-backup manual-backup"
echo -e "${BLUE}============================================${NC}"
