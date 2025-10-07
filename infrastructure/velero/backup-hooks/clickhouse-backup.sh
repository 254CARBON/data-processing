#!/bin/bash
# ClickHouse pre-backup hook for Velero
# Creates consistent backup using clickhouse-backup

set -euo pipefail

BACKUP_NAME="velero_$(date +%Y%m%d_%H%M%S)"

echo "Starting ClickHouse backup: $BACKUP_NAME"

# Create backup
clickhouse-backup create $BACKUP_NAME

# Verify backup
if clickhouse-backup list | grep -q $BACKUP_NAME; then
    echo "✅ ClickHouse backup created: $BACKUP_NAME"
    echo "$BACKUP_NAME" > /tmp/backup_name
else
    echo "❌ ClickHouse backup failed"
    exit 1
fi
