#!/bin/bash
# PostgreSQL pre-backup hook for Velero
# Ensures consistent backup by creating a logical dump

set -euo pipefail

BACKUP_DIR="/tmp/velero-backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="$BACKUP_DIR/postgres_backup_$TIMESTAMP.sql"

mkdir -p $BACKUP_DIR

echo "Starting PostgreSQL backup..."

# Create consistent backup
pg_dump -U $POSTGRES_USER -Fc -f $BACKUP_FILE

# Verify backup
if [ -f "$BACKUP_FILE" ]; then
    SIZE=$(du -h $BACKUP_FILE | cut -f1)
    echo "✅ Backup created successfully: $BACKUP_FILE ($SIZE)"
    echo "$BACKUP_FILE" > /tmp/backup_location
else
    echo "❌ Backup failed"
    exit 1
fi
