"""
Comprehensive audit logging system for the 254Carbon Data Processing Pipeline.
Implements security compliance, audit trails, and forensic capabilities.
"""

import os
import json
import logging
import hashlib
import hmac
from typing import Dict, Any, Optional, List, Union
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from enum import Enum
import asyncio
import aiofiles
from cryptography.fernet import Fernet

logger = logging.getLogger(__name__)


class AuditEventType(Enum):
    """Types of audit events."""
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    DATA_ACCESS = "data_access"
    DATA_MODIFICATION = "data_modification"
    DATA_DELETION = "data_deletion"
    CONFIGURATION_CHANGE = "configuration_change"
    SECURITY_EVENT = "security_event"
    SYSTEM_EVENT = "system_event"
    API_ACCESS = "api_access"
    DATABASE_ACCESS = "database_access"
    FILE_ACCESS = "file_access"
    NETWORK_ACCESS = "network_access"
    ADMIN_ACTION = "admin_action"
    USER_ACTION = "user_action"
    SERVICE_START = "service_start"
    SERVICE_STOP = "service_stop"
    DEPLOYMENT = "deployment"
    BACKUP = "backup"
    RESTORE = "restore"
    KEY_ROTATION = "key_rotation"
    CERTIFICATE_CHANGE = "certificate_change"


class AuditSeverity(Enum):
    """Severity levels for audit events."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class AuditEvent:
    """Audit event data structure."""
    event_id: str
    event_type: AuditEventType
    severity: AuditSeverity
    timestamp: datetime
    service_name: str
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    request_id: Optional[str] = None
    action: Optional[str] = None
    resource: Optional[str] = None
    resource_id: Optional[str] = None
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    result: Optional[str] = None
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    tags: Optional[List[str]] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert audit event to dictionary."""
        data = asdict(self)
        data['event_type'] = self.event_type.value
        data['severity'] = self.severity.value
        data['timestamp'] = self.timestamp.isoformat()
        return data
    
    def to_json(self) -> str:
        """Convert audit event to JSON string."""
        return json.dumps(self.to_dict(), default=str)
    
    def get_hash(self) -> str:
        """Get cryptographic hash of the audit event."""
        event_data = self.to_json()
        return hashlib.sha256(event_data.encode()).hexdigest()


class AuditLogger:
    """Main audit logging class."""
    
    def __init__(self, 
                 log_file: str = "/var/log/audit/audit.log",
                 enable_encryption: bool = True,
                 enable_integrity_check: bool = True,
                 max_file_size: int = 100 * 1024 * 1024,  # 100MB
                 backup_count: int = 10):
        
        self.log_file = log_file
        self.enable_encryption = enable_encryption
        self.enable_integrity_check = enable_integrity_check
        self.max_file_size = max_file_size
        self.backup_count = backup_count
        
        # Create log directory
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        # Initialize encryption if enabled
        if self.enable_encryption:
            self.encryption_key = self._get_or_create_encryption_key()
            self.cipher = Fernet(self.encryption_key)
        
        # Initialize integrity key
        if self.enable_integrity_check:
            self.integrity_key = self._get_or_create_integrity_key()
        
        # Event queue for async processing
        self.event_queue = asyncio.Queue()
        self.processing_task = None
    
    def _get_or_create_encryption_key(self) -> bytes:
        """Get or create encryption key for audit logs."""
        key_file = os.path.join(os.path.dirname(self.log_file), ".audit_key")
        
        if os.path.exists(key_file):
            with open(key_file, 'rb') as f:
                return f.read()
        else:
            key = Fernet.generate_key()
            with open(key_file, 'wb') as f:
                f.write(key)
            os.chmod(key_file, 0o600)  # Restrict access
            return key
    
    def _get_or_create_integrity_key(self) -> bytes:
        """Get or create integrity key for audit logs."""
        key_file = os.path.join(os.path.dirname(self.log_file), ".integrity_key")
        
        if os.path.exists(key_file):
            with open(key_file, 'rb') as f:
                return f.read()
        else:
            key = os.urandom(32)  # 256-bit key
            with open(key_file, 'wb') as f:
                f.write(key)
            os.chmod(key_file, 0o600)  # Restrict access
            return key
    
    def _generate_event_id(self) -> str:
        """Generate unique event ID."""
        timestamp = datetime.now(timezone.utc)
        random_part = os.urandom(8).hex()
        return f"audit_{timestamp.strftime('%Y%m%d_%H%M%S')}_{random_part}"
    
    def _calculate_integrity_hash(self, data: str) -> str:
        """Calculate HMAC hash for integrity verification."""
        return hmac.new(
            self.integrity_key,
            data.encode(),
            hashlib.sha256
        ).hexdigest()
    
    async def log_event(self, event: AuditEvent) -> bool:
        """Log an audit event asynchronously."""
        try:
            # Add event to queue for processing
            await self.event_queue.put(event)
            return True
        except Exception as e:
            logger.error(f"Failed to queue audit event: {e}")
            return False
    
    def log_event_sync(self, event: AuditEvent) -> bool:
        """Log an audit event synchronously."""
        try:
            # Generate event ID if not provided
            if not event.event_id:
                event.event_id = self._generate_event_id()
            
            # Ensure timestamp is set
            if not event.timestamp:
                event.timestamp = datetime.now(timezone.utc)
            
            # Convert to JSON
            event_json = event.to_json()
            
            # Add integrity hash if enabled
            if self.enable_integrity_check:
                integrity_hash = self._calculate_integrity_hash(event_json)
                event_data = {
                    'event': json.loads(event_json),
                    'integrity_hash': integrity_hash,
                    'timestamp': datetime.now(timezone.utc).isoformat()
                }
                event_json = json.dumps(event_data)
            
            # Encrypt if enabled
            if self.enable_encryption:
                event_json = self.cipher.encrypt(event_json.encode()).decode()
            
            # Write to log file
            with open(self.log_file, 'a') as f:
                f.write(event_json + '\n')
            
            logger.debug(f"Audit event logged: {event.event_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to log audit event: {e}")
            return False
    
    async def start_async_processing(self):
        """Start async processing of audit events."""
        self.processing_task = asyncio.create_task(self._process_event_queue())
    
    async def stop_async_processing(self):
        """Stop async processing of audit events."""
        if self.processing_task:
            self.processing_task.cancel()
            try:
                await self.processing_task
            except asyncio.CancelledError:
                pass
    
    async def _process_event_queue(self):
        """Process audit events from the queue."""
        while True:
            try:
                event = await self.event_queue.get()
                self.log_event_sync(event)
                self.event_queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error processing audit event: {e}")
    
    def search_events(self, 
                     event_type: Optional[AuditEventType] = None,
                     user_id: Optional[str] = None,
                     start_time: Optional[datetime] = None,
                     end_time: Optional[datetime] = None,
                     severity: Optional[AuditSeverity] = None,
                     limit: int = 100) -> List[AuditEvent]:
        """Search audit events based on criteria."""
        events = []
        
        try:
            with open(self.log_file, 'r') as f:
                for line in f:
                    try:
                        # Decrypt if enabled
                        if self.enable_encryption:
                            line = self.cipher.decrypt(line.encode()).decode()
                        
                        # Parse event data
                        event_data = json.loads(line)
                        
                        # Extract event if integrity check is enabled
                        if self.enable_integrity_check and 'event' in event_data:
                            event_json = json.dumps(event_data['event'])
                            calculated_hash = self._calculate_integrity_hash(event_json)
                            
                            if calculated_hash != event_data.get('integrity_hash'):
                                logger.warning("Audit event integrity check failed")
                                continue
                            
                            event_data = event_data['event']
                        
                        # Create audit event
                        event = self._dict_to_audit_event(event_data)
                        
                        # Apply filters
                        if event_type and event.event_type != event_type:
                            continue
                        if user_id and event.user_id != user_id:
                            continue
                        if start_time and event.timestamp < start_time:
                            continue
                        if end_time and event.timestamp > end_time:
                            continue
                        if severity and event.severity != severity:
                            continue
                        
                        events.append(event)
                        
                        if len(events) >= limit:
                            break
                            
                    except Exception as e:
                        logger.error(f"Error parsing audit event: {e}")
                        continue
        
        except FileNotFoundError:
            logger.warning(f"Audit log file not found: {self.log_file}")
        except Exception as e:
            logger.error(f"Error searching audit events: {e}")
        
        return events
    
    def _dict_to_audit_event(self, data: Dict[str, Any]) -> AuditEvent:
        """Convert dictionary to AuditEvent."""
        # Parse timestamp
        if isinstance(data['timestamp'], str):
            timestamp = datetime.fromisoformat(data['timestamp'].replace('Z', '+00:00'))
        else:
            timestamp = data['timestamp']
        
        return AuditEvent(
            event_id=data['event_id'],
            event_type=AuditEventType(data['event_type']),
            severity=AuditSeverity(data['severity']),
            timestamp=timestamp,
            service_name=data['service_name'],
            user_id=data.get('user_id'),
            session_id=data.get('session_id'),
            ip_address=data.get('ip_address'),
            user_agent=data.get('user_agent'),
            request_id=data.get('request_id'),
            action=data.get('action'),
            resource=data.get('resource'),
            resource_id=data.get('resource_id'),
            old_value=data.get('old_value'),
            new_value=data.get('new_value'),
            result=data.get('result'),
            error_message=data.get('error_message'),
            metadata=data.get('metadata'),
            tags=data.get('tags')
        )
    
    def get_audit_summary(self, 
                         start_time: Optional[datetime] = None,
                         end_time: Optional[datetime] = None) -> Dict[str, Any]:
        """Get audit summary statistics."""
        events = self.search_events(start_time=start_time, end_time=end_time, limit=10000)
        
        summary = {
            'total_events': len(events),
            'events_by_type': {},
            'events_by_severity': {},
            'events_by_service': {},
            'events_by_user': {},
            'failed_events': 0,
            'successful_events': 0
        }
        
        for event in events:
            # Count by type
            event_type = event.event_type.value
            summary['events_by_type'][event_type] = summary['events_by_type'].get(event_type, 0) + 1
            
            # Count by severity
            severity = event.severity.value
            summary['events_by_severity'][severity] = summary['events_by_severity'].get(severity, 0) + 1
            
            # Count by service
            service = event.service_name
            summary['events_by_service'][service] = summary['events_by_service'].get(service, 0) + 1
            
            # Count by user
            if event.user_id:
                summary['events_by_user'][event.user_id] = summary['events_by_user'].get(event.user_id, 0) + 1
            
            # Count success/failure
            if event.result == 'success':
                summary['successful_events'] += 1
            elif event.result == 'failure':
                summary['failed_events'] += 1
        
        return summary


class AuditMiddleware:
    """Middleware for automatic audit logging."""
    
    def __init__(self, audit_logger: AuditLogger):
        self.audit_logger = audit_logger
    
    def log_authentication(self, 
                          user_id: str,
                          success: bool,
                          ip_address: str,
                          user_agent: str,
                          service_name: str,
                          error_message: Optional[str] = None) -> str:
        """Log authentication event."""
        event = AuditEvent(
            event_id="",
            event_type=AuditEventType.AUTHENTICATION,
            severity=AuditSeverity.HIGH if not success else AuditSeverity.MEDIUM,
            timestamp=datetime.now(timezone.utc),
            service_name=service_name,
            user_id=user_id,
            ip_address=ip_address,
            user_agent=user_agent,
            action="login" if success else "login_failed",
            result="success" if success else "failure",
            error_message=error_message
        )
        
        self.audit_logger.log_event_sync(event)
        return event.event_id
    
    def log_data_access(self,
                       user_id: str,
                       resource: str,
                       resource_id: str,
                       action: str,
                       service_name: str,
                       ip_address: str,
                       success: bool = True) -> str:
        """Log data access event."""
        event = AuditEvent(
            event_id="",
            event_type=AuditEventType.DATA_ACCESS,
            severity=AuditSeverity.MEDIUM,
            timestamp=datetime.now(timezone.utc),
            service_name=service_name,
            user_id=user_id,
            ip_address=ip_address,
            action=action,
            resource=resource,
            resource_id=resource_id,
            result="success" if success else "failure"
        )
        
        self.audit_logger.log_event_sync(event)
        return event.event_id
    
    def log_configuration_change(self,
                                user_id: str,
                                resource: str,
                                old_value: str,
                                new_value: str,
                                service_name: str,
                                ip_address: str) -> str:
        """Log configuration change event."""
        event = AuditEvent(
            event_id="",
            event_type=AuditEventType.CONFIGURATION_CHANGE,
            severity=AuditSeverity.HIGH,
            timestamp=datetime.now(timezone.utc),
            service_name=service_name,
            user_id=user_id,
            ip_address=ip_address,
            action="update",
            resource=resource,
            old_value=old_value,
            new_value=new_value,
            result="success"
        )
        
        self.audit_logger.log_event_sync(event)
        return event.event_id
    
    def log_security_event(self,
                          event_type: str,
                          severity: AuditSeverity,
                          service_name: str,
                          description: str,
                          ip_address: Optional[str] = None,
                          user_id: Optional[str] = None) -> str:
        """Log security event."""
        event = AuditEvent(
            event_id="",
            event_type=AuditEventType.SECURITY_EVENT,
            severity=severity,
            timestamp=datetime.now(timezone.utc),
            service_name=service_name,
            user_id=user_id,
            ip_address=ip_address,
            action=event_type,
            result="success",
            metadata={'description': description}
        )
        
        self.audit_logger.log_event_sync(event)
        return event.event_id


# Global audit logger instance
_audit_logger = None

def get_audit_logger() -> AuditLogger:
    """Get the global audit logger instance."""
    global _audit_logger
    if _audit_logger is None:
        log_file = os.getenv('DATA_PROC_AUDIT_LOG_FILE', '/var/log/audit/audit.log')
        enable_encryption = os.getenv('DATA_PROC_AUDIT_ENCRYPTION', 'true').lower() == 'true'
        enable_integrity = os.getenv('DATA_PROC_AUDIT_INTEGRITY', 'true').lower() == 'true'
        
        _audit_logger = AuditLogger(
            log_file=log_file,
            enable_encryption=enable_encryption,
            enable_integrity_check=enable_integrity
        )
    return _audit_logger

def get_audit_middleware() -> AuditMiddleware:
    """Get an audit middleware instance."""
    return AuditMiddleware(get_audit_logger())


# Example usage and testing
if __name__ == "__main__":
    import tempfile
    import shutil
    
    # Create temporary directory for testing
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Test audit logger
        log_file = os.path.join(temp_dir, "audit.log")
        audit_logger = AuditLogger(log_file=log_file)
        
        # Test audit middleware
        audit_middleware = AuditMiddleware(audit_logger)
        
        # Test authentication logging
        print("Testing authentication logging...")
        auth_event_id = audit_middleware.log_authentication(
            user_id="user123",
            success=True,
            ip_address="192.168.1.100",
            user_agent="Mozilla/5.0",
            service_name="normalization-service"
        )
        print(f"Authentication event logged: {auth_event_id}")
        
        # Test data access logging
        print("\nTesting data access logging...")
        access_event_id = audit_middleware.log_data_access(
            user_id="user123",
            resource="market_ticks",
            resource_id="AAPL_20240101_100000",
            action="read",
            service_name="enrichment-service",
            ip_address="192.168.1.100"
        )
        print(f"Data access event logged: {access_event_id}")
        
        # Test configuration change logging
        print("\nTesting configuration change logging...")
        config_event_id = audit_middleware.log_configuration_change(
            user_id="admin",
            resource="database_config",
            old_value="pool_size=10",
            new_value="pool_size=20",
            service_name="aggregation-service",
            ip_address="192.168.1.100"
        )
        print(f"Configuration change event logged: {config_event_id}")
        
        # Test security event logging
        print("\nTesting security event logging...")
        security_event_id = audit_middleware.log_security_event(
            event_type="suspicious_activity",
            severity=AuditSeverity.HIGH,
            service_name="projection-service",
            description="Multiple failed authentication attempts detected",
            ip_address="192.168.1.200"
        )
        print(f"Security event logged: {security_event_id}")
        
        # Test event search
        print("\nTesting event search...")
        events = audit_logger.search_events(limit=10)
        print(f"Found {len(events)} events")
        
        for event in events:
            print(f"  - {event.event_type.value}: {event.action} by {event.user_id}")
        
        # Test audit summary
        print("\nTesting audit summary...")
        summary = audit_logger.get_audit_summary()
        print(f"Total events: {summary['total_events']}")
        print(f"Events by type: {summary['events_by_type']}")
        print(f"Events by severity: {summary['events_by_severity']}")
        
        print("\nAudit logging test completed successfully!")
        
    finally:
        # Clean up temporary directory
        shutil.rmtree(temp_dir)
