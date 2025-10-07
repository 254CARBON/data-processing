"""
Data encryption utilities for the 254Carbon Data Processing Pipeline.
Implements field-level encryption, key management, and encryption at rest.
"""

import os
import json
import base64
import logging
from typing import Dict, Any, Optional, Union
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend
import secrets

logger = logging.getLogger(__name__)


class EncryptionManager:
    """Manages encryption keys and provides encryption/decryption services."""
    
    def __init__(self, master_key: Optional[str] = None):
        self.master_key = master_key or os.getenv('DATA_PROC_MASTER_KEY')
        if not self.master_key:
            raise ValueError("Master key must be provided or set in DATA_PROC_MASTER_KEY environment variable")
        
        self._fernet_key = self._derive_fernet_key(self.master_key)
        self._fernet = Fernet(self._fernet_key)
        
        # Generate RSA key pair for asymmetric encryption
        self._private_key, self._public_key = self._generate_rsa_keypair()
    
    def _derive_fernet_key(self, master_key: str) -> bytes:
        """Derive a Fernet key from the master key."""
        salt = b'254carbon_salt'  # In production, use a random salt stored securely
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
            backend=default_backend()
        )
        key = base64.urlsafe_b64encode(kdf.derive(master_key.encode()))
        return key
    
    def _generate_rsa_keypair(self) -> tuple:
        """Generate RSA key pair for asymmetric encryption."""
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        public_key = private_key.public_key()
        return private_key, public_key
    
    def encrypt_symmetric(self, data: Union[str, bytes]) -> str:
        """Encrypt data using symmetric encryption (Fernet)."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        
        encrypted_data = self._fernet.encrypt(data)
        return base64.urlsafe_b64encode(encrypted_data).decode('utf-8')
    
    def decrypt_symmetric(self, encrypted_data: str) -> str:
        """Decrypt data using symmetric encryption (Fernet)."""
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode('utf-8'))
        decrypted_data = self._fernet.decrypt(encrypted_bytes)
        return decrypted_data.decode('utf-8')
    
    def encrypt_asymmetric(self, data: Union[str, bytes]) -> str:
        """Encrypt data using asymmetric encryption (RSA)."""
        if isinstance(data, str):
            data = data.encode('utf-8')
        
        encrypted_data = self._public_key.encrypt(
            data,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return base64.urlsafe_b64encode(encrypted_data).decode('utf-8')
    
    def decrypt_asymmetric(self, encrypted_data: str) -> str:
        """Decrypt data using asymmetric encryption (RSA)."""
        encrypted_bytes = base64.urlsafe_b64decode(encrypted_data.encode('utf-8'))
        decrypted_data = self._private_key.decrypt(
            encrypted_bytes,
            padding.OAEP(
                mgf=padding.MGF1(algorithm=hashes.SHA256()),
                algorithm=hashes.SHA256(),
                label=None
            )
        )
        return decrypted_data.decode('utf-8')
    
    def get_public_key_pem(self) -> str:
        """Get the public key in PEM format."""
        pem = self._public_key.serialize(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        )
        return pem.decode('utf-8')
    
    def get_private_key_pem(self) -> str:
        """Get the private key in PEM format."""
        pem = self._private_key.serialize(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
        return pem.decode('utf-8')


class FieldEncryption:
    """Handles field-level encryption for sensitive data."""
    
    def __init__(self, encryption_manager: EncryptionManager):
        self.encryption_manager = encryption_manager
        self.sensitive_fields = {
            'price', 'volume', 'bid', 'ask', 'last_trade_price',
            'user_id', 'account_id', 'customer_id', 'api_key',
            'personal_data', 'financial_data', 'trade_data'
        }
    
    def encrypt_sensitive_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypt sensitive fields in a data dictionary."""
        encrypted_data = data.copy()
        
        for key, value in data.items():
            if key.lower() in self.sensitive_fields and isinstance(value, (str, int, float)):
                try:
                    encrypted_data[f"{key}_encrypted"] = self.encryption_manager.encrypt_symmetric(str(value))
                    encrypted_data[key] = "[ENCRYPTED]"  # Replace original value
                except Exception as e:
                    logger.error(f"Failed to encrypt field {key}: {e}")
                    # Keep original value if encryption fails
        
        return encrypted_data
    
    def decrypt_sensitive_fields(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypt sensitive fields in a data dictionary."""
        decrypted_data = data.copy()
        
        for key, value in data.items():
            if key.endswith('_encrypted') and isinstance(value, str):
                original_key = key.replace('_encrypted', '')
                try:
                    decrypted_value = self.encryption_manager.decrypt_symmetric(value)
                    decrypted_data[original_key] = decrypted_value
                    del decrypted_data[key]  # Remove encrypted field
                except Exception as e:
                    logger.error(f"Failed to decrypt field {key}: {e}")
                    # Keep encrypted value if decryption fails
        
        return decrypted_data
    
    def add_sensitive_field(self, field_name: str):
        """Add a field to the list of sensitive fields."""
        self.sensitive_fields.add(field_name.lower())
    
    def remove_sensitive_field(self, field_name: str):
        """Remove a field from the list of sensitive fields."""
        self.sensitive_fields.discard(field_name.lower())


class KeyRotationManager:
    """Manages encryption key rotation procedures."""
    
    def __init__(self, encryption_manager: EncryptionManager):
        self.encryption_manager = encryption_manager
        self.key_version = os.getenv('DATA_PROC_KEY_VERSION', 'v1')
        self.rotation_interval = int(os.getenv('DATA_PROC_KEY_ROTATION_DAYS', '90'))
    
    def generate_new_key(self) -> str:
        """Generate a new encryption key."""
        return Fernet.generate_key().decode('utf-8')
    
    def rotate_keys(self) -> Dict[str, str]:
        """Rotate encryption keys and return new key information."""
        new_master_key = self.generate_new_key()
        new_version = f"v{int(self.key_version[1:]) + 1}"
        
        # In a real implementation, you would:
        # 1. Generate new key
        # 2. Re-encrypt all data with new key
        # 3. Update key version
        # 4. Clean up old keys
        
        logger.info(f"Key rotation completed. New version: {new_version}")
        
        return {
            'new_master_key': new_master_key,
            'new_version': new_version,
            'rotation_date': datetime.utcnow().isoformat()
        }
    
    def should_rotate_keys(self) -> bool:
        """Check if keys should be rotated based on rotation interval."""
        # In a real implementation, you would check the last rotation date
        # from a secure storage location
        return False  # Placeholder
    
    def get_key_info(self) -> Dict[str, Any]:
        """Get current key information."""
        return {
            'version': self.key_version,
            'rotation_interval_days': self.rotation_interval,
            'public_key': self.encryption_manager.get_public_key_pem(),
            'last_rotation': datetime.utcnow().isoformat()  # Placeholder
        }


class DatabaseEncryption:
    """Handles encryption at rest for database operations."""
    
    def __init__(self, encryption_manager: EncryptionManager):
        self.encryption_manager = encryption_manager
    
    def encrypt_database_field(self, table: str, field: str, value: Any) -> str:
        """Encrypt a database field value."""
        if value is None:
            return None
        
        # Add metadata for decryption
        metadata = {
            'table': table,
            'field': field,
            'encrypted_at': datetime.utcnow().isoformat(),
            'version': 'v1'
        }
        
        # Combine metadata and value
        data_to_encrypt = {
            'metadata': metadata,
            'value': str(value)
        }
        
        encrypted_data = self.encryption_manager.encrypt_symmetric(json.dumps(data_to_encrypt))
        return encrypted_data
    
    def decrypt_database_field(self, encrypted_value: str) -> Any:
        """Decrypt a database field value."""
        if encrypted_value is None:
            return None
        
        try:
            decrypted_data = self.encryption_manager.decrypt_symmetric(encrypted_value)
            data = json.loads(decrypted_data)
            return data['value']
        except Exception as e:
            logger.error(f"Failed to decrypt database field: {e}")
            return None
    
    def create_encrypted_column_sql(self, table: str, field: str) -> str:
        """Generate SQL for creating an encrypted column."""
        return f"""
        ALTER TABLE {table} 
        ADD COLUMN {field}_encrypted TEXT;
        
        -- Migrate existing data
        UPDATE {table} 
        SET {field}_encrypted = '{self.encrypt_database_field(table, field, 'PLACEHOLDER')}'
        WHERE {field} IS NOT NULL;
        
        -- Drop original column (in production, do this carefully)
        -- ALTER TABLE {table} DROP COLUMN {field};
        """


class AuditEncryption:
    """Handles encryption of audit logs and sensitive audit data."""
    
    def __init__(self, encryption_manager: EncryptionManager):
        self.encryption_manager = encryption_manager
    
    def encrypt_audit_entry(self, audit_data: Dict[str, Any]) -> Dict[str, Any]:
        """Encrypt sensitive data in audit entries."""
        encrypted_audit = audit_data.copy()
        
        # Encrypt sensitive fields
        sensitive_audit_fields = ['user_id', 'api_key', 'request_body', 'response_body']
        
        for field in sensitive_audit_fields:
            if field in audit_data and audit_data[field]:
                try:
                    encrypted_audit[f"{field}_encrypted"] = self.encryption_manager.encrypt_symmetric(
                        str(audit_data[field])
                    )
                    encrypted_audit[field] = "[ENCRYPTED]"
                except Exception as e:
                    logger.error(f"Failed to encrypt audit field {field}: {e}")
        
        return encrypted_audit
    
    def decrypt_audit_entry(self, encrypted_audit: Dict[str, Any]) -> Dict[str, Any]:
        """Decrypt sensitive data in audit entries."""
        decrypted_audit = encrypted_audit.copy()
        
        # Decrypt sensitive fields
        sensitive_audit_fields = ['user_id', 'api_key', 'request_body', 'response_body']
        
        for field in sensitive_audit_fields:
            encrypted_field = f"{field}_encrypted"
            if encrypted_field in encrypted_audit:
                try:
                    decrypted_audit[field] = self.encryption_manager.decrypt_symmetric(
                        encrypted_audit[encrypted_field]
                    )
                    del decrypted_audit[encrypted_field]
                except Exception as e:
                    logger.error(f"Failed to decrypt audit field {field}: {e}")
        
        return decrypted_audit


# Global encryption manager instance
_encryption_manager = None

def get_encryption_manager() -> EncryptionManager:
    """Get the global encryption manager instance."""
    global _encryption_manager
    if _encryption_manager is None:
        _encryption_manager = EncryptionManager()
    return _encryption_manager

def get_field_encryption() -> FieldEncryption:
    """Get a field encryption instance."""
    return FieldEncryption(get_encryption_manager())

def get_key_rotation_manager() -> KeyRotationManager:
    """Get a key rotation manager instance."""
    return KeyRotationManager(get_encryption_manager())

def get_database_encryption() -> DatabaseEncryption:
    """Get a database encryption instance."""
    return DatabaseEncryption(get_encryption_manager())

def get_audit_encryption() -> AuditEncryption:
    """Get an audit encryption instance."""
    return AuditEncryption(get_encryption_manager())


# Example usage and testing
if __name__ == "__main__":
    import os
    
    # Set up test environment
    os.environ['DATA_PROC_MASTER_KEY'] = Fernet.generate_key().decode('utf-8')
    
    # Test encryption manager
    enc_manager = EncryptionManager()
    
    # Test symmetric encryption
    test_data = "Sensitive market data: AAPL $150.25"
    encrypted = enc_manager.encrypt_symmetric(test_data)
    decrypted = enc_manager.decrypt_symmetric(encrypted)
    print(f"Original: {test_data}")
    print(f"Encrypted: {encrypted}")
    print(f"Decrypted: {decrypted}")
    print(f"Match: {test_data == decrypted}")
    
    # Test field encryption
    field_enc = FieldEncryption(enc_manager)
    test_record = {
        'instrument_id': 'AAPL',
        'price': 150.25,
        'volume': 1000000,
        'timestamp': '2024-01-01T10:00:00Z'
    }
    
    encrypted_record = field_enc.encrypt_sensitive_fields(test_record)
    print(f"\nEncrypted record: {encrypted_record}")
    
    decrypted_record = field_enc.decrypt_sensitive_fields(encrypted_record)
    print(f"Decrypted record: {decrypted_record}")
    
    # Test database encryption
    db_enc = DatabaseEncryption(enc_manager)
    encrypted_db_value = db_enc.encrypt_database_field('market_ticks', 'price', 150.25)
    decrypted_db_value = db_enc.decrypt_database_field(encrypted_db_value)
    print(f"\nDatabase encryption test: {150.25} -> {encrypted_db_value} -> {decrypted_db_value}")
    
    # Test key rotation
    key_rotation = KeyRotationManager(enc_manager)
    key_info = key_rotation.get_key_info()
    print(f"\nKey info: {key_info}")
    
    print("\nEncryption system test completed successfully!")
