"""Encryption Service - Master Password + DEK Architecture
Securely encrypts/decrypts connection credentials

Architecture:
1. User creates Master Password (stored only in memory/session)
2. System generates DEK (Data Encryption Key)
3. Master Password encrypts the DEK
4. DEK encrypts all connection credentials
5. Encrypted DEK stored in database
"""

import base64
import json
import os
from typing import Any

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC


class EncryptionError(Exception):
    """Base encryption error"""



class InvalidMasterPasswordError(EncryptionError):
    """Master password is incorrect"""



class EncryptionService:
    """Handles all encryption/decryption operations
    Uses Fernet (symmetric encryption) with key derivation
    """

    def __init__(self, master_password: str | None = None):
        """Initialize encryption service

        Args:
            master_password: User's master password (not stored anywhere)

        """
        self._master_password = master_password
        self._dek: bytes | None = None
        self._fernet: Fernet | None = None

    @staticmethod
    def generate_dek() -> bytes:
        """Generate a new Data Encryption Key (DEK)
        This is a random 32-byte key for Fernet

        Returns:
            Random DEK as bytes

        """
        return Fernet.generate_key()

    @staticmethod
    def derive_key_from_password(password: str, salt: bytes) -> bytes:
        """Derive encryption key from master password using PBKDF2

        Args:
            password: Master password
            salt: Salt for key derivation

        Returns:
            32-byte encryption key

        """
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,  # OWASP recommendation
        )
        return base64.urlsafe_b64encode(kdf.derive(password.encode()))

    def encrypt_dek(self, dek: bytes, master_password: str, salt: bytes) -> str:
        """Encrypt the DEK using master password

        Args:
            dek: Data Encryption Key to encrypt
            master_password: User's master password
            salt: Salt for key derivation

        Returns:
            Base64-encoded encrypted DEK

        """
        key = self.derive_key_from_password(master_password, salt)
        f = Fernet(key)
        encrypted_dek = f.encrypt(dek)
        return base64.b64encode(encrypted_dek).decode()

    def decrypt_dek(self, encrypted_dek: str, master_password: str, salt: bytes) -> bytes:
        """Decrypt the DEK using master password

        Args:
            encrypted_dek: Base64-encoded encrypted DEK
            master_password: User's master password
            salt: Salt for key derivation

        Returns:
            Decrypted DEK as bytes

        """
        key = self.derive_key_from_password(master_password, salt)
        f = Fernet(key)
        encrypted_dek_bytes = base64.b64decode(encrypted_dek.encode())
        return f.decrypt(encrypted_dek_bytes)

    def unlock(self, encrypted_dek: str, master_password: str, salt: bytes) -> None:
        """Unlock the encryption service by decrypting DEK with master password
        This must be called before encrypt/decrypt operations

        Args:
            encrypted_dek: Encrypted DEK from database
            master_password: User's master password
            salt: Salt for key derivation

        """
        self._dek = self.decrypt_dek(encrypted_dek, master_password, salt)
        self._fernet = Fernet(self._dek)
        self._master_password = master_password

    def is_unlocked(self) -> bool:
        """Check if encryption service is unlocked"""
        return self._fernet is not None

    def get_master_password(self) -> str | None:
        """Get the current master password"""
        return self._master_password

    def encrypt_config(self, config: dict[str, Any]) -> str:
        """Encrypt connection configuration

        Args:
            config: Dictionary with connection details (host, port, user, password, etc.)

        Returns:
            Base64-encoded encrypted config string

        Raises:
            RuntimeError: If service not unlocked

        """
        if not self.is_unlocked():
            raise RuntimeError("Encryption service not unlocked. Call unlock() first.")

        # Convert dict to JSON string
        config_json = json.dumps(config)

        # Encrypt
        encrypted = self._fernet.encrypt(config_json.encode())

        # Return as base64 string for database storage
        return base64.b64encode(encrypted).decode()

    def decrypt_config(self, encrypted_config: str) -> dict[str, Any]:
        """Decrypt connection configuration

        Args:
            encrypted_config: Base64-encoded encrypted config string

        Returns:
            Decrypted configuration dictionary

        Raises:
            RuntimeError: If service not unlocked

        """
        if not self.is_unlocked():
            raise RuntimeError("Encryption service not unlocked. Call unlock() first.")

        try:
            # Decode from base64
            encrypted_bytes = base64.b64decode(encrypted_config.encode())

            # Decrypt
            decrypted = self._fernet.decrypt(encrypted_bytes)

            # Parse JSON
            return json.loads(decrypted.decode())
        except Exception as e:
            raise EncryptionError(f"Failed to decrypt config: {e!s}")

    def lock(self) -> None:
        """Lock the encryption service (clear DEK from memory)
        Should be called when user logs out or session ends
        """
        self._dek = None
        self._fernet = None
        self._master_password = None


class MasterPasswordManager:
    """Manages master password setup and validation
    """

    @staticmethod
    def generate_salt() -> bytes:
        """Generate a random salt for key derivation"""
        return os.urandom(32)

    @staticmethod
    def initialize_system(master_password: str) -> dict[str, str]:
        """Initialize system with master password (first-time setup)

        Args:
            master_password: User's chosen master password

        Returns:
            Dictionary with encrypted_dek and salt (base64 encoded)

        """
        # Generate salt
        salt = MasterPasswordManager.generate_salt()

        # Generate DEK
        dek = EncryptionService.generate_dek()

        # Encrypt DEK with master password
        service = EncryptionService()
        encrypted_dek = service.encrypt_dek(dek, master_password, salt)

        return {"encrypted_dek": encrypted_dek, "salt": base64.b64encode(salt).decode()}

    @staticmethod
    def verify_master_password(master_password: str, encrypted_dek: str, salt_b64: str) -> bool:
        """Verify if master password is correct

        Args:
            master_password: Password to verify
            encrypted_dek: Stored encrypted DEK
            salt_b64: Base64-encoded salt

        Returns:
            True if password is correct, False otherwise

        """
        try:
            salt = base64.b64decode(salt_b64.encode())
            service = EncryptionService()
            service.decrypt_dek(encrypted_dek, master_password, salt)
            return True
        except Exception:
            return False


# Global encryption service instance
# Will be initialized when user unlocks with master password
_encryption_service: EncryptionService | None = None


def get_encryption_service() -> EncryptionService:
    """Get the global encryption service instance

    Returns:
        EncryptionService instance

    Raises:
        RuntimeError: If service not initialized

    """
    global _encryption_service
    if _encryption_service is None:
        raise RuntimeError("Encryption service not initialized")
    return _encryption_service


def initialize_encryption_service(encrypted_dek: str, master_password: str, salt: str) -> EncryptionService:
    """Initialize and unlock the global encryption service

    Args:
        encrypted_dek: Encrypted DEK from database
        master_password: User's master password
        salt: Base64-encoded salt

    Returns:
        Unlocked EncryptionService instance

    """
    global _encryption_service
    _encryption_service = EncryptionService()
    salt_bytes = base64.b64decode(salt.encode())
    _encryption_service.unlock(encrypted_dek, master_password, salt_bytes)
    return _encryption_service


def lock_encryption_service() -> None:
    """Lock and clear the global encryption service"""
    global _encryption_service
    if _encryption_service:
        _encryption_service.lock()
    _encryption_service = None
