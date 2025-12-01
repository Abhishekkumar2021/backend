"""Encryption Service - Master Password + DEK Architecture
Securely encrypts/decrypts connection credentials.
"""

import base64
import json
import os
from typing import Any

from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from app.core.logging import get_logger

logger = get_logger(__name__)


class EncryptionError(Exception):
    """Base encryption error"""


class InvalidMasterPasswordError(EncryptionError):
    """Master password is incorrect"""


class EncryptionService:
    """Handles all encryption/decryption operations using PBKDF2 + Fernet."""

    def __init__(self, master_password: str | None = None):
        self._master_password = master_password
        self._dek: bytes | None = None
        self._fernet: Fernet | None = None

        logger.debug(
            "encryption_service_initialized",
            master_password_provided=bool(master_password),
        )

    # ===================================================================
    # DEK Generation + Key Derivation
    # ===================================================================
    @staticmethod
    def generate_dek() -> bytes:
        """Generate a new random DEK."""
        logger.debug("dek_generation_requested")
        return Fernet.generate_key()

    @staticmethod
    def derive_key_from_password(password: str, salt: bytes) -> bytes:
        """Derive encryption key from master password using PBKDF2."""
        logger.debug("master_password_key_derivation_requested")

        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        return base64.urlsafe_b64encode(kdf.derive(password.encode()))

    # ===================================================================
    # DEK Encryption / Decryption
    # ===================================================================
    def encrypt_dek(self, dek: bytes, master_password: str, salt: bytes) -> str:
        """Encrypt DEK with master password."""
        logger.info("dek_encryption_requested")

        key = self.derive_key_from_password(master_password, salt)
        f = Fernet(key)
        encrypted_dek = f.encrypt(dek)

        logger.info("dek_encryption_completed")
        return base64.b64encode(encrypted_dek).decode()

    def decrypt_dek(self, encrypted_dek: str, master_password: str, salt: bytes) -> bytes:
        """Decrypt DEK with master password."""
        logger.info("dek_decryption_requested")

        try:
            key = self.derive_key_from_password(master_password, salt)
            f = Fernet(key)
            encrypted_bytes = base64.b64decode(encrypted_dek.encode())
            dek = f.decrypt(encrypted_bytes)

            logger.info("dek_decryption_completed")
            return dek

        except Exception as e:
            logger.error(
                "dek_decryption_failed",
                error=str(e),
                exc_info=True,
            )
            raise InvalidMasterPasswordError("Invalid master password")

    # ===================================================================
    # Unlock / Lock Service
    # ===================================================================
    def unlock(self, encrypted_dek: str, master_password: str, salt: bytes) -> None:
        """Unlock service by decrypting DEK."""
        logger.info("encryption_service_unlock_requested")

        try:
            self._dek = self.decrypt_dek(encrypted_dek, master_password, salt)
            self._fernet = Fernet(self._dek)
            self._master_password = master_password

            logger.info("encryption_service_unlocked")

        except InvalidMasterPasswordError:
            logger.warning(
                "encryption_service_unlock_failed_invalid_password",
            )
            raise

        except Exception as e:
            logger.error(
                "encryption_service_unlock_failed",
                error=str(e),
                exc_info=True,
            )
            raise EncryptionError("Unable to unlock encryption service")

    def is_unlocked(self) -> bool:
        return self._fernet is not None

    def get_master_password(self) -> str | None:
        return self._master_password

    # ===================================================================
    # Encrypt / Decrypt Config
    # ===================================================================
    def encrypt_config(self, config: dict[str, Any]) -> str:
        """Encrypt connection config (JSON)."""
        if not self.is_unlocked():
            logger.error("encrypt_config_failed_service_locked")
            raise RuntimeError("Encryption service not unlocked. Call unlock() first.")

        logger.debug("encrypt_config_requested")

        config_json = json.dumps(config)
        encrypted = self._fernet.encrypt(config_json.encode())

        logger.info("encrypt_config_completed")
        return base64.b64encode(encrypted).decode()

    def decrypt_config(self, encrypted_config: str) -> dict[str, Any]:
        """Decrypt connection config."""
        if not self.is_unlocked():
            logger.error("decrypt_config_failed_service_locked")
            raise RuntimeError("Encryption service not unlocked. Call unlock() first.")

        logger.debug("decrypt_config_requested")

        try:
            encrypted_bytes = base64.b64decode(encrypted_config.encode())
            decrypted = self._fernet.decrypt(encrypted_bytes)

            logger.info("decrypt_config_completed")
            return json.loads(decrypted.decode())

        except Exception as e:
            logger.error(
                "decrypt_config_failed",
                error=str(e),
                exc_info=True,
            )
            raise EncryptionError(f"Failed to decrypt config: {e!s}")

    # ===================================================================
    # Lock Service
    # ===================================================================
    def lock(self) -> None:
        """Clear DEK + password from memory."""
        logger.info("encryption_service_lock_requested")

        self._dek = None
        self._fernet = None
        self._master_password = None

        logger.info("encryption_service_locked")


# ===================================================================
# Master Password Manager
# ===================================================================
class MasterPasswordManager:
    """Manages master password creation and validation."""

    @staticmethod
    def generate_salt() -> bytes:
        logger.debug("salt_generation_requested")
        return os.urandom(32)

    @staticmethod
    def initialize_system(master_password: str) -> dict[str, str]:
        logger.info("encryption_system_initialization_requested")

        salt = MasterPasswordManager.generate_salt()
        dek = EncryptionService.generate_dek()

        service = EncryptionService()
        encrypted_dek = service.encrypt_dek(dek, master_password, salt)

        logger.info("encryption_system_initialized")

        return {
            "encrypted_dek": encrypted_dek,
            "salt": base64.b64encode(salt).decode(),
        }

    @staticmethod
    def verify_master_password(master_password: str, encrypted_dek: str, salt_b64: str) -> bool:
        logger.info("master_password_verification_requested")

        try:
            salt = base64.b64decode(salt_b64.encode())
            service = EncryptionService()
            service.decrypt_dek(encrypted_dek, master_password, salt)

            logger.info("master_password_verification_success")
            return True

        except Exception:
            logger.warning("master_password_verification_failed")
            return False


# ===================================================================
# Global Encryption Service Helpers
# ===================================================================
_encryption_service: EncryptionService | None = None


def get_encryption_service() -> EncryptionService:
    global _encryption_service
    if _encryption_service is None:
        logger.error("get_encryption_service_failed_not_initialized")
        raise RuntimeError("Encryption service not initialized")
    return _encryption_service


def initialize_encryption_service(encrypted_dek: str, master_password: str, salt: str) -> EncryptionService:
    logger.info("initialize_encryption_service_requested")

    global _encryption_service
    _encryption_service = EncryptionService()

    salt_bytes = base64.b64decode(salt.encode())
    _encryption_service.unlock(encrypted_dek, master_password, salt_bytes)

    logger.info("initialize_encryption_service_completed")
    return _encryption_service


def lock_encryption_service() -> None:
    logger.info("lock_encryption_service_requested")

    global _encryption_service
    if _encryption_service:
        _encryption_service.lock()

    _encryption_service = None

    logger.info("lock_encryption_service_completed")
