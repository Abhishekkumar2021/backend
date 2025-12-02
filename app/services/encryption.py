"""
Encryption Service — DEK + PBKDF2 + Fernet (Improved)
-----------------------------------------------------

Enhancements:
✓ Safer memory handling
✓ Stronger logging
✓ Unified Base64/JSON helpers
✓ Cleaner unlock flow
✓ Defensive validation
✓ Explicit error classes
✓ Centralized encode/decode helpers
"""

import base64
import json
import os
from typing import Any, Optional

from cryptography.fernet import Fernet, InvalidToken
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from app.core.logging import get_logger

logger = get_logger(__name__)


# ===================================================================
# Exceptions
# ===================================================================
class EncryptionError(Exception):
    """Base encryption error."""


class InvalidMasterPasswordError(EncryptionError):
    """Raised when the master password is incorrect or DEK decryption fails."""


class EncryptionServiceLockedError(EncryptionError):
    """Raised when encryption service is used before unlocking."""


# ===================================================================
# Helpers: JSON & Base64
# ===================================================================
def _b64encode_bytes(data: bytes) -> str:
    return base64.b64encode(data).decode("utf-8")


def _b64decode_to_bytes(encoded: str) -> bytes:
    try:
        return base64.b64decode(encoded.encode("utf-8"))
    except Exception as e:
        raise EncryptionError(f"Invalid Base64 string: {encoded}") from e


# ===================================================================
# Encryption Service (Config Encryption/Decryption)
# ===================================================================
class EncryptionService:
    """
    Handles:
    ✓ Unlocking (decrypting DEK)
    ✓ Encrypting connection configs
    ✓ Decrypting connection configs

    Requirements:
    - Master password is NOT stored permanently
    - DEK remains only in-memory after unlock
    """

    def __init__(self, master_password: Optional[str] = None):
        self._master_password = master_password
        self._dek: Optional[bytes] = None
        self._fernet: Optional[Fernet] = None

        logger.debug(
            "encryption_service_initialized",
            master_password_provided=bool(master_password),
        )

    # -------------------------------------------------------------------
    # Static — DEK generation + PBKDF2 key derivation
    # -------------------------------------------------------------------
    @staticmethod
    def generate_dek() -> bytes:
        logger.debug("dek_generate_requested")
        return Fernet.generate_key()

    @staticmethod
    def derive_key_from_password(password: str, salt: bytes) -> bytes:
        logger.debug("derive_key_from_master_password")

        if not password:
            raise InvalidMasterPasswordError("Master password cannot be empty")

        try:
            kdf = PBKDF2HMAC(
                algorithm=hashes.SHA256(),
                length=32,
                salt=salt,
                iterations=100_000,
            )
            return base64.urlsafe_b64encode(kdf.derive(password.encode("utf-8")))
        except Exception as e:
            logger.error("password_key_derivation_failed", error=str(e))
            raise EncryptionError("Key derivation failed") from e

    # -------------------------------------------------------------------
    # Encrypt/Decrypt DEK
    # -------------------------------------------------------------------
    def encrypt_dek(self, dek: bytes, master_password: str, salt: bytes) -> str:
        """
        Encrypt DEK using a master password.
        """
        logger.info("dek_encrypt_requested")

        key = self.derive_key_from_password(master_password, salt)
        f = Fernet(key)
        encrypted_dek = f.encrypt(dek)

        logger.info("dek_encrypt_completed")
        return _b64encode_bytes(encrypted_dek)

    def decrypt_dek(self, encrypted_dek: str, master_password: str, salt: bytes) -> bytes:
        """
        Decrypt DEK using master password. Raises InvalidMasterPasswordError on failure.
        """
        logger.info("dek_decrypt_requested")

        try:
            key = self.derive_key_from_password(master_password, salt)
            f = Fernet(key)

            encrypted_bytes = _b64decode_to_bytes(encrypted_dek)
            dek = f.decrypt(encrypted_bytes)

            if not dek or len(dek) < 32:
                raise InvalidMasterPasswordError("DEK is invalid or corrupted")

            logger.info("dek_decrypt_completed")
            return dek

        except InvalidToken:
            logger.warning("dek_decrypt_invalid_password")
            raise InvalidMasterPasswordError("Invalid master password")

        except Exception as e:
            logger.error("dek_decrypt_unexpected_failure", error=str(e), exc_info=True)
            raise EncryptionError("Unexpected error while decrypting DEK") from e

    # -------------------------------------------------------------------
    # Unlock / Lock service
    # -------------------------------------------------------------------
    def unlock(self, encrypted_dek: str, master_password: str, salt: bytes) -> None:
        """Unlock the encryption service by decrypting the DEK."""
        logger.info("encryption_unlock_requested")

        # DEK decrypt
        dek = self.decrypt_dek(encrypted_dek, master_password, salt)

        # Setup Fernet
        try:
            self._fernet = Fernet(dek)
            self._dek = dek
            self._master_password = master_password
        except Exception as e:
            logger.error("encryption_unlock_failed", error=str(e))
            raise EncryptionError("Unable to initialize encryption engine")

        logger.info("encryption_service_unlocked")

    def lock(self) -> None:
        """Clear sensitive data from memory."""
        logger.info("encryption_lock_requested")

        # Best-effort scrubbing
        self._dek = None
        self._master_password = None
        self._fernet = None

        logger.info("encryption_service_locked")

    def is_unlocked(self) -> bool:
        return self._fernet is not None

    # -------------------------------------------------------------------
    # Config Encryption/Decryption
    # -------------------------------------------------------------------
    def _require_unlocked(self):
        if not self.is_unlocked():
            logger.error("encryption_service_locked_error")
            raise EncryptionServiceLockedError("Encryption service not unlocked")

    def encrypt_config(self, config: dict[str, Any]) -> str:
        self._require_unlocked()

        logger.debug("encrypt_config_requested")
        try:
            blob = json.dumps(config).encode("utf-8")
            encrypted = self._fernet.encrypt(blob)
            return _b64encode_bytes(encrypted)
        except Exception as e:
            logger.error("encrypt_config_failed", error=str(e), exc_info=True)
            raise EncryptionError("Failed to encrypt config") from e

    def decrypt_config(self, encrypted_config: str) -> dict[str, Any]:
        self._require_unlocked()

        logger.debug("decrypt_config_requested")
        try:
            encrypted_bytes = _b64decode_to_bytes(encrypted_config)
            decrypted = self._fernet.decrypt(encrypted_bytes)
            return json.loads(decrypted.decode("utf-8"))
        except InvalidToken:
            raise EncryptionError("Encrypted config is corrupted or invalid")
        except Exception as e:
            logger.error("decrypt_config_failed", error=str(e), exc_info=True)
            raise EncryptionError("Failed to decrypt config") from e


# ===================================================================
# Master Password Manager (Bootstrapping)
# ===================================================================
class MasterPasswordManager:
    """Handles first-time initialization + validation."""

    @staticmethod
    def generate_salt() -> bytes:
        return os.urandom(32)

    @staticmethod
    def initialize_system(master_password: str) -> dict[str, str]:
        """
        Creates:
        ✓ salt (Base64)
        ✓ encrypted DEK (Base64)
        """
        logger.info("encryption_system_initialization_requested")

        salt = MasterPasswordManager.generate_salt()
        dek = EncryptionService.generate_dek()

        service = EncryptionService()
        encrypted_dek = service.encrypt_dek(dek, master_password, salt)

        logger.info("encryption_system_initialized")

        return {
            "encrypted_dek": encrypted_dek,
            "salt": _b64encode_bytes(salt),
        }

    @staticmethod
    def verify_master_password(master_password: str, encrypted_dek: str, salt_b64: str) -> bool:
        """
        Check if master password is correct *without* unlocking service globally.
        """
        logger.info("master_password_verification_requested")

        try:
            salt = _b64decode_to_bytes(salt_b64)
            service = EncryptionService()
            service.decrypt_dek(encrypted_dek, master_password, salt)

            logger.info("master_password_verification_success")
            return True

        except InvalidMasterPasswordError:
            logger.warning("master_password_verification_failed_invalid")
            return False

        except Exception:
            logger.warning("master_password_verification_failed_unexpected")
            return False


# ===================================================================
# Global Encryption Service (Session-scoped)
# ===================================================================
_encryption_service: Optional[EncryptionService] = None


def get_encryption_service() -> EncryptionService:
    if _encryption_service is None:
        logger.error("encryption_service_access_before_initialization")
        raise EncryptionServiceLockedError("Encryption service not initialized")
    return _encryption_service


def initialize_encryption_service(encrypted_dek: str, master_password: str, salt_b64: str) -> EncryptionService:
    """
    Initializes the global encryption service (used by Celery worker).
    """
    logger.info("initialize_encryption_service_requested")

    global _encryption_service
    _encryption_service = EncryptionService()

    salt = _b64decode_to_bytes(salt_b64)
    _encryption_service.unlock(encrypted_dek, master_password, salt)

    logger.info("initialize_encryption_service_completed")
    return _encryption_service


def lock_encryption_service():
    """Fully lock and destroy global encryption service."""
    logger.info("lock_encryption_service_requested")

    global _encryption_service
    if _encryption_service:
        _encryption_service.lock()

    _encryption_service = None
    logger.info("lock_encryption_service_completed")
