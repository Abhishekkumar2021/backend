"""System Management API Endpoints.

Handles system initialization, master password management, and system status.
"""

from typing import Dict

from fastapi import APIRouter, HTTPException, status

from app.core.database import get_db_session
from app.core.exceptions import InvalidMasterPasswordError
from app.core.logging import get_logger
from app.models.database import SystemConfig
from app.schemas.system import InitSystemRequest, InitSystemResponse, SystemStatusResponse
from app.services.encryption import (
    MasterPasswordManager,
    initialize_encryption_service,
    get_encryption_service,
    lock_encryption_service,
    EncryptionServiceLockedError,
)

logger = get_logger(__name__)
router = APIRouter()


# ===================================================================
# Helper Functions
# ===================================================================

def is_system_initialized(db) -> bool:
    """Return True if encrypted DEK + salt exist."""
    dek_config = db.query(SystemConfig).filter(SystemConfig.key == "encrypted_dek").first()
    salt_config = db.query(SystemConfig).filter(SystemConfig.key == "dek_salt").first()
    return dek_config is not None and salt_config is not None


def get_system_credentials(db) -> Dict[str, str]:
    """Fetch encrypted DEK + salt from DB."""
    dek_config = db.query(SystemConfig).filter(SystemConfig.key == "encrypted_dek").first()
    salt_config = db.query(SystemConfig).filter(SystemConfig.key == "dek_salt").first()

    if not dek_config or not salt_config:
        logger.warning("system_credentials_missing")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="System not initialized. Please initialize first.",
        )

    return {
        "encrypted_dek": dek_config.value,
        "salt": salt_config.value,
    }


# ===================================================================
# System Management Endpoints
# ===================================================================

@router.post(
    "/init",
    response_model=InitSystemResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Initialize system",
)
def initialize_system(request: InitSystemRequest) -> InitSystemResponse:
    """Initialize master password + DEK. Can only run once."""
    logger.info("system_initialization_requested")

    with get_db_session() as db:
        # Already initialized?
        if is_system_initialized(db):
            logger.warning("system_already_initialized")
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="System already initialized. Use /unlock instead.",
            )

        try:
            # Generate new DEK + encrypted wrapper
            result = MasterPasswordManager.initialize_system(request.master_password)

            # Persist encrypted values
            db.add(SystemConfig(
                key="encrypted_dek",
                value=result["encrypted_dek"],
                description="Encrypted Data Encryption Key",
            ))
            db.add(SystemConfig(
                key="dek_salt",
                value=result["salt"],
                description="Salt for DEK encryption",
            ))

            logger.info("encryption_keys_stored")

            # Unlock global encryption service
            initialize_encryption_service(
                result["encrypted_dek"],
                request.master_password,
                result["salt"],
            )

            logger.info("system_initialized_successfully")

            return InitSystemResponse(
                message="System initialized successfully. Encryption unlocked.",
                status="success",
            )

        except Exception as e:
            logger.error("system_initialization_failed", error=str(e), exc_info=True)
            raise HTTPException(
                status_code=500,
                detail=f"Failed to initialize system: {str(e)}",
            )


@router.post(
    "/unlock",
    response_model=InitSystemResponse,
    summary="Unlock system",
)
def unlock_system(request: InitSystemRequest) -> InitSystemResponse:
    """Unlock encryption engine using master password."""
    logger.info("system_unlock_requested")

    with get_db_session() as db:
        credentials = get_system_credentials(db)

    try:
        initialize_encryption_service(
            credentials["encrypted_dek"],
            request.master_password,
            credentials["salt"],
        )

        logger.info("system_unlocked_successfully")

        return InitSystemResponse(
            message="System unlocked successfully.",
            status="success",
        )

    except InvalidMasterPasswordError:
        logger.warning("invalid_master_password_attempt")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid master password.",
        )
    except Exception as e:
        logger.error("system_unlock_failed", error=str(e), exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to unlock system: {str(e)}",
        )


@router.post(
    "/lock",
    response_model=InitSystemResponse,
    summary="Lock system",
)
def lock_system() -> InitSystemResponse:
    """Clear DEK from memory."""
    logger.info("system_lock_requested")

    try:
        lock_encryption_service()
        logger.info("system_locked_successfully")

        return InitSystemResponse(
            message="System locked successfully. Call /unlock to resume.",
            status="success",
        )

    except Exception as e:
        logger.error("system_lock_failed", error=str(e), exc_info=True)
        raise HTTPException(
            status_code=500,
            detail=f"Failed to lock system: {str(e)}",
        )


@router.get(
    "/status",
    response_model=SystemStatusResponse,
    summary="Get system status",
)
def get_system_status() -> SystemStatusResponse:
    """Return initialized/unlocked state."""
    logger.debug("system_status_requested")

    with get_db_session() as db:
        initialized = is_system_initialized(db)

    unlocked = False
    if initialized:
        try:
            unlocked = get_encryption_service().is_unlocked()
        except EncryptionServiceLockedError:
            unlocked = False

    if not initialized:
        message = "System not initialized. Call /init."
    elif not unlocked:
        message = "System initialized but locked. Call /unlock."
    else:
        message = "System initialized and unlocked."

    logger.debug("system_status_retrieved", initialized=initialized, unlocked=unlocked)

    return SystemStatusResponse(
        initialized=initialized,
        unlocked=unlocked,
        message=message,
    )


@router.post(
    "/verify-password",
    response_model=InitSystemResponse,
    summary="Verify master password",
)
def verify_master_password(request: InitSystemRequest) -> InitSystemResponse:
    """Verify master password WITHOUT unlocking."""
    logger.info("password_verification_requested")

    with get_db_session() as db:
        credentials = get_system_credentials(db)

    valid = MasterPasswordManager.verify_master_password(
        request.master_password,
        credentials["encrypted_dek"],
        credentials["salt"],
    )

    if valid:
        logger.info("password_verification_succeeded")
        return InitSystemResponse(
            message="Master password is valid.",
            status="success",
        )

    logger.warning("password_verification_failed")
    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Invalid master password.",
    )