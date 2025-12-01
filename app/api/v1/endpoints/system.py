"""System Management API Endpoints.

Handles system initialization, master password management, and system status.
"""

from typing import Dict, Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.core.exceptions import EncryptionError, InvalidMasterPasswordError
from app.core.logging import get_logger
from app.models.database import SystemConfig
from app.services.encryption import (
    MasterPasswordManager,
    initialize_encryption_service,
    get_encryption_service,
    lock_encryption_service,
)

logger = get_logger(__name__)
router = APIRouter()


# ===================================================================
# Request/Response Models
# ===================================================================

class InitSystemRequest(BaseModel):
    """Request model for system initialization."""
    
    master_password: str = Field(
        ...,
        min_length=8,
        description="Master password for encryption (minimum 8 characters)",
    )


class InitSystemResponse(BaseModel):
    """Response model for system operations."""
    
    message: str = Field(..., description="Operation result message")
    status: str = Field(..., description="Operation status (success/error)")


class SystemStatusResponse(BaseModel):
    """Response model for system status."""
    
    initialized: bool = Field(..., description="Whether system is initialized")
    unlocked: bool = Field(..., description="Whether encryption service is unlocked")
    message: str = Field(..., description="Status message")


# ===================================================================
# Helper Functions
# ===================================================================

def is_system_initialized(db: Session) -> bool:
    """Check if system is initialized.
    
    Args:
        db: Database session
        
    Returns:
        True if system is initialized
    """
    dek_config = db.query(SystemConfig).filter(
        SystemConfig.key == "encrypted_dek"
    ).first()
    
    salt_config = db.query(SystemConfig).filter(
        SystemConfig.key == "dek_salt"
    ).first()
    
    return dek_config is not None and salt_config is not None


def get_system_credentials(db: Session) -> Dict[str, str]:
    """Get encrypted DEK and salt from database.
    
    Args:
        db: Database session
        
    Returns:
        Dictionary with encrypted_dek and salt
        
    Raises:
        HTTPException: If system not initialized
    """
    dek_config = db.query(SystemConfig).filter(
        SystemConfig.key == "encrypted_dek"
    ).first()
    
    salt_config = db.query(SystemConfig).filter(
        SystemConfig.key == "dek_salt"
    ).first()

    if not dek_config or not salt_config:
        logger.warning("system_credentials_missing")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="System not initialized. Please initialize the system first.",
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
def initialize_system(
    request: InitSystemRequest,
    db: Session = Depends(get_db),
) -> InitSystemResponse:
    """Initialize the system with a master password.
    
    This endpoint:
    1. Generates a Data Encryption Key (DEK)
    2. Encrypts the DEK with the master password
    3. Stores the encrypted DEK and salt in the database
    4. Unlocks the encryption service
    
    Can only be called once. If system is already initialized,
    use the /unlock endpoint instead.
    
    Args:
        request: Initialization request with master password
        db: Database session
        
    Returns:
        Success response
        
    Raises:
        HTTPException: If system is already initialized
    """
    logger.info("system_initialization_requested")
    
    # Check if already initialized
    if is_system_initialized(db):
        logger.warning("system_already_initialized")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="System already initialized. Use /unlock endpoint to unlock the system.",
        )

    try:
        # Initialize master password and DEK
        logger.debug("generating_encryption_keys")
        result = MasterPasswordManager.initialize_system(request.master_password)

        # Store in database
        dek_config = SystemConfig(
            key="encrypted_dek",
            value=result["encrypted_dek"],
            description="Encrypted Data Encryption Key",
        )
        
        salt_config = SystemConfig(
            key="dek_salt",
            value=result["salt"],
            description="Salt for DEK encryption",
        )

        db.add(dek_config)
        db.add(salt_config)
        db.commit()
        
        logger.info("encryption_keys_stored")

        # Initialize the global encryption service
        initialize_encryption_service(
            result["encrypted_dek"],
            request.master_password,
            result["salt"],
        )
        
        logger.info("system_initialized_successfully")

        return InitSystemResponse(
            message="System initialized successfully. Encryption service is now unlocked.",
            status="success",
        )
        
    except Exception as e:
        db.rollback()
        logger.error(
            "system_initialization_failed",
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to initialize system: {str(e)}",
        )


@router.post(
    "/unlock",
    response_model=InitSystemResponse,
    summary="Unlock system",
)
def unlock_system(
    request: InitSystemRequest,
    db: Session = Depends(get_db),
) -> InitSystemResponse:
    """Unlock the system with master password.
    
    Loads the DEK into memory for the encryption service.
    Must be called after system initialization or after the
    encryption service has been locked.
    
    Args:
        request: Unlock request with master password
        db: Database session
        
    Returns:
        Success response
        
    Raises:
        HTTPException: If system not initialized or password invalid
    """
    logger.info("system_unlock_requested")
    
    # Get encrypted credentials from database
    credentials = get_system_credentials(db)

    try:
        # Attempt to unlock encryption service
        initialize_encryption_service(
            credentials["encrypted_dek"],
            request.master_password,
            credentials["salt"],
        )
        
        logger.info("system_unlocked_successfully")
        
        return InitSystemResponse(
            message="System unlocked successfully. Encryption service is now active.",
            status="success",
        )
        
    except InvalidMasterPasswordError:
        logger.warning("invalid_master_password_attempt")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid master password. Please try again.",
        )
    except Exception as e:
        logger.error(
            "system_unlock_failed",
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to unlock system: {str(e)}",
        )


@router.post(
    "/lock",
    response_model=InitSystemResponse,
    summary="Lock system",
)
def lock_system() -> InitSystemResponse:
    """Lock the encryption service.
    
    Clears the DEK from memory. After locking, you must call
    /unlock to use encryption features again.
    
    Returns:
        Success response
    """
    logger.info("system_lock_requested")
    
    try:
        lock_encryption_service()
        
        logger.info("system_locked_successfully")
        
        return InitSystemResponse(
            message="System locked successfully. Call /unlock to resume operations.",
            status="success",
        )
        
    except Exception as e:
        logger.error(
            "system_lock_failed",
            error=str(e),
            exc_info=True,
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to lock system: {str(e)}",
        )


@router.get(
    "/status",
    response_model=SystemStatusResponse,
    summary="Get system status",
)
def get_system_status(
    db: Session = Depends(get_db),
) -> SystemStatusResponse:
    """Get current system status.
    
    Returns information about:
    - Whether system is initialized
    - Whether encryption service is unlocked
    
    Args:
        db: Database session
        
    Returns:
        System status information
    """
    logger.debug("system_status_requested")
    
    initialized = is_system_initialized(db)
    
    # Check if encryption service is unlocked
    unlocked = False
    if initialized:
        try:
            encryption_service = get_encryption_service()
            unlocked = encryption_service.is_unlocked()
        except RuntimeError:
            # Service not initialized yet
            unlocked = False
    
    # Build status message
    if not initialized:
        message = "System not initialized. Please call /init endpoint."
    elif not unlocked:
        message = "System initialized but locked. Please call /unlock endpoint."
    else:
        message = "System is initialized and unlocked. Ready for operations."
    
    logger.debug(
        "system_status_retrieved",
        initialized=initialized,
        unlocked=unlocked,
    )
    
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
def verify_master_password(
    request: InitSystemRequest,
    db: Session = Depends(get_db),
) -> InitSystemResponse:
    """Verify if a master password is correct.
    
    This endpoint does NOT unlock the system, it only verifies
    the password. Use /unlock to actually unlock the system.
    
    Args:
        request: Request with master password to verify
        db: Database session
        
    Returns:
        Verification result
        
    Raises:
        HTTPException: If system not initialized
    """
    logger.info("password_verification_requested")
    
    # Get encrypted credentials
    credentials = get_system_credentials(db)
    
    # Verify password
    is_valid = MasterPasswordManager.verify_master_password(
        request.master_password,
        credentials["encrypted_dek"],
        credentials["salt"],
    )
    
    if is_valid:
        logger.info("password_verification_succeeded")
        return InitSystemResponse(
            message="Master password is valid.",
            status="success",
        )
    else:
        logger.warning("password_verification_failed")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid master password.",
        )