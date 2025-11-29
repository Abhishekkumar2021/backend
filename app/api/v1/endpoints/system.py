from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.api import deps
from app.core.config import settings
from app.models import db_models
from app.services.encryption import MasterPasswordManager, get_encryption_service, initialize_encryption_service

router = APIRouter()

class InitSystemRequest(BaseModel):
    master_password: str

class InitSystemResponse(BaseModel):
    message: str
    status: str

@router.post("/init", response_model=InitSystemResponse)
def initialize_system(
    request: InitSystemRequest,
    db: Session = Depends(deps.get_db)
):
    """
    Initialize the system with a master password.
    This generates the DEK, encrypts it, and stores it in the DB.
    """
    # Check if already initialized
    existing_config = db.query(db_models.SystemConfig).filter(db_models.SystemConfig.key == "encrypted_dek").first()
    if existing_config:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="System already initialized"
        )
    
    # Initialize master password and DEK
    result = MasterPasswordManager.initialize_system(request.master_password)
    
    # Store in DB
    dek_config = db_models.SystemConfig(
        key="encrypted_dek",
        value=result["encrypted_dek"],
        description="Encrypted Data Encryption Key"
    )
    salt_config = db_models.SystemConfig(
        key="dek_salt",
        value=result["salt"],
        description="Salt for DEK encryption"
    )
    
    db.add(dek_config)
    db.add(salt_config)
    db.commit()
    
    # Initialize the global encryption service
    initialize_encryption_service(
        result["encrypted_dek"],
        request.master_password,
        result["salt"]
    )
    
    return {"message": "System initialized successfully", "status": "success"}

@router.post("/unlock", response_model=InitSystemResponse)
def unlock_system(
    request: InitSystemRequest,
    db: Session = Depends(deps.get_db)
):
    """
    Unlock the system with master password.
    Loads the DEK into memory for the encryption service.
    """
    # Get encrypted DEK and salt from DB
    dek_config = db.query(db_models.SystemConfig).filter(db_models.SystemConfig.key == "encrypted_dek").first()
    salt_config = db.query(db_models.SystemConfig).filter(db_models.SystemConfig.key == "dek_salt").first()
    
    if not dek_config or not salt_config:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="System not initialized"
        )
    
    try:
        initialize_encryption_service(
            dek_config.value,
            request.master_password,
            salt_config.value
        )
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid master password"
        )
        
    return {"message": "System unlocked successfully", "status": "success"}
