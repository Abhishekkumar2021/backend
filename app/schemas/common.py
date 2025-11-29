from typing import Generic, TypeVar, List, Optional
from pydantic import BaseModel

T = TypeVar('T')

class ResponseBase(BaseModel):
    success: bool = True
    message: Optional[str] = None

class DataResponse(ResponseBase, Generic[T]):
    data: T
