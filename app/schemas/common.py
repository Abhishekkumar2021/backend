from typing import Generic, TypeVar

from pydantic import BaseModel

T = TypeVar("T")


class ResponseBase(BaseModel):
    success: bool = True
    message: str | None = None


class DataResponse(ResponseBase, Generic[T]):
    data: T
