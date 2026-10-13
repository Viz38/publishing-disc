from pydantic import BaseModel, Field
from typing import Optional, Dict, Any

class RunRequest(BaseModel):
    start_row: int = 3
    mode: str = "full"
    sheet_id: Optional[str] = None

class ProgressState(BaseModel):
    current: int = 0
    total: int = 0
    success: int = 0
    fail: int = 0

class APIResponse(BaseModel):
    status: str
    message: Optional[str] = None
    data: Optional[Dict[str, Any]] = None
