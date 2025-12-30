from pydantic import BaseModel, Field
from datetime import datetime

class Event(BaseModel):
    schema_version: int = 1
    event_id: str
    event_time: datetime
    user_id: str
    event_type: str
    payload: dict = Field(default_factory=dict)
