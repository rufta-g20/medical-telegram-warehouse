from pydantic import BaseModel
from typing import List, Optional

class ProductRanking(BaseModel):
    product_name: str
    mention_count: int

class ChannelActivity(BaseModel):
    channel_name: str
    total_messages: int
    avg_views: float

class MessageSearch(BaseModel):
    message_id: int
    channel_name: str
    message_text: str
    view_count: Optional[int]

class VisualStats(BaseModel):
    image_category: str
    total_count: int
    avg_confidence: float