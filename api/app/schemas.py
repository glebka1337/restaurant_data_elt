# api/app/schemas.py
from pydantic import BaseModel
from typing import List
from datetime import datetime
import uuid

class Item(BaseModel):
    product_id: int
    name: str       
    quantity: int
    price: float    

class Order(BaseModel):
    order_id: uuid.UUID 
    user_id: int        
    restaurant_id: int  
    status: str
    created_at: datetime
    items: List[Item]
    total_price: float