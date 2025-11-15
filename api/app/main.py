# api/app/main.py
import uuid
from fastapi import FastAPI
from typing import Any, Dict, List
import random

from .fake_db import USERS_DB as user_fake_db
from .generator import generate_fake_orders, fake
from .schemas import Order, User

app = FastAPI(
    title="Restaurant Source API",
    description="Simulates new, CONSISTENT orders for the KFR pipeline."
)

@app.get("/")
def read_root():
    """ Simple health check """
    return {"status": "API is running. Check /docs"}

# Creates a route that returns a batch of fake but consistent orders
@app.get("/api/v1/new_orders", response_model=List[Order])
def get_new_orders(batch_size: int = 100):
    """
    Generates and returns a batch of N fake but consistent orders.
    Airflow will call this endpoint.
    """
    orders = generate_fake_orders(batch_size=batch_size)
    return orders

@app.get("/api/v1/new_users")
def get_new_users() -> List[User]:
    num = random.randint(1, 6)
    
    return [
        User(
            user_id=uuid.uuid4(),
            email=fake.unique.email(),
            username=fake.user_name()
        )
        for i in range(num)
    ]   
    
@app.get("/api/v1/users/snapshot", response_model=List[User])
def get_all_users_snapshot() -> List[User]:
    return [
        User(
            user_id=user["user_id"],
            email=user["email"],
            username=user["username"]
        )
        for user in user_fake_db
    ]