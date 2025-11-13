# api/app/main.py
from fastapi import FastAPI
from typing import List

from .generator import generate_fake_orders
from .schemas import Order

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