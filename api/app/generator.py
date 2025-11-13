# api/app/generator.py
from faker import Faker
from random import choice
from datetime import datetime
import uuid
from typing import List
from .schemas import Order, Item
from . import fake_db

fake = Faker()
STATUSES = ['pending', 'delivered', 'cancelled']

def generate_fake_orders(batch_size: int = 100) -> List[Order]:
    """
    Generates a batch of N FAKE but CONSISTENT orders.
    """
    orders_batch = []
    for _ in range(batch_size):

        restaurant = fake_db.get_random_restaurant()
        
        user = fake_db.get_random_user()
        
        order_items_data = fake_db.get_random_items_for_order(restaurant["restaurant_id"])
        
        order_items_models = [Item(**item_data) for item_data in order_items_data]
        
        order_total = sum(item.price * item.quantity for item in order_items_models)

        order = Order(
            order_id=uuid.uuid4(),
            user_id=user["user_id"],
            restaurant_id=restaurant["restaurant_id"],
            status=choice(STATUSES),
            created_at=fake.date_time_between(start_date='-1h', end_date='now'),
            items=order_items_models,
            total_price=round(order_total, 2)
        )
        orders_batch.append(order)
        
    return orders_batch