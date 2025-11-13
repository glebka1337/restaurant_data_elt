# api/app/fake_db.py
from faker import Faker
import random

fake = Faker()

USERS_DB = [
    {
        "user_id": i + 1,
        "email": fake.unique.email(),
        "username": fake.user_name()
    }
    for i in range(50)
]

RESTAURANTS_DB = [
    {
        "restaurant_id": i + 1,
        "name": fake.company()
    }
    for i in range(20)
]

PRODUCT_NAMES = ['Pizza', 'Sushi Roll', 'Burger', 'Khachapuri', 'Borscht', 'Cola', 'Salad']
PRODUCTS_DB = []
for i in range(100):
    PRODUCTS_DB.append({
        "product_id": i + 1,
        "name": random.choice(PRODUCT_NAMES),
        "price": round(random.uniform(50.0, 500.0), 2),
        "restaurant_id": random.randint(1, 20)
    })

# Helpers

def get_random_user():
    """ Take a consistent user """
    return random.choice(USERS_DB)

def get_random_restaurant():
    """ Take a consistent restaurant """
    return random.choice(RESTAURANTS_DB)

def get_random_items_for_order(restaurant_id: int, max_items: int = 3):
    """ Take a consistent items for order """
    possible_products = [p for p in PRODUCTS_DB if p["restaurant_id"] == restaurant_id]
    
    if not possible_products:
        possible_products = [random.choice(PRODUCTS_DB)]
        
    num_items = random.randint(1, max_items)
    items_list = []
    
    for _ in range(num_items):
        product = random.choice(possible_products)
        items_list.append({
            "product_id": product["product_id"],
            "name": product["name"],
            "quantity": random.randint(1, 2),
            "price": product["price"] 
        })
    return items_list