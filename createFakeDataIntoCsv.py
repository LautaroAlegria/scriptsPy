import csv
import random
from faker import Faker
from datetime import datetime

# Initialize Faker
fake = Faker()

# Number of records to generate (Modify these values as needed)

NUM_CUSTOMERS = 50000 #The Value will be duplicated :)
NUM_PRODUCTS = 500000
NUM_ORDERS = 2000000
NUM_ORDER_ITEMS = 5000000
NUM_TRANSACTIONS = 2000000

# File paths for CSV exports
CUSTOMERS_FILE = "customers.csv"
PRODUCTS_FILE = "products.csv"
ORDERS_FILE = "orders.csv"
ORDER_ITEMS_FILE = "order_items.csv"
TRANSACTIONS_FILE = "transactions.csv"

def generate_customers():
    """Generate fake customers and save to CSV without duplicates."""
    with open(CUSTOMERS_FILE, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["customer_id", "name", "email", "address", "city", "state", "zip_code", "registration_date"])
        
        for customer_id in range(1, NUM_CUSTOMERS + 1):
            email = f"user{customer_id}@example.com"  # Ensures uniqueness
            writer.writerow([
                customer_id,
                fake.name(),
                email,
                fake.address(),
                fake.city(),
                fake.state(),
                fake.zipcode(),
                fake.date_this_decade()
            ])
    
    print(f"✅ {NUM_CUSTOMERS} unique customers saved to {CUSTOMERS_FILE}")


def generate_products():
    """Generate fake products and save to CSV."""
    with open(PRODUCTS_FILE, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["product_id", "product_name", "category", "price", "brand"])
        
        for product_id in range(1, NUM_PRODUCTS + 1):
            writer.writerow([
                product_id,
                fake.word(),
                fake.word(),
                round(random.uniform(5, 500), 2),
                fake.company()
            ])
    print(f"✅ {NUM_PRODUCTS} products saved to {PRODUCTS_FILE}")

def generate_orders():
    """Generate fake orders and save to CSV."""
    with open(ORDERS_FILE, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["order_id", "customer_id", "order_date", "total_amount", "status"])
        
        for order_id in range(1, NUM_ORDERS + 1):
            writer.writerow([
                order_id,
                random.randint(1, NUM_CUSTOMERS),
                fake.date_between(start_date='-6y', end_date='today'),
                round(random.uniform(20, 1000), 2),
                random.choice(["Completed", "Cancelled", "Pending"])
            ])
    print(f"✅ {NUM_ORDERS} orders saved to {ORDERS_FILE}")

def generate_order_items():
    """Generate fake order items and save to CSV."""
    with open(ORDER_ITEMS_FILE, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount"])
        
        for order_item_id in range(1, NUM_ORDER_ITEMS + 1):
            writer.writerow([
                order_item_id,
                random.randint(1, NUM_ORDERS),
                random.randint(1, NUM_PRODUCTS),
                random.randint(1, 10),
                round(random.uniform(5, 500), 2),
                round(random.uniform(0, 50), 2)
            ])
    print(f"✅ {NUM_ORDER_ITEMS} order items saved to {ORDER_ITEMS_FILE}")

def generate_transactions():
    """Generate fake transactions and save to CSV."""
    with open(TRANSACTIONS_FILE, mode="w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["transaction_id", "order_id", "transaction_date", "payment_method", "payment_status", "amount"])
        
        for transaction_id in range(1, NUM_TRANSACTIONS + 1):
            writer.writerow([
                transaction_id,
                random.randint(1, NUM_ORDERS),
                fake.date_between(start_date='-6y', end_date='today'),
                random.choice(["Credit Card", "PayPal", "Bank Transfer"]),
                random.choice(["Completed", "Failed"]),
                round(random.uniform(20, 1000), 2)
            ])
    print(f"✅ {NUM_TRANSACTIONS} transactions saved to {TRANSACTIONS_FILE}")

if __name__ == "__main__":
    start_time = datetime.now()
    print("🚀 Starting data generation...")

    generate_customers()
    generate_products()
    generate_orders()
    generate_order_items()
    generate_transactions()

    print(f"🎉 Data generation complete! Time taken: {datetime.now() - start_time}")
