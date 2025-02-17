import os
import random
from faker import Faker
from django.core.management.base import BaseCommand
from dbtest.models import Customer, Product, Order, OrderItem, Transaction

# Initialize Faker
fake = Faker()

class Command(BaseCommand):
    help = 'Generate fake data for the database'

    def handle(self, *args, **kwargs):
        self.create_customers()
        self.create_products()
        self.create_orders()
        self.create_order_items()
        self.create_transactions()
        self.stdout.write(self.style.SUCCESS('Data generation complete.'))

    def create_customers(self, n=10000):
        """Generate and insert fake customers."""
        customers = [
            Customer(
                name=fake.name(),
                email=fake.email(),
                address=fake.address(),
                city=fake.city(),
                state=fake.state(),
                zip_code=fake.zipcode(),
                registration_date=fake.date_this_decade()
            )
            for _ in range(n)
        ]
        Customer.objects.bulk_create(customers)
        self.stdout.write(f"{n} customers inserted.")

    def create_products(self, n=5000):
        """Generate and insert fake products."""
        products = [
            Product(
                product_name=fake.word(),
                category=fake.word(),
                price=round(random.uniform(5, 500), 2),
                brand=fake.company()
            )
            for _ in range(n)
        ]
        Product.objects.bulk_create(products)
        self.stdout.write(f"{n} products inserted.")

    def create_orders(self, n=20000):
        """Generate and insert fake orders."""
        orders = [
            Order(
                customer_id=random.randint(1, 1000),
                order_date=fake.date_between(start_date='-6y', end_date='today'),
                total_amount=round(random.uniform(20, 1000), 2),
                status=random.choice(["Completed", "Cancelled", "Pending"])
            )
            for _ in range(n)
        ]
        Order.objects.bulk_create(orders)
        self.stdout.write(f"{n} orders inserted.")

    def create_order_items(self, n=50000):
        """Generate and insert fake order items."""
        order_items = [
            OrderItem(
                order_id=random.randint(1, 2000),
                product_id=random.randint(1, 500),
                quantity=random.randint(1, 10),
                unit_price=round(random.uniform(5, 500), 2),
                discount=round(random.uniform(0, 50), 2)
            )
            for _ in range(n)
        ]
        OrderItem.objects.bulk_create(order_items)
        self.stdout.write(f"{n} order items inserted.")

    def create_transactions(self, n=20000):
        """Generate and insert fake transactions."""
        transactions = [
            Transaction(
                order_id=random.randint(1, 2000),
                transaction_date=fake.date_between(start_date='-6y', end_date='today'),
                payment_method=random.choice(["Credit Card", "PayPal", "Bank Transfer"]),
                payment_status=random.choice(["Completed", "Failed"]),
                amount=round(random.uniform(20, 1000), 2)
            )
            for _ in range(n)
        ]
        Transaction.objects.bulk_create(transactions)
        self.stdout.write(f"{n} transactions inserted.")
