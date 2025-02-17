from django.core.management.base import BaseCommand
import time
import clickhouse_connect

# ClickHouse connection details
CH_HOST = "clickhouse"  # Use 'localhost' if running outside Docker
CH_PORT = "8123"
CH_USER = "clickhouse"
CH_PASSWORD = "clickhouse"

# PostgreSQL connection details
PG_HOST = "db:5432"
PG_DB = "postgres"
PG_USER = "postgres"
PG_PASSWORD = "postgres"

PG_CONN_STR = f"postgresql('{PG_HOST}', '{PG_DB}', '{{}}', '{PG_USER}', '{PG_PASSWORD}')"


class Command(BaseCommand):  # This is the required class
    help = "Migrates data from PostgreSQL to ClickHouse"

    def handle(self, *args, **kwargs):
        """Main execution function for the management command."""
        start_time = time.time()
        self.migrate_data()
        duration = time.time() - start_time
        self.stdout.write(self.style.SUCCESS(f"Data Migration Took {duration:.2f} seconds."))

    def connect_clickhouse(self):
        """Connect to ClickHouse."""
        return clickhouse_connect.get_client(
            host=CH_HOST, port=CH_PORT, username=CH_USER, password=CH_PASSWORD
        )

    def create_clickhouse_tables(self, client):
        """Create ClickHouse tables if they don't exist."""
        tables = {
            "dim_customers": """
                CREATE TABLE IF NOT EXISTS dim_customers (
                    customer_id UInt32,
                    name String,
                    email String,
                    address String,
                    city String,
                    state String,
                    zip_code String,
                    registration_date Date
                ) ENGINE = MergeTree() ORDER BY customer_id;
            """,
            "dim_products": """
                CREATE TABLE IF NOT EXISTS dim_products (
                    product_id UInt32,
                    product_name String,
                    category String,
                    price Decimal(10, 2),
                    brand String
                ) ENGINE = MergeTree() ORDER BY product_id;
            """,
            "dim_time": """
                CREATE TABLE IF NOT EXISTS dim_time (
                    date Date,
                    year UInt16,
                    quarter UInt8,
                    month UInt8,
                    day UInt8,
                    week UInt8
                ) ENGINE = MergeTree() ORDER BY date;
            """,
            "sales": """
                CREATE TABLE IF NOT EXISTS sales (
                    order_id UInt32,
                    customer_id UInt32,
                    product_id UInt32,
                    transaction_id UInt32,
                    order_date Date,
                    quantity UInt32,
                    unit_price Decimal(10, 2),
                    total_amount Decimal(10, 2),
                    discount Decimal(10, 2),
                    payment_method String,
                    payment_status String,
                    amount Decimal(10, 2),
                    year UInt16,
                    quarter UInt8,
                    month UInt8,
                    day UInt8,
                    week UInt8
                ) ENGINE = MergeTree() ORDER BY order_id;
            """,
        }

        for table, query in tables.items():
            client.command(query)
            self.stdout.write(self.style.SUCCESS(f"{table} table created or already exists."))

    def migrate_table_via_clickhouse(self, table_name, query):
        """Migrate data using ClickHouse's direct PostgreSQL query."""
        ch_client = self.connect_clickhouse()
        ch_client.command(f"TRUNCATE TABLE {table_name};")  # Clear existing data
        ch_client.command(query)
        self.stdout.write(self.style.SUCCESS(f"Migrated data to {table_name} via direct ClickHouse SQL."))

    def migrate_data(self):
        """Perform data migration from PostgreSQL to ClickHouse using direct queries."""
        ch_client = self.connect_clickhouse()
        self.create_clickhouse_tables(ch_client)

        migrations = {
            "dim_customers": f"""
                INSERT INTO dim_customers
                SELECT * FROM {PG_CONN_STR.format('dbtest_customer')};
            """,
            "dim_products": f"""
                INSERT INTO dim_products
                SELECT * FROM {PG_CONN_STR.format('dbtest_product')};
            """,
            "dim_time": f"""
                INSERT INTO dim_time
                SELECT DISTINCT order_date,
                    toYear(order_date) AS year,
                    toQuarter(order_date) AS quarter,
                    toMonth(order_date) AS month,
                    toDayOfMonth(order_date) AS day,
                    toWeek(order_date) AS week
                FROM {PG_CONN_STR.format('dbtest_order')};
            """,
            "sales": f"""
                INSERT INTO sales 
                SELECT 
                    o.order_id,
                    o.customer_id,
                    oi.product_id,
                    t.transaction_id,
                    o.order_date,
                    oi.quantity,
                    oi.unit_price,
                    o.total_amount,
                    oi.discount,
                    t.payment_method,
                    t.payment_status,
                    t.amount,
                    toYear(o.order_date) AS year,
                    toQuarter(o.order_date) AS quarter,
                    toMonth(o.order_date) AS month,
                    toDayOfMonth(o.order_date) AS day,
                    toWeek(o.order_date) AS week
                FROM {PG_CONN_STR.format('dbtest_order')} o
                JOIN {PG_CONN_STR.format('dbtest_orderitem')} oi 
                    ON o.order_id = oi.order_id
                JOIN {PG_CONN_STR.format('dbtest_transaction')} t 
                    ON o.order_id = t.order_id;
            """,
        }

        for table, query in migrations.items():
            self.migrate_table_via_clickhouse(table, query)

        self.stdout.write(self.style.SUCCESS("Data migration completed!"))
