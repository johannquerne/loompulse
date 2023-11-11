import mysql.connector
import random
import uuid
from datetime import datetime, timedelta

# Function to generate a random date between 2023-01-01 and today
def random_start_date():
    start_date = datetime(2023, 1, 1)
    end_date = datetime.now()
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

# Function to generate a random date for orders, considering user start date
def random_order_date(user_start_date):
    if user_start_date is not None:
        end_date = datetime.now()
        delta = end_date - user_start_date
        random_days = random.randint(0, delta.days)
        return user_start_date + timedelta(days=random_days)
    else:
        return None

# Function to generate a random UUID in binary format
def generate_uuid_binary():
    return uuid.uuid4().bytes

# List of common names
common_names = ["Jack", "Peter", "Tim", "Emma", "Sophia", "Oliver", "Lucas", "Liam", "Mia", "Ava"]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='host',
    user='user',
    password='password',
    database='demo_cohorts'
)

cursor = conn.cursor()

# Create the users table with INT AUTO_INCREMENT PRIMARY KEY and a UUID field
create_users_table_query = """
CREATE TABLE IF NOT EXISTS users (
    UserId INT AUTO_INCREMENT PRIMARY KEY,
    UUID BINARY(16),
    Name VARCHAR(255),
    StartDate DATE
)
"""
cursor.execute(create_users_table_query)

# Create the orders table if it doesn't exist
create_orders_table_query = """
CREATE TABLE IF NOT EXISTS orders (
    OrderId INT AUTO_INCREMENT PRIMARY KEY,
    UUID BINARY(16),
    OrderDate DATE,
    Amount DECIMAL(10, 2)
)
"""
cursor.execute(create_orders_table_query)

# Generate and insert 10,000 random users into the users table
insert_user_query = "INSERT INTO users (UUID, Name, StartDate) VALUES (%s, %s, %s)"
users = []

for _ in range(10000):
    user_uuid = generate_uuid_binary()
    name = random.choice(common_names)
    start_date = random_start_date()
    users.append((user_uuid, name, start_date))

cursor.executemany(insert_user_query, users)
conn.commit()

# Generate and insert 25,000 random orders into the orders table
insert_order_query = "INSERT INTO orders (UUID, OrderDate, Amount) VALUES (%s, %s, %s)"
orders = []


for user in users:
    user_id = user[0]
    user_start_date = user[2]

    if user_start_date is not None:
        num_orders = random.randint(1, 10)
        for _ in range(num_orders):
            order_date = random_order_date(user_start_date)
            amount = round(random.uniform(10.0, 100.0), 2)
            orders.append((user_id, order_date, amount))

# Insert the orders in smaller chunks to avoid exceeding MySQL's max_allowed_packet
chunk_size = 1000
for i in range(0, len(orders), chunk_size):
    chunk = orders[i:i + chunk_size]
    cursor.executemany(insert_order_query, chunk)
    conn.commit()

print("Data has been successfully inserted into the database.")

# Close the database connection
cursor.close()
conn.close()
