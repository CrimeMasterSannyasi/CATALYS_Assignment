"""
Generate Sample Sales Transactional Data
This script creates realistic e-commerce sales data for testing the ETL pipeline
"""

import csv
import random
from datetime import datetime, timedelta

# Sample data pools
CUSTOMERS = [f"CUST{str(i).zfill(4)}" for i in range(1, 201)]  # 200 customers
PRODUCTS = [
    ("PROD001", "Laptop Pro 15", "Electronics", 1299.99, 899.99),
    ("PROD002", "Wireless Mouse", "Electronics", 29.99, 12.00),
    ("PROD003", "USB-C Cable", "Accessories", 19.99, 5.00),
    ("PROD004", "Desk Chair", "Furniture", 249.99, 120.00),
    ("PROD005", "Standing Desk", "Furniture", 499.99, 250.00),
    ("PROD006", "Monitor 27in", "Electronics", 349.99, 180.00),
    ("PROD007", "Keyboard Mechanical", "Electronics", 89.99, 40.00),
    ("PROD008", "Webcam HD", "Electronics", 79.99, 35.00),
    ("PROD009", "Headphones Wireless", "Electronics", 149.99, 60.00),
    ("PROD010", "Notebook Set", "Office Supplies", 12.99, 3.00),
    ("PROD011", "Pen Pack", "Office Supplies", 5.99, 1.50),
    ("PROD012", "Desk Lamp LED", "Furniture", 39.99, 18.00),
    ("PROD013", "Phone Stand", "Accessories", 15.99, 4.00),
    ("PROD014", "Cable Organizer", "Accessories", 11.99, 3.50),
    ("PROD015", "Laptop Bag", "Accessories", 49.99, 22.00),
]

CUSTOMER_SEGMENTS = ["Premium", "Standard", "Budget"]
ORDER_STATUSES = ["completed", "pending", "cancelled", "returned"]

def generate_sales_data(num_orders=10000, start_date="2024-01-01", end_date="2024-02-08"):
    """Generate sales orders"""
    
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    
    orders = []
    
    for i in range(1, num_orders + 1):
        order_id = f"ORD{str(i).zfill(6)}"
        customer_id = random.choice(CUSTOMERS)
        product_id, product_name, category, unit_price, cost = random.choice(PRODUCTS)
        
        # Random order date
        days_diff = (end - start).days
        order_date = start + timedelta(days=random.randint(0, days_diff))
        
        # Quantity with some variation
        quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 30, 12, 5, 3])[0]
        
        # Occasional discount
        discount_pct = random.choices([0, 5, 10, 15, 20], weights=[60, 20, 10, 7, 3])[0]
        discount_amount = round(unit_price * quantity * (discount_pct / 100), 2)
        
        line_total = round(unit_price * quantity - discount_amount, 2)
        
        # Status distribution
        status = random.choices(ORDER_STATUSES, weights=[80, 10, 5, 5])[0]
        
        # Shipping info
        shipping_cost = random.choices([0, 5.99, 9.99, 14.99], weights=[20, 50, 20, 10])[0]
        
        # Timestamp
        order_timestamp = order_date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )
        
        orders.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "order_date": order_date.strftime("%Y-%m-%d"),
            "order_timestamp": order_timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            "quantity": quantity,
            "unit_price": unit_price,
            "discount_amount": discount_amount,
            "shipping_cost": shipping_cost,
            "line_total": line_total,
            "status": status,
            "payment_method": random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"]),
            "last_modified": order_timestamp.strftime("%Y-%m-%d %H:%M:%S")
        })
    
    return orders

def add_data_quality_issues(orders):
    """Introduce some data quality issues for testing validation"""
    
    # Add duplicates (same order_id)
    if len(orders) > 100:
        orders.append(orders[50].copy())  # Duplicate order
        orders.append(orders[75].copy())  # Another duplicate
    
    # Add null values in critical fields
    if len(orders) > 200:
        orders[100]["customer_id"] = ""
        orders[150]["order_id"] = None
        orders[175]["unit_price"] = None
    
    # Add invalid amounts
    if len(orders) > 300:
        orders[200]["line_total"] = -50.00  # Negative amount
        orders[250]["quantity"] = 0  # Zero quantity
        orders[275]["unit_price"] = 0  # Zero price
    
    # Add future dates
    if len(orders) > 400:
        orders[300]["order_date"] = "2025-12-31"  # Future date
    
    return orders

def generate_customer_data():
    """Generate customer dimension data"""
    customers = []
    
    first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa", 
                   "James", "Maria", "William", "Jennifer", "Richard", "Linda", "Thomas"]
    last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", 
                  "Davis", "Rodriguez", "Martinez", "Hernandez", "Lopez", "Wilson", "Anderson"]
    
    for i, customer_id in enumerate(CUSTOMERS):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        customers.append({
            "customer_id": customer_id,
            "customer_name": f"{first_name} {last_name}",
            "email": f"{first_name.lower()}.{last_name.lower()}{i}@email.com",
            "phone": f"555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
            "segment": random.choice(CUSTOMER_SEGMENTS),
            "registration_date": (datetime(2023, 1, 1) + timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d"),
            "is_active": random.choice([True, True, True, False]),  # 75% active
        })
    
    return customers

def generate_product_data():
    """Generate product dimension data"""
    products = []
    
    for product_id, product_name, category, unit_price, cost in PRODUCTS:
        products.append({
            "product_id": product_id,
            "product_name": product_name,
            "category": category,
            "subcategory": random.choice(["Premium", "Standard", "Budget"]),
            "unit_price": unit_price,
            "cost": cost,
            "supplier": random.choice(["Supplier A", "Supplier B", "Supplier C"]),
            "sku": f"SKU-{product_id}",
            "stock_quantity": random.randint(10, 500),
        })
    
    return products

def save_to_csv(data, filename, fieldnames):
    """Save data to CSV file"""
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"✓ Generated {filename} with {len(data)} records")

if __name__ == "__main__":
    print("Generating sample sales data...")
    
    # Generate orders
    orders = generate_sales_data(num_orders=10000)
    orders = add_data_quality_issues(orders)
    
    save_to_csv(
        orders,
        "sales_orders.csv",
        ["order_id", "customer_id", "product_id", "order_date", "order_timestamp", 
         "quantity", "unit_price", "discount_amount", "shipping_cost", "line_total", 
         "status", "payment_method", "last_modified"]
    )
    
    # Generate customers
    customers = generate_customer_data()
    save_to_csv(
        customers,
        "customers.csv",
        ["customer_id", "customer_name", "email", "phone", "segment", 
         "registration_date", "is_active"]
    )
    
    # Generate products
    products = generate_product_data()
    save_to_csv(
        products,
        "products.csv",
        ["product_id", "product_name", "category", "subcategory", "unit_price", 
         "cost", "supplier", "sku", "stock_quantity"]
    )
    
    print("\n✓ Sample sales data generation complete!")
    print(f"  - {len(orders)} sales orders (with data quality issues)")
    print(f"  - {len(customers)} customers")
    print(f"  - {len(products)} products")