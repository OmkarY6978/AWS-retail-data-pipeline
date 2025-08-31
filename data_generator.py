import csv
import json
import random
import datetime
import time
import sys
import boto3

# --- Configuration ---
PRODUCTS = [
    {"id": "P001", "name": "Laptop", "category": "Electronics", "price": 1200.00},
    {"id": "P002", "name": "Smartphone", "category": "Electronics", "price": 800.00},
    {"id": "P003", "name": "T-Shirt", "category": "Apparel", "price": 25.00},
    {"id": "P004", "name": "Jeans", "category": "Apparel", "price": 75.00},
    {"id": "P005", "name": "Coffee Maker", "category": "Home Goods", "price": 100.00},
    {"id": "P006", "name": "Book: The Data Engineer", "category": "Books", "price": 30.00},
]

COUNTRIES = ["USA", "Canada", "UK", "Germany", "France", "Australia"]

# AWS Kinesis Stream Name (for real-time mode)
KINESIS_STREAM_NAME = "online-retail-stream"

# --- Helper Functions ---

def generate_random_sale():
    """Generates a single, random sales record."""
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    order_date = datetime.datetime.now() - datetime.timedelta(days=random.randint(0, 365))
    
    return {
        "order_id": f"ORD{random.randint(10000, 99999)}",
        "product_id": product["id"],
        "product_name": product["name"],
        "category": product["category"],
        "price": product["price"],
        "quantity": quantity,
        "order_date": order_date.isoformat(),
        "customer_id": f"CUST{random.randint(100, 999)}",
        "country": random.choice(COUNTRIES)
    }

def create_batch_data(filename="historical_sales_data.csv", num_records=10000):
    """Creates a CSV file with historical sales data."""
    print(f"Generating {num_records} records for batch processing...")
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = [
            "order_id", "product_id", "product_name", "category", "price",
            "quantity", "order_date", "customer_id", "country"
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for i in range(num_records):
            writer.writerow(generate_random_sale())
            if (i + 1) % 1000 == 0:
                print(f"  ...generated {i+1} records")
    print(f"Successfully created '{filename}'")

def stream_real_time_data():
    """Generates and sends data to an AWS Kinesis stream."""
    try:
        kinesis_client = boto3.client('kinesis')
        # Check if stream exists
        kinesis_client.describe_stream(StreamName=KINESIS_STREAM_NAME)
        print(f"Connected to Kinesis stream: {KINESIS_STREAM_NAME}")
    except kinesis_client.exceptions.ResourceNotFoundException:
        print(f"Error: Kinesis stream '{KINESIS_STREAM_NAME}' not found.")
        print("Please create the data stream in the AWS Kinesis console.")
        return
    except Exception as e:
        print(f"An error occurred connecting to Kinesis: {e}")
        print("Please ensure your AWS credentials and region are configured correctly.")
        return

    print("Starting real-time data stream. Press Ctrl+C to stop.")
    while True:
        try:
            sale_record = generate_random_sale()
            sale_json = json.dumps(sale_record)
            
            # Send data to Kinesis
            response = kinesis_client.put_record(
                StreamName=KINESIS_STREAM_NAME,
                Data=sale_json,
                PartitionKey=sale_record["order_id"]  # A partition key helps distribute data
            )
            
            print(f"Sent record: {sale_record['order_id']} | Shard ID: {response['ShardId']}")
            time.sleep(random.uniform(0.5, 2.0))  # Wait for a random interval
            
        except KeyboardInterrupt:
            print("\nStopping data stream.")
            break
        except Exception as e:
            print(f"An error occurred while sending data: {e}")
            time.sleep(5)


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ["batch", "real-time"]:
        print("Usage: python data_generator.py [batch|real-time]")
        sys.exit(1)
        
    mode = sys.argv[1]
    
    if mode == "batch":
        create_batch_data()
    elif mode == "real-time":
        stream_real_time_data()
