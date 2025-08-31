import csv
import random
from datetime import datetime, timedelta
import uuid
import argparse
import json
import time
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

# --- Configuration ---
PRODUCTS = {
    "Electronics": [("Laptop", 999.99), ("Smartphone", 799.99), ("Headphones", 149.99), ("Smart Watch", 249.99)],
    "Books": [("The Midnight Library", 15.99), ("Project Hail Mary", 17.99), ("Dune", 10.99), ("Klara and the Sun", 14.99)],
    "Home Goods": [("Coffee Maker", 89.99), ("Blender", 49.99), ("Air Fryer", 119.99), ("Scented Candle", 19.99)],
    "Apparel": [("T-Shirt", 25.00), ("Jeans", 75.00), ("Sneakers", 120.00), ("Jacket", 150.00)]
}

COUNTRIES = ["USA", "Canada", "UK", "Germany", "France", "Australia", "Japan"]

# --- Helper Functions ---
def get_random_product():
    """Selects a random product category and a random product from it."""
    category = random.choice(list(PRODUCTS.keys()))
    product_name, price = random.choice(PRODUCTS[category])
    return category, product_name, price

def get_random_date(start_date, end_date):
    """Generates a random datetime between two dates."""
    time_between_dates = end_date - start_date
    seconds_between_dates = time_between_dates.total_seconds()
    random_number_of_seconds = random.randrange(int(seconds_between_dates))
    random_date = start_date + timedelta(seconds=random_number_of_seconds)
    return random_date.isoformat()

def generate_single_sale_record():
    """Generates one fake sales record as a dictionary."""
    category, product_name, price = get_random_product()
    quantity = random.randint(1, 5)
    return {
        "order_id": str(uuid.uuid4()),
        "product_id": str(uuid.uuid4()),
        "product_name": product_name,
        "category": category,
        "price": price,
        "quantity": quantity,
        "order_date": datetime.now().isoformat(),
        "customer_id": str(uuid.uuid4()),
        "country": random.choice(COUNTRIES),
        "total_price": round(price * quantity, 2)
    }

# --- Mode 1: Batch File Generation ---
def generate_batch_data(filename="historical_sales_data.csv", num_records=1000):
    """Generates a CSV file with a specified number of historical sales records."""
    print(f"Generating {num_records} historical sales records into {filename}...")
    fieldnames = [
        "order_id", "product_id", "product_name", "category", "price",
        "quantity", "order_date", "customer_id", "country", "total_price"
    ]
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    with open(filename, mode='w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        
        for _ in range(num_records):
            category, product_name, price = get_random_product()
            quantity = random.randint(1, 5)
            writer.writerow({
                "order_id": str(uuid.uuid4()),
                "product_id": str(uuid.uuid4()),
                "product_name": product_name,
                "category": category,
                "price": price,
                "quantity": quantity,
                "order_date": get_random_date(start_date, end_date),
                "customer_id": str(uuid.uuid4()),
                "country": random.choice(COUNTRIES),
                "total_price": round(price * quantity, 2)
            })
    print("Batch data generation complete.")

# --- Mode 2: Real-time Data Streaming ---
def stream_real_time_data(stream_name, region):
    """Generates and streams sales data to a Kinesis Data Stream in real-time."""
    print(f"Starting real-time data stream to Kinesis '{stream_name}' in region '{region}'.")
    print("Press Ctrl+C to stop.")
    
    try:
        kinesis_client = boto3.client('kinesis', region_name=region)
        
        while True:
            record = generate_single_sale_record()
            
            # The record must be sent as bytes, so we convert the dictionary to a JSON string first
            data = json.dumps(record).encode('utf-8')
            
            # PartitionKey helps Kinesis distribute data across shards. 
            # Using order_id ensures records for the same order go to the same shard.
            partition_key = record['order_id']
            
            # Send the record to Kinesis
            kinesis_client.put_record(
                StreamName=stream_name,
                Data=data,
                PartitionKey=partition_key
            )
            
            print(f"Sent record: {record['order_id']} | Product: {record['product_name']}")
            
            # Wait for 1 second before sending the next record
            time.sleep(1)
            
    except NoCredentialsError:
        print("\nERROR: AWS credentials not found.")
        print("Please configure your AWS credentials (e.g., run 'aws configure').")
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print(f"\nERROR: Kinesis stream '{stream_name}' not found in region '{region}'.")
            print("Please check the stream name and region.")
        else:
            print(f"\nAn AWS client error occurred: {e}")
    except KeyboardInterrupt:
        print("\nStream stopped by user.")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

# --- Main Execution ---
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Generator for Sales Data Pipeline.")
    
    # Create subparsers for the two modes: batch and stream
    subparsers = parser.add_subparsers(dest="mode", required=True, help="The mode to run the script in.")
    
    # Batch mode parser
    parser_batch = subparsers.add_parser("batch", help="Generate a batch CSV file of historical data.")
    parser_batch.add_argument("--records", type=int, default=1000, help="Number of records to generate.")
    parser_batch.add_argument("--filename", type=str, default="historical_sales_data.csv", help="Output CSV file name.")
    
    # Stream mode parser
    parser_stream = subparsers.add_parser("stream", help="Stream real-time data to AWS Kinesis.")
    parser_stream.add_argument("--stream-name", type=str, required=True, help="The name of the Kinesis Data Stream.")
    # THIS IS THE CORRECTED LINE:
    parser_stream.add_argument("--region", type=str, required=True, help="The AWS region of the Kinesis stream.")
    
    args = parser.parse_args()
    
    # Execute the appropriate function based on the mode
    if args.mode == "batch":
        generate_batch_data(filename=args.filename, num_records=args.records)
    elif args.mode == "stream":
        stream_real_time_data(stream_name=args.stream_name, region=args.region)

