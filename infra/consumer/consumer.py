# consumer.py
import json
import boto3
import time
from kafka import KafkaConsumer

# Establish Minio connection
try:
    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9002",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
    )
    print("Connected to Minio successfully!")

except Exception as e:
    print(f"Error connecting to Minio: {e}")
    exit(1)

bucket_name = "bronze-stocks-transaction"  # Define bucket_name

# Ensure the bucket exists
try:
    s3.head_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' already exists.")
except Exception:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' created.")

# Establish kafkaConsumer connection
try:
    consumer = KafkaConsumer(
        "stock-quotes",
        bootstrap_servers=["localhost:29092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="bronze-stock-consumer1",
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=60000 # Timeout to exit if no messages
    )
    print("Kafka Consumer connected and listening to 'stock-quotes' topic...")

except Exception as e:
    print(f"Error connecting to Kafka Consumer: {e}")
    exit(1)

# Consume messages and upload to Minio
try:
    for message in consumer:
        stock_data = message.value
        symbol = stock_data.get("symbol", "unknown_symbol")
        timestamp = stock_data.get("fetched_at", int(time.time()))
        file_name = f"{symbol}/{timestamp}.json"
        
        # Upload to Minio
        s3.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json.dumps(stock_data),
            ContentType="application/json"
        )

        price = stock_data.get('c', 'N/A')
        change = stock_data.get('dp', 'N/A')
        
        print(f"Uploaded {file_name} to bucket '{bucket_name}'")
        print(f"Price: ${price} | Change: {change}%")
        print(f"[{time.strftime('%H:%M:%S')}] Uploaded: {file_name}")

except Exception as e:
    print(f"Error during consumption or upload: {e}")
    exit(1)