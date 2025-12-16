import time
import json
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
import signal
import sys

# Define api variables
API_KEY = "CTMYYRQGHY8AFU10"
BASE_URL = "https://www.alphavantage.co/query"
SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]
KAFKA_TOPIC = "stock-quotes"

# Graceful shutdown handler
def signal_handler(sig, frame):
    print('Shutting down gracefully...')
    producer.flush()
    producer.close()
    print('Producer closed successfully')
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

# Initial setup for Kafka producer with retry logic
def create_producer(max_retries=5):
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=["localhost:29092"],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            print("Connected to Kafka!\n")
            return producer
        except NoBrokersAvailable:
            print(f"Attempt {attempt + 1}/{max_retries}: Kafka not ready, retrying in 5s...")
            time.sleep(5)
    
    raise Exception("Could not connect to Kafka. Ensure it's running: docker-compose ps")

producer = create_producer()

# Function to fetch stock data from Alpha Vantage API
def fetch_quote(symbol):
    url = f"{BASE_URL}?function=GLOBAL_QUOTE&symbol={symbol}&apikey={API_KEY}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        
        # Check API limit message
        if 'Note' in data:
            print(f"API Rate Limit: {data['Note']}")
            return None
        
        # Alpha Vantage API format
        if 'Global Quote' in data and data['Global Quote']:
            quote = data['Global Quote']
            
            # Message to validate we got data
            if not quote.get('05. price'):
                print(f"No data available for {symbol}")
                return None
            
            return {
                'c': float(quote.get('05. price', 0)),
                'd': float(quote.get('09. change', 0)),
                'dp': float(quote.get('10. change percent', '0%').replace('%', '')),
                'h': float(quote.get('03. high', 0)),
                'l': float(quote.get('04. low', 0)),
                'o': float(quote.get('02. open', 0)),
                'pc': float(quote.get('08. previous close', 0)),
                't': int(time.time()),
                'symbol': symbol,
                'fetched_at': int(time.time())
            }
        else:
            print(f"Invalid response for {symbol}: {data}")
            return None
            
    except requests.exceptions.Timeout:
        print(f"Timeout fetching {symbol}")
        return None
    except requests.exceptions.ConnectionError:
        print(f"Connection error fetching {symbol}")
        return None
    except ValueError as e:
        print(f"JSON parsing error for {symbol}: {e}")
        return None
    except Exception as e:
        print(f"Error fetching {symbol}: {e}")
        return None

# Callback for successful message delivery
def on_send_success(record_metadata):
    print(f"Delivered to partition {record_metadata.partition} offset {record_metadata.offset}")

# Callback for failed message delivery
def on_send_error(excp):
    print(f"Failed to send: {excp}")

# Main loop to fetch and send data to Kafka
print("Starting stock data producer (Alpha Vantage API)...")
print(f"Monitoring symbols: {', '.join(SYMBOLS)}")
print(f"Refresh interval: 60 seconds")
print(f" Note: Alpha Vantage free tier = 25 requests/day, 5 requests/minute\n")

iteration = 0
while True:
    iteration += 1
    print(f"{'='*60}")
    print(f"Iteration #{iteration} - {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}")
    
    success_count = 0
    for symbol in SYMBOLS:
        quote = fetch_quote(symbol)
        if quote:
            try:
                # Send with callbacks for better monitoring
                future = producer.send(KAFKA_TOPIC, value=quote)
                
                # Add callbacks (non-blocking)
                future.add_callback(on_send_success)
                future.add_errback(on_send_error)
                
                print(f"Sent {symbol}: Price=${quote.get('c', 0):.2f} "
                      f"Change={quote.get('dp', 0):+.2f}%")
                success_count += 1
                
                # Rate limiting: Alpha Vantage allows 5 calls/minute
                time.sleep(12)  # 60s / 5 calls = 12s between calls
                
            except KafkaError as e:
                print(f"Kafka error sending {symbol}: {e}")
        else:
            print(f"Skipped {symbol} (fetch failed)")
    
    # Flush to ensure messages are sent
    producer.flush()
    
    print(f"\nSummary: {success_count}/{len(SYMBOLS)} symbols sent successfully")
    print(f"Waiting 60 seconds before next fetch...")
    print(f"{'='*60}\n")
    time.sleep(60)