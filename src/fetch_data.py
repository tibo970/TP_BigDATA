import requests
import json
import time
import os
from dotenv import load_dotenv
from kafka import KafkaProducer

# Load environment variables from the env file
load_dotenv('ATT80546.env')

# Configuration
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'velib-stations')
VELIB_INTERVAL = int(os.getenv('VELIB_INTERVAL', 30))

def fetch_velib_data():
    url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records?limit=20"
    
    try:
        print(f"Fetching data from: {url}...")
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        print(f"Successfully fetched {len(data.get('results', []))} records.")
        return data.get('results', [])

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}")
        return []

def main():
    print(f"Initializing Kafka producer on {KAFKA_BROKER}...")
    try:
        # Create Kafka producer
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    except Exception as e:
        print(f"Failed to connect to Kafka: {e}")
        return

    print(f"Starting to stream data to topic '{KAFKA_TOPIC}' every {VELIB_INTERVAL} seconds...")
    
    try:
        while True:
            records = fetch_velib_data()
            
            if records:
                # Send each record to the Kafka topic
                for record in records:
                    producer.send(KAFKA_TOPIC, record)
                
                # Flush the producer to ensure messages are sent
                producer.flush()
                print(f"Sent {len(records)} records to Kafka topic '{KAFKA_TOPIC}'.")
            
            print(f"Waiting for {VELIB_INTERVAL} seconds...\n")
            time.sleep(VELIB_INTERVAL)
            
    except KeyboardInterrupt:
        print("\nStopping data stream.")
    finally:
        producer.close()
        print("Kafka producer closed.")

if __name__ == "__main__":
    main()
