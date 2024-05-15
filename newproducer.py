from kafka import KafkaProducer
from pymongo import MongoClient
import json
import time

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def fetch_data_from_mongodb():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['music_database']
    collection = db['track_recommendations']
    return list(collection.find({}, {'_id': 0}))

if __name__ == "__main__":
    producer = create_kafka_producer()
    data = fetch_data_from_mongodb()
    
    # Continuously send data
    while True:
        for item in data:
            producer.send('track_recommendations_topic', value=item)
            producer.flush()
            print(f"Sent to Kafka: {item}")
            time.sleep(1)  # Optional: sleep for a second to avoid overwhelming the consumer

        # Optional: Refresh data periodically if the database updates
        time.sleep(60)  # Wait a minute before sending data again
        data = fetch_data_from_mongodb()  # Re-fetch data if there are updates in MongoDB

