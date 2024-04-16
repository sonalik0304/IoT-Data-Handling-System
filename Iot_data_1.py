import pika
import json
from pymongo import MongoClient

# RabbitMQ Settings
RABBITMQ_HOST = "localhost"
RABBITMQ_PORT = 5672  # Default RabbitMQ port for AMQP
RABBITMQ_QUEUE = "mqtt_queue"

# MongoDB Settings
MONGODB_HOST = "localhost"
MONGODB_PORT = 27017
MONGODB_DB = "iot_data"
MONGODB_COLLECTION = "messages"

def insert_dummy_message():
    # Insert dummy message into MongoDB
    mongo_client = MongoClient(MONGODB_HOST, MONGODB_PORT)
    db = mongo_client[MONGODB_DB]
    collection = db[MONGODB_COLLECTION]
    dummy_message = {"dummy_key": "dummy_value"}
    collection.insert_one(dummy_message)
    print("Dummy message inserted into MongoDB.")

def process_message(ch, method, properties, body):
    try:
        message = json.loads(body)
        # Process message logic goes here
        print("Received message:", message)

    except Exception as e:
        print("Error processing message:", e)

def main():
    # Insert dummy message into MongoDB
    insert_dummy_message()

    # Check database connection
    try:
        mongo_client = MongoClient(MONGODB_HOST, MONGODB_PORT)
        mongo_client.server_info()  # Test connection
        print("Connected to MongoDB successfully.")
    except Exception as e:
        print("Failed to connect to MongoDB:", e)
        return
    connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST, RABBITMQ_PORT))

    # Connect to RabbitMQ
    # connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_HOST, RABBITMQ_PORT))
    channel = connection.channel()

    # Set up message consuming
    # channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=process_message, auto_ack=True)

    # Declare the queue
    channel.queue_declare(queue=RABBITMQ_QUEUE)

# Set up message consuming
    channel.basic_consume(queue=RABBITMQ_QUEUE, on_message_callback=process_message, auto_ack=True)

    

    # Start consuming messages
    print("Waiting for messages. To exit press CTRL+C")
    channel.start_consuming()

if __name__ == "__main__":
    main()
