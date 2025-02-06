from confluent_kafka import Producer
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AppConfigs:
    application_id = "HelloProducer"
    bootstrap_servers = "localhost:9092"
    topic_name = "hello-producer-topic2"
    num_events = 10

def delivery_report(err, msg):
    """Callback function to check message delivery status."""
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def create_producer():
    """Create and return a Kafka producer."""
    conf = {
        'bootstrap.servers': AppConfigs.bootstrap_servers,
        'client.id': AppConfigs.application_id
    }
    return Producer(conf)

def produce_messages(producer):
    """Produce messages to the Kafka topic."""
    logger.info("Start sending messages...")
    for i in range(1, AppConfigs.num_events + 1):
        message = f"Simple Message-{i}"
        producer.produce(
            AppConfigs.topic_name,
            key=str(i),
            value=message,
            callback=delivery_report
        )
        # Poll for events to trigger the delivery report callback
        producer.poll(0)
    logger.info("Finished - Closing Kafka Producer.")
    producer.flush()  # Wait for all messages to be delivered

if __name__ == "__main__":
    logger.info("Creating Kafka Producer...")
    producer = create_producer()
    produce_messages(producer)