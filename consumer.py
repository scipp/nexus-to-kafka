import argparse
import signal
import logging
from confluent_kafka import Consumer, KafkaException, KafkaError
from streaming_data_types import deserialise_ev44

# Configuration for Kafka consumer
conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "foo",
    "auto.offset.reset": "smallest",
}

# Create Kafka consumer
consumer = Consumer(conf)

# Flag to control the running state
running = True


def basic_consume_loop(consumer, topics):
    """Consume messages from Kafka topics."""
    try:
        consumer.subscribe(topics)
        while running:
            msg = consumer.poll(timeout=2.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logging.info(
                        f"{msg.topic()} [{msg.partition()}] reached end at offset {msg.offset()}"
                    )
                else:
                    raise KafkaException(msg.error())
            else:
                # Process the message
                logging.info(deserialise_ev44(msg.value()))
    except Exception as e:
        logging.error(f"Error in consume loop: {e}")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()


def shutdown(signum, frame):
    """Signal handler for graceful shutdown."""
    global running
    running = False
    logging.info("Shutting down...")


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def main():
    """Main function to set up signal handling and start the consume loop."""
    parser = argparse.ArgumentParser(
        description="Consume events generated by nexus2kafka"
    )
    parser.add_argument(
        "--topic", type=str, help="The topic to read events", required=True
    )
    args = parser.parse_args()

    setup_logging()
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)
    basic_consume_loop(consumer, [args.topic])


if __name__ == "__main__":
    main()
