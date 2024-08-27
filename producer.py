import argparse
import logging
import tomllib
import scippnexus as snx
from utils import create_producer, publish_events, get_forward_delta, yield_events


def load_config(config_file):
    """Load configuration from a TOML file."""
    with open(config_file, "rb") as f:
        return tomllib.load(f)


def setup_logging():
    """Set up logging configuration."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )


def create_kafka_producer(config):
    """Create a Kafka producer using the provided configuration."""
    kafka_address = config["kafka"]["kafka_address"]
    queue_buffering_max_messages = config["kafka"]["queue_buffering_max_messages"]
    linger_ms = config["kafka"]["linger_ms"]
    return create_producer(kafka_address, queue_buffering_max_messages, linger_ms)


def process_events(producer, topic, snx_file_path):
    """Process events from the Nexus file and publish them to Kafka."""
    snx_file = snx.File(snx_file_path)
    instrument = snx_file["entry/instrument"]

    for detector in instrument[snx.NXdetector]:
        event_data = instrument[snx.NXdetector][detector]["event_data/"][...]
        event_gen = yield_events(detector, event_data)
        forward_delta = get_forward_delta(event_data)
        logging.info("Producing events .... ")
        publish_events(producer, topic, forward_delta, event_gen)


def main():
    """Main function to parse arguments and run the event processing."""
    parser = argparse.ArgumentParser(
        description="Generate ev44 events from a Nexus file."
    )
    parser.add_argument(
        "--topic", type=str, help="The topic to publish events", required=True
    )
    parser.add_argument(
        "--file", type=str, help="The path to the Nexus file", required=True
    )
    parser.add_argument(
        "--config",
        type=str,
        help="The path to the configuration file",
        default="config.toml",
    )

    # Parse arguments
    args = parser.parse_args()

    # Set up logging
    setup_logging()

    # Load configuration
    try:
        config = load_config(args.config)
    except FileNotFoundError:
        logging.error(f"Configuration file {args.config} not found.")
        return
    except tomllib.TOMLDecodeError:
        logging.error(f"Error decoding the configuration file {args.config}.")
        return

    # Create Kafka producer
    producer = create_kafka_producer(config)

    # Process events
    process_events(producer, args.topic, args.file)


if __name__ == "__main__":
    main()
