import numpy as np
from confluent_kafka import Producer
from streaming_data_types import deserialise_ev44, serialise_ev44
from time import time_ns


def create_producer(kafka_address, queue_buffering_max_messages, linger_ms):
    """Create a Kafka producer with the given configuration."""
    conf = {
        "bootstrap.servers": kafka_address,
        "queue.buffering.max.messages": queue_buffering_max_messages,
        "linger.ms": linger_ms,
    }
    return Producer(conf)


def get_forward_delta(event_data):
    """Calculate the forward delta time from the event data."""
    return int(time_ns() - event_data.coords["event_time_zero"][0].value.astype("int"))


def delivery_callback(err, msg):
    """Callback function for message delivery."""
    if err:
        print(f"ERROR: Message failed delivery: {err}")
    else:
        print(f"Produced event to topic {msg.topic()}: {deserialise_ev44(msg.value())}")


def yield_events(source_name, event_data):
    """Yield events from the event data."""
    for bin_data in event_data:
        temp = bin_data.data.values
        event_time_offset = temp.coords["event_time_offset"].values
        event_id = temp.coords["event_id"].values
        event_time_zero = bin_data.coords["event_time_zero"].values
        event_index = np.asarray([0])

        yield {
            "source_name": source_name,
            "reference_time": event_time_zero,
            "reference_time_index": event_index,
            "time_of_flight": event_time_offset,
            "pixel_id": event_id,
        }


def publish_events(producer, topic, forward_delta, event_generator):
    """Publish events to the specified Kafka topic."""
    for m_id, event in enumerate(event_generator):
        buf = serialise_ev44(**event, message_id=m_id)
        publish_time = int(
            (forward_delta + event["reference_time"].astype("int")) * 0.000001
        )  # ns to ms
        producer.produce(topic, value=buf, timestamp=publish_time)
    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()
