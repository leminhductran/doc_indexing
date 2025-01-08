from quixstreams import Application
import json
import query_analyzer
import os


def connect_kafka():
    app = Application(
        broker_address=os.getenv("BROKER_ADDRESS"),
        loglevel=os.getenv("LOG_LEVEL"),
        consumer_group=os.getenv("CONSUMER_GROUP"),  
        auto_offset_reset="latest",  
    )

    return app


def consume_data():
    app = connect_kafka()

    with app.get_consumer() as consumer:
        consumer.subscribe([os.getenv("TOPIC")])

        while True:
            msg = consumer.poll(1)
            print("Waiting...")

            if msg is None:
                continue

            if msg.error() is not None:
                print(f"Error: {msg.error()}")
                continue

            value = json.loads(msg.value())
            print(value)
            query_analyzer.analyze(value)
