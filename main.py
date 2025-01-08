from dotenv import load_dotenv
load_dotenv(dotenv_path=".env", override=True)


import threading
from consumer import consume_data
from database import client
from server import app


def run_consumer():
    consume_data()


def main():
    consumer_thread = threading.Thread(target=run_consumer)

    consumer_thread.start()
    app.run(debug=True)


if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print("Shutting down...")

    finally:
        client.close()
