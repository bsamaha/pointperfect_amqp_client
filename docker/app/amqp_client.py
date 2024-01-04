import asyncio
import json
import logging
import queue
import time
from urllib.parse import urlparse
from serial import Serial, SerialException
from pyubx2 import UBXReader, NMEA_PROTOCOL
import aio_pika
import paho.mqtt.client as mqtt
import os

from config import load_config_from_yaml, Config


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    filename="client.log",
)

# Use environment variables with defaults
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "192.168.1.153")
PORT = os.getenv("PORT", "/dev/ttyACM0")
DEVICE_ID = os.getenv("DEVICE_ID", "blake_test_rpi")

GNSS_MESSAGES = {"GNGGA"}
RABBITMQ_HOST = "192.168.1.153"
RABBITMQ_PORT = 5672
RABBITMQ_USERNAME = "guest"
RABBITMQ_PASSWORD = "guest"
EXCHANGE_NAME = "gnss_data_exchange"
ROUTING_KEY = "gnss_routing_key"
MAX_RECONNECT_ATTEMPTS = 5
RECONNECT_DELAY = 1  # in seconds, will be doubled with each attempt


class AsyncRabbitMQClient:
    def __init__(self):
        self.connection = None
        self.channel = None

    async def connect(self):
        try:
            # Using aio_pika for async connection
            self.connection = await aio_pika.connect_robust(
                f"amqp://{RABBITMQ_USERNAME}:{RABBITMQ_PASSWORD}@{RABBITMQ_HOST}:{RABBITMQ_PORT}/"
            )
            self.channel = await self.connection.channel()
            await self.channel.declare_exchange(
                EXCHANGE_NAME, aio_pika.ExchangeType.DIRECT, durable=True
            )
        except Exception as e:
            logger.error(f"Failed to connect to RabbitMQ: {e}")
            raise

    async def publish_message(self, message):
        try:
            # Obtain a reference to the custom exchange object
            exchange = await self.channel.get_exchange(EXCHANGE_NAME)
            # Publish message to the specific exchange with the correct routing key
            await exchange.publish(
                aio_pika.Message(body=message.encode()),
                routing_key=ROUTING_KEY,
            )
            logger.info(f"AMQP sent: {message}")
        except Exception as e:
            logger.error(f"Failed to send message: {e}")
            raise

    async def close(self):
        if self.channel:
            await self.channel.close()
        if self.connection:
            await self.connection.close()
            logger.info("RabbitMQ connection closed.")


class SerialCommunication:
    def __init__(self, port, baudrate, timeout):
        self.stream = None
        self.message_queue = queue.Queue()
        self.running = True
        self._open_connection(port, baudrate, timeout)
        self.thread = asyncio.create_task(self.send_messages())

    def _open_connection(self, port, baudrate, timeout):
        try:
            self.stream = Serial(port, baudrate, timeout=timeout)
            logger.info(f"Serial port {port} opened successfully.")
        except SerialException as e:
            logger.error(f"Failed to open serial port: {e}")
            raise ConnectionError(f"Failed to open serial port: {e}")

    async def read_and_send_data(self, amqp_client):
        while True:
            if self.stream and self.stream.in_waiting:
                try:
                    ubx_reader = UBXReader(self.stream, protfilter=NMEA_PROTOCOL)
                    _, parsed_data = ubx_reader.read()
                    if parsed_data and parsed_data.identity in GNSS_MESSAGES:
                        logger.info(f"Received GNSS message: {parsed_data}")
                        asyncio.create_task(
                            self.process_and_send(parsed_data, amqp_client)
                        )
                except SerialException as e:
                    logger.error(f"Error reading data from serial port: {e}")
                    break
            else:
                await asyncio.sleep(0.01)  # Short sleep to yield control

    async def process_and_send(self, parsed_data, amqp_client):
        diff_age = parsed_data.diffAge if parsed_data.diffAge != "" else -1

        data_dict = {
            "time": str(parsed_data.time),
            "full_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "lat": parsed_data.lat,
            "NS": parsed_data.NS,
            "lon": parsed_data.lon,
            "EW": parsed_data.EW,
            "quality": parsed_data.quality,
            "numSV": parsed_data.numSV,
            "HDOP": parsed_data.HDOP,
            "alt": parsed_data.alt,
            "altUnit": parsed_data.altUnit,
            "sep": parsed_data.sep,
            "sepUnit": parsed_data.sepUnit,
            "diffAge": diff_age,
            "diffStation": parsed_data.diffStation,
            # processedTime can be used to compare transit with time
            "processedTime": f"{time.time():.3f}",
            "deviceID": DEVICE_ID,
        }

        start_time = time.time()
        json_data = json.dumps(data_dict)
        logger.debug(f"Sending JSON data: {json_data}")

        # Correctly await the publish_message coroutine
        await amqp_client.publish_message(json_data)
        send_time = time.time() - start_time
        logger.debug(f"Time taken to send message: {send_time} seconds")

    def enqueue_message(self, message):
        self.message_queue.put(message)

    async def send_messages(self):
        while self.running:
            if not self.message_queue.empty():
                message = self.message_queue.get()
                try:
                    self.stream.write(message)
                    logger.info(f"Serial Message Sent: {message}")
                except SerialException as e:
                    logger.error(f"Error sending message: {e}")

            await asyncio.sleep(0.01)

    def close(self):
        self.running = False
        self.thread.cancel()
        if self.stream:
            self.stream.close()
            logger.info("Serial port closed.")


class PointPerfectClient:
    REGION_MAPPING = {
        "S2655E13470": "au",
        "N5245E01185": "eu",
        "N3895E13960": "jp",  # East
        "N3310E13220": "jp",  # West
        "N3630E12820": "kr",
        "N3920W09660": "us",
    }

    def __init__(self, config, serial_communication):
        self.serial_communication = serial_communication
        self.config = config
        self.mqtt_client = None
        self.current_topics = {}

        parsed_uri = urlparse(self.config.MQTT_SERVER_URI)
        self.mqtt_server = parsed_uri.hostname
        self.mqtt_port = parsed_uri.port or 8883  # Default MQTT SSL port

        self._initialize_mqtt_client()

    def _initialize_mqtt_client(self):
        self.mqtt_client = mqtt.Client(client_id=self.config.MQTT_CLIENT_ID)
        self.mqtt_client.tls_set(
            certfile=self.config.MQTT_CERT_FILE, keyfile=self.config.MQTT_KEY_FILE
        )
        self.mqtt_client.enable_logger()
        self.mqtt_client.on_connect = self._on_mqtt_connect
        self.mqtt_client.on_disconnect = self._on_mqtt_disconnect
        self.mqtt_client.on_message = self._handle_message

    def _on_mqtt_connect(self, client, userdata, flags, rc):
        if rc == 0:
            logger.info("Connected to MQTT server")
            self.subscribe("/pp/ubx/0236/Lb", qos=1)
            self.subscribe("/pp/Lb/us", qos=0)
            self.subscribe("/pp/ubx/mga", qos=1)
        else:
            logger.error(f"Failed to connect to MQTT server, return code {rc}")

    def _on_mqtt_disconnect(self, client, userdata, rc):
        logger.info(
            "Disconnected from MQTT server" if rc == 0 else "Unexpected MQTT disconnect"
        )
        self.current_topics = {}

    def _handle_message(self, client, userdata, msg):
        try:
            logger.debug(f"Received message on topic {msg.topic}")
            # Handle different message types here
            self.serial_communication.enqueue_message(msg.payload)
        except Exception as e:
            logger.exception(f"Error handling message on {msg.topic}", exc_info=True)

    def connect(self):
        try:
            self.mqtt_client.connect(self.mqtt_server, self.mqtt_port)
            self.mqtt_client.loop_start()
        except Exception as e:
            logger.exception("Error connecting to MQTT server", exc_info=True)

    def disconnect(self):
        try:
            self.mqtt_client.disconnect()
            self.mqtt_client.loop_stop()
        except Exception as e:
            logger.exception("Error disconnecting MQTT client", exc_info=True)

    def subscribe(self, topic, qos=0):
        if topic not in self.current_topics:
            self.mqtt_client.subscribe(topic, qos)
            self.current_topics[topic] = qos
            logger.info(f"Subscribed to {topic} with QoS {qos}")

    def unsubscribe(self, topic):
        if topic in self.current_topics:
            self.mqtt_client.unsubscribe(topic)
            del self.current_topics[topic]
            logger.info(f"Unsubscribed from {topic}")


async def main():
    # Load configuration from the YAML file
    config = load_config_from_yaml("config.yaml")

    # Initialize RabbitMQ client and connect
    rabbitmq_client = AsyncRabbitMQClient()
    await rabbitmq_client.connect()

    # Initialize SerialCommunication with the config
    serial_comm = SerialCommunication(config.PORT, config.BAUDRATE, config.TIMEOUT)

    # Initialize PointPerfectClient with the config and serial communication
    point_perfect_client = PointPerfectClient(config, serial_comm)

    # Connect the PointPerfectClient
    point_perfect_client.connect()

    try:
        # Gather and run asynchronous tasks
        await asyncio.gather(
            serial_comm.read_and_send_data(rabbitmq_client),
            # Add other async tasks if necessary
        )
    finally:
        # Clean up
        serial_comm.close()
        point_perfect_client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
