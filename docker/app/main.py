import asyncio
import json
import logging
import os
import queue
import sys
import time
from datetime import datetime
import aio_pika
import pytz
from serial import Serial, SerialException
from pyubx2 import UBXReader, UBX_PROTOCOL, NMEA_PROTOCOL, RTCM3_PROTOCOL
from config import load_config
from pointperfect_client import PointPerfectClient

# Configure logging to write to stdout
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],  # Log to stdout
)

# Use environment variables with defaults
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
PORT = os.getenv("PORT", "/dev/ttyACM0")
DEVICE_ID = os.getenv("DEVICE_ID", "blake_test_rpi")

GNSS_MESSAGES = {"GNGGA"}
RABBITMQ_PORT = os.getenv("RABBITMQ_PORT", "5672")
RABBITMQ_USERNAME = os.getenv("RABBITMQ_USERNAME", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", "gnss_exchange")
ROUTING_KEY = os.getenv("ROUTING_KEY", "gnss_data")
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
            logger.error("Failed to connect to RabbitMQ: %s", e)
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
            logger.info("AMQP sent: %s", message)
        except Exception as e:
            logger.error("Failed to send message: %s", e)
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
            logger.info("Serial port %s opened successfully.", port)
        except SerialException as e:
            logger.error("Failed to open serial port: %s", e)
            raise ConnectionError(f"Failed to open serial port: {e}") from e

    async def read_and_send_data(self, amqp_client):
        while True:
            if self.stream and self.stream.in_waiting:
                try:
                    ubx_reader = UBXReader(self.stream, protfilter=UBX_PROTOCOL | NMEA_PROTOCOL | RTCM3_PROTOCOL)
                    _, parsed_data = ubx_reader.read()
                    if parsed_data and parsed_data.identity in GNSS_MESSAGES:
                        logger.info("Received GNSS message: %s", parsed_data)
                        asyncio.create_task(
                            self.process_and_send(parsed_data, amqp_client)
                        )
                except SerialException as e:
                    logger.error("Error reading data from serial port: %s", e)
                    break
            else:
                await asyncio.sleep(0.01)  # Short sleep to yield control

    async def process_and_send(self, parsed_data, amqp_client):
        diff_age = parsed_data.diffAge if parsed_data.diffAge != "" else -1

        # Get current local date and time
        local_datetime = datetime.now()

        # Convert local datetime to UTC
        utc_datetime = local_datetime.astimezone(pytz.utc)

        # Extract the date part in UTC
        utc_date = utc_datetime.date()

        # Combine UTC date with parsed_data.time
        try:
            full_datetime = datetime.combine(utc_date, parsed_data.time)
        except Exception as e:
            logger.info("Currently no time value from receiver: %s", e)
            full_datetime = datetime.now()

        # No need to set timezone as it's already in UTC
        full_datetime_str = full_datetime.isoformat()

        data_dict = {
            "message_type": parsed_data.identity,
            "full_time": full_datetime_str,
            "lat": parsed_data.lat,
            "ns": parsed_data.NS,
            "lon": parsed_data.lon,
            "ew": parsed_data.EW,
            "quality": parsed_data.quality,
            "num_sv": parsed_data.numSV,
            "hdop": parsed_data.HDOP,
            "alt": parsed_data.alt,
            "alt_unit": parsed_data.altUnit,
            "sep": parsed_data.sep,
            "sep_unit": parsed_data.sepUnit,
            "diff_age": diff_age,
            "diff_station": parsed_data.diffStation,
            "processed_time": f"{int(time.time()*1000)}",
            "device_id": DEVICE_ID,
        }

        start_time = time.time()
        json_data = json.dumps(data_dict)
        logger.debug("Sending JSON data: %s", json_data)

        # Correctly await the publish_message coroutine
        await amqp_client.publish_message(json_data)
        send_time = time.time() - start_time
        logger.debug("Time taken to send message: %s seconds", send_time)

    def enqueue_message(self, message):
        self.message_queue.put(message)

    async def send_messages(self):
        while self.running:
            if not self.message_queue.empty():
                message = self.message_queue.get()
                try:
                    self.stream.write(message)
                    logger.info("Serial Message Sent: %s", message)
                except SerialException as e:
                    logger.error("Error sending message: %s", e)

            await asyncio.sleep(0.01)

    def close(self):
        self.running = False
        self.thread.cancel()
        if self.stream:
            self.stream.close()
            logger.info("Serial port closed.")


async def main():
    # get environment variables for the config.yaml
    config_filepath = os.getenv(
        "CONFIG_FILE",
    )
    # Load configuration from the YAML file
    config = load_config(config_filepath)
    config.create_temp_files()
    logger.debug("Cert file path: %s", config.mqtt_cert_file)
    logger.debug("Key file path: %s", config.mqtt_key_file)
    logger.debug("CA file path: %s", config.mqtt_root_ca_file)

    # Initialize RabbitMQ client and connect
    rabbitmq_client = AsyncRabbitMQClient()
    await rabbitmq_client.connect()

    # Initialize SerialCommunication with the config
    serial_comm = SerialCommunication(config.port, config.baudrate, config.timeout)

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
