import asyncio
import logging
import sys
from datetime import datetime
import aio_pika
import pytz
from serial_communication import SerialCommunication
from config import load_config
from pointperfect_client import PointPerfectClient
from rabbit import AsyncRabbitMQClient

# Configure logging to write to stdout
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],  # Log to stdout
)

MAX_RECONNECT_ATTEMPTS = 5
RECONNECT_DELAY = 1  # in seconds, will be doubled with each attempt


async def main():
    # Load configuration directly using the load_config function
    config = load_config()
    # Initialize RabbitMQ client and connect, now passing the config object
    rabbitmq_client = AsyncRabbitMQClient(config)
    await rabbitmq_client.connect()

    # Initialize SerialCommunication with the config
    serial_comm = SerialCommunication(
        port=config.port,
        baudrate=config.baudrate,
        timeout=config.timeout,
        device_id=config.device_id,
        gnss_messages=config.gnss_messages,
        amqp_client=rabbitmq_client
    )
    # Initialize PointPerfectClient with the config and serial communication
    point_perfect_client = PointPerfectClient(config, serial_comm)

    # Connect the PointPerfectClient
    point_perfect_client.connect()

    try:
        # Gather and run asynchronous tasks
        await asyncio.gather(
            serial_comm.read_and_send_data(),
            # Add other async tasks if necessary
        )
    finally:
        # Clean up
        serial_comm.close()
        point_perfect_client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())