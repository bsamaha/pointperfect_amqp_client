import asyncio
import logging
import sys
import os
import json
import uuid
from config import load_config
from rabbit import AsyncRabbitMQClient
from serial_communication import SerialCommunication
from pointperfect_client import PointPerfectClient
from common.experiments import Experiment
from common.hardware import Hardware

# Configure logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.getLevelName(os.getenv('LOGGING_LEVEL', 'INFO').upper()),
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)

async def setup_clients(config):
    rabbitmq_client = AsyncRabbitMQClient(config)
    await rabbitmq_client.connect()
    serial_comm = SerialCommunication(
        port=config.port,
        baudrate=config.baudrate,
        timeout=config.timeout,
        device_id=config.device_id,
        gnss_messages=config.gnss_messages,
        amqp_client=rabbitmq_client,
        experiment_id=config.experiment_id
    )
    point_perfect_client = PointPerfectClient(config, serial_comm)
    point_perfect_client.connect()
    return rabbitmq_client, serial_comm, point_perfect_client

async def register_device(config, rabbitmq_client):
    hardware = Hardware()
    hardware.set_default_attributes()
    hardware.update_from_config(config)
    hardware_dict = hardware.to_dict()
    hardware_dict["message_type"] = "device_registration"
    hardware_correlation_id = str(uuid.uuid4())
    # Send the device registration request and wait for the response
    try:
        response = await rabbitmq_client.publish_rpc_request(
            json_data=hardware_dict,
            routing_key="device_registration_key",  # Use the actual routing key for device registration
            correlation_id=hardware_correlation_id
        )

        # Assuming the response is JSON-encoded, decode it
        response_data = json.loads(response)

        # Handle the response data as needed
        logger.info("Device registration response received: %s", response_data)
        config.device_id = response_data


    except Exception as e:
        logger.error("Failed to register device: %s", e)
        raise
        # Handle the error appropriately (e.g., retry, exit, etc.)

async def main_loop(config):
    rabbitmq_client, serial_comm, point_perfect_client = await setup_clients(config)
    try:
        await register_device(config, rabbitmq_client)
        logger.info("Starting to read and send data")
        serial_comm = SerialCommunication(
            port=config.port,
            baudrate=config.baudrate,
            timeout=config.timeout,
            device_id=config.device_id,
            gnss_messages=config.gnss_messages,
            amqp_client=rabbitmq_client,
            experiment_id=config.experiment_id
        ) 
        await asyncio.gather(serial_comm.read_and_send_data())
    finally:
        serial_comm.close()
        point_perfect_client.disconnect()

async def main():
    config = load_config()
    retry_delay = 1  # Initial retry delay in seconds
    max_retry_delay = 32  # Maximum retry delay, to prevent excessive wait times

    while True:
        try:
            logger.info("Starting main loop")
            await main_loop(config)
            break  # Exit loop if main_loop completes successfully
        except Exception as e:
            logger.error("An error occurred: %s. Reconnecting in %s seconds...", e, retry_delay)
            await asyncio.sleep(retry_delay)
            retry_delay = min(retry_delay * 2, max_retry_delay)  # Exponential backoff with a cap
        finally:
            logger.info("Cleaning up resources before retrying...")
            # Here, add any necessary cleanup logic

if __name__ == "__main__":
    asyncio.run(main())