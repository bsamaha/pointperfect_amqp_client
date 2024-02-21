import asyncio
import logging
import sys
import os
import uuid
import json
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
        # Handle the error appropriately (e.g., retry, exit, etc.)

async def main():
    config = load_config()
    logger.info("GNSS Messages: %s", config.gnss_messages)
    rabbitmq_client, serial_comm, point_perfect_client = await setup_clients(config)

    logger.info("Experiment registration complete")
    await register_device(config, rabbitmq_client)
    # await register_experiment(config, rabbitmq_client)
    try:
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

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    pending = asyncio.all_tasks(loop)
    for task in pending:
        task.cancel()
        try:
            loop.run_until_complete(task)
        except asyncio.CancelledError:
            pass
    loop.close()