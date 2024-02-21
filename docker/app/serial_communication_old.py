import asyncio
import queue
import time
import json
from datetime import datetime
import pytz
from serial import Serial, SerialException
from pyubx2 import UBXReader, UBX_PROTOCOL, NMEA_PROTOCOL, RTCM3_PROTOCOL
import logging

logger = logging.getLogger(__name__)

class SerialCommunication:
    def __init__(self, port, baudrate, timeout, device_id, experiment_id, amqp_client, gnss_messages):
        self.stream = None
        self.message_queue = queue.Queue()
        self.running = True
        self.device_id = device_id
        self.amqp_client = amqp_client
        self._open_connection(port, baudrate, timeout)
        self.gnss_messages = gnss_messages
        self.experiment_id = experiment_id
        self.thread = asyncio.create_task(self.send_messages())

    def _open_connection(self, port, baudrate, timeout):
        try:
            self.stream = Serial(port, baudrate, timeout=timeout)
            logger.info("Serial port %s opened successfully.", port)
        except SerialException as e:
            logger.error("Failed to open serial port: %s", e)
            raise ConnectionError(f"Failed to open serial port: {e}") from e

    async def read_and_send_data(self):
        while True:
            if self.stream and self.stream.in_waiting:
                data_dict = MessageProcessor.process_data(stream=self.stream, device_id=self.device_id, gnss_messages=self.gnss_messages, experiment_id=self.experiment_id)
                if data_dict:
                    logger.info("Received GNSS message: %s", data_dict)
                    await self.amqp_client.publish_message(json.dumps(data_dict))
            else:
                await asyncio.sleep(0.01)  # Short sleep to yield control

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

class MessageProcessor:
    @staticmethod
    def process_data(stream, device_id, gnss_messages, experiment_id):
        try:
            ubx_reader = UBXReader(stream, protfilter=UBX_PROTOCOL | NMEA_PROTOCOL | RTCM3_PROTOCOL)
            _, parsed_data = ubx_reader.read()
            if parsed_data and parsed_data.identity in gnss_messages:
                if parsed_data.identity == "NAV-PVT":
                    logger.info("Processing NAV-PVT message: %s", parsed_data)
                else:
                    logger.info("Processing non-NAV-PVT message: %s", parsed_data)
                local_datetime = datetime.now()
                utc_datetime = local_datetime.astimezone(pytz.utc)
                utc_date = utc_datetime.date()
                try:
                    full_datetime = datetime.combine(utc_date, parsed_data.time)
                except Exception as e:
                    logger.info("Currently no time value from receiver: %s", e)
                    full_datetime = datetime.now()
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
                    "device_id": device_id,
                    "experiment_id": experiment_id
                }
                return data_dict
        except SerialException as e:
            logger.error("Error reading data from serial port: %s", e)
            return None