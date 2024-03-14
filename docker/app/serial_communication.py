# Import necessary modules at the top
from abc import ABC, abstractmethod
from datetime import datetime, timezone
import time
from pyubx2 import UBXReader, NMEA_PROTOCOL, RTCM3_PROTOCOL, UBX_PROTOCOL
from serial import SerialException
import logging
import os
import asyncio
import queue
from serial import Serial
import json

logger = logging.getLogger(__name__)
logger.setLevel(os.getenv("LOG_LEVEL", "INFO"))

# Define the base handler interface
class MessageHandler(ABC):
    @abstractmethod
    def process(self, parsed_data, device_id, experiment_id):
        pass

# Implement a handler for GNGGA messages
class GNGGAHandler(MessageHandler):
    def process(self, parsed_data, device_id, experiment_id):
        # Assuming 'diff_age' needs to be handled or set to a default value
        diff_age = getattr(parsed_data, 'diff_age', None)
        data_dict = {
            "message_type": parsed_data.identity,
            "full_time": datetime.now().isoformat(),  # Simplified for demonstration
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
    

class NAVPVTHandler(MessageHandler):
    def process(self, parsed_data, device_id, experiment_id):
        logger.info("Processing NAV-PVT message: %s", parsed_data)
        # Convert iTOW (Time of Week) to a datetime object, assuming iTOW is in seconds
        # This is a placeholder; adjust according to your actual time handling needs
        full_time = datetime.now(tz=timezone.utc).isoformat()  # Simplified for demonstration

        data_dict = {
            "message_type": "NAV-PVT",
            "full_time": full_time,
            "year": parsed_data.year,
            "month": parsed_data.month,
            "day": parsed_data.day,
            "hour": parsed_data.hour,
            "min": parsed_data.min,
            "second": parsed_data.second,
            "validDate": parsed_data.validDate,
            "validTime": parsed_data.validTime,
            "tAcc": parsed_data.tAcc,
            "fixType": parsed_data.fixType,
            "gnssFixOk": parsed_data.gnssFixOk,
            "numSV": parsed_data.numSV,
            "lon": parsed_data.lon,
            "lat": parsed_data.lat,
            "height": parsed_data.height,
            "hMSL": parsed_data.hMSL,
            "hAcc": parsed_data.hAcc,
            "vAcc": parsed_data.vAcc,
            "velN": parsed_data.velN,
            "velE": parsed_data.velE,
            "velD": parsed_data.velD,
            "gSpeed": parsed_data.gSpeed,
            "headMot": parsed_data.headMot,
            "sAcc": parsed_data.sAcc,
            "headAcc": parsed_data.headAcc,
            "pDOP": parsed_data.pDOP,
            "device_id": device_id,
            "experiment_id": experiment_id
        }
        return data_dict

class MessageReader:
    """
    Responsible for reading messages from a stream.
    """
    @staticmethod
    def read_messages(stream, protfilter):
        ubx_reader = UBXReader(stream, protfilter=protfilter)
        while stream.in_waiting:
            try:
                _, parsed_data = ubx_reader.read()
                if parsed_data is None:
                    break  # No more data available
                yield parsed_data
            except Exception as e:
                logger.error("Error reading GNSS message: %s", e)
                continue  # Proceed with the next iteration even if the current one fails

class MessageProcessor:
    """
    Processes GNSS messages using registered handlers.
    """
    handlers = {
        'GNGGA': GNGGAHandler(),
        'NAV-PVT': NAVPVTHandler(),
    }

    @classmethod
    def process_data(cls, stream, device_id, gnss_messages, experiment_id):
        message_reader = MessageReader()
        try:
            for parsed_data in message_reader.read_messages(stream, UBX_PROTOCOL | NMEA_PROTOCOL | RTCM3_PROTOCOL):
                if parsed_data.identity not in gnss_messages:
                    logger.debug("Message type not in GNSS messages: %s", parsed_data.identity)
                    continue

                handler = cls.handlers.get(parsed_data.identity)
                if not handler:
                    logger.warning("No handler for message type: %s", parsed_data.identity)
                    continue

                return handler.process(parsed_data, device_id, experiment_id)
        except SerialException as e:
            logger.error("Error reading data from serial port: %s", e)
        except Exception as e:
            logger.error("Unexpected error in MessageProcessor: %s", e)
        finally:
            if stream.in_waiting:
                logger.debug("Data still available in stream after processing.")
            else:
                logger.debug("No more data available in stream.")
        return None

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

    
    def enqueue_message(self, message):
        self.message_queue.put(message)

    def _open_connection(self, port, baudrate, timeout):
        try:
            self.stream = Serial(port, baudrate, timeout=timeout)
            logger.info("Serial port %s opened successfully.", port)
        except SerialException as e:
            logger.error("Failed to open serial port: %s", e)
            raise ConnectionError(f"Failed to open serial port: {e}") from e

    async def read_and_send_data(self):
        while True:
            if not (self.stream and self.stream.in_waiting):
                await asyncio.sleep(0.01)  # Short sleep to yield control
                continue

            data_dict = MessageProcessor.process_data(self.stream, self.device_id, self.gnss_messages, self.experiment_id)
            if data_dict:
                logger.info("Received GNSS message: %s", data_dict)
                await self.amqp_client.publish_message(json.dumps(data_dict))


    async def send_messages(self):
        while self.running:
            if not self.message_queue.empty():
                message = self.message_queue.get()
                try:
                    self.stream.write(message)
                    logger.debug("Serial Message Sent: %s", message)
                except SerialException as e:
                    logger.error("Error sending message: %s", e)
            await asyncio.sleep(0.01)

    def close(self):
        self.running = False
        self.thread.cancel()
        if self.stream:
            self.stream.close()
            logger.info("Serial port closed.")