import logging
import paho.mqtt.client as mqtt
from urllib.parse import urlparse
from utils import map_tls_version

logger = logging.getLogger(__name__)


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

        parsed_uri = urlparse(self.config.mqtt_server_uri)
        self.mqtt_server = parsed_uri.hostname
        self.mqtt_port = parsed_uri.port or 8883  # Default MQTT SSL port
        self.tls_version = map_tls_version(
            self.config.tls_version
        )  # Map the TLS version string to the ssl constant

        self._initialize_mqtt_client()

    def _initialize_mqtt_client(self):
        self.mqtt_client = mqtt.Client(client_id=self.config.mqtt_client_id)

        tls_params = {
            "certfile": self.config.mqtt_cert_file,
            "keyfile": self.config.mqtt_key_file,
            "tls_version": self.tls_version,
            "ca_certs": self.config.mqtt_root_ca_file,
        }

        self.mqtt_client.tls_set(**tls_params)
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
            logger.error("Failed to connect to MQTT server, return code %s", rc)

    def _on_mqtt_disconnect(self, client, userdata, rc):
        logger.info(
            "Disconnected from MQTT server" if rc == 0 else "Unexpected MQTT disconnect"
        )
        self.current_topics = {}

    def _handle_message(self, client, userdata, msg):
        try:
            logger.debug("Received message on topic %s", msg.topic)
            # Handle different message types here
            self.serial_communication.enqueue_message(msg.payload)
        except Exception:
            logger.exception("Error handling message on %s", msg.topic)

    def connect(self):
        try:
            self.mqtt_client.connect(self.mqtt_server, self.mqtt_port)
            self.mqtt_client.loop_start()
        except Exception:
            logger.exception("Error connecting to MQTT server", exc_info=True)

    def disconnect(self):
        try:
            self.mqtt_client.disconnect()
            self.mqtt_client.loop_stop()
        except Exception:
            logger.exception("Error disconnecting MQTT client", exc_info=True)

    def subscribe(self, topic, qos=0):
        if topic not in self.current_topics:
            self.mqtt_client.subscribe(topic, qos)
            self.current_topics[topic] = qos
            logger.info("Subscribed to %s with QoS %s", topic, qos)

    def unsubscribe(self, topic):
        if topic in self.current_topics:
            self.mqtt_client.unsubscribe(topic)
            del self.current_topics[topic]
            logger.info("Unsubscribed from %s", topic)
