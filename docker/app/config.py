import yaml
import os
import json
import tempfile
from pyubx2 import NMEA_PROTOCOL, UBX_PROTOCOL, RTCM3_PROTOCOL
import logging

logger = logging.getLogger(__name__)

# Constants for certificate and key formatting
KEY_HEADER = "-----BEGIN RSA PRIVATE KEY-----\n"
KEY_FOOTER = "\n-----END RSA PRIVATE KEY-----\n"
CERT_HEADER = "-----BEGIN CERTIFICATE-----\n"
CERT_FOOTER = "\n-----END CERTIFICATE-----\n"


class Config:
    PORT = None
    BAUDRATE = None
    TIMEOUT = None
    PROTOCOL_FILTER = None
    LOCALIZED = None
    REGION = None

    def __init__(self, yaml_config):
        self.update_from_yaml(yaml_config)
        self.load_ucenter_json_file(yaml_config["UCENTER_JSON_FILE"])
        self.load_mqtt_credentials()

    def update_from_yaml(self, config_data):
        # Update all attributes from the YAML file
        self.PORT = config_data.get("PORT", self.PORT)
        self.UBX_FILENAME = config_data.get("UBX_FILENAME", False)
        self.BAUDRATE = config_data.get("BAUDRATE", self.BAUDRATE)
        self.TIMEOUT = config_data.get("TIMEOUT", self.TIMEOUT)
        self.PROTOCOL_FILTER = config_data.get("PROTOCOL_FILTER", self.PROTOCOL_FILTER)
        self.LOCALIZED = config_data.get("LOCALIZED", self.LOCALIZED)
        self.REGION = config_data.get("REGION", self.REGION)
        for key, value in config_data.items():
            setattr(self, key, value)

    @classmethod
    def load_ucenter_json_file(cls, filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as file:
                data = json.load(file)
                mqtt_config = data.get("MQTT", {})

                connectivity = mqtt_config.get("Connectivity", {})
                cls.MQTT_CLIENT_ID = connectivity.get("ClientID")
                cls.MQTT_SERVER_URI = connectivity.get("ServerURI")

                credentials = connectivity.get("ClientCredentials", {})
                cls.MQTT_KEY = credentials.get("Key")
                cls.MQTT_CERT = credentials.get("Cert")
                cls.MQTT_ROOT_CA = credentials.get("RootCA")

                subscriptions = mqtt_config.get("Subscriptions", {})
                cls.MQTT_SUBSCRIPTION_TOPICS = subscriptions.get("Key", {}).get(
                    "KeyTopics", []
                )
                cls.MQTT_ASSISTNOW_TOPICS = subscriptions.get("AssistNow", {}).get(
                    "AssistNowTopics", []
                )
                cls.MQTT_DATA_TOPICS = subscriptions.get("Data", {}).get(
                    "DataTopics", []
                )

                cls.MQTT_DYNAMIC_KEYS = mqtt_config.get("dynamickeys", {})

        except FileNotFoundError as e:
            raise FileNotFoundError(f"File not found: {filepath}") from e
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(
                f"File is not valid JSON: {filepath}", e.doc, e.pos
            )

    def load_mqtt_credentials(self):
        cert_content = CERT_HEADER + self.MQTT_CERT + CERT_FOOTER
        key_content = KEY_HEADER + self.MQTT_KEY + KEY_FOOTER

        # Create temporary files for certificate and key
        self.MQTT_CERT_FILE = create_temp_file(
            cert_content, prefix="device-", suffix="-pp-cert.crt"
        )
        self.MQTT_KEY_FILE = create_temp_file(
            key_content, prefix="device-", suffix="-pp-key.pem"
        )

        # Verify the files exist
        if not os.path.exists(self.MQTT_CERT_FILE) or not os.path.exists(
            self.MQTT_KEY_FILE
        ):
            logger.error("Failed to create MQTT certificate or key file.")
            raise FileNotFoundError("Failed to create MQTT certificate or key file.")


def load_config_from_yaml(yaml_file_path):
    with open(yaml_file_path, "r") as file:
        config_data = yaml.safe_load(file)
    return Config(config_data)


def create_temp_file(content, prefix, suffix):
    """
    Create a temporary file with the given content.
    """
    file_descriptor, file_path = tempfile.mkstemp(prefix=prefix, suffix=suffix)
    with os.fdopen(file_descriptor, "w") as temp_file:
        temp_file.write(content)
    return file_path
