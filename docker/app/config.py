from pydantic import BaseModel, Field
import yaml
import json
import tempfile
from typing import Any, List, Optional, Dict
import logging

# Logger setup
logger = logging.getLogger(__name__)


class AppConfig(BaseModel):
    port: str = Field(..., alias="PORT")
    baudrate: int = Field(..., alias="BAUDRATE")
    timeout: float = Field(..., alias="TIMEOUT")
    ucenter_json_file: str = Field(..., alias="UCENTER_JSON_FILE")
    localized: bool = Field(False, alias="LOCALIZED")
    region: str = Field(..., alias="REGION")
    lband: bool = Field(False, alias="LBAND")
    ubx_filename: Optional[str] = Field(None, alias="UBX_FILENAME")
    # Flatten MQTT Config
    mqtt_client_id: str
    mqtt_server_uri: str
    mqtt_key: str
    mqtt_cert: str
    mqtt_root_ca: Optional[str] = None
    mqtt_subscription_topics: List[str] = []
    mqtt_assistnow_topics: List[str] = []
    mqtt_data_topics: List[str] = []
    mqtt_dynamic_keys: Dict[str, Any]
    # Temporary file paths, not loaded from config but generated
    mqtt_cert_file: Optional[str] = None
    mqtt_key_file: Optional[str] = None
    mqtt_root_ca_file: Optional[str] = None
    tls_version: Optional[str] = None

    @classmethod
    def load_from_yaml(cls, yaml_file_path: str) -> "AppConfig":
        with open(yaml_file_path, "r", encoding="utf-8") as file:
            config_data = yaml.safe_load(file)

        if json_file_path := config_data.get("UCENTER_JSON_FILE", ""):
            with open(json_file_path, "r", encoding="utf-8") as file:
                mqtt_data = json.load(file)
                # Extract and rename MQTT settings to match AppConfig fields
                mqtt_config = mqtt_data["MQTT"]["Connectivity"]
                mqtt_subscriptions = mqtt_data["MQTT"].get("Subscriptions", {})
                config_data["mqtt_client_id"] = mqtt_config["ClientID"]
                config_data["mqtt_server_uri"] = mqtt_config["ServerURI"]
                # Renaming 'Key' to 'mqtt_key' and 'Cert' to 'mqtt_cert' based on JSON structure
                config_data["mqtt_key"] = mqtt_config["ClientCredentials"]["Key"]
                config_data["mqtt_cert"] = mqtt_config["ClientCredentials"]["Cert"]
                config_data["mqtt_root_ca"] = mqtt_config["ClientCredentials"].get(
                    "RootCA"
                )
                # Handle subscriptions
                config_data["mqtt_subscription_topics"] = mqtt_subscriptions.get(
                    "Key", {}
                ).get("KeyTopics", [])
                config_data["mqtt_assistnow_topics"] = mqtt_subscriptions.get(
                    "AssistNow", {}
                ).get("AssistNowTopics", [])
                # Flatten data topics
                data_topics = []
                for topic_group in mqtt_subscriptions.get("Data", {}).get(
                    "DataTopics", []
                ):
                    data_topics.extend(topic_group.split(";"))
                config_data["mqtt_data_topics"] = data_topics
                # Dynamic keys
                config_data["mqtt_dynamic_keys"] = mqtt_data["MQTT"].get(
                    "dynamickeys", {}
                )
                ssl_config = mqtt_data["MQTT"]["Connectivity"].get("SSL", {})
                config_data["tls_version"] = ssl_config.get("Protocol", "TLSv1.2")

        logger.info("Loaded configuration: %s", config_data)
        return cls.__config__.model_validate(config_data)

    def create_temp_files(self):
        # Create temporary files for certs and keys, if necessary
        self.mqtt_cert_file = (
            self._create_temp_file(self.mqtt_cert, "CERTIFICATE")
            if self.mqtt_cert
            else None
        )
        self.mqtt_key_file = (
            self._create_temp_file(self.mqtt_key, "RSA PRIVATE KEY")
            if self.mqtt_key
            else None
        )
        self.mqtt_root_ca_file = (
            self._create_temp_file(content=self.mqtt_root_ca, file_type="CERTIFICATE")
            if self.mqtt_root_ca
            else None
        )

    @staticmethod
    def _create_temp_file(content: str, file_type: str) -> str:
        content_formatted = (
            f"-----BEGIN {file_type}-----\n{content}\n-----END {file_type}-----"
        )
        temp_file = tempfile.NamedTemporaryFile(
            delete=False, mode="w", prefix=f"{file_type.lower()}_", suffix=".pem"
        )
        temp_file.write(content_formatted)
        temp_file.flush()
        return temp_file.name


def load_config(yaml_file_path: str) -> AppConfig:
    try:
        return AppConfig.load_from_yaml(yaml_file_path)
    except Exception as e:
        logger.error("Failed to load configuration: %s", e)
        raise
