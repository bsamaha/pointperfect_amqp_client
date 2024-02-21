from pydantic import BaseModel, Field
import json
import tempfile
from typing import Any, List, Optional, Dict, Set
import logging
import os

logger = logging.getLogger(__name__)

class UcenterJsonConfigLoader:
    def load_config(self, path: str) -> dict:
        with open(path, "r", encoding="utf-8") as file:
            config_data = json.load(file)["MQTT"]
        # Process MQTT configuration
        mqtt_config = config_data["Connectivity"]
        mqtt_subscriptions = config_data.get("Subscriptions", {})
        config = {
            "mqtt_client_id": mqtt_config["ClientID"],
            "mqtt_server_uri": mqtt_config["ServerURI"],
            "mqtt_key": mqtt_config["ClientCredentials"]["Key"],
            "mqtt_cert": mqtt_config["ClientCredentials"]["Cert"],
            "mqtt_root_ca": mqtt_config["ClientCredentials"].get("RootCA"),
            "mqtt_subscription_topics": mqtt_subscriptions.get("Key", {}).get("KeyTopics", []),
            "mqtt_assistnow_topics": mqtt_subscriptions.get("AssistNow", {}).get("AssistNowTopics", []),
            "mqtt_data_topics": self._flatten_data_topics(mqtt_subscriptions.get("Data", {}).get("DataTopics", [])),
            "mqtt_dynamic_keys": config_data.get("dynamickeys", {}),
            "tls_version": mqtt_config["SSL"].get("Protocol", "TLSv1.2"),
        }
        return config

    @staticmethod
    def _flatten_data_topics(data_topics):
        flattened_topics = []
        for topic_group in data_topics:
            flattened_topics.extend(topic_group.split(";"))
        return flattened_topics

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
        temp_file.close()
        return temp_file.name

class EnvironmentConfigLoader:
    @staticmethod
    def load_env_variables() -> dict:
        return {
            "PORT": os.getenv("PORT", "/dev/ttyUSB0"),
            "BAUDRATE": int(os.getenv("BAUDRATE", "9600")),
            "TIMEOUT": float(os.getenv("TIMEOUT", "1.0")),
            "RABBITMQ_HOST": os.getenv("RABBITMQ_HOST", "localhost"),
            "RABBITMQ_PORT": os.getenv("RABBITMQ_PORT", "5672"),
            "RABBITMQ_USERNAME": os.getenv("RABBITMQ_USERNAME", "guest"),
            "RABBITMQ_PASSWORD": os.getenv("RABBITMQ_PASSWORD", "guest"),
            "EXCHANGE_NAME": os.getenv("EXCHANGE_NAME", "gnss_exchange"),
            "ROUTING_KEY": os.getenv("ROUTING_KEY", "gnss_data"),
            "UCENTER_JSON_FILE": os.getenv("UCENTER_JSON_FILE"),
            "GNSS_MESSAGES": os.getenv("GNSS_MESSAGES", ""),
            "LOGGING_LEVEL": os.getenv("LOGGING_LEVEL", "INFO"),
            "DEVICE_ID": os.getenv("DEVICE_ID", ""),
            "EXPERIMENT_ID": os.getenv("EXPERIMENT_ID", "1"),
            "ALIAS": os.getenv("ALIAS", "blake_test_homeserver")
        }

class AppConfig(BaseModel):
    port: str = Field(..., alias="PORT")
    baudrate: int = Field(..., alias="BAUDRATE")
    timeout: float = Field(..., alias="TIMEOUT")
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
    rabbitmq_host: str = Field("localhost", alias="RABBITMQ_HOST")
    rabbitmq_port: str = Field("5672", alias="RABBITMQ_PORT")
    rabbitmq_username: str = Field("guest", alias="RABBITMQ_USERNAME")
    rabbitmq_password: str = Field("guest", alias="RABBITMQ_PASSWORD")
    exchange_name: str = Field("gnss_exchange", alias="EXCHANGE_NAME")
    routing_key: str = Field("gnss_data", alias="ROUTING_KEY")
    device_id: str = Field("blake_test_rpi", alias="DEVICE_ID")
    gnss_messages: Set[str] = Field(set(os.getenv("GNSS_MESSAGES", "").split(',')) if os.getenv("GNSS_MESSAGES") else set(), alias="GNSS_MESSAGES")
    logging_level: str = Field("INFO", alias="LOGGING_LEVEL")
    experiment_id: str = Field("1", alias="EXPERIMENT_ID")
    alias: str = Field("blake_test_homeserver", alias="ALIAS")

    @classmethod
    def from_env_and_json(cls, env_loader: EnvironmentConfigLoader, json_loader: UcenterJsonConfigLoader) -> "AppConfig":
        env_variables = env_loader.load_env_variables()
        gnss_messages_env = env_variables.get("GNSS_MESSAGES")
        if gnss_messages_env:
            env_variables["GNSS_MESSAGES"] = set(gnss_messages_env.split(","))
        json_file_path = env_variables.get("UCENTER_JSON_FILE")
        if not json_file_path:
            raise ValueError("UCENTER_JSON_FILE environment variable is not set or does not point to a valid file.")
        json_config = json_loader.load_config(json_file_path)
        
        # Create temporary files for MQTT credentials
        if "mqtt_cert" in json_config and json_config["mqtt_cert"]:
            json_config["mqtt_cert_file"] = json_loader._create_temp_file(json_config["mqtt_cert"], "CERTIFICATE")
        if "mqtt_key" in json_config and json_config["mqtt_key"]:
            json_config["mqtt_key_file"] = json_loader._create_temp_file(json_config["mqtt_key"], "RSA PRIVATE KEY")  # Updated to match old code
        if "mqtt_root_ca" in json_config and json_config["mqtt_root_ca"]:
            json_config["mqtt_root_ca_file"] = json_loader._create_temp_file(json_config["mqtt_root_ca"], "CERTIFICATE")
        
        config_data = {**env_variables, **json_config}
        return cls(**config_data)

def load_config() -> AppConfig:
    try:
        env_loader = EnvironmentConfigLoader()
        json_loader = UcenterJsonConfigLoader()
        config = AppConfig.from_env_and_json(env_loader, json_loader)
        return config
    except Exception as e:
        logger.error("Failed to load configuration: %s", e)
        raise
