import aio_pika
import asyncio
import logging

logger = logging.getLogger(__name__)

class AsyncRabbitMQClient:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.channel = None

    async def connect(self):
        try:
            # Using aio_pika for async connection, utilizing the config object
            self.connection = await aio_pika.connect_robust(
                f"amqp://{self.config.rabbitmq_username}:{self.config.rabbitmq_password}@{self.config.rabbitmq_host}:{self.config.rabbitmq_port}/"
            )
            self.channel = await self.connection.channel()
            await self.channel.declare_exchange(
                self.config.exchange_name, aio_pika.ExchangeType.DIRECT, durable=True
            )
        except Exception as e:
            logger.error("Failed to connect to RabbitMQ: %s", e)
            raise

    async def publish_message(self, message):
        try:
            # Obtain a reference to the custom exchange object
            exchange = await self.channel.get_exchange(self.config.exchange_name)
            # Publish message to the specific exchange with the correct routing key
            await exchange.publish(
                aio_pika.Message(body=message.encode()),
                routing_key=self.config.routing_key,
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