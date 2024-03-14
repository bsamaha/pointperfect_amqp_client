import aio_pika
import logging
import json
import asyncio

logger = logging.getLogger(__name__)

class AsyncRabbitMQClient:
    def __init__(self, config):
        self.config = config
        self.connection = None
        self.channel = None
        self.exchange = None
        self.callback_queue = None
        self.futures = {}

    async def connect(self):
        try:
            await self._setup_connection()
            await self._setup_channel()
            await self._declare_exchange()
        except Exception as e:
            logger.error("Failed to connect to RabbitMQ: %s", e)
            raise

    async def _setup_connection(self):
        # Using getattr to provide default values if the attributes are not set
        retry_attempts = getattr(self.config, 'RABBITMQ_RETRY_ATTEMPTS', 5)
        retry_delay = getattr(self.config, 'RABBITMQ_INITIAL_RETRY_DELAY', 1)  # Default to 1 second
        heartbeat = getattr(self.config, 'RABBITMQ_HEARTBEAT', 60)  # Default to 60 seconds
        connection_timeout = getattr(self.config, 'RABBITMQ_CONNECTION_TIMEOUT', 30)  # Default to 30 seconds

        # Construct the connection string
        connection_string = f"amqp://{self.config.rabbitmq_username}:{self.config.rabbitmq_password}@{self.config.rabbitmq_host}:{self.config.rabbitmq_port}/"
        
        for attempt in range(retry_attempts):
            try:
                self.connection = await aio_pika.connect_robust(
                    connection_string,
                    heartbeat=heartbeat,
                    timeout=connection_timeout,
                    # If using SSL/TLS, configure the ssl_options here
                    # ssl_options=ssl.create_default_context(ssl.Purpose.CLIENT_AUTH),
                )
                logger.info("Successfully connected to RabbitMQ.")
                return  # Successful connection, exit the method
            except aio_pika.exceptions.AMQPConnectionError as e:
                logger.warning("Connection attempt %d failed: %s", attempt + 1, e)
                if attempt < retry_attempts - 1:
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 2  # Exponential backoff
                else:
                    logger.error("All connection attempts failed")
                    raise


    async def _setup_channel(self):
        self.channel = await self.connection.channel()

    async def _declare_exchange(self):
        self.exchange = await self.channel.declare_exchange(
            self.config.exchange_name, aio_pika.ExchangeType.DIRECT, durable=True
        )

    # Basic Publish
    async def publish_message(self, message_data, routing_key=None):
        try:
            message = self._prepare_message(message_data)
            routing_key = routing_key or self.config.routing_key
            await self.exchange.publish(message, routing_key=routing_key)
            logger.info("AMQP sent: %s", message.body)
        except Exception as e:
            logger.error("Failed to send message: %s", e)
            raise

    def _prepare_message(self, message_data, correlation_id=None, reply_to=None):
        if not isinstance(message_data, aio_pika.Message):
            if isinstance(message_data, dict):
                message_data = json.dumps(message_data)
            message_body = message_data.encode('utf-8')
            message = aio_pika.Message(
                body=message_body,
                correlation_id=correlation_id,
                reply_to=reply_to,
                content_type='application/json',
            )
        else:
            message = message_data
        return message

    # RPC Calls with Direct Reply-To
    async def start_rpc_consumer(self):
        self.callback_queue = await self.channel.declare_queue(exclusive=True)
        await self.callback_queue.consume(self.on_response, no_ack=True)

    async def publish_rpc_request(self, json_data, routing_key, correlation_id):
        await self.start_rpc_consumer()
        message = self._prepare_message(json_data, correlation_id, reply_to=self.callback_queue.name)
        await self.publish_message(message, routing_key=routing_key)
        future = asyncio.get_event_loop().create_future()
        self.futures[correlation_id] = future
        return await future

    async def on_response(self, message):
        correlation_id = message.correlation_id
        if correlation_id in self.futures:
            self.futures[correlation_id].set_result(message.body)
            del self.futures[correlation_id]
            logger.info("Received RPC reply: %s", message.body)

    # General Methods
    async def start_consuming(self, queue_name, callback):
        if queue_name == 'amq.rabbitmq.reply-to':
            logger.error("Direct consumption from 'amq.rabbitmq.reply-to' is not supported in this context.")
            return
        queue = await self.channel.declare_queue(queue_name, durable=True)
        await queue.consume(callback, no_ack=True)
        logger.info("Started consuming from %s", queue_name)

    async def close(self):
        if self.channel:
            await self.channel.close()
        if self.connection:
            await self.connection.close()
            logger.info("RabbitMQ connection closed.")
