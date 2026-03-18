from fastapi import FastAPI
import aio_pika
import json
import asyncio
import os

class RabbitMQGateway:

    def __init__(self):
        self.connection = None
        self.channel = None
        self.exchange = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust(f"amqp://admin:{os.getenv("DB_PASSWORD")}@rabbitmq/")
        self.channel = await self.connection.channel()

    async def close(self):
        if self.connection:
            await self.connection.close()


async def publish(rabbit: RabbitMQGateway, data: dict, exchange_name, routing_key=None):
    channel = rabbit.channel

    exchange = await channel.declare_exchange(
        exchange_name,
        aio_pika.ExchangeType.FANOUT,
        durable=True
    )

    message_body = json.dumps(data).encode()
    message = aio_pika.Message(
        body=message_body,
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
    )

    await exchange.publish(message, routing_key=routing_key or "")

app = FastAPI()