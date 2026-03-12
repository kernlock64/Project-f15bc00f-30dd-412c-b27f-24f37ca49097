from fastapi import FastAPI
import random
import aio_pika
import os
import json
import asyncio

class RabbitMQGateway:

    def __init__(self):
        self.connection = None
        self.channel = None
        self.exchange = None

    async def connect(self):
        self.connection = await aio_pika.connect_robust("amqp://guest:guest@127.0.0.1/")
        self.channel = self.connection.channel

    async def close(self):
        if self.connection:
            await self.connection.close()


async def publish(rabbit: RabbitMQGateway, data: dict, routing_key=None):
    channel = rabbit.channel

    exchange = await channel.declare_exchange(
        os.getenv("PUB"),
        aio_pika.ExchangeType.FANOUT,
        durable=True
    )

    message_body = json.dumps(data).encode()
    message = aio_pika.Message(
        body=message_body,
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
    )
    if not routing_key:
        await exchange.publish(message, routing_key="")

    await exchange.publish(message, routing_key=routing_key)

'''TEST DATA/TEST ENDPOINTS'''

app = FastAPI()

@app.get("/posts/get-post")
async def get_post():
    return {"status": "True",
            "p_image": str(random.randint(1, 100)),
            "p_title": str(random.randint(1, 100)),
            }

@app.get("/posts/get-posts")
async def get_posts():
    return {"status": "True",
            "posts": []}

@app.post("/posts/deletion")
async def get_post():
    return {"status": "True",
            }


'''TEST DATA/TEST ENDPOINTS'''