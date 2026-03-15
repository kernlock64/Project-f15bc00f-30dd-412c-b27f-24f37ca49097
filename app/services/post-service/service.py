from fastapi import FastAPI, Depends, Request, Form
import httpx
from sqlalchemy import *
import uuid
import json
import random

from httpx import Limits, AsyncClient, HTTPError

from db_init import get_async_session, engine
from model import PostDB
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession

import asyncio
import aio_pika
from aio_pika import ExchangeType

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


limits = Limits(
    max_connections=1000,
    max_keepalive_connections=100,
)


@asynccontextmanager
async def lifespan(cl: FastAPI):
    client = httpx.AsyncClient(limits=limits, timeout=5)
    cl.state.http_client = client
    factory = get_async_session()
    app.state.db = factory
    rabbit = RabbitMQGateway()
    await rabbit.connect()
    app.state.rabbit = rabbit.channel
    try:
        yield
    finally:
        await client.aclose()
        await engine.dispose()
        await rabbit.close()


app = FastAPI(lifespan=lifespan)


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
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
    )
    if not routing_key:
        await exchange.publish(message, routing_key="")

    await exchange.publish(message, routing_key=routing_key)


async def delete_user(
        uid: str,
        eng,
):
    cls = PostDB

    async with eng() as db:
        users = await db.execute(select(cls).where(cls.id == uid))
        user = users.scalar_one_or_none()

        await db.delete(user)
        await db.commit()
    return {
        "status": "True",
    }


async def listen_deletion(channel):
    exchange = await channel.declare_exchange("v1.exchange.deletion", ExchangeType.FANOUT)
    queue = await channel.declare_queue(exclusive=True)

    queue.bind(exchange)

    async with queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                data = json.loads(message.body.decode())
                uid = data["X-User-Id"]
                await delete_user(uid, app.state.db)

async def main(rabbit: RabbitMQGateway):
    channel = rabbit.channel

    await asyncio.gather(
        listen_deletion(channel),
    )


async def get_httpx(request: Request) -> httpx.AsyncClient:
    return request.app.state.http_client


async def get_db():
    factory = get_async_session()
    async with factory() as session:
        yield session

'''TEST DATA/TEST ENDPOINTS'''

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

asyncio.run(main(app.state.rabbit))