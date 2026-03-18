import os
import uuid
import json
import random

from fastapi import FastAPI, File, UploadFile, Depends, Request, Form, Body
import httpx
from datetime import datetime, timezone
from sqlalchemy import *
from httpx import Limits, AsyncClient
from db_init import get_async_session, engine
from model import PostDB, CategoryDB
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
import aio_pika
import asyncio


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

MEDIA_KEY = os.getenv("MEDIA_KEY")

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

    user_actions = await rabbit.channel.declare_exchange(
        "v1.exchange.user_actions",
        aio_pika.ExchangeType.DIRECT,
        durable=True
    )

    deletion = await rabbit.channel.declare_queue("v1.queue.deletion")
    await deletion.bind(exchange=user_actions, routing_key="v1.queue.deletion")

    app.state.d_queue = deletion

    app.state.task = asyncio.create_task((main()))

    try:
        yield
    finally:
        await client.aclose()
        await engine.dispose()
        await rabbit.close()
        app.state.task.cancel()

async def get_httpx(request: Request) -> httpx.AsyncClient:
    return request.app.state.http_client


app = FastAPI(lifespan=lifespan)


async def get_db():
    factory = app.state.db
    async with factory() as session:
        yield session


async def delete_user(
        uid: str,
        eng,
):
    cls = PostDB
    async with eng() as db:
        db_r = await db.execute(select(cls.directory_name).where(cls.id == uid))
        image_ids = await db_r.scalars().all()
        await db.execute(delete(cls).where(cls.id == uid))
        client = app.state.http_client
        for i in image_ids:
            await client.delete(f"http://media-service:5001/media/delete-image/{i}",
                                headers={"X-Key": MEDIA_KEY, "X-Directory": i})
        await db.commit()
    return {
        "status": "True",
    }


async def listen_deletion():
    try:
        async with app.state.d_queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    data = json.loads(message.body.decode())
                    uid = data["X-User-Id"]

                    await delete_user(uid, app.state.db)
    except Exception as e:
        print(e)


async def main():
    await asyncio.gather(
        listen_deletion(),
    )

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

# asyncio.run(main(app.state.rabbit))