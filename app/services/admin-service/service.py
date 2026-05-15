from fastapi import FastAPI, Depends, Request, Form
import httpx
from sqlalchemy import *
import uuid
import json
import os
from datetime import *
import jwt

from httpx import Limits, AsyncClient, HTTPError

from db_init import get_async_session, engine
from model import TicketsDB, PriceListsDB
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from expiringdict import ExpiringDict

import asyncio
import aio_pika

limits = Limits(
    max_connections=1000,
    max_keepalive_connections=100,
)

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

@asynccontextmanager
async def lifespan(cl: FastAPI):
    client = httpx.AsyncClient(limits=limits, timeout=5)
    cl.state.http_client = client
    try:
        yield
    finally:
        await client.aclose()

async def get_httpx() -> httpx.AsyncClient:
    return app.state.http_client


async def get_db():
    factory = app.state.db
    async with factory() as session:
        yield session

app = FastAPI()

async def send_request(
        dest: str,
        data: dict,
        client: httpx.AsyncClient=get_httpx(),
):

    dest_url = {
        "u": "http://auth-service:5000/api/v1/users/manip",
        "p": "http://auth-service:5000/api/v1/posts/manip",
        "a": "http://auth-service:5000/api/v1/posts/manip",
    }
    if dest not in [
        "dp",
        "pp",
        "ap",
        "du",
        "pu",
        "aa",
    ]:
        return {
            "Invalid destination!"
        }

    payload = {
            "thm": "admin",
            "dest": dest,
            "exp": datetime.now(tz=timezone.utc) + timedelta(seconds=300)
    }
    prv_key = os.getenv("SECRET_KEY").replace("\\n", "\n")
    access_t = jwt.encode(payload, prv_key, algorithm="ES256")

    headers = {
        "admin_t": access_t,
        "data": data,
    }

    resp = await client.post(dest_url[dest[2]], headers=headers)

    return resp

@app.post("/api/v1/management/web-health/push")
async def push():
    pass

@app.get("/api/v1/management/web-health/get-ps")
async def get_posts():
    pass

@app.get("/api/v1/management/web-health/get-p")
async def get_post():
    pass

@app.get("/api/v1/management/web-health/get-u")
async def get_user():
    pass

@app.get("/api/v1/management/web-health/get-us")
async def get_users():
    pass

@app.get("/api/v1/management/web-health/m/{destination}")
async def manipulate(
        destination: str,
):
    pass

@app.post("/api/v1/management/web-health/request-a")
async def request():
    pass

@app.get("/api/v1/management/web-health/request-g")
async def requests_get():
    pass

@app.post("/api/v1/management/web-health/request-c")
async def requests_close():
    pass