import os
import uuid
import json
import aiofiles
import io

from fastapi import FastAPI, File, UploadFile, Depends, Request, Form, Body
import httpx
from datetime import datetime, timezone
from sqlalchemy import *
from httpx import Limits, AsyncClient
from db_init import get_async_session, engine
from model import CustomersDB, ProdDB
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

    async with aiofiles.open("static/default_avatar.png", mode="rb") as f:
        avatar_cached = await f.read()

    deletion = await rabbit.channel.declare_queue("v1.queue.deletion")
    await deletion.bind(exchange=user_actions, routing_key="v1.queue.deletion")

    changes = await rabbit.channel.declare_queue("v1.queue.changes")
    await changes.bind(exchange=user_actions, routing_key="v1.queue.changes")

    app.state.d_queue = deletion
    app.state.c_queue = changes
    app.state.avatar_cached = avatar_cached
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

async def get_avatar():
    return app.state.avatar_cached


app = FastAPI(lifespan=lifespan)


async def get_db():
    factory = app.state.db
    async with factory() as session:
        yield session


async def delete_user(
        uid: str,
        role: str,
        eng,
):

    cls = CustomersDB if role == "1" else ProdDB


    async with eng() as db:

        users = await db.execute(select(cls).where(cls.id == uid))
        user = users.scalar_one_or_none()
        avatar = user.image

        await db.delete(user)


        client = app.state.http_client

        await client.delete(f"http://media-service:5001/api/v1/media/delete-image/{avatar}",
                                 headers={"X-Key": MEDIA_KEY})

        await db.commit()

    return {
        "status": "True",
    }

async def change_email(
        uid: str,
        role: str,
        eng,
        changed: str
):
    async with eng() as db:
        cls = CustomersDB if role == "1" else ProdDB
        users = await db.execute(select(cls).where(cls.id == uid))
        user = users.scalar_one_or_none()

        user.email = changed
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
                    role = data["X-Role"]

                    await delete_user(uid, role, app.state.db)
    except Exception as e:
        print(e)

async def listen_changes():
    try:
        async with app.state.c_queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    data = json.loads(message.body.decode())
                    uid = data["X-User-Id"]
                    role = data["X-Role"]
                    value = data["X-Changed-Value"]

                    await change_email(uid, role, app.state.db, value)
    except Exception as e:
        print(e)


async def main():
    await asyncio.gather(
        listen_deletion(),
        listen_changes()
    )


@app.get("/api/v1/users/get-user")
async def get_user(
        request: Request,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):
    uid = request.headers.get("X-User-Id")
    role = request.headers.get("X-Role")

    res1 = await client.get(f"http://favourite-service:5005/api/v1/favourites/get-favourites", headers=request.headers)

    if res1.json()["status"] != "True":
        return {
            "status": "False",
            "message": "Something went wrong!",
        }
    if role == "1":
        users = await db.execute(select(CustomersDB).where(CustomersDB.id == uid))
        user = users.scalar_one_or_none()
        return {
            "id": str(user.id),
            "username": str(user.username),
            "email": str(user.email),
            "image": str(user.image),
            "date_created": str(user.date_created),
            "full_name": str(user.full_name),
            "favourites": str(res1.json()["favourites"]),
        }
    if role == "2":

        res2 = await client.get(f"http://post-service:5003/api/v1/posts/get-posts", headers=request.headers)
        if res2.json()["status"] != "True":
            return {
                "status": "False",
                "message": "Something went wrong!",
            }

        users = await db.execute(select(ProdDB).where(ProdDB.id == uid))
        user = users.scalar_one_or_none()
        return {
            "id": str(user.id),
            "name": str(user.name),
            "email": str(user.email),
            "image": str(user.image),
            "date_created": str(user.date_created),
            "full_name": str(user.full_name),
            "telephone": str(user.telephone),
            "description": str(user.description),
            "delivery_description": str(user.delivery_description),
            "favourites": str(res1.json()["favourites"]),
            "posts": str(res2.json()["posts"]),
        }
    else:
        return {
            "status": "False",
            "message": "Unknown error!",
        }

@app.get("/api/v1/users/{uid}")
async def get_user(
        request: Request,
        uid: str,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):

    res = await client.get(f"http://post-service:5003/api/v1/posts/get-posts", headers=request.headers)
    if res.json()["status"] != "True":
        return {
                "status": "False",
                "message": "Something went wrong!",
        }

    users = await db.execute(select(ProdDB).where(ProdDB.id == uid))
    user = users.scalar_one_or_none()
    return {
            "id": str(user.id),
            "name": str(user.name),
            "email": str(user.email),
            "image": str(user.image),
            "date_created": str(user.date_created),
            "full_name": str(user.full_name),
            "telephone": str(user.telephone),
            "description": str(user.description),
            "delivery_description": str(user.delivery_description),
            "posts": str(res.json()["posts"]),
    }


@app.post("/api/v1/users/add-user")
async def add_user(
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
        username: str = Form(...),
        email: str = Form(...),
        role: str = Form(...),
        uid: str = Form(...),
        telephone: str = Form(...),
        avatar = Depends(get_avatar),
):
    async def read_file():
        stream = io.BytesIO(avatar)
        while chunk := stream.read(64 * 1024):
            yield chunk

    filename = str(uuid.uuid4())

    req = client.build_request(
        method="POST",
        url="http://media-service:5001/api/v1/media/add-image/png",
        headers={"X-Key": MEDIA_KEY, "X-Name": filename},
        content=read_file(),
    )

    resp = await client.send(req, stream=True)

    if resp.status_code != 200:
        return {
            "status": "False",
            "message": "Error adding image",
        }
    else:
        user = CustomersDB(id=uid, username=username, email=email, date_created=datetime.now(timezone.utc),
                           image=str(filename + '.' + "png")) if role == "1" else ProdDB(
            name=username, id=uid, email=email,
            date_created=datetime.now(
                timezone.utc),
            image=str(filename + '.' + "png"),
            telephone=telephone, )
        db.add(user)
        await db.commit()

        return {
            "status": "True",
        }


@app.post("/api/v1/users/change-userdata")
async def change_userdata(
        request: Request,
        db: AsyncSession = Depends(get_db),
        changed_data: dict = Body,
):
    uid = request.headers.get("X-User-Id")
    role = request.headers.get("X-Role")
    cls = CustomersDB if role == "1" else ProdDB

    users = await db.execute(select(cls).where(cls.id == uid))
    user = users.scalar_one_or_none()

    for key, value in changed_data.items():
        if not key in cls.__table__.columns:
            return {
                "status": "False",
                "message": "No such parameter!",
            }
        setattr(user, key, value)

    return {
        "status": "True",
    }


@app.post("/api/v1/users/change-avatar")
async def change_avatar(
        request: Request,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
        avatar: UploadFile = File(),
):
    uid = request.headers.get("X-User-Id")
    role = request.headers.get("X-Role")
    cls = CustomersDB if role == "1" else ProdDB

    filename = await db.execute(select(cls.image).where(cls.id == uid))

    async def read_file():
        while chunk := await avatar.read(64 * 1024):
            yield chunk

    req = client.build_request(
        method="POST",
        url=f"http://media-service:5001/api/v1/media/add-image/{filename}",
        headers={"X-Key": MEDIA_KEY, "X-Name": filename},
        content=read_file(),
    )

    resp = await client.send(req, stream=True)

    if resp.status_code != 200:
        return {
            "status": "False",
            "message": "Error adding image",
        }
    else:
        return {
            "status": "True",
        }
