from fastapi import FastAPI, Depends, Request, Form, status
from fastapi.responses import JSONResponse
import httpx
from sqlalchemy import *
import uuid
import json
import os

from httpx import Limits, AsyncClient, HTTPError

from db_init import get_async_session, engine
from model import FavouriteDB
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from expiringdict import ExpiringDict

import asyncio
import aio_pika

limits = Limits(
    max_connections=700,
    max_keepalive_connections=50,
)

cache = ExpiringDict(max_len=100, max_age_seconds=180)


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

@app.exception_handler(Exception)
async def universal_server_error_handler(request: Request, exc: Exception):

    print(exc)

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Что-то пошло не так. Пожалуйста, обратитесь в службу поддержки, если ошибка повторится."}
    )

async def get_db():
    factory = app.state.db
    async with factory() as session:
        yield session


async def delete_user(
        uid: str,
        eng,
):
    cls = FavouriteDB
    async with eng() as db:
        await db.execute(delete(cls).where(cls.id == uid))
        await db.commit()

async def listen_deletion():
        async with app.state.d_queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    data = json.loads(message.body.decode())
                    uid = data["X-User-Id"]

                    await delete_user(uid, app.state.db)

async def changes_favourites(pid, eng, changed_data: dict) -> dict:
    cls = FavouriteDB
    async with eng() as db:

        posts = await db.execute(select(cls).where(cls.id == pid))
        post = posts.scalar_one_or_none()

        for key, value in changed_data.items():
            if not key in cls.__table__.columns and key not in ["pid", "uid", "id"]:
                raise Exception("No such parameter!")

            setattr(post, key, value)

            await db.commit()

    return None

async def deletion_favourites(pid, eng):
    cls = FavouriteDB
    async with eng() as db:
        await db.execute(delete(cls).where(cls.pid == pid))
        await db.commit()

async def listen_changes():
        async with app.state.d_queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    data = json.loads(message.body.decode())

                    if data["action"] == "post_deletion":
                        await deletion_favourites(data["pid"], app.state.db)

                    if data["action"] == "post_changes":
                        await changes_favourites(data["pid"], app.state.db, data["changes"])


async def main():
    await asyncio.gather(
        listen_deletion(),
        listen_changes()
    )


@app.get("/api/v1/favourites/get-favourites")
async def get_favourites(
        request: Request,
        db: AsyncSession = Depends(get_db),
):
        user_id = request.headers["X-User-Id"]
        fav = await db.execute(select(FavouriteDB).where(FavouriteDB.uid == str(user_id)))
        return {
            "favourites": [
                i for i in fav.scalars().all()
            ]
        }

@app.get("/api/v1/favourites/is-favourite")
async def is_favourite(
        request: Request,
        db: AsyncSession = Depends(get_db),
        post_id: int = Form(...),
):
        uid = request.headers["X-User-Id"]
        res = await db.execute(select(FavouriteDB.uid).where(FavouriteDB.pid == str(post_id)))
        favourite_uid = res.scalar_one_or_none()
        if favourite_uid != uid:
            raise Exception("No such post!")

        return None


@app.post("/api/v1/favourites/{post_id}")
async def favourite_manager(
        request: Request,
        post_id: int,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):
        uid = request.headers["X-User-Id"]
        res = await db.execute(
            select(FavouriteDB).where(FavouriteDB.uid == str(uid), FavouriteDB.pid == str(post_id)))
        res = res.scalar_one_or_none()
        if not res:
            return await add_favourite(post_id, uid, db, client)
        else:
            return await delete_favourite(post_id, uid, db)


async def add_favourite(
        post_id: int,
        user_id: str,
        db: AsyncSession,
        client: AsyncClient,
):

        c = cache.get(post_id)
        if not c:
            resp = await client.get("http://post-service:5003/api/v1/posts/get-post")

            if resp.status_code != 200:
                print(resp.text, resp.status_code)
                raise Exception

            cache[post_id] = resp.json()

            r = resp.json()

            db.add(
                FavouriteDB(id=str(uuid.uuid4()), pid=post_id, uid=user_id, p_title=str(r["p_title"]),
                            p_image=str(r["p_image"])))
            await db.commit()

        else:
            db.add(
                FavouriteDB(id=str(uuid.uuid4()), pid=post_id, uid=user_id, p_title=str(c["p_title"]),
                            p_image=str(c["p_image"])))

            await db.commit()

async def delete_favourite(
        post_id: int,
        user_id: str,
        db: AsyncSession,
):
        await db.execute(
            delete(FavouriteDB).where(FavouriteDB.uid == str(user_id), FavouriteDB.pid == str(post_id)))
        await db.commit()
