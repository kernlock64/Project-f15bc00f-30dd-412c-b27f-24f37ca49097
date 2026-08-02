from fastapi import FastAPI, Depends
from fastapi.exceptions import HTTPException
import httpx
from sqlalchemy import *
import uuid
import json
import os

from httpx import Limits, AsyncClient
from db_init import get_async_session, engine
from model import FavouriteDB
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
from expiringdict import ExpiringDict

import asyncio
import aio_pika
from shared.exception_handler import *
from shared.logger_middleware import *
from shared.logger import *

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

app.add_exception_handler(Exception, universal_server_error_handler)
app.add_exception_handler(StarletteException, universal_server_http_error_handler)
app.middleware("http")(request_trace)


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
        await db.execute(delete(cls).where(cls.uid == uid))
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


@app.get("/api/v1/favourites/is-favourite/{post_id}")
async def is_favourite(
        request: Request,
        post_id: str,
        db: AsyncSession = Depends(get_db),
):
    uid = request.headers["X-User-Id"]
    res = await db.execute(select(FavouriteDB.uid).where(FavouriteDB.pid == str(post_id)))
    favourite_uid = res.scalar_one_or_none()
    if favourite_uid != uid:
        return {
            "is_favourite": False
        }

    return {
        "is_favourite": True
    }


@app.post("/api/v1/favourites/{post_id}")
async def favourite_manager(
        request: Request,
        post_id: str,
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
        post_id: str,
        user_id: str,
        db: AsyncSession,
        client: AsyncClient,
):
    c = cache.get(post_id)
    if not c:
        resp = await client.get(f"http://post-service:5003/api/v1/posts/post/{post_id}")

        if resp.status_code != 200:
            logger.error(
                "External server error has occurred!", extra={
                    "status_code": resp.status_code,
                    "post_id": post_id,
                    "error": resp.text[:500],
                    "request_id": request_id.get(),
                }
            )

            raise HTTPException(
                status_code=resp.status_code,
                detail=resp.json().get("detail"),
            )

        cache[post_id] = resp.json()

        r = resp.json()

        db.add(
            FavouriteDB(id=str(uuid.uuid4()), pid=post_id, uid=user_id, p_title=str(r["title"]),
                        p_image=str(r["title_image"])))
        await db.commit()

        logger.info(
            "Favourites mark has been successfully added!", extra={
                "post_id": post_id,
                "user_id": user_id,
                "request_id": request_id.get(),
            }
        )

    else:
        db.add(
            FavouriteDB(id=str(uuid.uuid4()), pid=post_id, uid=user_id, p_title=str(c["title"]),
                        p_image=str(c["title_image"])))

        await db.commit()

        logger.info(
            "Favourites mark has been successfully added!", extra={
                "post_id": post_id,
                "user_id": user_id,
                "request_id": request_id.get(),
            }
        )


async def delete_favourite(
        post_id: int,
        user_id: str,
        db: AsyncSession,
):
    await db.execute(
        delete(FavouriteDB).where(FavouriteDB.uid == str(user_id), FavouriteDB.pid == str(post_id)))
    await db.commit()

    logger.info(
        "Favourites mark has been successfully deleted!", extra={
            "post_id": post_id,
            "user_id": user_id,
            "request_id": request_id.get(),
        }
    )
