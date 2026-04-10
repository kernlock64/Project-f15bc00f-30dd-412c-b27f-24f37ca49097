import os
import uuid
import json
import random
from meilisearch_python_sdk import AsyncClient as MeiliClient

from fastapi import FastAPI, File, UploadFile, Depends, Request, Form, Body
import httpx
from datetime import datetime, timezone
from sqlalchemy import *
from httpx import Limits, AsyncClient
from db_init import get_async_session, engine
from model import PostDB
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
import aio_pika
import asyncio
from category_init import *


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

MEILI_URL = "http://meilisearch:7700"
MEILI_KEY = os.getenv("MEILISEARCH_KEY")
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
    meili_engine = MeiliClient(url=MEILI_URL, api_key=MEILI_KEY)

    index = meili_engine.index("products")
    await index.update_filterable_attributes([
        "price",
        "category_id",
        "attributes",
    ])
    await index.update_searchable_attributes([
        "title",
        "description"
    ])
    await index.update_displayed_attributes(["id", "title", "price", "image_url", "categories"])
    await index.update_ranking_rules([
        "words",
        "typo",
        "proximity",
        "attribute",
        "sort",
        "exactness"
    ])

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
    app.state.meilisearch = meili_engine
    app.state.task = asyncio.create_task((main()))

    try:
        yield
    finally:
        await client.aclose()
        await engine.dispose()
        await rabbit.close()
        await meili_engine.aclose()
        app.state.task.cancel()

async def get_httpx(request: Request) -> httpx.AsyncClient:
    return request.app.state.http_client


app = FastAPI(lifespan=lifespan)


async def get_db():
    factory = app.state.db
    async with factory() as session:
        yield session

async def get_meili():
    return app.state.meilisearch


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
            await client.delete(f"http://media-service:5001/api/v1/media/delete-image/{i}",
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

@app.get("/api/v1/posts/{path:path}")
async def get_posts(
        path: str,
):

    category_id = PATH_TO_ID.get(path)

    if not category_id:
        return {
            "status": "False",
            "message": "Страницы не существует!"
        }

    category = CATEGORIES_CACHE.get(category_id)

    if category["is_leaf"]:
        return {
            "status": "True",
            "category_id": category_id,
            "title": category["name"],
            "metadata": {
                "api_url": "/api/v1/posts/search",
                "base_filter": f"category_id = {category_id}",
                "index": "posts",
            }
        }

    else:
        cat = [i for i in CATEGORIES_CACHE.values() if i["parent_id"] == category_id]
        return {
            "status": "True",
            "categories": cat,

        }

@app.get("/api/v1/posts/search")
async def search_posts(
        page: int = 1,
        limit: int = 10,
        fil: str = None,
        q: str = None,
        meili: MeiliClient = Depends(get_meili),
):
    result = await meili.index("posts").search(q, filter=fil, page=page, limit=limit)

    return {
        "status": "True",
        "posts": result.hits if result.hits else [],
    }

@app.get("/api/v1/posts/{post_id}")
async def get_post(
        post_id: int,
        db: AsyncSession = Depends(get_db),
):
    res = await db.execute(select(PostDB).where(PostDB.id == int(post_id)))
    post = res.scalar_one_or_none()


    return {
        "status": "True",
        "title": post.title,
        "author_id": post.author_id,
        "description": post.description,
        "images": post.images,
        "directory_name": post.directory_name,
        "posted_at": post.posted_at,
        "category_id": post.category_id,

    }

@app.post("/api/v1/posts/add-post")
async def add_post():
    pass

@app.post("/api/v1/posts/changes-post")
async def changes_post():
    pass

@app.post("/api/v1/posts/delete-post")
async def delete_post():
    pass

@app.post("/api/v1/posts/delete-post")
async def delete_post():
    pass

@app.post("/api/v1/posts/report-post")
async def report_post():
    pass