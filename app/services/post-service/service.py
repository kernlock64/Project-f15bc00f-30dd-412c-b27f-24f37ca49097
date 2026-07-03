import os
import json
from geoalchemy2 import WKTElement
from meilisearch_python_sdk import AsyncClient as MeiliClient
from meilisearch_python_sdk.models.settings import MeilisearchSettings

from datetime import *
from fastapi import FastAPI, File, UploadFile, Depends, Request, Form, Body
import httpx
from sqlalchemy import *
from httpx import Limits, AsyncClient
from db_init import get_async_session, engine
from model import PostDB
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncSession
import aio_pika
import asyncio
from category_init import *
import secrets


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

    index = meili_engine.index("posts")

    task = await index.update_settings(MeilisearchSettings(
        filterable_attributes=[
            "price",
            "category_id",
            "attributes.*",
            "author_id"
        ],
        searchable_attributes=[
            "title",
            "description",
        ],
        displayed_attributes=[
            "id",
            "title",
            "price",
            "title_image",
            "category_id",
            "author_id",
            "author_name",
            "author_image",
            "attributes",
        ],
        ranking_rules=[
            "words",
            "typo",
            "proximity",
            "attribute",
            "sort",
            "exactness"
        ]
    ))

    await meili_engine.wait_for_task(task.task_uid)

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


@app.post("/api/v1/posts/manip")
async def manipulate(
        request: Request,
        db: AsyncSession = Depends(get_db),
        data: dict = Body,
        client: AsyncClient = Depends(get_httpx),
        meili: MeiliClient = Depends(get_meili),
):
    if request.headers.get("X-User-Id") != '0' or request.headers.get("X-Role") != '0':
        return {
            "status": "False",
            "message": "Unauthorized access!"
        }

    destination = request.headers.get("X-Scope")

    if destination[1] != "p":
        return {
            "status": "False",
            "message": "Incorrect scope!"
        }

    else:
        try:
            match destination[0]:

                case "d":
                    metadata = data["metadata"]
                    post_id = metadata["post_id"]

                    posts = await db.execute(select(PostDB).where(PostDB.id == int(post_id)))
                    post = posts.scalar_one_or_none()

                    if not post:
                        return {
                            "status": "False",
                            "message": "No such post!"
                        }

                    await client.delete(f"http://media-service:5001/api/v1/media/delete-image/{post.directory_name}",
                                        headers={"X-Key": MEDIA_KEY, "X-Directory": post.directory_name})

                    await db.delete(post)

                    await db.commit()

                    return {
                        "status": "True",
                    }

                case "a":

                    data = data["metadata"]
                    metadata = data["metadata"]
                    posts = data["posts"]

                    url = "http://user-service:5004/api/v1/users/user-check"

                    uid = metadata["author_id"]
                    price_list_name = metadata["price-list-name"]

                    req = await client.get(url, headers={"User-Id": uid, "Role": "2"})
                    if req.json()["status"] != "True":
                        return {
                            "status": "False",
                            "message": "Something went wrong!"
                        }

                    dat = req.json()
                    author_name = dat["author_name"]
                    author_image = dat["author_image"]

                    if metadata["is_images"] == "t":
                        for post in posts:
                            post_id = str((lambda: ''.join(
                                secrets.choice("abcdefghijkmnopqrstuvwxyzABCDEFGHIJKLMNPQRSTUVWXYZ23456789") for _ in
                                range(19)))())

                            p = PostDB(id=post_id, author_id=uid, title=post["title"],
                                       posted_at=datetime.now(timezone.utc),
                                       description=post["description"],
                                       images=dict(post).get("images"),
                                       directory_name=dict(post).get("images_dir"),
                                       category_id=post["category_path"],
                                       price_list_name=price_list_name,
                                       coordinates=WKTElement(
                                           f"POINT({post["coordinates"][0]} {post["coordinates"][1]})", srid=4326),
                                       price=int(post["price"]),
                                       )
                            db.add(p)
                            await db.commit()

                            client = meili.index("posts")

                            await client.add_documents([
                                {"id": post_id, "title": post["title"], "price": post["price"],
                                 "title_image": str(str(post["images_dir"]) + "/" + str(post['title_image'])),
                                 "category_id":
                                     post["category_path"],
                                 "author_id": uid,
                                 "author_name": author_name,
                                 "author_image": author_image,
                                 "attributes": {(j := k.split(":"))[0]: j[1] for k in post["attributes"].split(",")},
                                 }
                            ])

                    elif metadata["is_images"] == "f":
                        for post in posts:
                            post_id = str((lambda: ''.join(
                                secrets.choice("abcdefghijkmnopqrstuvwxyzABCDEFGHIJKLMNPQRSTUVWXYZ23456789") for _ in
                                range(19)))())

                            p = PostDB(id=post_id, author_id=uid, title=post["title"],
                                       posted_at=datetime.now(timezone.utc),
                                       description=post["description"],
                                       category_id=post["category_path"],
                                       price_list_name=price_list_name,
                                       coordinates=WKTElement(
                                           f"POINT({post["coordinates"][0]} {post["coordinates"][1]})", srid=4326),
                                       price=int(post["price"]),
                                       )
                            db.add(p)
                            await db.commit()

                            client = meili.index("posts")

                            await client.add_documents([
                                {"id": post_id, "title": post["title"], "price": post["price"],
                                 "category_id":
                                     post["category_path"],
                                 "author_id": uid,
                                 "author_name": author_name,
                                 "author_image": author_image,
                                 "attributes": {(j := k.split(":"))[0]: j[1] for k in post["attributes"].split(",")},
                                 }
                            ], primary_key="id")

                case "u":
                    cls = PostDB

                    metadata = data["metadata"]
                    changed_data = data["changed_data"]

                    post_id = metadata["post_id"]

                    posts = await db.execute(select(cls).where(PostDB.id == int(post_id)))
                    post = posts.scalar_one_or_none()

                    if not post:
                        return {
                            "status": "False",
                            "message": "No such post!",
                        }

                    for key, value in changed_data.items():
                        if not key in cls.__table__.columns and key not in ["author_id", "price_list_name",
                                                                            "date_created",
                                                                            "posted_at",
                                                                            "images", "id"]:
                            return {
                                "status": "False",
                                "message": "No such parameter!",
                            }
                        setattr(post, key, value)
                    await db.commit()
                case "g":

                    metadata = data["metadata"]
                    post_id = metadata["post_id"]

                    try:
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
                            "price_list_id": post.price_list_id,
                            "coordinates": post.coordinates,

                        }
                    except Exception as e:
                        return {
                            "status": "False",
                            "message": "No such post!",
                        }
                case "s":

                    metadata = data["metadata"]
                    q = metadata["query"]
                    fil = metadata["filter"]
                    page = metadata["page"]
                    limit = metadata["limit"]

                    meili = await get_meili()

                    if not q:
                        return {
                            "status": "False",
                            "message": "No query!",
                        }

                    if not page or not limit:
                        return {
                            "status": "False",
                            "message": "Incorrect request!",
                        }

                    result = await meili.index("posts").search(q, filter=fil, page=page, hits_per_page=limit)

                    return {
                            "status": "True",
                            "posts": result.hits if result.hits else [],
                        }

        except Exception as e:
            return {
                "status": "False",
                "message": str(e)
            }

        return {
            "status": "True",
        }


@app.get("/api/v1/posts/search")
async def search_posts(
        page: int = 1,
        limit: int = 10,
        fil: str = None,
        q: str = None,
        meili: MeiliClient = Depends(get_meili),
):
    offset = page * limit

    result = await meili.index("posts").search(q, filter=fil, offset=offset, limit=limit)

    return {
        "status": "True",
        "posts": result.hits if result.hits else [],
    }


@app.get("/api/v1/posts/{post_id}")
async def get_post(
        post_id: int,
        db: AsyncSession = Depends(get_db),
):
    try:
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
            "price_list_id": post.price_list_id,
            "coordinates": post.coordinates,

        }
    except Exception as e:
        return {
            "status": "False",
            "message": "No such post!",
        }

@app.post("/api/v1/posts/add-post")
async def add_post(
        request: Request,
        db: AsyncSession = Depends(get_db),
        changed_data: dict = Body,
):
    role = request.headers.get("X-Role")

    if role != "2":
        return {
            "status": "False",
            "message": "Unauthorized!"
        }

    return {
        "status": "False",
    }


@app.post("/api/v1/posts/changes-post")
async def changes_post(
        request: Request,
        post_id: int,
        db: AsyncSession = Depends(get_db),
        changed_data: dict = Body,
):
    role = request.headers.get("X-Role")

    if role != "2":
        return {
            "status": "False",
            "message": "Unauthorized!"
        }

    uid = request.headers.get("X-User-Id")
    cls = PostDB

    posts = await db.execute(select(cls).where(PostDB.id == int(post_id), PostDB.author_id == uid))
    post = posts.scalar_one_or_none()

    if not post:
        return {
            "status": "False",
            "message": "No such post!",
        }

    for key, value in changed_data.items():
        if not key in cls.__table__.columns and key not in ["author_id", "price_list_id", "date_created", "posted_at",
                                                            "images"]:
            return {
                "status": "False",
                "message": "No such parameter!",
            }
        setattr(post, key, value)
    await db.commit()

    return {
        "status": "True",
    }


@app.post("/api/v1/posts/delete-post")
async def delete_post(
        post_id: int,
        request: Request,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):
    role = request.headers.get("X-Role")

    if role != "2":
        return {
            "status": "False",
            "message": "Unauthorized!"
        }

    uid = request.headers.get("X-User-Id")

    if uid == "-1":
        return {
            "status": "False",
            "message": "Unauthorized!"
        }

    posts = await db.execute(select(PostDB).where(PostDB.id == int(post_id), PostDB.author_id == uid))
    post = posts.scalar_one_or_none()

    if not post:
        return {
            "status": "False",
            "message": "No such post!"
        }

    await client.delete(f"http://media-service:5001/api/v1/media/delete-image/{post.directory_name}",
                        headers={"X-Key": MEDIA_KEY, "X-Directory": post.directory_name})

    await db.delete(post)

    await db.commit()

    return {
        "status": "True",
    }


@app.post("/api/v1/posts/report-post")
async def report_post(
        request: Request,
        post_id: str,
        data: str,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):
    uid = request.headers.get("X-User-Id")

    if len(data) > 1500:
        return {
            "status": "False",
            "message": "Something went wrong!",
        }

    if await db.execute(exists(PostDB).where(PostDB.id == uid)):
        metadata = {
            "uid": uid,
            "role": request.headers.get("X-Role"),
            "post_id": post_id,
            "date_created": datetime.now(timezone.utc),
            "data": data,
        }

        await client.post("http://admin-service:5006/api/v1/management/web-health/request", data=metadata)

        return {
            "status": "True",
        }

    return {
        "status": "False",
        "message": "Something went wrong!",
    }


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
