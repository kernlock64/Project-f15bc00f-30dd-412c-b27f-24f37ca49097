import os
import json
import sqlalchemy
from geoalchemy2 import WKTElement
from meilisearch_python_sdk import AsyncClient as MeiliClient
from meilisearch_python_sdk.models.settings import MeilisearchSettings

from datetime import *
from fastapi import FastAPI, Depends, Request, Body, status, HTTPException, Form
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
from shared.exception_handler import *


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

    await meili_engine.create_index(uid="posts")
    index = meili_engine.index("posts")

    await index.update(primary_key="id")

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


app.add_exception_handler(Exception, universal_server_error_handler)
app.add_exception_handler(StarletteException, universal_server_http_error_handler)

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
        db_r = await db.execute(select(cls.directory_name).where(cls.author_id == uid))
        image_ids = db_r.scalars().all()
        await db.execute(delete(cls).where(cls.author_id == uid))
        client = app.state.http_client
        for i in image_ids:
            res = await client.delete(f"http://media-service:5001/api/v1/media/delete-image/{i}",
                                      headers={"X-Key": MEDIA_KEY, "X-Directory": i})

            if res.status_code != 200:
                print(res.text, res.status_code, )
                raise HTTPException(
                    status_code=res.status_code,
                    detail=res.json().get("details"),
                )

        await db.commit()


async def listen_deletion():
    async with app.state.d_queue.iterator() as queue_iter:
        async for message in queue_iter:
            async with message.process():
                data = json.loads(message.body.decode())
                uid = data["X-User-Id"]

                await delete_user(uid, app.state.db)


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
        raise HTTPException(
            status_code=403,
            detail="FORBIDDEN"
        )

    destination = request.headers.get("X-Scope")

    if destination[1] != "p":
        raise HTTPException(
            status_code=403,
            detail="MISS_SCOPE"
        )

    else:
        match destination[0]:

            case "d":
                metadata = data["metadata"]
                post_id = metadata.get("post_id")

                if not post_id:
                    price_list_name = metadata.get("price-list-name")

                    if not price_list_name:
                        raise HTTPException(
                            status_code=404,
                            detail="NO_SUCH_PRICE_LIST"
                        )

                    db_r = await db.execute(
                        select(PostDB.directory_name).where(PostDB.price_list_name == price_list_name))
                    image_ids = db_r.scalars().all()
                    await db.execute(delete(PostDB).where(PostDB.price_list_name == price_list_name))
                    for i in image_ids:
                        res = await client.delete(f"http://media-service:5001/api/v1/media/delete-image/{i}",
                                                  headers={"X-Key": MEDIA_KEY, "X-Directory": i})

                        if res.status_code != 200:
                            print(res.text, res.status_code, )
                            raise HTTPException(
                                status_code=res.status_code,
                                detail=res.json().get("details"),
                            )

                    await db.commit()
                    return None

                posts = await db.execute(select(PostDB).where(PostDB.id == str(post_id)))
                post = posts.scalar_one_or_none()

                if not post:
                    raise HTTPException(
                        status_code=404,
                        detail="NO_SUCH_POST"
                    )

                resp = await client.delete(
                    f"http://media-service:5001/api/v1/media/delete-image/{post.directory_name}",
                    headers={"X-Key": MEDIA_KEY, "X-Directory": post.directory_name})

                if resp.status_code != 200:
                    print(resp.text, resp.status_code)
                    raise HTTPException(
                        status_code=resp.status_code,
                        detail=resp.json().get("details"),
                    )

                await db.delete(post)

                await db.commit()
                return None

            case "a":

                data = data["metadata"]
                metadata = data["metadata"]
                posts = data["posts"]

                url = "http://user-service:5004/api/v1/users/user-check"

                uid = metadata["author_id"]
                price_list_name = metadata["price-list-name"]

                req = await client.get(url, headers={"User-Id": uid, "Role": "2"})
                if req.status_code != 200:
                    print(req.text, req.status_code)
                    raise HTTPException(
                        status_code=req.status_code,
                        detail=req.json().get("details"),
                    )

                dat = req.json()
                author_name = dat["author_name"]
                author_image = dat["author_image"]

                try:

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
                                       title_image=str(str(post["images_dir"]) + "/" + str(post['title_image']))
                                       )
                            db.add(p)

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

                            await db.commit()

                            return None

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

                            await db.commit()
                            return None

                except sqlalchemy.exc.IntegrityError:
                    raise HTTPException(
                        status_code=400,
                        detail="ALREADY_EXISTS"
                    )

            case "u":
                cls = PostDB

                metadata = data["metadata"]
                changed_data = metadata["changed_data"]

                post_id = metadata["post_id"]

                posts = await db.execute(select(cls).where(PostDB.id == str(post_id)))
                post = posts.scalar_one_or_none()

                if not post:
                    raise HTTPException(
                        status_code=404,
                        detail="NO_SUCH_POST"
                    )

                for key, value in changed_data.items():
                    if not key in cls.__table__.columns and key not in ["author_id", "price_list_name",
                                                                        "date_created",
                                                                        "posted_at",
                                                                        "images", "id"]:
                        raise HTTPException(
                            status_code=404,
                            detail="NO_SUCH_PARAMETER"
                        )
                    setattr(post, key, value)
                await db.commit()
                return None
            case "g":

                metadata = data["metadata"]
                post_id = metadata["post_id"]

                res = await db.execute(select(PostDB).where(PostDB.id == str(post_id)))
                post = res.scalar_one_or_none()

                if not post:
                    raise HTTPException(
                        status_code=404,
                        detail="NO_SUCH_POST"
                    )

                return {
                    "title": post.title,
                    "author_id": post.author_id,
                    "description": post.description,
                    "images": post.images,
                    "directory_name": post.directory_name,
                    "posted_at": post.posted_at,
                    "category_id": post.category_id,
                    "price_list_id": post.price_list_id,
                    "coordinates": post.coordinates,
                    "title_image": post.title_image,

                }

            case "s":

                metadata = data["metadata"]
                q = metadata["query"]
                fil = metadata["filter"]
                page = metadata["page"]
                limit = metadata["limit"]

                meili = await get_meili()

                if not q:
                    raise HTTPException(
                        status_code=404,
                        detail="NO_QUERY"
                    )

                if not page or not limit:
                    raise HTTPException(
                        status_code=400,
                    )
                result = await meili.index("posts").search(q, filter=fil, page=page, hits_per_page=limit)

                return {
                    "posts": result.hits if result.hits else [],
                }

            case _:
                raise HTTPException(
                    status_code=404,
                    detail="Incorrect scope!",
                )


@app.get("/api/v1/posts/search")
async def search_posts(
        page: int = 1,
        limit: int = 10,
        fil: str = None,
        q: str = None,
        meili: MeiliClient = Depends(get_meili),
):
    offset = (page - 1) * limit

    result = await meili.index("posts").search(q, filter=fil, offset=offset, limit=limit)

    return {
        "posts": result.hits if result.hits else [],
    }


@app.get("/api/v1/posts/post/{post_id}")
async def get_post(
        request: Request,
        post_id: str,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):
    res = await db.execute(select(PostDB).where(PostDB.id == str(post_id)))
    post = res.scalar_one_or_none()

    if not post:
        raise HTTPException(
            status_code=404,
            detail="No such post!"
        )

    res1 = await client.get(f"http://favourite-service:5005/api/v1/favourites/is-favourite/{post_id}",
                            headers=request.headers)

    if res1.status_code != 200:
        print(res1.text, res1.status_code)
        raise HTTPException(
            status_code=res1.status_code,
            detail=res1.json().get("details"),
        )

    res2 = await client.get(f"http://user-service:5004/api/v1/users/producers/{post.author_id}")

    if res2.status_code != 200:
        print(res2.text, res2.status_code)
        raise HTTPException(
            status_code=res2.status_code,
            detail=res2.json().get("details"),
        )

    return {
        "post_id": post.id,
        "title": post.title,
        "author_id": post.author_id,
        "author": res2.json(),
        "description": post.description,
        "images": post.images,
        "directory_name": post.directory_name,
        "posted_at": post.posted_at,
        "category_id": post.category_id,
        "price_list_name": post.price_list_name,
        "coordinates": post.coordinate,
        "title_image": post.title_image,
        "is_favourite": "True" if res1.json()["is_favourite"] else "False",
    }


@app.post("/api/v1/posts/add-post")
async def add_post(
        db: AsyncSession = Depends(get_db),
        changed_data: dict = Body,
):
    raise HTTPException(
        status_code=503,
        detail="Ведутся технические работы. Пожалуйста, попробуйте позже."
    )


@app.post("/api/v1/posts/changes-post")
async def changes_post(
        request: Request,
        post_id: int,
        db: AsyncSession = Depends(get_db),
        changed_data: dict = Body,
):
    role = request.headers.get("X-Role")

    if role != "2":
        raise HTTPException(
            status_code=403,
            detail="Unauthorized!",
        )

    uid = request.headers.get("X-User-Id")
    cls = PostDB

    posts = await db.execute(select(cls).where(PostDB.id == int(post_id), PostDB.author_id == uid))
    post = posts.scalar_one_or_none()

    if not post:
        raise HTTPException(
            status_code=404,
            detail="NO_SUCH_POST"
        )

    for key, value in changed_data.items():
        if not key in cls.__table__.columns and key not in ["author_id", "price_list_id", "date_created", "posted_at",
                                                            "images"]:
            raise HTTPException(
                status_code=404,
                detail="NO_SUCH_PARAMETER"
            )
        setattr(post, key, value)

    await db.commit()
    return None


@app.post("/api/v1/posts/delete-post")
async def delete_post(
        post_id: int,
        request: Request,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):
    role = request.headers.get("X-Role")

    if role != "2":
        raise HTTPException(
            status_code=403,
            detail="Unauthorized!",
        )

    uid = request.headers.get("X-User-Id")

    if uid == "-1":
        raise HTTPException(
            status_code=403,
            detail="Unauthorized!",
        )

    posts = await db.execute(select(PostDB).where(PostDB.id == int(post_id), PostDB.author_id == uid))
    post = posts.scalar_one_or_none()

    if not post:
        raise HTTPException(
            status_code=404,
            detail="No such post!",
        )
    resp = await client.delete(f"http://media-service:5001/api/v1/media/delete-image/{post.directory_name}",
                               headers={"X-Key": MEDIA_KEY, "X-Directory": post.directory_name})

    if resp.status_code != 200:
        print(resp.text, resp.status_code)
        raise HTTPException(
            status_code=resp.status_code,
            detail=resp.json().get("details"),
        )

    await db.delete(post)

    await db.commit()


@app.post("/api/v1/posts/report-post")
async def report_post(
        request: Request,
        post_id: str = Form(...),
        data: str = Form(...),
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
):
    uid = request.headers.get("X-User-Id")

    if len(data) > 1500:
        return {
            "status": "False",
            "message": "Something went wrong!",
        }
    if (await db.execute((exists(PostDB).where(PostDB.id == post_id).select()))).scalar():
        metadata = {
            "uid": uid,
            "details": {
                "role": request.headers.get("X-Role"),
                "post_id": post_id,
                "text": data, },
            "theme": "REPORT"
        }

        req = await client.post("http://admin-service:5006/api/v1/management/web-health/request", json=metadata)

        if req.status_code != 200:
            print(req.status_code, req.text)
            raise HTTPException(
                status_code=req.status_code,
                detail=req.json().get("details"),
            )

        return None

    else:
        raise HTTPException(
            status_code=404,
            detail="No such post!",
        )


@app.get("/api/v1/posts/{path:path}")
async def get_posts(
        path: str,
):
    if path.isdigit():
        raise HTTPException(
            status_code=400,
            detail="Invalid data!",
        )

    category_id = PATH_TO_ID.get(path)

    if not category_id:
        raise HTTPException(
            status_code=404,
            detail="No such category!",
        )

    category = CATEGORIES_CACHE.get(category_id)

    if category["is_leaf"]:
        raise HTTPException(
            status_code=404,
            detail="Cannot fetch subcategories for a leaf category. Use the search endpoint directly",
        )

    else:
        cat = [i for i in CATEGORIES_CACHE.values() if i["parent_id"] == category_id]
        return {
            "categories": cat,

        }
