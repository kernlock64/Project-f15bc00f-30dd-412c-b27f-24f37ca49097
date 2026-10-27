import copy
import json
import sqlalchemy
from geoalchemy2 import WKTElement
from meilisearch_python_sdk import AsyncClient as MeiliClient
from meilisearch_python_sdk.models.settings import MeilisearchSettings

from datetime import *
from fastapi import FastAPI, Depends, Body, HTTPException, Form
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
from shared.logger_middleware import *
from shared.logger import *


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


class ViewsManager:

    def __init__(self):
        self.total_views = {}
        self.recent_views = {}

    def add(self, post_id, ip):
        if post_id not in self.recent_views:
            self.recent_views[post_id] = set()
            self.total_views[post_id] = 0

        if ip in self.recent_views[post_id]:
            return

        self.recent_views[post_id].add(ip)
        self.total_views[post_id] += 1

    async def flush(self, eng, meili: MeiliClient):

        async with eng() as db:

                to_update = []

                if not self.total_views:
                    return

                t_table = values(
                    column("p_id", String),
                    column("i_views", Integer),
                    name="t_table"
                ).data([(p_id, i_views) for p_id, i_views in self.total_views.items()])

                res = await db.execute(
                    update(PostDB)
                    .where(PostDB.id == t_table.c.p_id)
                    .values(views=PostDB.views + t_table.c.i_views)
                    .returning(PostDB.id, PostDB.views)
                )

                for row in res:
                    to_update.append(
                        {"id": row[0], "views": row[1]}
                    )

                if to_update:
                    await meili.index("posts").update_documents(to_update)
                    await db.commit()

                self.recent_views = {}
                self.total_views = {}



MEILI_URL = "http://meilisearch:7700"
MEILI_KEY = os.getenv("MEILISEARCH_KEY")
MEDIA_KEY = os.getenv("MEDIA_KEY")

limits = Limits(
    max_connections=1000,
    max_keepalive_connections=100,
)


@asynccontextmanager
async def lifespan(cl: FastAPI):
    views_manager = ViewsManager()
    app.state.views_manager = views_manager
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
            "attributes",
            "author_id"
        ],
        sortable_attributes=[
            "date",
            "price"
        ],
        searchable_attributes=[
            "title",
            "description",
        ],
        displayed_attributes=[
            "id",
            "title",
            "description",
            "date",
            "price",
            "title_image",
            "category_id",
            "author_id",
            "author_name",
            "author_image",
            "attributes",
            "views",
            "path",
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
app.middleware("http")(request_trace)


async def get_db():
    factory = app.state.db
    async with factory() as session:
        yield session


async def get_views_manager(
):
    return app.state.views_manager


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
                logger.error(
                    "External server error has occurred!", extra={
                        "status_code": res.status_code,
                        "user_id": uid,
                        "error": res.text[:500],
                        "request_id": request_id.get(),
                    }
                )
                raise HTTPException(
                    status_code=res.status_code,
                    detail=res.json().get("details"),
                )

        await db.commit()


async def wait_for_batch():
    while True:
        await asyncio.sleep(100)
        views_manager = app.state.views_manager
        await views_manager.flush(app.state.db, app.state.meilisearch)


async def listen_deletion():
        async with app.state.d_queue.iterator() as queue_iter:
            async for message in queue_iter:
                async with message.process():
                    data = json.loads(message.body.decode())
                    uid = data["X-User-Id"]
                    await delete_user(uid, app.state.db)



async def main():
    await asyncio.gather(
        wait_for_batch(),
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
                            logger.error(
                                "External server error has occurred!", extra={
                                    "status_code": res.status_code,
                                    "post_id": post_id,
                                    "error": res.text[:500],
                                    "request_id": request_id.get(),
                                }
                            )
                            raise HTTPException(
                                status_code=res.status_code,
                                detail=res.json().get("details"),
                            )

                    await db.commit()

                    logger.info(
                        "Post has been successfully deleted!", extra={
                            "post_id": post_id,
                            "request_id": request_id.get(),
                        }
                    )

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
                        detail=resp.json().get("details"),
                    )

                await db.delete(post)

                await db.commit()

                logger.info(
                    "Post has been successfully deleted!", extra={
                        "post_id": post_id,
                        "request_id": request_id.get(),
                    }
                )

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
                    logger.error(
                        "External server error has occurred!", extra={
                            "status_code": req.status_code,
                            "user_id": uid,
                            "error": req.text[:500],
                            "request_id": request_id.get(),
                        }
                    )
                    raise HTTPException(
                        status_code=req.status_code,
                        detail=req.json().get("details"),
                    )

                dat = req.json()
                author_name = dat["author_name"]
                author_image = dat["author_image"]

                try:

                    if metadata["is_images"] == "t":
                        date1 = datetime.now(timezone.utc)
                        for post in posts:
                            post_id = str((lambda: ''.join(
                                secrets.choice("abcdefghijkmnopqrstuvwxyzABCDEFGHIJKLMNPQRSTUVWXYZ23456789") for _ in
                                range(19)))())

                            p = PostDB(id=post_id, author_id=uid, title=post["title"],
                                       posted_at=date1,
                                       description=post["description"],
                                       images=dict(post).get("images"),
                                       directory_name=dict(post).get("images_dir"),
                                       category_id=post["category_path"],
                                       price_list_name=price_list_name,
                                       coordinates=WKTElement(
                                           f"POINT({post["coordinates"][0]} {post["coordinates"][1]})", srid=4326),
                                       price=int(post["price"]),
                                       title_image=str(str(post["images_dir"]) + "/" + str(post['title_image'])),
                                       attributes={(j := k.split(":"))[0]: j[1] for k in
                                                   post["attributes"].split(",")} if
                                       post["attributes"] != "" else [],
                                       views=0,
                                       )
                            db.add(p)

                            client = meili.index("posts")

                            await client.add_documents([
                                {"id": post_id, "title": post["title"], "price": post["price"],
                                 "date": date1.timestamp(),
                                 "title_image": str(str(post["images_dir"]) + "/" + str(post['title_image'])),
                                 "category_id":
                                     post["category_path"],
                                 "description": post["description"],
                                 "author_id": uid,
                                 "author_name": author_name,
                                 "author_image": author_image,
                                 "attributes": {(j := k.split(":"))[0]: j[1] for k in post["attributes"].split(",")} if
                                 post["attributes"] != "" else None,
                                 "views": 0,
                                 }
                            ])

                            await db.commit()

                            logger.info(
                                "Post has been successfully added!", extra={
                                    "post_id": post_id,
                                    "author_id": uid,
                                    "request_id": request_id.get(),
                                }
                            )

                        return None

                    elif metadata["is_images"] == "f":
                        date2 = datetime.now(timezone.utc)
                        for post in posts:
                            post_id = str((lambda: ''.join(
                                secrets.choice("abcdefghijkmnopqrstuvwxyzABCDEFGHIJKLMNPQRSTUVWXYZ23456789") for _ in
                                range(19)))())

                            p = PostDB(id=post_id, author_id=uid, title=post["title"],
                                       posted_at=date2,
                                       description=post["description"],
                                       category_id=post["category_path"],
                                       price_list_name=price_list_name,
                                       coordinates=WKTElement(
                                           f"POINT({post["coordinates"][0]} {post["coordinates"][1]})", srid=4326),
                                       price=int(post["price"]),
                                       attributes={(j := k.split(":"))[0]: j[1] for k in
                                                   post["attributes"].split(",")} if
                                       post["attributes"] != "" else [],
                                       views=0,
                                       )
                            db.add(p)

                            client = meili.index("posts")

                            await client.add_documents([
                                {"id": post_id, "title": post["title"], "price": post["price"],
                                 "description": post["description"],
                                 "date": date2.timestamp(),
                                 "category_id":
                                     post["category_path"],
                                 "author_id": uid,
                                 "author_name": author_name,
                                 "author_image": author_image,
                                 "attributes": {(j := k.split(":"))[0]: j[1] for k in post["attributes"].split(",")} if
                                 post["attributes"] != "" else None,
                                 "views": 0,
                                 }
                            ], primary_key="id")

                            await db.commit()

                            logger.info(
                                "Post has been successfully added!", extra={
                                    "post_id": post_id,
                                    "author_id": uid,
                                    "request_id": request_id.get(),
                                }
                            )

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

                logger.info(
                    "Post has been successfully updated!", extra={
                        "post_id": post_id,
                        "request_id": request_id.get(),
                    }
                )

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
        sort: str = None,
        q: str = None,
        category: str = None,
        meili: MeiliClient = Depends(get_meili),
):
    if category:
        if (cat_id := PATH_TO_ID.get(category)) is not None:
            if not fil:
                fil = f"category_id={cat_id}"
            else:
                fil = f"({fil}) AND category_id={cat_id}"

    offset = (page - 1) * limit
    sort = [sort] if sort else []

    result = await meili.index("posts").search(q, filter=fil, offset=offset, limit=limit, sort=sort,
                                               facets=["category_id"])
    return {
        "count": result.estimated_total_hits,
        "categories": {int(CATEGORIES_CACHE[int(i[0])]["id"]): {"id": CATEGORIES_CACHE[int(i[0])]["id"],
                                                           "name": CATEGORIES_CACHE[int(i[0])]["name"],
                                                           "slug": CATEGORIES_CACHE[int(i[0])]["slug"],
                                                           "path": CATEGORIES_CACHE[int(i[0])]["path"],
                                                           "count": int(i[1])} for i in
                       dict(dict(result).get("facet_distribution", {}).get("category_id", {})).items()},
        "posts": result.hits if result.hits else [],
    }

def get_path(category_id: int, data):
    if category_id == 0:
        return 0

    data.append({"id": CATEGORIES_CACHE[category_id]["id"], "name": CATEGORIES_CACHE[category_id]["name"],
                 "slug": CATEGORIES_CACHE[category_id]["slug"],})

    return get_path(CATEGORIES_CACHE[category_id]["parent_id"], data)

@app.get("/api/v1/posts/post/{post_id}")
async def get_post(
        request: Request,
        post_id: str,
        db: AsyncSession = Depends(get_db),
        client: AsyncClient = Depends(get_httpx),
        views_manager: ViewsManager = Depends(get_views_manager),
):
    res = await db.execute(select(PostDB).where(PostDB.id == str(post_id)))
    post = res.scalar_one_or_none()

    categories = []

    if not post:
        raise HTTPException(
            status_code=404,
            detail="No such post!"
        )

    views_manager.add(post_id, str(request.headers.get("X-Real-Ip")))

    res1 = await client.get(f"http://favourite-service:5005/api/v1/favourites/is-favourite/{post_id}",
                            headers=request.headers)

    if res1.status_code != 200:
        logger.error(
            "External server error has occurred!", extra={
                "status_code": res1.status_code,
                "post_id": post_id,
                "error": res1.text[:500],
                "request_id": request_id.get(),
            }
        )

        raise HTTPException(
            status_code=res1.status_code,
            detail=res1.json().get("details"),
        )

    res2 = await client.get(f"http://user-service:5004/api/v1/users/producers/post/{post.author_id}")

    if res2.status_code != 200:
        logger.error(
            "External server error has occurred!", extra={
                "status_code": res2.status_code,
                "post_id": post_id,
                "error": res2.text[:500],
                "request_id": request_id.get(),
            }
        )
        raise HTTPException(
            status_code=res2.status_code,
            detail=res2.json().get("details"),
        )

    get_path(int(post.category_id), categories)

    return {
        "post_id": post.id,
        "views": post.views,
        "price": post.price,
        "title": post.title,
        "author_id": post.author_id,
        "author": res2.json(),
        "attributes": post.attributes,
        "description": post.description,
        "breadcrumbs": categories,
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

    logger.info(
        "Post has been successfully updated!", extra={
            "post_id": post_id,
            "user_id": uid,
            "request_id": request_id.get(),
        }
    )

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
        logger.error(
            "External server error has occurred!", extra={
                "status_code": resp.status_code,
                "user_id": uid,
                "error": resp.text[:500],
                "request_id": request_id.get(),
            }
        )

        raise HTTPException(
            status_code=resp.status_code,
            detail=resp.json().get("details"),
        )

    await db.delete(post)

    logger.info(
        "Post has been successfully deleted!", extra={
            "post_id": post_id,
            "user_id": uid,
            "request_id": request_id.get(),
        }
    )

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
            logger.error(
                "External server error has occurred!", extra={
                    "status_code": req.status_code,
                    "user_id": uid,
                    "error": req.text[:500],
                    "request_id": request_id.get(),
                }
            )
            raise HTTPException(
                status_code=req.status_code,
                detail=req.json().get("details"),
            )

        logger.info(
            "User report has been successfully submitted!", extra={
                "post_id": post_id,
                "user_id": uid,
                "request_id": request_id.get(),
            }
        )

        return None

    else:
        raise HTTPException(
            status_code=404,
            detail="No such post!",
        )


@app.get("/api/v1/posts/{path:path}")
async def get_posts(
        path: str = None,
        db: AsyncSession = Depends(get_db),
):
    if path.isdigit():
        raise HTTPException(
            status_code=400,
            detail="Invalid data!",
        )

    if not path:
        cat = [copy.deepcopy(i) for i in CATEGORIES_CACHE.values() if not i["parent_id"]]
        for k in cat:
            k["subcategory"] = [copy.deepcopy(i) for i in CATEGORIES_CACHE.values() if i["parent_id"] == k["id"]]
        return {
            "categories": cat,
        }

    category_id = PATH_TO_ID.get(path)

    if not category_id:
        raise HTTPException(
            status_code=404,
            detail="No such category!",
        )

    category = CATEGORIES_CACHE.get(category_id)

    if category["is_leaf"]:
        return "Cannot fetch subcategories for a leaf category. Use the search endpoint directly"

    else:
        cat = [i for i in CATEGORIES_CACHE.values() if i["parent_id"] == category_id]
        leaves_id = []
        for i in cat:
            leaves_id += LEAVES_MAP.get(i["id"])
        list(set(leaves_id))

        final = {i: 0 for i in leaves_id}

        total = await db.execute(
            select(PostDB.category_id, func.count(PostDB.id))
            .where(PostDB.category_id.in_(leaves_id))
            .group_by(PostDB.category_id)
        )

        all_total = dict(total.all())
        all_total.update(final)

        cnt = 0
        for m in LEAVES_MAP.get(category["id"]):
            cnt += all_total[m]
        category["count"] = cnt

        for k in cat:
            cnt = 0
            for j in LEAVES_MAP.get(k["id"]):
                cnt += all_total[j]
            k["count"] = cnt

        return {
            "category": category,
            "categories": cat,
        }
