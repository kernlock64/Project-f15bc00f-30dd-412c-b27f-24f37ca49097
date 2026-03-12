from fastapi import FastAPI, Depends, Request, HTTPException
import httpx
from starlette.responses import StreamingResponse
from httpx import Limits, AsyncClient
from contextlib import asynccontextmanager
import os
import json

import aio_pika
import asyncio

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


async def publish(rabbit: RabbitMQGateway, data: dict, routing_key=None):
    channel = rabbit.channel

    exchange = await channel.declare_exchange(
        os.getenv("PUB"),
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

limits = Limits(
    max_connections=700,
    max_keepalive_connections=50,
)

MEDIA_KEY = os.getenv("MEDIA_KEY")

@asynccontextmanager
async def lifespan(cl: FastAPI):
    client = httpx.AsyncClient(limits=limits, timeout=5)
    cl.state.http_client = client
    try:
        yield
    finally:
        await client.aclose()


async def get_httpx(request: Request) -> httpx.AsyncClient:
    return request.app.state.http_client

app = FastAPI(lifespan=lifespan)

@app.get("/media/get-image/{filename}")
async def get_image(filename: str,
                    request: Request,
              client: AsyncClient = Depends(get_httpx)):

    try:
        req = client.build_request(
            method=request.method,
            url=f"http://filer:8888/{filename}",
            content=request.stream(),
        )

        request = await client.send(req, stream=True)

        return StreamingResponse(
            content=request.aiter_raw(),
            status_code=request.status_code,
            headers=request.headers,
        )
    except HTTPException as e:
        return {
            "status": "False",
            "message": str(e),
        }

@app.post("/media/add-image/{ext}")
async def add_image(
        ext: str,
        request: Request,
        client: AsyncClient = Depends(get_httpx),
        p_directory: str = None):
    try:

        key = request.headers.get("X-Key")
        if key != MEDIA_KEY:
            return {
                "status": "False",
                "message": "Unauthorized!",
            }

        name = str(request.headers.get("X-Name")) + f".{ext}"

        url = "http://filer:8888/" + name if not p_directory else "http://filer:8888/" + p_directory + "/" + name

        await client.send(client.build_request(
            method="PUT",
            url=url,
            content=request.stream(),
            headers={"Content-Type": "application/octet-stream"}
        ))

        return {
            "status": "True",
            "filename": name,
        }
    except HTTPException as e:
        return {
            "status": "False",
            "message": str(e),
        }


@app.delete("/media/delete-image/{filename}")
async def delete_image(
        filename: str, request: Request,
        directory: bool = False,
        client: AsyncClient = Depends(get_httpx)):
    try:

        key = request.headers.get("X-Key")
        if key != MEDIA_KEY:
            return {
                "status": "False",
                "message": "Unauthorized!",
            }

        if directory:
            url=f"http://filer:8888/{filename}?recursive=true"
        else:
            url=f"http://filer:8888/{filename}"

        await client.send(client.build_request(
            method="DELETE",
            url=url,
            content=request.stream()
        ))

        return {
            "status": "True",
        }
    except HTTPException as e:
        return {
            "status": "False",
            "message": str(e),
        }


@app.post("/media/change-image/{filename}")
async def change_image(
        filename: str,
        request: Request,
        client: AsyncClient = Depends(get_httpx), ):
    try:

        key = request.headers.get("X-Key")
        if key != MEDIA_KEY:
            return {
                "status": "False",
                "message": "Unauthorized!",
            }

        await client.send(client.build_request(
            method="PUT",
            url=f"http://filer:8888/{filename}",
            content=request.stream(),
            headers={"Content-Type": "application/octet-stream"}
        ))

        return {
            "status": "True",
        }
    except HTTPException as e:
        return {
            "status": "False",
            "message": str(e),
        }
