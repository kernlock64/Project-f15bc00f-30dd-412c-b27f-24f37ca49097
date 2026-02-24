import uuid

from fastapi import FastAPI, Depends, Request, HTTPException
import httpx
from starlette.responses import StreamingResponse
from httpx import Limits, AsyncClient
from contextlib import asynccontextmanager

limits = Limits(
    max_connections=700,
    max_keepalive_connections=50,
)



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
                    client: AsyncClient = Depends(get_httpx),):
    try:
        name = str(uuid.uuid4()) + f".{ext}"

        await client.send(client.build_request(
            method="PUT",
            url=f"http://filer:8888/{name}",
            content=request.stream(),
            headers={"Content-Type": "application/octet-stream"}
        ))

        return {
            "message": "True",
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
                 client: AsyncClient = Depends(get_httpx)):
    try:

        await client.send(client.build_request(
            method="DELETE",
            url=f"http://filer:8888/{filename}",
            content=request.stream()
        ))

        return {
            "message": "True",
        }
    except HTTPException as e:
        return {
            "status": "False",
            "message": str(e),
        }