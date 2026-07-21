from fastapi import FastAPI, Depends, Request, HTTPException, status
from fastapi.responses import JSONResponse
import httpx
from starlette.responses import StreamingResponse
from httpx import Limits, AsyncClient
from contextlib import asynccontextmanager
import os
from starlette.exceptions import HTTPException as StarletteException
from fastapi.middleware.cors import CORSMiddleware

MEDIA_KEY = os.getenv("MEDIA_KEY")

limits = Limits(
    max_connections=1000,
    max_keepalive_connections=100,
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

'''ВРЕМЕННО'''
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
'''ВРЕМЕННО'''


@app.exception_handler(StarletteException)
async def universal_server_http_error_handler(req: Request, exc: StarletteException):
    return JSONResponse(
            status_code=exc.status_code,
            content={
                "detail": exc.detail,}
        )

@app.exception_handler(Exception)
async def universal_server_error_handler(req: Request, exc: Exception):

    print(exc)

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Что-то пошло не так. Пожалуйста, обратитесь в службу поддержки, если ошибка повторится."}
    )

@app.get("/api/v1/media/get-image/{filename:path}")
async def get_image(filename: str,
                    request: Request,
              client: AsyncClient = Depends(get_httpx)):

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


@app.post("/api/v1/media/add-image/{ext}")
async def add_image(
        ext: str,
        request: Request,
        client: AsyncClient = Depends(get_httpx),
        p_directory: str = None):

        key = request.headers.get("X-Key")
        if key != MEDIA_KEY:
            raise HTTPException(
                status_code=403,
                detail="FORBIDDEN",
            )

        name = str(request.headers.get("X-Name")) + f".{ext}"

        url = "http://filer:8888/" + name if not p_directory else "http://filer:8888/" + p_directory + "/" + name

        await client.send(client.build_request(
            method="PUT",
            url=url,
            content=request.stream(),
            headers={"Content-Type": "application/octet-stream"}
        ))

        return {
            "filename": name,
        }


@app.delete("/api/v1/media/delete-image/{filename}")
async def delete_image(
        filename: str, request: Request,
        client: AsyncClient = Depends(get_httpx)):

        directory = request.headers.get("X-Directory")

        key = request.headers.get("X-Key")
        if key != MEDIA_KEY:
            raise HTTPException(
                status_code=403,
                detail="FORBIDDEN",
            )

        if directory:
            url=f"http://filer:8888/{filename}?recursive=true"
        else:
            url=f"http://filer:8888/{filename}"

        await client.send(client.build_request(
            method="DELETE",
            url=url,
            content=request.stream()
        ))

@app.post("/api/v1/media/change-image/{filename}")
async def change_image(
        filename: str,
        request: Request,
        client: AsyncClient = Depends(get_httpx), ):

        key = request.headers.get("X-Key")
        if key != MEDIA_KEY:
            raise HTTPException(
                status_code=403,
                detail="FORBIDDEN",
            )

        await client.send(client.build_request(
            method="PUT",
            url=f"http://filer:8888/{filename}",
            content=request.stream(),
            headers={"Content-Type": "application/octet-stream"}
        ))

