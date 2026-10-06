from fastapi import FastAPI, Depends, HTTPException
import httpx
from starlette.responses import StreamingResponse
from httpx import Limits, AsyncClient
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from shared.exception_handler import *
from shared.logger_middleware import *
from shared.logger import *

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

app.add_exception_handler(Exception, universal_server_error_handler)
app.add_exception_handler(StarletteException, universal_server_http_error_handler)
app.middleware("http")(request_trace)

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

    logger.info(
        "Image has been successfully uploaded!", extra={
            "i_name": name,
            "request_id": request_id,
        }
    )

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
        url = f"http://filer:8888/{filename}?recursive=true"
    else:
        url = f"http://filer:8888/{filename}"

    await client.send(client.build_request(
        method="DELETE",
        url=url,
        content=request.stream()
    ))

    logger.info(
        "Image has been successfully deleted!", extra={
            "i_name": filename,
            "request_id": request_id,
        }
    )


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

    logger.info(
        "Image has been successfully updated!", extra={
            "i_name": filename,
            "request_id": request_id,
        }
    )
