import uuid
from contextvars import ContextVar
from fastapi import FastAPI, Request

request_id: ContextVar[str] = ContextVar("request_id", default="")


@app.middleware("http")
async def trace_requests_middleware(request: Request, call_next):
    trace_id = request.headers.get("X-Request-Id", str(uuid.uuid4()))
    token = request_id_ctx_var.set(trace_id)
    try:
        response = await call_next(request)
        response.headers["X-Request-Id"] = trace_id
        return response
    finally:
        request_id_ctx_var.reset(token)
