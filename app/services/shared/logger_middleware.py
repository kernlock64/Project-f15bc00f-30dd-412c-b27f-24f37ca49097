import uuid
from contextvars import ContextVar
from fastapi import Request

request_id: ContextVar[str] = ContextVar("request_id", default="")


async def request_trace(request: Request, call_next):
    trace_id = request.headers.get("X-Request-Id", str(uuid.uuid4()))
    token = request_id.set(trace_id)
    try:
        response = await call_next(request)
        response.headers["X-Request-Id"] = trace_id
        return response
    finally:
        request_id.reset(token)
