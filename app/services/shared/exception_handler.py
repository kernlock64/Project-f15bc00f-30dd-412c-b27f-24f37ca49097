from fastapi.responses import JSONResponse
from starlette import status
from starlette.requests import Request
from starlette.exceptions import HTTPException as StarletteException
from shared.logger import logger
from shared.logger_middleware import request_id


async def universal_server_http_error_handler(req: Request, exc: StarletteException):
    logger.warn(
        f"Some client-side error has occurred on method {req.method} {req.url.path}", extra={
            "exception": exc.detail,
            "request_id": request_id.get(),
        }
    )

    return JSONResponse(
        status_code=exc.status_code,
        content={
            "detail": exc.detail,
        }
    )


async def universal_server_error_handler(req: Request, exc: Exception):
    logger.error(
        f"Some sever-side error has occurred on method {req.method} {req.url.path}", extra={
            "exception": exc,
            "request_id": request_id.get(),
        }, exc_info=True
    )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Что-то пошло не так. Пожалуйста, обратитесь в службу поддержки, если ошибка повторится."}
    )
