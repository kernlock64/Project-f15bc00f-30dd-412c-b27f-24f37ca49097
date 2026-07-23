from fastapi.responses import JSONResponse
from starlette import status
from starlette.requests import Request
from starlette.exceptions import HTTPException as StarletteException

async def universal_server_http_error_handler(req: Request, exc: StarletteException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "detail": exc.detail, }
    )

async def universal_server_error_handler(req: Request, exc: Exception):
    print(exc)

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "detail": "Что-то пошло не так. Пожалуйста, обратитесь в службу поддержки, если ошибка повторится."}
    )