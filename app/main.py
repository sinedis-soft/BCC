import logging
import secrets

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from app.core.shared import BANK_LOG_FILE, bank_log_json, iso_now, log_exception, log_json, logger, utc_now
from app.db import init_db
from app.routes.payments import router as payments_router
from app.settings import settings

app = FastAPI(title=settings.app_name)
app.include_router(payments_router)


@app.middleware("http")
async def request_logging_middleware(request: Request, call_next):
    started_at = utc_now()
    request_id = secrets.token_hex(8)

    log_json(
        "http_request_started",
        {
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "query": dict(request.query_params),
            "client_ip": request.client.host if request.client else None,
        },
    )

    try:
        response = await call_next(request)
    except Exception as exc:
        duration_ms = int((utc_now() - started_at).total_seconds() * 1000)
        logger.error(
            "http_request_failed",
            extra={
                "event": "http_request_failed",
                "payload": {
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "duration_ms": duration_ms,
                },
            },
            exc_info=(type(exc), exc, exc.__traceback__),
        )
        raise

    duration_ms = int((utc_now() - started_at).total_seconds() * 1000)
    log_json(
        "http_request_finished",
        {
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
        },
    )
    response.headers["X-Request-ID"] = request_id
    return response


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    log_json(
        "http_exception",
        {
            "method": request.method,
            "path": request.url.path,
            "status_code": exc.status_code,
            "detail": exc.detail,
        },
        level=logging.WARNING,
    )
    return JSONResponse(status_code=exc.status_code, content={"detail": exc.detail}, headers=exc.headers or None)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    log_exception("unhandled_exception", exc)
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


@app.on_event("startup")
async def startup_event():
    init_db()

    log_json(
        "app_started",
        {
            "app_name": settings.app_name,
            "bank_log_file": str(BANK_LOG_FILE),
            "merchant": settings.merchant,
            "notify_url": settings.notify_url,
            "backref": settings.backref,
            "notify_basic_enabled": settings.notify_basic_enabled,
            "bank_log_full_http": settings.bank_log_full_http,
            "bank_log_include_sensitive": settings.bank_log_include_sensitive,
        },
    )

    bank_log_json(
        "BANK LOGGER STARTED",
        {
            "logged_at": iso_now(),
            "bank_log_file": str(BANK_LOG_FILE),
            "merchant": settings.merchant,
            "terminal": settings.terminal,
            "notify_url": settings.notify_url,
            "backref": settings.backref,
            "notify_basic_enabled": settings.notify_basic_enabled,
            "bank_log_full_http": settings.bank_log_full_http,
            "bank_log_include_sensitive": settings.bank_log_include_sensitive,
        },
    )
