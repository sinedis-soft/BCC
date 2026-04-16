from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from html import escape
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional
import hashlib
import hmac
import json
import logging
import re
import secrets
import string
import sys
import traceback

import httpx
from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse

from app.settings import settings

APP_ROOT = Path(__file__).resolve().parent.parent.parent
BANK_LOG_FILE = Path(settings.bank_log_file)
if not BANK_LOG_FILE.is_absolute():
    BANK_LOG_FILE = APP_ROOT / BANK_LOG_FILE

TWOPLACES = Decimal("0.01")
DECIMAL_ZERO = Decimal("0.00")

SENSITIVE_KEYS_EXACT = {
    "authorization",
    "proxy-authorization",
    "cookie",
    "set-cookie",
    "password",
    "passwd",
    "secret",
    "token",
    "access_token",
    "refresh_token",
    "api_key",
    "apikey",
    "client_secret",
    "card",
    "pan",
    "card_number",
    "cvv",
    "cvc",
    "exp",
    "expiry",
    "p_sign",
    "signature",
    "bcc_payload_json",
    "bitrix_snapshot_json",
    "callback_json",
    "refund_callback_json",
    "status_checks_json",
}

SENSITIVE_KEY_PARTS = (
    "password",
    "secret",
    "token",
    "auth",
    "sign",
    "signature",
    "cookie",
    "card",
    "pan",
    "cvv",
    "cvc",
    "iban",
    "account",
)


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "func": record.funcName,
            "line": record.lineno,
        }

        if hasattr(record, "event"):
            payload["event"] = record.event

        if hasattr(record, "payload"):
            payload["payload"] = sanitize_for_app_log(record.payload)

        if hasattr(record, "error"):
            payload["error"] = sanitize_for_app_log(record.error)

        if record.exc_info:
            exc_type, exc_value, exc_tb = record.exc_info
            payload["exception"] = {
                "type": exc_type.__name__ if exc_type else None,
                "message": str(exc_value) if exc_value else None,
                "traceback": "".join(traceback.format_exception(exc_type, exc_value, exc_tb)),
            }

        return safe_json_dumps(payload)


def build_stdout_handler() -> logging.Handler:
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(getattr(logging, str(getattr(settings, "log_level", "INFO")).upper(), logging.INFO))
    handler.setFormatter(JsonFormatter())
    return handler


def build_bank_file_handler() -> logging.Handler:
    max_bytes = int(getattr(settings, "bank_log_max_bytes", 10 * 1024 * 1024))
    backup_count = int(getattr(settings, "bank_log_backup_count", 10))

    BANK_LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

    handler = RotatingFileHandler(
        BANK_LOG_FILE,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    return handler


logger = logging.getLogger("bcc-payments")
logger.handlers.clear()
logger.setLevel(getattr(logging, str(getattr(settings, "log_level", "INFO")).upper(), logging.INFO))
logger.propagate = False
logger.addHandler(build_stdout_handler())

bank_logger = logging.getLogger("bcc-bank-exchange")
bank_logger.handlers.clear()
bank_logger.setLevel(logging.INFO)
bank_logger.propagate = False
bank_logger.addHandler(build_bank_file_handler())


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def stable_json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str, separators=(",", ":"), sort_keys=True)


def safe_json_dumps(data: Any) -> str:
    return json.dumps(data, ensure_ascii=False, default=str, separators=(",", ":"))


def build_idempotency_key(operation_type: str, payload: dict) -> str:
    source = f"{operation_type}:{stable_json_dumps(payload)}"
    return hashlib.sha256(source.encode("utf-8")).hexdigest()


def parse_json_object(value: Optional[str]) -> Optional[dict]:
    if not value:
        return None
    try:
        data = json.loads(value)
        return data if isinstance(data, dict) else None
    except Exception:
        return None


def _is_sensitive_key(key: str) -> bool:
    k = str(key).strip().lower()
    if k in SENSITIVE_KEYS_EXACT:
        return True
    return any(part in k for part in SENSITIVE_KEY_PARTS)


def redact_for_log(data: Any) -> Any:
    if isinstance(data, dict):
        result = {}
        for key, value in data.items():
            if _is_sensitive_key(str(key)):
                result[key] = "***redacted***"
            else:
                result[key] = redact_for_log(value)
        return result

    if isinstance(data, list):
        return [redact_for_log(item) for item in data]

    if isinstance(data, tuple):
        return tuple(redact_for_log(item) for item in data)

    if isinstance(data, bytes):
        return f"<bytes:{len(data)}>"

    return data


def strip_bitrix_log_fields(data: Any) -> Any:
    if not isinstance(data, dict):
        return data
    cleaned = dict(data)
    cleaned.pop("bitrix_snapshot", None)
    cleaned.pop("bitrix_snapshot_json", None)
    return cleaned


def strip_session_log_fields(data: Any) -> Any:
    if not isinstance(data, dict):
        return data
    cleaned = dict(data)
    cleaned.pop("bcc_payload_json", None)
    cleaned.pop("callback_json", None)
    cleaned.pop("refund_callback_json", None)
    cleaned.pop("status_checks_json", None)
    return cleaned


def sanitize_for_app_log(data: Any) -> Any:
    data = strip_bitrix_log_fields(data)
    data = strip_session_log_fields(data)
    data = redact_for_log(data)
    return data


def log_json(title: str, data: Any, level: int = logging.INFO) -> None:
    payload = sanitize_for_app_log(data)
    logger.log(level, title, extra={"event": title, "payload": payload})


def log_exception(title: str, exc: Exception) -> None:
    logger.error(
        title,
        extra={
            "event": title,
            "error": {
                "type": exc.__class__.__name__,
                "message": str(exc),
            },
        },
        exc_info=(type(exc), exc, exc.__traceback__),
    )


def to_decimal(value: Any, *, allow_empty: bool = False, quantize: bool = True) -> Optional[Decimal]:
    if value is None:
        return None if allow_empty else DECIMAL_ZERO

    if isinstance(value, Decimal):
        amount = value
    elif isinstance(value, int):
        amount = Decimal(value)
    elif isinstance(value, float):
        amount = Decimal(str(value))
    elif isinstance(value, str):
        raw = value.strip()
        if raw == "":
            return None if allow_empty else DECIMAL_ZERO

        if "|" in raw:
            raw_amount, raw_currency = raw.split("|", 1)
            raw = raw_amount.strip()
            currency = raw_currency.strip().upper()
            if currency != "KZT":
                raise HTTPException(
                    status_code=400,
                    detail=f"Unsupported currency: {currency}. Only KZT is allowed.",
                )

        normalized = raw.replace(",", ".").strip()
        try:
            amount = Decimal(normalized)
        except (InvalidOperation, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Invalid decimal amount format") from exc
    else:
        raw = str(value).strip()
        if raw == "":
            return None if allow_empty else DECIMAL_ZERO
        normalized = raw.replace(",", ".").strip()
        try:
            amount = Decimal(normalized)
        except (InvalidOperation, ValueError) as exc:
            raise HTTPException(status_code=400, detail="Invalid decimal amount format") from exc

    if quantize:
        return amount.quantize(TWOPLACES, rounding=ROUND_HALF_UP)
    return amount


def decimal_to_str(value: Decimal) -> str:
    return str(to_decimal(value, allow_empty=False, quantize=True))


def parse_decimal_amount(value: Any, *, allow_empty: bool = False) -> Optional[Decimal]:
    return to_decimal(value, allow_empty=allow_empty, quantize=True)


def same_utc_date(dt1: datetime, dt2: datetime) -> bool:
    return dt1.astimezone(timezone.utc).date() == dt2.astimezone(timezone.utc).date()


def append_comment(existing_comments: Optional[str], message: str) -> str:
    base = (existing_comments or "").strip()
    msg = message.strip()
    if not base:
        return msg
    return f"{base}\n{msg}"


def load_json_list(value: Optional[str]) -> list:
    if not value:
        return []

    try:
        data = json.loads(value)
        if isinstance(data, list):
            return data
        return [data]
    except Exception:
        return []


def append_json_list(value: Optional[str], item: dict) -> str:
    items = load_json_list(value)
    items.append(item)
    return json.dumps(items, ensure_ascii=False)


def get_first_value(value: Any) -> Any:
    if isinstance(value, list):
        return value[0] if value else None
    return value


def get_multifield_value(items: Any) -> Optional[str]:
    if isinstance(items, list) and items:
        return items[0].get("VALUE")
    return None


def map_policy_type(policy_type_id: Any) -> Optional[str]:
    value = str(policy_type_id).strip() if policy_type_id is not None else ""
    if value == "429":
        return "Зеленая Карта"
    if value == "425":
        return "ОСГО ВТС нерезидента"
    return None


def only_digits(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def format_amount_for_bcc(amount_value: Any) -> str:
    amount = parse_decimal_amount(amount_value)
    return decimal_to_str(amount)


def validate_test_amount_kzt(amount_value: Any) -> None:
    amount = parse_decimal_amount(amount_value)
    min_amount = parse_decimal_amount(settings.min_test_amount_kzt)
    if amount < min_amount:
        raise HTTPException(
            status_code=400,
            detail=(
                f"Amount is too low for BCC test flows. "
                f"Use amount >= {decimal_to_str(min_amount)} KZT."
            ),
        )


def generate_timestamp() -> str:
    return utc_now().strftime("%Y%m%d%H%M%S")


def generate_secure_token() -> str:
    return secrets.token_urlsafe(24)


def generate_order_id(deal_id: int) -> str:
    ts = utc_now().strftime("%Y%m%d%H%M%S")
    deal_part = str(deal_id).zfill(8)
    rnd = str(secrets.randbelow(10000)).zfill(4)
    order_id = f"{ts}{deal_part}{rnd}"
    return order_id[:32]


def generate_nonce(length_bytes: int = 16) -> str:
    return secrets.token_hex(length_bytes).upper()


def generate_merch_rn_id(length: int = 16) -> str:
    alphabet = string.ascii_uppercase + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


def lp(value: Any) -> str:
    value_str = "" if value is None else str(value)
    return f"{len(value_str)}{value_str}"


def build_desc(name: Any, last_name: Any, product: Any, invoice: Any, policy_type_value: Optional[str]) -> str:
    parts = [
        (name or "").strip(),
        (last_name or "").strip(),
        "за",
        str(product or "").strip(),
        "по счету",
        str(invoice or "").strip(),
    ]
    desc = " ".join(part for part in parts if part)

    if policy_type_value is not None:
        desc = f"{desc} {policy_type_value}"

    return desc.strip()[:125]


def split_phone(phone: str) -> tuple[str, str]:
    digits = only_digits(phone)

    if not digits:
        return "7", "0000000000"

    if digits.startswith("7") and len(digits) >= 11:
        return "7", digits[1:11]

    if digits.startswith("8") and len(digits) >= 11:
        return "7", digits[1:11]

    return "7", digits[-10:] if len(digits) >= 10 else digits


def normalize_client_ip(request: Request) -> str:
    client_ip = request.client.host if request.client else "0.0.0.0"
    if client_ip in ("127.0.0.1", "::1"):
        return "0.0.0.0"
    return client_ip


def html_message(title: str, body: str, status_code: int = 200) -> HTMLResponse:
    html = f"""
    <!doctype html>
    <html lang=\"ru\">
    <head>
      <meta charset=\"utf-8\">
      <title>{escape(title)}</title>
      <style>
        body {{
            font-family: Arial, sans-serif;
            max-width: 720px;
            margin: 40px auto;
            line-height: 1.5;
            color: #222;
        }}
        .box {{
            border: 1px solid #ddd;
            border-radius: 8px;
            padding: 24px;
        }}
      </style>
    </head>
    <body>
      <div class=\"box\">
        <h1>{escape(title)}</h1>
        <p>{escape(body)}</p>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html, status_code=status_code)


def is_html_content_type(content_type: str) -> bool:
    ct = (content_type or "").lower()
    return "text/html" in ct or "application/xhtml+xml" in ct


def is_text_content_type(content_type: str) -> bool:
    ct = (content_type or "").lower()
    return "text/plain" in ct or ct.startswith("text/")


def looks_like_html(text: str) -> bool:
    if not text:
        return False
    sample = text[:1000].lower()
    html_markers = ("<!doctype", "<html", "<head", "<body", "<form", "<script", "<title")
    return any(marker in sample for marker in html_markers)


def parse_key_value_text(text: str) -> Optional[dict]:
    raw = (text or "").strip()
    if not raw:
        return {}

    normalized = raw.replace("\r", "\n")
    parts = re.split(r"[&\n]+", normalized)
    parsed = {}

    for part in parts:
        part = part.strip()
        if not part:
            continue
        if "=" not in part:
            return None
        k, v = part.split("=", 1)
        k = k.strip()
        v = v.strip()
        if not k:
            return None
        parsed[k] = v

    return parsed


def truncate_text(text: str, max_len: int = 4000) -> str:
    if text is None:
        return ""
    text = str(text)
    if len(text) <= max_len:
        return text
    return text[:max_len] + f"... [truncated, total_len={len(text)}]"


def extract_important_bank_fields(data: Any) -> dict:
    if not isinstance(data, dict):
        return {}

    keys_to_extract = [
        "RC",
        "RESULT",
        "STATUS",
        "APPROVAL",
        "APPROVAL_CODE",
        "ACTION",
        "RESPCODE",
        "MESSAGE",
        "MSG",
        "ERROR",
        "ORDER",
        "TRTYPE",
        "TRAN_TRTYPE",
        "RRN",
        "INT_REF",
        "AMOUNT",
        "ORG_AMOUNT",
        "CURRENCY",
        "TERMINAL",
    ]

    result = {}
    for key in keys_to_extract:
        if key in data:
            result[key] = data[key]
    return result


def sanitize_headers_for_log(headers: Any, *, full: bool) -> dict:
    raw_headers = dict(headers or {})
    include_sensitive = should_include_sensitive_bank_log_data()

    if full:
        return raw_headers if include_sensitive else redact_for_log(raw_headers)

    selected = {}
    for key in ("content-type", "location", "authorization"):
        if key in raw_headers:
            selected[key] = raw_headers[key]
        elif key.title() in raw_headers:
            selected[key.title()] = raw_headers[key.title()]
    return selected if include_sensitive else redact_for_log(selected)


def should_include_sensitive_bank_log_data() -> bool:
    return bool(
        getattr(settings, "bank_log_full_http", False)
        and getattr(settings, "bank_log_include_sensitive", False)
        and settings.is_test_merchant
    )


def sanitize_httpx_request_for_log(request: httpx.Request) -> dict:
    full_http = bool(getattr(settings, "bank_log_full_http", False))
    include_sensitive = should_include_sensitive_bank_log_data()
    body_bytes = request.content or b""
    text = body_bytes.decode("utf-8", errors="replace") if isinstance(body_bytes, (bytes, bytearray)) else str(body_bytes or "")
    parsed_kv = parse_key_value_text(text) if text else None

    meta = {
        "method": request.method,
        "url": str(request.url),
        "headers": sanitize_headers_for_log(request.headers, full=full_http),
        "content_length": len(body_bytes),
    }

    if parsed_kv is not None:
        meta["body_kind"] = "key_value"
        meta["parsed_body"] = parsed_kv if include_sensitive else redact_for_log(parsed_kv)
        meta["important_fields"] = extract_important_bank_fields(parsed_kv)
        if full_http:
            meta["raw_text"] = text
        return meta

    if text:
        meta["body_kind"] = "text"
        meta["raw_text"] = text if full_http else truncate_text(text, 4000)
    else:
        meta["body_kind"] = "empty"

    return meta


def sanitize_bank_response_for_log(response: httpx.Response) -> dict:
    full_http = bool(getattr(settings, "bank_log_full_http", False))
    include_sensitive = should_include_sensitive_bank_log_data()
    content_type = response.headers.get("content-type", "")
    text = response.text or ""
    parsed_json = None
    parsed_kv = None

    try:
        parsed_json = response.json()
    except Exception:
        parsed_json = None

    if parsed_json is None:
        parsed_kv = parse_key_value_text(text)

    meta = {
        "status_code": response.status_code,
        "reason_phrase": response.reason_phrase,
        "content_type": content_type,
        "content_length": len(text),
        "headers": sanitize_headers_for_log(response.headers, full=full_http),
        "request": sanitize_httpx_request_for_log(response.request),
    }

    if isinstance(parsed_json, dict):
        meta["body_kind"] = "json"
        meta["parsed_body"] = parsed_json if include_sensitive else redact_for_log(parsed_json)
        meta["important_fields"] = extract_important_bank_fields(parsed_json)
        return meta

    if isinstance(parsed_json, list):
        meta["body_kind"] = "json_list"
        meta["parsed_body"] = parsed_json if include_sensitive else redact_for_log(parsed_json)
        return meta

    if parsed_kv is not None:
        meta["body_kind"] = "key_value"
        meta["parsed_body"] = parsed_kv if include_sensitive else redact_for_log(parsed_kv)
        meta["important_fields"] = extract_important_bank_fields(parsed_kv)
        return meta

    if is_html_content_type(content_type) or looks_like_html(text):
        meta["body_kind"] = "html"
        meta["body_preview"] = text if full_http else truncate_text(text, 1500)
        return meta

    if is_text_content_type(content_type):
        meta["body_kind"] = "text"
        meta["raw_text"] = text if full_http else truncate_text(text, 4000)
        return meta

    meta["body_kind"] = "unknown"
    meta["raw_text"] = text if full_http else truncate_text(text, 4000)
    return meta


def bank_log_json(title: str, data: Any, level: int = logging.INFO) -> None:
    try:
        include_sensitive = should_include_sensitive_bank_log_data()
        safe_data = data if include_sensitive else redact_for_log(data)
        if isinstance(safe_data, (dict, list, tuple)):
            payload = json.dumps(safe_data, ensure_ascii=False, indent=2, default=str)
        else:
            payload = str(safe_data)
    except Exception:
        payload = repr(data)

    bank_logger.log(level, "%s\n%s", title, payload)

    for handler in bank_logger.handlers:
        try:
            handler.flush()
        except Exception:
            pass


def bank_log_outgoing_request(url: str, payload: dict) -> None:
    bank_log_json(
        "BANK OUTGOING REQUEST",
        {
            "logged_at": iso_now(),
            "url": url,
            "payload": payload,
            "important_fields": extract_important_bank_fields(payload),
        },
    )


def bank_log_incoming_response(url: str, response: httpx.Response) -> None:
    bank_log_json(
        "BANK INCOMING RESPONSE",
        {
            "logged_at": iso_now(),
            "url": url,
            "response": sanitize_bank_response_for_log(response),
        },
    )


def bank_log_notify_callback(data: dict, method: str) -> None:
    safe_payload = dict(data)
    if "Authorization" in safe_payload:
        safe_payload["Authorization"] = "***redacted***"

    bank_log_json(
        "BANK NOTIFY CALLBACK",
        {
            "logged_at": iso_now(),
            "source": "notify",
            "method": method,
            "payload": safe_payload,
            "important_fields": extract_important_bank_fields(safe_payload),
        },
    )


def bank_log_backref(data: dict, method: str) -> None:
    bank_log_json(
        "BANK BACKREF CALLBACK",
        {
            "logged_at": iso_now(),
            "source": "backref",
            "method": method,
            "payload": data,
            "important_fields": extract_important_bank_fields(data),
        },
    )


def parse_request_kv_data_sync(request: Request) -> dict:
    return {}


async def parse_request_kv_data(request: Request) -> dict:
    if request.method == "POST":
        content_type = (request.headers.get("content-type") or "").lower()

        if "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
            form = await request.form()
            return {k: v for k, v in form.items()}

        body = await request.body()
        text = body.decode("utf-8", errors="replace")
        parsed = parse_key_value_text(text)
        if parsed is not None:
            return parsed

        raise HTTPException(status_code=400, detail="Unsupported POST body format for callback")

    return dict(request.query_params)


def log_notify_raw_request(request: Request) -> None:
    log_json(
        "bcc_notify_raw_request",
        {
            "method": request.method,
            "path": request.url.path,
            "query": dict(request.query_params),
            "client_ip": request.client.host if request.client else None,
            "headers": {
                "content_type": request.headers.get("content-type"),
                "authorization_present": bool(request.headers.get("Authorization")),
                "user_agent": request.headers.get("user-agent"),
                "host": request.headers.get("host"),
            },
        },
        level=logging.WARNING,
    )
