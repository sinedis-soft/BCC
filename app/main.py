from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from enum import Enum
from html import escape
from pathlib import Path
from typing import Any, Optional
import base64
import hashlib
import hmac
import json
import logging
import re
import secrets
import string
import sys
import traceback
from logging.handlers import RotatingFileHandler

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from app.bitrix_client import BitrixAPIError, get_contact, get_deal, update_deal
from app.db import (
    create_payment_operation,
    create_payment_session,
    expire_stale_sessions_for_deal,
    finish_payment_operation,
    get_latest_payment_session_by_deal_id,
    get_payment_operation,
    get_payment_session_by_order,
    get_payment_session_by_token,
    immediate_transaction,
    init_db,
    list_recent_payments,
    update_payment_session,
)
from app.settings import settings


APP_ROOT = Path(__file__).resolve().parent.parent
BANK_LOG_FILE = Path(settings.bank_log_file)
if not BANK_LOG_FILE.is_absolute():
    BANK_LOG_FILE = APP_ROOT / BANK_LOG_FILE

TWOPLACES = Decimal("0.01")
DECIMAL_ZERO = Decimal("0.00")


class PaymentStatus(str, Enum):
    CREATED = "created"
    PENDING = "pending"
    PAID = "paid"
    FAILED = "failed"
    EXPIRED = "expired"
    REFUND_PENDING = "refund_pending"
    REFUNDED = "refunded"
    REFUND_FAILED = "refund_failed"

    @classmethod
    def from_value(cls, value: Any) -> "PaymentStatus":
        raw = str(value or "").strip().lower()
        try:
            return cls(raw)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"Unsupported payment status: {value}") from exc

    def upper_value(self) -> str:
        return self.value.upper()


class BccTrType(str, Enum):
    PAYMENT = "1"
    REFUND = "14"
    REVERSAL = "22"
    STATUS_CHECK = "90"
    CONNECTION_CHECK = "800"

    @classmethod
    def from_value(cls, value: Any) -> "BccTrType":
        raw = str(value or "").strip()
        try:
            return cls(raw)
        except ValueError as exc:
            allowed = ", ".join(item.value for item in cls)
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported TRTYPE: {value}. Allowed: {allowed}",
            ) from exc


class BccTranTrType(str, Enum):
    PAYMENT = "1"
    REFUND = "14"
    REVERSAL = "22"

    @classmethod
    def from_value(cls, value: Any) -> "BccTranTrType":
        raw = str(value or "").strip()
        try:
            return cls(raw)
        except ValueError as exc:
            allowed = ", ".join(item.value for item in cls)
            raise HTTPException(
                status_code=400,
                detail=f"Unsupported TRAN_TRTYPE: {value}. Allowed: {allowed}",
            ) from exc


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

app = FastAPI(title=settings.app_name)


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


def build_status_check_history_item(
    source: str,
    payload: dict,
    response: Optional[dict] = None,
    tran_trtype: Optional[BccTranTrType] = None,
) -> dict:
    item = {
        "source": source,
        "logged_at": iso_now(),
        "payload": payload,
    }
    if response is not None:
        item["response"] = response
    if tran_trtype is not None:
        item["tran_trtype"] = tran_trtype.value
    return item


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


def build_mac_data_trtype_1(payload: dict) -> str:
    return (
        lp(payload["AMOUNT"])
        + lp(payload["CURRENCY"])
        + lp(payload["ORDER"])
        + lp(payload["MERCHANT"])
        + lp(payload["TERMINAL"])
        + lp(payload["MERCH_GMT"])
        + lp(payload["TIMESTAMP"])
        + lp(payload["TRTYPE"])
        + lp(payload["NONCE"])
    )


def build_mac_data_trtype_14(payload: dict) -> str:
    return (
        lp(payload["ORDER"])
        + lp(payload["ORG_AMOUNT"])
        + lp(payload["AMOUNT"])
        + lp(payload["CURRENCY"])
        + lp(payload["RRN"])
        + lp(payload["INT_REF"])
        + lp(payload["TERMINAL"])
        + lp(payload["TIMESTAMP"])
        + lp(payload["TRTYPE"])
        + lp(payload["NONCE"])
    )


def build_mac_data_trtype_90(payload: dict) -> str:
    return (
        lp(payload["ORDER"])
        + lp(payload["TERMINAL"])
        + lp(payload["TIMESTAMP"])
        + lp(payload["TRTYPE"])
        + lp(payload["NONCE"])
    )


def calculate_p_sign(mac_data: str, hex_key: str) -> str:
    if not hex_key:
        raise HTTPException(status_code=500, detail="BCC_MAC_KEY_HEX is empty")

    key_bytes = bytes.fromhex(hex_key)
    return hmac.new(
        key_bytes,
        mac_data.encode("utf-8"),
        hashlib.sha1,
    ).hexdigest().upper()


def normalize_client_ip(request: Request) -> str:
    client_ip = request.client.host if request.client else "0.0.0.0"
    if client_ip in ("127.0.0.1", "::1"):
        return "0.0.0.0"
    return client_ip


def html_message(title: str, body: str, status_code: int = 200) -> HTMLResponse:
    html = f"""
    <!doctype html>
    <html lang="ru">
    <head>
      <meta charset="utf-8">
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
      <div class="box">
        <h1>{escape(title)}</h1>
        <p>{escape(body)}</p>
      </div>
    </body>
    </html>
    """
    return HTMLResponse(content=html, status_code=status_code)


def session_status(session: dict) -> PaymentStatus:
    return PaymentStatus.from_value(session["status"])


def is_expired(session: dict) -> bool:
    expires = datetime.fromisoformat(session["expires_at"])
    return expires < utc_now()


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


def sanitize_bank_response_for_log(response: httpx.Response) -> dict:
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
        "headers": {
            "content-type": content_type,
            "location": response.headers.get("location"),
        },
    }

    if isinstance(parsed_json, dict):
        meta["body_kind"] = "json"
        meta["parsed_body"] = redact_for_log(parsed_json)
        meta["important_fields"] = extract_important_bank_fields(parsed_json)
        return meta

    if isinstance(parsed_json, list):
        meta["body_kind"] = "json_list"
        meta["parsed_body"] = redact_for_log(parsed_json)
        return meta

    if parsed_kv is not None:
        meta["body_kind"] = "key_value"
        meta["parsed_body"] = redact_for_log(parsed_kv)
        meta["important_fields"] = extract_important_bank_fields(parsed_kv)
        return meta

    if is_html_content_type(content_type) or looks_like_html(text):
        meta["body_kind"] = "html"
        meta["body_preview"] = truncate_text(text, 1500)
        return meta

    if is_text_content_type(content_type):
        meta["body_kind"] = "text"
        meta["raw_text"] = truncate_text(text, 4000)
        return meta

    meta["body_kind"] = "unknown"
    meta["raw_text"] = truncate_text(text, 4000)
    return meta


def bank_log_json(title: str, data: Any, level: int = logging.INFO) -> None:
    try:
        safe_data = redact_for_log(data)
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
    # not used; kept intentionally absent to avoid sync form parsing
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


async def verify_bitrix_webhook_token(deal_id: int, token: Optional[str]) -> dict:
    field_code = (settings.webhook_secret or "").strip()

    if not field_code:
        raise HTTPException(
            status_code=500,
            detail="WEBHOOK_SECRET is not configured. Expected Bitrix deal field code in .env",
        )

    if token is None or str(token).strip() == "":
        raise HTTPException(status_code=403, detail="Webhook token is empty")

    try:
        deal = await get_deal(deal_id)
    except BitrixAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    field_value = get_first_value(deal.get(field_code))
    field_value_str = str(field_value).strip() if field_value is not None else ""

    if not field_value_str:
        raise HTTPException(
            status_code=403,
            detail=f"Webhook token field '{field_code}' is empty in Bitrix deal",
        )

    if not secrets.compare_digest(field_value_str, str(token).strip()):
        raise HTTPException(status_code=403, detail="Invalid webhook token")

    return deal


async def append_deal_comment(deal_id: int, message: str, extra_fields: Optional[dict] = None) -> None:
    try:
        deal = await get_deal(deal_id)
        fields = dict(extra_fields or {})
        fields["COMMENTS"] = append_comment(deal.get("COMMENTS"), message)
        await update_deal(deal_id, fields)
    except Exception as exc:
        log_exception("append_deal_comment_failed", exc)


async def resolve_refund_amount_from_bitrix(deal_id: int) -> Decimal:
    try:
        deal = await get_deal(deal_id)
    except BitrixAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    raw_value = get_first_value(deal.get(settings.field_payment_refund_amount))

    if raw_value is None or str(raw_value).strip() == "":
        message = (
            "При попытке совершить возврат выступила ошибка "
            "из-за отсутствия суммы в поле PAYMENT_REFUND_AMOUNT"
        )
        await append_deal_comment(
            deal_id,
            message,
            extra_fields={settings.field_payment_status: PaymentStatus.REFUND_FAILED.upper_value()}
            if settings.field_payment_status else None,
        )
        raise HTTPException(status_code=400, detail=message)

    try:
        amount = parse_decimal_amount(raw_value, allow_empty=False)
    except HTTPException:
        message = (
            "При попытке совершить возврат выступила ошибка "
            "из-за некорректной суммы в поле PAYMENT_REFUND_AMOUNT"
        )
        await append_deal_comment(
            deal_id,
            message,
            extra_fields={settings.field_payment_status: PaymentStatus.REFUND_FAILED.upper_value()}
            if settings.field_payment_status else None,
        )
        raise

    if amount <= DECIMAL_ZERO:
        message = (
            "При попытке совершить возврат выступила ошибка "
            "из-за отсутствия суммы в поле PAYMENT_REFUND_AMOUNT"
        )
        await append_deal_comment(
            deal_id,
            message,
            extra_fields={settings.field_payment_status: PaymentStatus.REFUND_FAILED.upper_value()}
            if settings.field_payment_status else None,
        )
        raise HTTPException(status_code=400, detail=message)

    return amount


def get_refundable_balance(session: dict) -> Decimal:
    original_amount = parse_decimal_amount(session["amount"])
    status = session_status(session)

    if status in {PaymentStatus.REFUNDED, PaymentStatus.REFUND_PENDING}:
        return DECIMAL_ZERO

    return original_amount


async def bcc_post_form(payload: dict, url: Optional[str] = None) -> dict:
    target_url = url or settings.bcc_trtype1_url

    bank_log_outgoing_request(target_url, payload)

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        response = await client.post(target_url, data=payload)

    bank_log_incoming_response(target_url, response)

    response.raise_for_status()

    text = response.text.strip()
    if not text:
        return {"raw_text": ""}

    try:
        return response.json()
    except Exception:
        pass

    parsed_kv = parse_key_value_text(text)
    if parsed_kv is not None:
        return parsed_kv

    return {"raw_text": text}


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
        },
    )


async def build_payment_snapshot_from_bitrix(deal_id: int) -> dict:
    try:
        deal = await get_deal(deal_id)
    except BitrixAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    field_payment = get_first_value(deal.get(settings.field_payment))
    field_invoice = get_first_value(deal.get(settings.field_invoice))
    field_product = get_first_value(deal.get(settings.field_product))
    field_policy_type_id = get_first_value(deal.get(settings.field_policy_type))
    contact_id = deal.get("CONTACT_ID")

    if not field_payment:
        raise HTTPException(status_code=400, detail=f"{settings.field_payment} is empty")
    if not contact_id:
        raise HTTPException(status_code=400, detail="CONTACT_ID is empty")
    if not field_invoice:
        raise HTTPException(status_code=400, detail=f"{settings.field_invoice} is empty")

    try:
        amount_str, currency = str(field_payment).split("|")
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid format in {settings.field_payment}. Expected 'amount|KZT'",
        ) from exc

    if currency.strip().upper() != "KZT":
        raise HTTPException(status_code=400, detail="Currency is not KZT")

    payment_amount = format_amount_for_bcc(amount_str)
    validate_test_amount_kzt(payment_amount)

    try:
        contact = await get_contact(int(contact_id))
    except BitrixAPIError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    name = (contact.get("NAME") or "").strip()
    last_name = (contact.get("LAST_NAME") or "").strip()
    email = get_multifield_value(contact.get("EMAIL"))
    phone = get_multifield_value(contact.get("PHONE"))
    policy_type_value = map_policy_type(field_policy_type_id)
    desc_value = build_desc(name, last_name, field_product, field_invoice, policy_type_value)

    snapshot = {
        "deal_id": int(deal_id),
        "contact_id": int(contact_id),
        "amount": payment_amount,
        "currency": "398",
        "invoice": str(field_invoice),
        "product": str(field_product or ""),
        "policy_type": policy_type_value or "",
        "customer_name": name,
        "customer_last_name": last_name,
        "customer_email": email or "",
        "customer_phone": phone or "",
        "description": desc_value,
    }

    log_json("bitrix_payment_snapshot_built", snapshot)
    return snapshot


def build_bcc_payload_from_session(session: dict, client_ip: str) -> tuple[dict, str]:
    timestamp_value = generate_timestamp()
    nonce_value = generate_nonce()

    trtype = BccTrType.PAYMENT

    payload = {
        "AMOUNT": decimal_to_str(parse_decimal_amount(session["amount"])),
        "CURRENCY": session["currency"],
        "ORDER": session["order_id"],
        "MERCH_RN_ID": session["merch_rn_id"],
        "DESC": session["description"],
        "MERCHANT": settings.merchant,
        "MERCH_NAME": settings.merch_name,
        "MERCH_URL": settings.merch_url,
        "COUNTRY": settings.country,
        "BRANDS": settings.brands,
        "TERMINAL": settings.terminal,
        "TIMESTAMP": timestamp_value,
        "MERCH_GMT": settings.merch_gmt,
        "TRTYPE": trtype.value,
        "BACKREF": settings.backref,
        "LANG": settings.lang,
        "NONCE": nonce_value,
        "P_SIGN": "",
        "MK_TOKEN": settings.mk_token,
        "NOTIFY_URL": settings.notify_url,
        "CLIENT_IP": client_ip,
    }

    mac_data_string = build_mac_data_trtype_1(payload)
    payload["P_SIGN"] = calculate_p_sign(mac_data_string, settings.mac_key_hex)

    return payload, mac_data_string


def render_bcc_redirect_form_with_minfo(action_url: str, payload: dict, phone: str) -> str:
    cc, subscriber = split_phone(phone or "")
    hidden_inputs = []

    for key, value in payload.items():
        safe_key = escape(str(key), quote=True)
        safe_value = escape("" if value is None else str(value), quote=True)
        hidden_inputs.append(f'<input type="hidden" name="{safe_key}" value="{safe_value}">')

    inputs_html = "\n    ".join(hidden_inputs)

    return f"""<!doctype html>
<html lang="ru">
<head>
  <meta charset="utf-8">
  <title>Переход к оплате</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    body {{
      font-family: Arial, sans-serif;
      max-width: 720px;
      margin: 40px auto;
      line-height: 1.5;
      color: #222;
      padding: 0 16px;
    }}
    .box {{
      border: 1px solid #ddd;
      border-radius: 8px;
      padding: 24px;
    }}
    .meta {{
      color: #666;
      font-size: 14px;
    }}
  </style>
</head>
<body>
  <div class="box">
    <h1>Переход на страницу оплаты</h1>
    <p>Сейчас вы будете перенаправлены на защищённую страницу банка.</p>
    <p class="meta">Если переход не выполнится автоматически, нажмите кнопку ниже.</p>

    <form id="bcc-payment-form" method="post" action="{escape(action_url, quote=True)}">
      {inputs_html}
      <input type="hidden" name="M_INFO" id="m_info" value="">
      <noscript>
        <button type="submit">Перейти к оплате</button>
      </noscript>
    </form>
  </div>

  <script>
    (function() {{
      const mInfo = {{
        browserScreenHeight: String(window.outerHeight || screen.height || 0),
        browserScreenWidth: String(window.outerWidth || screen.width || 0),
        mobilePhone: {{
          cc: {json.dumps(cc)},
          subscriber: {json.dumps(subscriber)}
        }}
      }};

      document.getElementById("m_info").value = btoa(JSON.stringify(mInfo));
      document.getElementById("bcc-payment-form").submit();
    }})();
  </script>
</body>
</html>"""


def build_bitrix_fields_for_session(payment_url: str, status_value: PaymentStatus, order_id: str) -> dict:
    bitrix_fields = {}
    if settings.field_payment_url:
        bitrix_fields[settings.field_payment_url] = payment_url
    if settings.field_payment_status:
        bitrix_fields[settings.field_payment_status] = status_value.upper_value()
    if settings.field_payment_order:
        bitrix_fields[settings.field_payment_order] = order_id
    return bitrix_fields


async def sync_session_to_bitrix(deal_id: int, payment_url: str, status_value: PaymentStatus, order_id: str) -> None:
    bitrix_fields = build_bitrix_fields_for_session(payment_url, status_value, order_id)

    if not bitrix_fields:
        return

    await update_deal(deal_id, bitrix_fields)


async def create_payment_session_for_deal(deal_id: int) -> dict:
    snapshot = await build_payment_snapshot_from_bitrix(deal_id)
    now = utc_now()
    now_iso = now.isoformat()

    with immediate_transaction() as conn:
        expire_stale_sessions_for_deal(deal_id, now_iso, conn)

        existing = get_latest_payment_session_by_deal_id(deal_id, conn=conn)
        if existing:
            existing_status = session_status(existing)
            if existing_status in {PaymentStatus.CREATED, PaymentStatus.PENDING} and not is_expired(existing):
                payment_url = f"{settings.public_base_url}/pay/{existing['token']}"
                result = {
                    "payment_url": payment_url,
                    "token": existing["token"],
                    "order_id": existing["order_id"],
                    "status": existing["status"],
                    "expires_at": existing["expires_at"],
                    "amount": decimal_to_str(parse_decimal_amount(existing["amount"])),
                    "currency": existing["currency"],
                    "reused": True,
                }
                log_json("payment_session_reused", result)
            else:
                result = None
        else:
            result = None

        if result is None:
            created_session = None
            last_error = None

            for _ in range(5):
                token = generate_secure_token()
                order_id = generate_order_id(deal_id)
                merch_rn_id = generate_merch_rn_id()
                expires_at = now + timedelta(minutes=settings.payment_link_ttl_minutes)

                session_data = {
                    **snapshot,
                    "amount": decimal_to_str(parse_decimal_amount(snapshot["amount"])),
                    "token": token,
                    "order_id": order_id,
                    "merch_rn_id": merch_rn_id,
                    "status": PaymentStatus.CREATED.value,
                    "expires_at": expires_at.isoformat(),
                    "created_at": now_iso,
                    "updated_at": now_iso,
                }

                try:
                    created_session = create_payment_session(session_data, conn=conn)
                    break
                except Exception as exc:
                    last_error = exc

                expire_stale_sessions_for_deal(deal_id, now_iso, conn)
                existing = get_latest_payment_session_by_deal_id(deal_id, conn=conn)
                if existing:
                    existing_status = session_status(existing)
                    if existing_status in {PaymentStatus.CREATED, PaymentStatus.PENDING} and not is_expired(existing):
                        created_session = existing
                        break

            if created_session is None:
                raise HTTPException(status_code=409, detail=f"Could not create payment session: {last_error}")

            payment_url = f"{settings.public_base_url}/pay/{created_session['token']}"
            result = {
                "payment_url": payment_url,
                "token": created_session["token"],
                "order_id": created_session["order_id"],
                "merch_rn_id": created_session["merch_rn_id"],
                "status": created_session["status"],
                "expires_at": created_session["expires_at"],
                "amount": decimal_to_str(parse_decimal_amount(created_session["amount"])),
                "currency": created_session["currency"],
                "reused": False,
            }

    try:
        await sync_session_to_bitrix(
            deal_id=deal_id,
            payment_url=result["payment_url"],
            status_value=PaymentStatus.from_value(result["status"]),
            order_id=result["order_id"],
        )
    except BitrixAPIError as exc:
        log_exception("sync_session_to_bitrix_bitrix_error", exc)
    except Exception as exc:
        log_exception("sync_session_to_bitrix_failed", exc)

    log_json("payment_session_ready", result)
    return result


def get_status_for_deal(deal_id: int) -> dict:
    session = get_latest_payment_session_by_deal_id(deal_id)

    if not session:
        raise HTTPException(status_code=404, detail="Payment session not found for deal")

    return {
        "deal_id": session["deal_id"],
        "token": session["token"],
        "order_id": session["order_id"],
        "merch_rn_id": session.get("merch_rn_id"),
        "status": session["status"],
        "bank_status": session["bank_status"],
        "amount": decimal_to_str(parse_decimal_amount(session["amount"])),
        "currency": session["currency"],
        "rrn": session.get("rrn"),
        "int_ref": session.get("int_ref"),
        "result_code": session.get("result_code"),
        "rc_code": session.get("rc_code"),
        "created_at": session["created_at"],
        "opened_at": session["opened_at"],
        "started_at": session["started_at"],
        "callback_received_at": session["callback_received_at"],
        "paid_at": session["paid_at"],
        "refunded_at": session["refunded_at"],
        "expires_at": session["expires_at"],
        "last_notify_trtype": session.get("last_notify_trtype"),
        "status_checks_json": session.get("status_checks_json"),
        "refund_callback_json": session.get("refund_callback_json"),
        "version": session.get("version"),
    }


async def bcc_refund_for_session(session: dict, refund_amount: Decimal) -> dict:
    if not session.get("rrn"):
        raise HTTPException(status_code=400, detail="RRN is missing for refund")
    if not session.get("int_ref"):
        raise HTTPException(status_code=400, detail="INT_REF is missing for refund")
    if not session.get("merch_rn_id"):
        raise HTTPException(status_code=400, detail="MERCH_RN_ID is missing for refund")

    original_amount = parse_decimal_amount(session["amount"])
    refund_amount = parse_decimal_amount(refund_amount)
    refund_amount_str = decimal_to_str(refund_amount)
    original_amount_str = decimal_to_str(original_amount)

    timestamp_value = generate_timestamp()
    nonce_value = generate_nonce()

    paid_at_raw = session.get("paid_at")
    is_same_day_full_refund = False
    if paid_at_raw:
        try:
            paid_at_dt = datetime.fromisoformat(paid_at_raw)
            is_same_day_full_refund = (
                refund_amount == original_amount and same_utc_date(paid_at_dt, utc_now())
            )
        except Exception:
            pass

    trtype = BccTrType.REVERSAL if is_same_day_full_refund else BccTrType.REFUND

    payload = {
        "ORG_AMOUNT": original_amount_str,
        "AMOUNT": refund_amount_str,
        "CURRENCY": session["currency"],
        "ORDER": session["order_id"],
        "MERCH_RN_ID": session["merch_rn_id"],
        "RRN": session["rrn"],
        "INT_REF": session["int_ref"],
        "TERMINAL": settings.terminal,
        "TIMESTAMP": timestamp_value,
        "MERCH_GMT": settings.merch_gmt,
        "TRTYPE": trtype.value,
        "BACKREF": settings.backref,
        "LANG": settings.lang,
        "NONCE": nonce_value,
        "P_SIGN": "",
        "NOTIFY_URL": settings.notify_url,
    }

    mac_data = build_mac_data_trtype_14(payload)
    payload["P_SIGN"] = calculate_p_sign(mac_data, settings.mac_key_hex)

    response_data = await bcc_post_form(payload, url=settings.bcc_trtype1_url)

    return {
        "request_payload": payload,
        "response": response_data,
        "trtype": trtype.value,
    }


def apply_payment_callback_update(current: dict, *, raw_result: str, data: dict, bank_result: dict) -> dict:
    current_status = session_status(current)
    success_codes = {"0", "00", "000", "APPROVED", "SUCCESS"}
    is_success = raw_result in success_codes

    if current_status in {PaymentStatus.REFUND_PENDING, PaymentStatus.REFUNDED, PaymentStatus.REFUND_FAILED}:
        return {
            "status": current["status"],
            "bank_status": current.get("bank_status") or raw_result or "CALLBACK_IGNORED_AFTER_REFUND",
            "callback_json": json.dumps({"notify": data, "verified_by_bank": bank_result}, ensure_ascii=False),
            "callback_received_at": iso_now(),
            "result_code": str(bank_result.get("RESULT") or "").strip().upper() or None,
            "rc_code": str(bank_result.get("RC") or "").strip().upper() or None,
            "rrn": str(bank_result.get("RRN") or data.get("RRN") or "").strip() or current.get("rrn"),
            "int_ref": str(bank_result.get("INT_REF") or data.get("INT_REF") or "").strip() or current.get("int_ref"),
            "last_notify_trtype": BccTrType.PAYMENT.value,
            "updated_at": iso_now(),
        }

    next_status = current_status
    if is_success:
        if current_status in {PaymentStatus.CREATED, PaymentStatus.PENDING, PaymentStatus.FAILED, PaymentStatus.PAID}:
            next_status = PaymentStatus.PAID
    else:
        if current_status in {PaymentStatus.CREATED, PaymentStatus.PENDING, PaymentStatus.FAILED}:
            next_status = PaymentStatus.FAILED

    return {
        "status": next_status.value,
        "bank_status": raw_result or "CALLBACK_VERIFIED",
        "callback_json": json.dumps({"notify": data, "verified_by_bank": bank_result}, ensure_ascii=False),
        "callback_received_at": iso_now(),
        "paid_at": iso_now() if next_status == PaymentStatus.PAID and not current.get("paid_at") else current.get("paid_at"),
        "result_code": str(bank_result.get("RESULT") or "").strip().upper() or None,
        "rc_code": str(bank_result.get("RC") or "").strip().upper() or None,
        "rrn": str(bank_result.get("RRN") or data.get("RRN") or "").strip() or current.get("rrn"),
        "int_ref": str(bank_result.get("INT_REF") or data.get("INT_REF") or "").strip() or current.get("int_ref"),
        "last_notify_trtype": BccTrType.PAYMENT.value,
        "updated_at": iso_now(),
    }


def apply_refund_callback_update(current: dict, *, trtype: BccTrType, raw_result: str, data: dict, bank_result: dict) -> dict:
    current_status = session_status(current)
    success_codes = {"0", "00", "000", "APPROVED", "SUCCESS"}
    is_success = raw_result in success_codes

    if current_status == PaymentStatus.REFUNDED:
        next_status = PaymentStatus.REFUNDED
    elif is_success:
        if current_status in {PaymentStatus.REFUND_PENDING, PaymentStatus.REFUND_FAILED, PaymentStatus.REFUNDED}:
            next_status = PaymentStatus.REFUNDED
        else:
            next_status = current_status
    else:
        if current_status == PaymentStatus.REFUND_PENDING:
            next_status = PaymentStatus.REFUND_FAILED
        elif current_status == PaymentStatus.REFUNDED:
            next_status = PaymentStatus.REFUNDED
        else:
            next_status = current_status

    return {
        "status": next_status.value if isinstance(next_status, PaymentStatus) else str(next_status),
        "bank_status": raw_result or "CALLBACK_VERIFIED",
        "refund_callback_json": json.dumps({"notify": data, "verified_by_bank": bank_result}, ensure_ascii=False),
        "callback_received_at": iso_now(),
        "refunded_at": iso_now() if next_status == PaymentStatus.REFUNDED and not current.get("refunded_at") else current.get("refunded_at"),
        "result_code": str(bank_result.get("RESULT") or "").strip().upper() or None,
        "rc_code": str(bank_result.get("RC") or "").strip().upper() or None,
        "rrn": str(bank_result.get("RRN") or data.get("RRN") or "").strip() or current.get("rrn"),
        "int_ref": str(bank_result.get("INT_REF") or data.get("INT_REF") or "").strip() or current.get("int_ref"),
        "last_notify_trtype": trtype.value,
        "updated_at": iso_now(),
    }


async def mark_refund_for_deal(deal_id: int, reason: Optional[str] = None) -> dict:
    requested_amount = await resolve_refund_amount_from_bitrix(deal_id)

    with immediate_transaction() as conn:
        session = get_latest_payment_session_by_deal_id(deal_id, conn=conn)

        if not session:
            raise HTTPException(status_code=404, detail="Payment session not found for deal")

        current_status = session_status(session)
        if current_status not in {PaymentStatus.PAID, PaymentStatus.REFUND_PENDING, PaymentStatus.REFUNDED}:
            raise HTTPException(
                status_code=400,
                detail="При попытке совершить возврат выступила ошибка: возврат возможен только для оплаченного платежа",
            )

        refundable_balance = get_refundable_balance(session)
        if refundable_balance <= DECIMAL_ZERO:
            if current_status == PaymentStatus.REFUND_PENDING:
                return {
                    "deal_id": session["deal_id"],
                    "token": session["token"],
                    "order_id": session["order_id"],
                    "status": session["status"],
                    "refund_amount": session.get("bank_status"),
                    "reason": reason or "",
                    "duplicate": True,
                }
            raise HTTPException(
                status_code=400,
                detail="При попытке совершить возврат выступила ошибка: отсутствует доступный остаток для возврата",
            )

        effective_amount = min(parse_decimal_amount(requested_amount), parse_decimal_amount(refundable_balance))
        effective_amount_str = decimal_to_str(effective_amount)

        refund_idempotency_key = build_idempotency_key(
            "refund",
            {
                "session_token": session["token"],
                "order_id": session["order_id"],
                "amount": effective_amount_str,
                "reason": reason or "",
            },
        )

        operation, created = create_payment_operation(
            operation_type="refund",
            idempotency_key=refund_idempotency_key,
            session_token=session["token"],
            status="processing",
            request_json=stable_json_dumps(
                {
                    "deal_id": deal_id,
                    "order_id": session["order_id"],
                    "amount": effective_amount_str,
                    "reason": reason or "",
                }
            ),
            created_at=iso_now(),
            updated_at=iso_now(),
            conn=conn,
        )

        if not created:
            existing_response = parse_json_object(operation.get("response_json"))
            if existing_response:
                existing_response["duplicate"] = True
                return existing_response

            return {
                "deal_id": session["deal_id"],
                "token": session["token"],
                "order_id": session["order_id"],
                "status": session["status"],
                "refund_amount": effective_amount_str,
                "reason": reason or "",
                "duplicate": True,
            }

        session = update_payment_session(
            session["token"],
            {
                "status": PaymentStatus.REFUND_PENDING.value,
                "bank_status": f"REFUND_PROCESSING:{effective_amount_str}",
                "updated_at": iso_now(),
            },
            expected_version=session["version"],
            conn=conn,
        )

    try:
        bcc_result = await bcc_refund_for_session(session, effective_amount)
    except Exception as exc:
        with immediate_transaction() as conn:
            fresh = get_payment_session_by_token(session["token"], conn=conn)
            if fresh and session_status(fresh) == PaymentStatus.REFUND_PENDING:
                update_payment_session(
                    fresh["token"],
                    {
                        "status": PaymentStatus.REFUND_FAILED.value,
                        "bank_status": "REFUND_REQUEST_ERROR",
                        "updated_at": iso_now(),
                    },
                    expected_version=fresh["version"],
                    conn=conn,
                )
            operation = get_payment_operation("refund", refund_idempotency_key, conn=conn)
            if operation:
                finish_payment_operation(
                    operation["id"],
                    status="failed",
                    response_json=stable_json_dumps({"error": str(exc)}),
                    updated_at=iso_now(),
                    conn=conn,
                )

        message = f"При попытке совершить возврат выступила ошибка: {str(exc)}"
        await append_deal_comment(
            deal_id,
            message,
            extra_fields={settings.field_payment_status: PaymentStatus.REFUND_FAILED.upper_value()}
            if settings.field_payment_status else None,
        )
        raise

    with immediate_transaction() as conn:
        fresh = get_payment_session_by_token(session["token"], conn=conn)
        updated = update_payment_session(
            fresh["token"],
            {
                "status": PaymentStatus.REFUND_PENDING.value,
                "bank_status": f"REFUND_REQUESTED:{effective_amount_str}",
                "refund_callback_json": json.dumps(
                    {
                        "refund_request": bcc_result["response"],
                        "refund_payload": bcc_result["request_payload"],
                        "refund_amount": effective_amount_str,
                        "refund_reason": reason or "",
                        "refund_trtype": bcc_result["trtype"],
                        "logged_at": iso_now(),
                    },
                    ensure_ascii=False,
                ),
                "updated_at": iso_now(),
            },
            expected_version=fresh["version"],
            conn=conn,
        )

        operation = get_payment_operation("refund", refund_idempotency_key, conn=conn)
        result = {
            "deal_id": updated["deal_id"],
            "token": updated["token"],
            "order_id": updated["order_id"],
            "status": updated["status"],
            "refund_amount": effective_amount_str,
            "reason": reason or "",
            "refund_trtype": bcc_result["trtype"],
            "bcc_response": bcc_result["response"],
        }
        if operation:
            finish_payment_operation(
                operation["id"],
                status="completed",
                response_json=stable_json_dumps(result),
                updated_at=iso_now(),
                conn=conn,
            )

    bitrix_fields = {}
    if settings.field_payment_status:
        bitrix_fields[settings.field_payment_status] = PaymentStatus.REFUND_PENDING.upper_value()
    if settings.field_payment_refund_amount:
        bitrix_fields[settings.field_payment_refund_amount] = effective_amount_str

    await append_deal_comment(
        updated["deal_id"],
        f"Совершен возврат денежных средств в размере {effective_amount_str} KZT",
        extra_fields=bitrix_fields or None,
    )

    log_json("payment_refund_requested", result)
    return result


async def record_status_check_history(
    token: str,
    history_item: dict,
    *,
    last_notify_trtype: str,
) -> None:
    try:
        with immediate_transaction() as conn:
            session = get_payment_session_by_token(token, conn=conn)
            if not session:
                return
            update_payment_session(
                token,
                {
                    "status_checks_json": append_json_list(session.get("status_checks_json"), history_item),
                    "last_notify_trtype": last_notify_trtype,
                    "updated_at": iso_now(),
                },
                expected_version=session["version"],
                conn=conn,
            )
    except Exception as exc:
        log_exception("status_check_history_update_failed", exc)


async def bcc_status_check_for_session(
    session: dict,
    tran_trtype: BccTranTrType = BccTranTrType.PAYMENT,
) -> dict:
    timestamp_value = generate_timestamp()
    nonce_value = generate_nonce()

    payload = {
        "TERMINAL": settings.terminal,
        "TRTYPE": BccTrType.STATUS_CHECK.value,
        "TRAN_TRTYPE": tran_trtype.value,
        "ORDER": session["order_id"],
        "TIMESTAMP": timestamp_value,
        "MERCH_GMT": settings.merch_gmt,
        "NONCE": nonce_value,
        "P_SIGN": "",
        "NOTIFY_URL": settings.notify_url,
    }

    mac_data = build_mac_data_trtype_90(payload)
    payload["P_SIGN"] = calculate_p_sign(mac_data, settings.mac_key_hex)

    response_data = await bcc_post_form(payload)

    history_item = build_status_check_history_item(
        source="request",
        payload=payload,
        response=response_data,
        tran_trtype=tran_trtype,
    )

    await record_status_check_history(
        session["token"],
        history_item,
        last_notify_trtype=BccTrType.STATUS_CHECK.value,
    )

    return response_data


async def bcc_connection_check() -> dict:
    payload = {
        "TERMINAL": settings.terminal,
        "TRTYPE": BccTrType.CONNECTION_CHECK.value,
        "BACKREF": settings.backref,
        "LANG": settings.lang,
        "NOTIFY_URL": settings.notify_url,
    }

    return await bcc_post_form(payload)


def build_basic_auth_expected_value(username: str, password: str) -> str:
    raw = f"{username}:{password}".encode("utf-8")
    encoded = base64.b64encode(raw).decode("ascii")
    return f"Basic {encoded}"


def verify_notify_basic_auth(authorization_header: Optional[str]) -> None:
    username = (getattr(settings, "notify_basic_username", "") or "").strip()
    password = getattr(settings, "notify_basic_password", "") or ""
    realm = (getattr(settings, "notify_basic_realm", "BCC Notify") or "BCC Notify").strip()

    if not username:
        raise HTTPException(status_code=500, detail="Notify Basic Auth username is not configured")

    expected = build_basic_auth_expected_value(username, password)
    actual = (authorization_header or "").strip()

    if not actual:
        raise HTTPException(
            status_code=401,
            detail="Missing Authorization header",
            headers={"WWW-Authenticate": f'Basic realm="{realm}"'},
        )

    if not secrets.compare_digest(actual, expected):
        raise HTTPException(
            status_code=401,
            detail="Invalid Authorization header",
            headers={"WWW-Authenticate": f'Basic realm="{realm}"'},
        )


def validate_notify_invariants(session: dict, data: dict) -> BccTrType:
    required = ["ORDER", "TRTYPE", "TERMINAL", "TIMESTAMP", "NONCE", "P_SIGN"]
    missing = [k for k in required if not str(data.get(k) or "").strip()]
    if missing:
        raise HTTPException(status_code=400, detail=f"Missing notify fields: {', '.join(missing)}")

    order_id = str(data.get("ORDER") or "").strip()
    trtype = BccTrType.from_value(data.get("TRTYPE"))
    terminal = str(data.get("TERMINAL") or "").strip()
    currency = str(data.get("CURRENCY") or "").strip()
    amount_raw = data.get("AMOUNT")

    if order_id != str(session["order_id"]).strip():
        raise HTTPException(status_code=403, detail="ORDER mismatch")

    if terminal != str(settings.terminal).strip():
        raise HTTPException(status_code=403, detail="TERMINAL mismatch")

    merchant = str(data.get("MERCHANT") or "").strip()
    if merchant and merchant != str(settings.merchant).strip():
        raise HTTPException(status_code=403, detail="MERCHANT mismatch")

    if trtype == BccTrType.PAYMENT:
        if amount_raw not in (None, ""):
            notify_amount = parse_decimal_amount(amount_raw)
            session_amount = parse_decimal_amount(session["amount"])
            if notify_amount != session_amount:
                raise HTTPException(status_code=403, detail="AMOUNT mismatch")
        if currency and currency != str(session["currency"]).strip():
            raise HTTPException(status_code=403, detail="CURRENCY mismatch")

    if trtype in {BccTrType.REFUND, BccTrType.REVERSAL}:
        org_amount_raw = data.get("ORG_AMOUNT")
        rrn = str(data.get("RRN") or "").strip()
        int_ref = str(data.get("INT_REF") or "").strip()
        merch_rn_id = str(data.get("MERCH_RN_ID") or "").strip()

        if currency and currency != str(session["currency"]).strip():
            raise HTTPException(status_code=403, detail="CURRENCY mismatch")

        if org_amount_raw not in (None, ""):
            notify_org_amount = parse_decimal_amount(org_amount_raw)
            session_amount = parse_decimal_amount(session["amount"])
            if notify_org_amount != session_amount:
                raise HTTPException(status_code=403, detail="ORG_AMOUNT mismatch")

        if session.get("rrn") and rrn and rrn != str(session["rrn"]).strip():
            raise HTTPException(status_code=403, detail="RRN mismatch")

        if session.get("int_ref") and int_ref and int_ref != str(session["int_ref"]).strip():
            raise HTTPException(status_code=403, detail="INT_REF mismatch")

        if session.get("merch_rn_id") and merch_rn_id and merch_rn_id != str(session["merch_rn_id"]).strip():
            raise HTTPException(status_code=403, detail="MERCH_RN_ID mismatch")

    return trtype


def is_success_bank_result(data: dict) -> bool:
    raw_result = str(data.get("RESULT") or data.get("RC") or data.get("STATUS") or "").strip().upper()
    return raw_result in {"0", "00", "000", "APPROVED", "SUCCESS"}


def compare_notify_and_bank_response(notify_data: dict, bank_data: dict, *, trtype: BccTrType) -> None:
    text_fields = ["ORDER", "TERMINAL", "RRN", "INT_REF"]

    for field in text_fields:
        notify_value = str(notify_data.get(field) or "").strip()
        bank_value = str(bank_data.get(field) or "").strip()

        if notify_value and bank_value and notify_value != bank_value:
            raise HTTPException(status_code=403, detail=f"Notify mismatch for {field}")

    if trtype in {BccTrType.PAYMENT, BccTrType.REFUND, BccTrType.REVERSAL}:
        notify_currency = str(notify_data.get("CURRENCY") or "").strip()
        bank_currency = str(bank_data.get("CURRENCY") or "").strip()
        if notify_currency and bank_currency and notify_currency != bank_currency:
            raise HTTPException(status_code=403, detail="Notify mismatch for CURRENCY")

        notify_amount_raw = notify_data.get("AMOUNT")
        bank_amount_raw = bank_data.get("AMOUNT")
        if notify_amount_raw not in (None, "") and bank_amount_raw not in (None, ""):
            notify_amount = parse_decimal_amount(notify_amount_raw)
            bank_amount = parse_decimal_amount(bank_amount_raw)
            if notify_amount != bank_amount:
                raise HTTPException(status_code=403, detail="Notify mismatch for AMOUNT")


async def verify_notify_with_bank(session: dict, notify_data: dict, trtype: BccTrType) -> dict:
    if trtype == BccTrType.PAYMENT:
        bank_result = await bcc_status_check_for_session(session, tran_trtype=BccTranTrType.PAYMENT)
    elif trtype == BccTrType.REFUND:
        bank_result = await bcc_status_check_for_session(session, tran_trtype=BccTranTrType.REFUND)
    elif trtype == BccTrType.REVERSAL:
        bank_result = await bcc_status_check_for_session(session, tran_trtype=BccTranTrType.REVERSAL)
    elif trtype == BccTrType.STATUS_CHECK:
        return {"ok": True, "bank_result": None}
    else:
        raise HTTPException(status_code=400, detail=f"Unsupported TRTYPE: {trtype.value}")

    if not isinstance(bank_result, dict):
        raise HTTPException(status_code=502, detail="Invalid bank verification response")

    if not is_success_bank_result(bank_result):
        raise HTTPException(status_code=403, detail="Bank did not confirm operation status")

    compare_notify_and_bank_response(notify_data, bank_result, trtype=trtype)

    return {"ok": True, "bank_result": bank_result}


@app.get("/")
async def root():
    return {"status": "ok"}


@app.post("/payments/create/{deal_id}")
async def create_payment(deal_id: int):
    return await create_payment_session_for_deal(deal_id)


@app.get("/pay/{token}", response_class=HTMLResponse)
async def pay_page(token: str, request: Request):
    client_ip = normalize_client_ip(request)

    with immediate_transaction() as conn:
        session = get_payment_session_by_token(token, conn=conn)

        if not session:
            raise HTTPException(status_code=404, detail="Payment session not found")

        current_status = session_status(session)

        if current_status == PaymentStatus.PAID:
            return html_message("Оплата уже получена", "Этот счёт уже оплачен.")
        if current_status == PaymentStatus.REFUNDED:
            return html_message("Платёж возвращён", "По этой ссылке платёж уже возвращён.")
        if current_status == PaymentStatus.REFUND_PENDING:
            return html_message("Возврат в обработке", "Для этого платежа уже инициирован возврат.")
        if current_status == PaymentStatus.EXPIRED:
            return html_message("Ссылка истекла", "Срок действия платёжной ссылки истёк.", 410)

        if is_expired(session):
            update_payment_session(
                token,
                {"status": PaymentStatus.EXPIRED.value, "updated_at": iso_now()},
                expected_version=session["version"],
                conn=conn,
            )
            return html_message("Ссылка истекла", "Срок действия платёжной ссылки истёк.", 410)

        payload = None
        mac_data_len = None

        if current_status == PaymentStatus.PENDING and session.get("bcc_payload_json"):
            payload = parse_json_object(session.get("bcc_payload_json"))
            if payload:
                payload["AMOUNT"] = decimal_to_str(parse_decimal_amount(payload.get("AMOUNT")))
                mac_data_len = len(build_mac_data_trtype_1({**payload, "P_SIGN": ""}))

        if payload is None:
            payload, mac_data = build_bcc_payload_from_session(session, client_ip)
            mac_data_len = len(mac_data)

            session = update_payment_session(
                token,
                {
                    "status": PaymentStatus.PENDING.value,
                    "opened_at": session.get("opened_at") or iso_now(),
                    "started_at": session.get("started_at") or iso_now(),
                    "bcc_payload_json": json.dumps(payload, ensure_ascii=False),
                    "bank_status": "FORM_RENDERED",
                    "updated_at": iso_now(),
                },
                expected_version=session["version"],
                conn=conn,
            )

    log_json(
        "payment_form_rendered",
        {
            "token": token,
            "order_id": session["order_id"],
            "deal_id": session["deal_id"],
            "client_ip": client_ip,
            "mac_data_len": mac_data_len,
        },
    )

    bank_log_json(
        "BANK REDIRECT PREPARED",
        {
            "logged_at": iso_now(),
            "order_id": session["order_id"],
            "deal_id": session["deal_id"],
            "token": session["token"],
            "bcc_url": settings.bcc_trtype1_url,
            "amount": session["amount"],
            "currency": session["currency"],
            "client_ip": client_ip,
        },
    )

    html = render_bcc_redirect_form_with_minfo(
        action_url=settings.bcc_trtype1_url,
        payload=payload,
        phone=session["customer_phone"] or "",
    )
    return HTMLResponse(content=html, status_code=200)


@app.api_route("/bcc/backref", methods=["GET", "POST"])
async def bcc_backref(request: Request):
    data = await parse_request_kv_data(request)
    bank_log_backref(data, request.method)

    order_id = str(data.get("ORDER", "")).strip()
    session = get_payment_session_by_order(order_id) if order_id else None

    title = "Возврат с платёжной страницы"
    if session:
        body = (
            f"Заказ: {session['order_id']}. "
            f"Текущий локальный статус: {session['status']}. "
            f"Окончательное подтверждение ожидается по серверному уведомлению банка."
        )
    else:
        body = (
            "Платёжная система вернула клиента на сайт. "
            "Окончательный статус будет подтверждён серверным уведомлением банка."
        )

    return html_message(title, body)


@app.api_route("/bcc/notify", methods=["GET", "POST"])
async def bcc_notify(request: Request):
    log_notify_raw_request(request)

    authorization_header = request.headers.get("Authorization")
    verify_notify_basic_auth(authorization_header)

    data = await parse_request_kv_data(request)

    bank_log_notify_callback(
        {
            **data,
            "_auth_header_present": bool(authorization_header),
            "_method": request.method,
            "_content_type": request.headers.get("content-type"),
            "_client_ip": request.client.host if request.client else None,
        },
        request.method,
    )

    order_id = str(data.get("ORDER") or "").strip()
    if not order_id:
        raise HTTPException(status_code=400, detail="ORDER is missing")

    session = get_payment_session_by_order(order_id)
    if not session:
        raise HTTPException(status_code=404, detail="Payment session not found for ORDER")

    trtype = validate_notify_invariants(session, data)

    notify_idempotency_key = build_idempotency_key(
        "notify",
        {
            "order_id": order_id,
            "trtype": trtype.value,
            "payload": {k: str(v) for k, v in sorted(data.items())},
        },
    )

    with immediate_transaction() as conn:
        operation, created = create_payment_operation(
            operation_type="notify",
            idempotency_key=notify_idempotency_key,
            session_token=session["token"],
            status="processing",
            request_json=stable_json_dumps({"order_id": order_id, "trtype": trtype.value, "data": data}),
            created_at=iso_now(),
            updated_at=iso_now(),
            conn=conn,
        )

        if not created:
            existing_response = parse_json_object(operation.get("response_json"))
            if existing_response is not None:
                existing_response["duplicate"] = True
                return JSONResponse(existing_response)
            return JSONResponse({"ok": True, "duplicate": True})

    if trtype == BccTrType.STATUS_CHECK:
        tran_trtype_value = str(data.get("TRAN_TRTYPE") or "").strip()
        tran_trtype = BccTranTrType.from_value(tran_trtype_value) if tran_trtype_value else None

        history_item = build_status_check_history_item(
            source="notify",
            payload=data,
            response=None,
            tran_trtype=tran_trtype,
        )

        await record_status_check_history(
            session["token"],
            history_item,
            last_notify_trtype=BccTrType.STATUS_CHECK.value,
        )

        with immediate_transaction() as conn:
            operation = get_payment_operation("notify", notify_idempotency_key, conn=conn)
            if operation:
                finish_payment_operation(
                    operation["id"],
                    status="completed",
                    response_json=stable_json_dumps(
                        {"ok": True, "ignored": True, "trtype": BccTrType.STATUS_CHECK.value}
                    ),
                    updated_at=iso_now(),
                    conn=conn,
                )

        return JSONResponse({"ok": True, "ignored": True, "trtype": BccTrType.STATUS_CHECK.value})

    verification = await verify_notify_with_bank(session, data, trtype)
    bank_result = verification["bank_result"]

    raw_result = str(
        bank_result.get("RESULT") or bank_result.get("RC") or bank_result.get("STATUS") or ""
    ).strip().upper()

    with immediate_transaction() as conn:
        current = get_payment_session_by_order(order_id, conn=conn)
        if not current:
            raise HTTPException(status_code=404, detail="Payment session not found for ORDER")

        validate_notify_invariants(current, data)

        if trtype == BccTrType.PAYMENT:
            fields = apply_payment_callback_update(
                current,
                raw_result=raw_result,
                data=data,
                bank_result=bank_result,
            )
        elif trtype in {BccTrType.REFUND, BccTrType.REVERSAL}:
            fields = apply_refund_callback_update(
                current,
                trtype=trtype,
                raw_result=raw_result,
                data=data,
                bank_result=bank_result,
            )
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported TRTYPE: {trtype.value}")

        updated = update_payment_session(
            current["token"],
            fields,
            expected_version=current["version"],
            conn=conn,
        )

        operation = get_payment_operation("notify", notify_idempotency_key, conn=conn)
        if operation:
            finish_payment_operation(
                operation["id"],
                status="completed",
                response_json=stable_json_dumps({"ok": True}),
                updated_at=iso_now(),
                conn=conn,
            )

    if trtype == BccTrType.PAYMENT:
        next_status = PaymentStatus.from_value(updated["status"])

        if settings.field_payment_status:
            try:
                await update_deal(updated["deal_id"], {settings.field_payment_status: next_status.upper_value()})
            except Exception as exc:
                log_exception("update_deal_payment_status_failed", exc)

        log_json(
            "payment_notify_processed",
            {
                "deal_id": updated["deal_id"],
                "order_id": updated["order_id"],
                "status": updated["status"],
                "trtype": trtype.value,
                "result_code": updated.get("result_code"),
                "rc_code": updated.get("rc_code"),
            },
        )
        return JSONResponse({"ok": True})

    if trtype in {BccTrType.REFUND, BccTrType.REVERSAL}:
        next_status = PaymentStatus.from_value(updated["status"])

        bitrix_fields = {}
        if settings.field_payment_status:
            bitrix_fields[settings.field_payment_status] = next_status.upper_value()

        try:
            if next_status == PaymentStatus.REFUNDED:
                await append_deal_comment(
                    updated["deal_id"],
                    f"Возврат подтверждён банком по заказу {updated['order_id']}",
                    extra_fields=bitrix_fields or None,
                )
            elif next_status == PaymentStatus.REFUND_FAILED:
                await append_deal_comment(
                    updated["deal_id"],
                    f"Банк не подтвердил возврат по заказу {updated['order_id']}",
                    extra_fields=bitrix_fields or None,
                )
        except Exception as exc:
            log_exception("append_refund_comment_failed", exc)

        log_json(
            "refund_notify_processed",
            {
                "deal_id": updated["deal_id"],
                "order_id": updated["order_id"],
                "status": updated["status"],
                "trtype": trtype.value,
                "result_code": updated.get("result_code"),
                "rc_code": updated.get("rc_code"),
            },
        )

        return JSONResponse({"ok": True})

    raise HTTPException(status_code=400, detail=f"Unsupported TRTYPE: {trtype.value}")


@app.get("/payments/{token}")
async def get_payment_status(token: str):
    session = get_payment_session_by_token(token)

    if not session:
        raise HTTPException(status_code=404, detail="Payment session not found")

    return {
        "token": session["token"],
        "deal_id": session["deal_id"],
        "order_id": session["order_id"],
        "merch_rn_id": session.get("merch_rn_id"),
        "status": session["status"],
        "bank_status": session["bank_status"],
        "amount": decimal_to_str(parse_decimal_amount(session["amount"])),
        "currency": session["currency"],
        "invoice": session["invoice"],
        "product": session["product"],
        "customer_name": session["customer_name"],
        "customer_last_name": session["customer_last_name"],
        "rrn": session.get("rrn"),
        "int_ref": session.get("int_ref"),
        "result_code": session.get("result_code"),
        "rc_code": session.get("rc_code"),
        "created_at": session["created_at"],
        "opened_at": session["opened_at"],
        "started_at": session["started_at"],
        "callback_received_at": session["callback_received_at"],
        "paid_at": session["paid_at"],
        "refunded_at": session["refunded_at"],
        "expires_at": session["expires_at"],
        "last_notify_trtype": session.get("last_notify_trtype"),
        "status_checks_json": session.get("status_checks_json"),
        "refund_callback_json": session.get("refund_callback_json"),
        "version": session.get("version"),
    }


@app.get("/payments")
async def list_payments(limit: int = 50):
    limit = max(1, min(limit, 200))
    return {"items": list_recent_payments(limit)}


@app.post("/bcc/check-connection")
async def manual_bcc_check_connection():
    return await bcc_connection_check()


@app.get("/payments/status/bcc/{deal_id}")
async def manual_bcc_status_check(deal_id: int, tran_trtype: str = "1"):
    session = get_latest_payment_session_by_deal_id(deal_id)
    if not session:
        raise HTTPException(status_code=404, detail="Payment session not found for deal")

    parsed_tran_trtype = BccTranTrType.from_value(tran_trtype)
    return await bcc_status_check_for_session(session, tran_trtype=parsed_tran_trtype)


@app.post("/payments/refund/{deal_id}")
async def manual_refund_payment(deal_id: int, reason: Optional[str] = Query(None)):
    return await mark_refund_for_deal(deal_id, reason)


@app.api_route("/api/v1/payments/create", methods=["GET", "POST"])
async def bitrix_webhook_create(
    dealId: int = Query(...),
    token: str = Query(...),
    domain: Optional[str] = Query(None),
):
    await verify_bitrix_webhook_token(dealId, token)
    result = await create_payment_session_for_deal(dealId)

    return {
        "ok": True,
        "event": "payment.create",
        "dealId": dealId,
        "domain": domain,
        "result": result,
    }


@app.api_route("/api/v1/payments/status", methods=["GET", "POST"])
async def bitrix_webhook_status(
    dealId: int = Query(...),
    token: str = Query(...),
    domain: Optional[str] = Query(None),
    tranTrtype: str = Query("1"),
):
    await verify_bitrix_webhook_token(dealId, token)

    session = get_latest_payment_session_by_deal_id(dealId)
    if not session:
        raise HTTPException(status_code=404, detail="Payment session not found for deal")

    parsed_tran_trtype = BccTranTrType.from_value(tranTrtype)
    local_result = get_status_for_deal(dealId)
    bcc_result = await bcc_status_check_for_session(session, tran_trtype=parsed_tran_trtype)

    return {
        "ok": True,
        "event": "payment.status",
        "dealId": dealId,
        "domain": domain,
        "result": local_result,
        "bcc_result": bcc_result,
    }


@app.api_route("/api/v1/payments/refund", methods=["GET", "POST"])
async def bitrix_webhook_refund(
    dealId: int = Query(...),
    token: str = Query(...),
    reason: Optional[str] = Query(None),
    domain: Optional[str] = Query(None),
):
    await verify_bitrix_webhook_token(dealId, token)

    result = await mark_refund_for_deal(dealId, reason)

    return {
        "ok": True,
        "event": "payment.refund",
        "dealId": dealId,
        "domain": domain,
        "result": result,
    }