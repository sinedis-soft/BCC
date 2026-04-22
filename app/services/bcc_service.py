from datetime import datetime
from enum import Enum
from html import escape
from typing import Any, Optional
import base64
import hmac
import json

import httpx
from fastapi import HTTPException

from app.core.shared import (
    append_json_list,
    bank_log_incoming_response,
    bank_log_outgoing_request,
    decimal_to_str,
    extract_important_bank_fields,
    generate_nonce,
    generate_timestamp,
    iso_now,
    load_json_list,
    log_exception,
    lp,
    parse_decimal_amount,
    parse_key_value_text,
    same_utc_date,
    split_phone,
)
from app.db import get_payment_session_by_token, immediate_transaction, update_payment_session
from app.settings import settings


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
    return hmac.new(key_bytes, mac_data.encode("utf-8"), "sha1").hexdigest().upper()


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
    logo_url = (getattr(settings, "brand_logo_url", "") or "").strip()
    logo_html = f'<img src="{escape(logo_url, quote=True)}" alt="Dionis Insurance" class="logo">' if logo_url else ""

    for key, value in payload.items():
        safe_key = escape(str(key), quote=True)
        safe_value = escape("" if value is None else str(value), quote=True)
        hidden_inputs.append(f'<input type="hidden" name="{safe_key}" value="{safe_value}">')

    inputs_html = "\n    ".join(hidden_inputs)

    return f"""<!doctype html>
<html lang=\"ru\">
<head>
  <meta charset=\"utf-8\">
  <title>Переход к оплате</title>
  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
  <style>
    :root {{
      --background:#ffffff;
      --foreground:#171717;
      --brand-blue:#23376c;
      --brand-blue-dark:#0f2238;
      --brand-gold-ui:#ebca45;
      --brand-blue-text:#23376c;
      --font-text: system-ui, -apple-system, "Segoe UI", Roboto, Arial, "Helvetica Neue", Helvetica, sans-serif;
      --ease-spring: cubic-bezier(0.16, 1, 0.3, 1);
      --radius-12: 12px;
      --shadow-soft: 0 10px 28px rgba(16, 28, 53, 0.12);
      --gold-grad: linear-gradient(180deg, rgba(255,255,255,.22) 0%, rgba(255,255,255,0) 55%);
    }}
    * {{
      box-sizing: border-box;
    }}
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: var(--background);
      color: var(--foreground);
      font-family: var(--font-text);
      padding: 24px 16px;
    }}
    .box {{
      width: 100%;
      max-width: 720px;
      border: 1px solid #e2e8f0;
      border-radius: var(--radius-12);
      box-shadow: var(--shadow-soft);
      padding: 24px;
      background: #fff;
    }}
    .logo {{
      width: min(220px, 55vw);
      display: block;
      margin: 0 auto 18px;
      border-radius: 50%;
      object-fit: cover;
    }}
    h1 {{
      margin: 0 0 12px;
      color: var(--brand-blue-dark);
      line-height: 1.2;
    }}
    .meta {{
      color: #64748b;
      font-size: 14px;
      margin: 0 0 16px;
    }}
    .actions {{
      display: flex;
      justify-content: center;
      margin-top: 12px;
    }}
    .btn-primary {{
      appearance: none;
      border: 0;
      border-radius: var(--radius-12);
      padding: 12px 18px;
      cursor: pointer;
      font: inherit;
      font-weight: 700;
      transition: transform 180ms var(--ease-spring), filter 180ms var(--ease-spring);
      background-color: var(--brand-gold-ui);
      background-image: var(--gold-grad);
      color: var(--brand-blue-text);
      box-shadow: var(--shadow-soft);
    }}
    .btn-primary:hover {{
      transform: translateY(-1px);
      filter: brightness(1.02);
    }}
    .btn-primary:active {{
      transform: translateY(0);
    }}
  </style>
</head>
<body>
  <div class=\"box\">
    {logo_html}
    <h1>Переход на страницу оплаты</h1>
    <p>Сейчас вы будете перенаправлены на защищённую страницу банка.</p>
    <p class=\"meta\">Автопереход произойдёт через 1–2 секунды. Если этого не случилось, нажмите кнопку.</p>

    <form id=\"bcc-payment-form\" method=\"post\" action=\"{escape(action_url, quote=True)}\">
      {inputs_html}
      <input type=\"hidden\" name=\"M_INFO\" id=\"m_info\" value=\"\">
      <div class=\"actions\">
        <button type=\"submit\" class=\"btn-primary\">Перейти к оплате</button>
      </div>
      <noscript>
        <div class=\"actions\">
          <button type=\"submit\" class=\"btn-primary\">Перейти к оплате</button>
        </div>
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
      setTimeout(function() {{
        document.getElementById("bcc-payment-form").submit();
      }}, 1200);
    }})();
  </script>
</body>
</html>"""


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


async def bcc_refund_for_session(session: dict, refund_amount, utc_now) -> dict:
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
            is_same_day_full_refund = refund_amount == original_amount and same_utc_date(paid_at_dt, utc_now())
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
    return {"request_payload": payload, "response": response_data, "trtype": trtype.value}


async def record_status_check_history(token: str, history_item: dict, *, last_notify_trtype: str) -> None:
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
    if not bool(getattr(settings, "notify_basic_enabled", True)):
        return

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

    if not hmac.compare_digest(actual, expected):
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