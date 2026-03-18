from decimal import Decimal
from typing import Optional
import secrets

from fastapi import HTTPException

from app.bitrix_client import BitrixAPIError, get_contact, get_deal, update_deal
from app.core.shared import (
    DECIMAL_ZERO,
    append_comment,
    build_desc,
    decimal_to_str,
    format_amount_for_bcc,
    get_first_value,
    get_multifield_value,
    log_exception,
    log_json,
    map_policy_type,
    parse_decimal_amount,
    validate_test_amount_kzt,
)
from app.domain.status_machine import PaymentStatus
from app.settings import settings


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
