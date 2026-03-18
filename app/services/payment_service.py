from datetime import datetime, timedelta
from decimal import Decimal
from typing import Optional
import json

from fastapi import HTTPException

from app.bitrix_client import BitrixAPIError, update_deal
from app.core.shared import (
    DECIMAL_ZERO,
    build_idempotency_key,
    decimal_to_str,
    iso_now,
    log_exception,
    log_json,
    parse_decimal_amount,
    parse_json_object,
    utc_now,
    generate_merch_rn_id,
    generate_order_id,
    generate_secure_token,
    stable_json_dumps,
)
from app.db import (
    create_payment_operation,
    create_payment_session,
    expire_stale_sessions_for_deal,
    finish_payment_operation,
    get_latest_payment_session_by_deal_id,
    get_payment_operation,
    get_payment_session_by_token,
    immediate_transaction,
    update_payment_session,
)
from app.domain.status_machine import PaymentStatus, get_refundable_balance, session_status, is_expired
from app.services.bcc_service import bcc_refund_for_session
from app.services.bitrix_sync_service import (
    append_deal_comment,
    build_payment_snapshot_from_bitrix,
    resolve_refund_amount_from_bitrix,
    sync_session_to_bitrix,
)
from app.settings import settings


async def create_payment_session_for_deal(deal_id: int) -> dict:
    snapshot = await build_payment_snapshot_from_bitrix(deal_id)
    now = utc_now()
    now_iso = now.isoformat()

    with immediate_transaction() as conn:
        expire_stale_sessions_for_deal(deal_id, now_iso, conn)

        existing = get_latest_payment_session_by_deal_id(deal_id, conn=conn)
        if existing:
            existing_status = session_status(existing)
            if existing_status in {PaymentStatus.CREATED, PaymentStatus.PENDING} and not is_expired(existing, utc_now):
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
                    if existing_status in {PaymentStatus.CREATED, PaymentStatus.PENDING} and not is_expired(existing, utc_now):
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
        bcc_result = await bcc_refund_for_session(session, effective_amount, utc_now)
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
