from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
import json

from app.bitrix_client import update_deal
from app.core.shared import (
    bank_log_backref,
    bank_log_json,
    bank_log_notify_callback,
    build_idempotency_key,
    decimal_to_str,
    html_message,
    iso_now,
    log_exception,
    log_json,
    log_notify_raw_request,
    normalize_client_ip,
    parse_decimal_amount,
    parse_json_object,
    parse_request_kv_data,
    stable_json_dumps,
    utc_now,
)
from app.db import (
    create_payment_operation,
    finish_payment_operation,
    get_latest_payment_session_by_deal_id,
    get_payment_operation,
    get_payment_session_by_order,
    get_payment_session_by_token,
    immediate_transaction,
    list_recent_payments,
    update_payment_session,
)
from app.domain.status_machine import (
    PaymentStatus,
    apply_payment_callback_update,
    apply_refund_callback_update,
    is_expired,
    session_status,
)
from app.services.bcc_service import (
    BccTrType,
    BccTranTrType,
    bcc_connection_check,
    bcc_status_check_for_session,
    build_bcc_payload_from_session,
    build_status_check_history_item,
    record_status_check_history,
    render_bcc_redirect_form_with_minfo,
    validate_notify_invariants,
    verify_notify_basic_auth,
    verify_notify_with_bank,
    build_mac_data_trtype_1,
)
from app.services.bitrix_sync_service import append_deal_comment, verify_bitrix_webhook_token
from app.services.payment_service import create_payment_session_for_deal, get_status_for_deal, mark_refund_for_deal
from app.settings import settings

router = APIRouter()


@router.get("/")
async def root():
    return {"status": "ok"}


@router.post("/payments/create/{deal_id}")
async def create_payment(deal_id: int):
    return await create_payment_session_for_deal(deal_id)


@router.get("/pay/{token}", response_class=HTMLResponse)
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

        if is_expired(session, utc_now):
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


@router.api_route("/bcc/backref", methods=["GET", "POST"])
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


@router.api_route("/bcc/notify", methods=["GET", "POST"])
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
                    response_json=stable_json_dumps({"ok": True, "ignored": True, "trtype": BccTrType.STATUS_CHECK.value}),
                    updated_at=iso_now(),
                    conn=conn,
                )

        return JSONResponse({"ok": True, "ignored": True, "trtype": BccTrType.STATUS_CHECK.value})

    verification = await verify_notify_with_bank(session, data, trtype)
    bank_result = verification["bank_result"]
    raw_result = str(bank_result.get("RESULT") or bank_result.get("RC") or bank_result.get("STATUS") or "").strip().upper()

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
                payment_trtype=BccTrType.PAYMENT.value,
            )
        elif trtype in {BccTrType.REFUND, BccTrType.REVERSAL}:
            fields = apply_refund_callback_update(
                current,
                trtype=trtype.value,
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


@router.get("/payments/{token}")
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


@router.get("/payments")
async def list_payments(limit: int = 50):
    limit = max(1, min(limit, 200))
    return {"items": list_recent_payments(limit)}


@router.post("/bcc/check-connection")
async def manual_bcc_check_connection():
    return await bcc_connection_check()


@router.get("/payments/status/bcc/{deal_id}")
async def manual_bcc_status_check(deal_id: int, tran_trtype: str = "1"):
    session = get_latest_payment_session_by_deal_id(deal_id)
    if not session:
        raise HTTPException(status_code=404, detail="Payment session not found for deal")

    parsed_tran_trtype = BccTranTrType.from_value(tran_trtype)
    return await bcc_status_check_for_session(session, tran_trtype=parsed_tran_trtype)


@router.post("/payments/refund/{deal_id}")
async def manual_refund_payment(deal_id: int, reason: str | None = Query(None)):
    return await mark_refund_for_deal(deal_id, reason)


@router.api_route("/api/v1/payments/create", methods=["GET", "POST"])
async def bitrix_webhook_create(
    dealId: int = Query(...),
    token: str = Query(...),
    domain: str | None = Query(None),
):
    await verify_bitrix_webhook_token(dealId, token)
    result = await create_payment_session_for_deal(dealId)
    return {"ok": True, "event": "payment.create", "dealId": dealId, "domain": domain, "result": result}


@router.api_route("/api/v1/payments/status", methods=["GET", "POST"])
async def bitrix_webhook_status(
    dealId: int = Query(...),
    token: str = Query(...),
    domain: str | None = Query(None),
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


@router.api_route("/api/v1/payments/refund", methods=["GET", "POST"])
async def bitrix_webhook_refund(
    dealId: int = Query(...),
    token: str = Query(...),
    reason: str | None = Query(None),
    domain: str | None = Query(None),
):
    await verify_bitrix_webhook_token(dealId, token)
    result = await mark_refund_for_deal(dealId, reason)
    return {"ok": True, "event": "payment.refund", "dealId": dealId, "domain": domain, "result": result}
