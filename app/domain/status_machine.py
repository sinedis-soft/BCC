from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any
import json

from fastapi import HTTPException

from app.core.shared import DECIMAL_ZERO, iso_now, parse_decimal_amount


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


def session_status(session: dict) -> PaymentStatus:
    return PaymentStatus.from_value(session["status"])


def is_expired(session: dict, utc_now) -> bool:
    expires = datetime.fromisoformat(session["expires_at"])
    return expires < utc_now()


def get_refundable_balance(session: dict) -> Decimal:
    original_amount = parse_decimal_amount(session["amount"])
    status = session_status(session)

    if status in {PaymentStatus.REFUNDED, PaymentStatus.REFUND_PENDING}:
        return DECIMAL_ZERO

    return original_amount


def apply_payment_callback_update(current: dict, *, raw_result: str, data: dict, bank_result: dict, payment_trtype: str) -> dict:
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
            "last_notify_trtype": payment_trtype,
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
        "last_notify_trtype": payment_trtype,
        "updated_at": iso_now(),
    }


def apply_refund_callback_update(current: dict, *, trtype: str, raw_result: str, data: dict, bank_result: dict) -> dict:
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
        "last_notify_trtype": trtype,
        "updated_at": iso_now(),
    }
