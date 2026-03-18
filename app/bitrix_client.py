import json
import logging

import httpx

from app.settings import settings


logger = logging.getLogger("bcc-payments.bitrix")


class BitrixAPIError(Exception):
    """Raised when Bitrix API returns an error or invalid response."""


def _safe_json(data):
    try:
        return json.dumps(data, ensure_ascii=False, indent=2, default=str)
    except Exception:
        return repr(data)


async def _bitrix_get(url: str, params: dict) -> dict:
    logger.info("Bitrix GET request | url=%s | params=%s", url, params)

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        response = await client.get(url, params=params)

    logger.info(
        "Bitrix GET response | status_code=%s | url=%s",
        response.status_code,
        str(response.request.url),
    )

    response.raise_for_status()
    data = response.json()

    logger.info("Bitrix GET response body:\n%s", _safe_json(data))

    if "error" in data:
        raise BitrixAPIError(
            f"Bitrix error: {data.get('error')} - {data.get('error_description')}"
        )

    if "result" not in data:
        raise BitrixAPIError("Bitrix response does not contain result")

    return data["result"]


async def _bitrix_post(url: str, payload: dict) -> dict:
    logger.info("Bitrix POST request | url=%s", url)
    logger.info("Bitrix POST payload:\n%s", _safe_json(payload))

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        response = await client.post(url, json=payload)

    logger.info(
        "Bitrix POST response | status_code=%s | url=%s",
        response.status_code,
        str(response.request.url),
    )

    response.raise_for_status()
    data = response.json()

    logger.info("Bitrix POST response body:\n%s", _safe_json(data))

    if "error" in data:
        raise BitrixAPIError(
            f"Bitrix error: {data.get('error')} - {data.get('error_description')}"
        )

    return data


async def get_deal(deal_id: int) -> dict:
    logger.info("Get deal called | deal_id=%s", deal_id)
    return await _bitrix_get(settings.deal_get_url, {"ID": deal_id})


async def get_contact(contact_id: int) -> dict:
    logger.info("Get contact called | contact_id=%s", contact_id)
    return await _bitrix_get(settings.contact_get_url, {"ID": contact_id})


async def update_deal(deal_id: int, fields: dict) -> dict:
    logger.info("Update deal called | deal_id=%s", deal_id)
    logger.info("Update deal fields:\n%s", _safe_json(fields))

    if not fields:
        logger.warning("Update deal skipped because fields are empty")
        return {"result": True}

    data = await _bitrix_post(
        settings.deal_update_url,
        {"id": deal_id, "fields": fields},
    )

    if data.get("result") is not True:
        raise BitrixAPIError(f"Bitrix update failed: {data}")

    logger.info("Update deal success | deal_id=%s", deal_id)
    return data
