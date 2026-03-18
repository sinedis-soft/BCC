# BCC payments service

Интеграционный сервис на FastAPI для:
- чтения данных сделки и контакта из Bitrix,
- создания локальной платёжной сессии,
- редиректа клиента в BCC,
- обработки bank callback / status check / refund flow.

## Project structure

- `app/main.py` — создание FastAPI app, middleware, startup, exception handlers.
- `app/routes/payments.py` — HTTP endpoints и webhook routes.
- `app/services/payment_service.py` — orchestration для create/status/refund flow.
- `app/services/bcc_service.py` — интеграция с BCC.
- `app/services/bitrix_sync_service.py` — интеграция с Bitrix.
- `app/domain/status_machine.py` — доменные статусы и переходы.
- `app/db.py` — SQLite schema и CRUD-операции.

## Local setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure environment

Создайте `.env` и задайте обязательные переменные сервиса:

```env
BITRIX_BASE_URL=
BITRIX_WEBHOOK_TOKEN=
BCC_MERCHANT=
BCC_MERCH_NAME=
BCC_TERMINAL=
BCC_BACKREF=
BCC_NOTIFY_URL=
BCC_MERCH_URL=
BCC_MAC_KEY_HEX=
BCC_NOTIFY_BASIC_ENABLED=true
BCC_NOTIFY_BASIC_USERNAME=
BCC_NOTIFY_BASIC_PASSWORD=
```

`BCC_NOTIFY_BASIC_USERNAME` и `BCC_NOTIFY_BASIC_PASSWORD` обязательны только когда `BCC_NOTIFY_BASIC_ENABLED=true`.

Дополнительно можно настроить `DB_PATH`, `PUBLIC_BASE_URL`, Bitrix field codes и прочие параметры из `app/settings.py`.

Для тестов можно полностью отключить проверку Basic Auth на `/bcc/notify`, установив:

```env
BCC_NOTIFY_BASIC_ENABLED=false
```

Когда появятся реальные банковские данные, верните `BCC_NOTIFY_BASIC_ENABLED=true` и заполните `BCC_NOTIFY_BASIC_USERNAME`/`BCC_NOTIFY_BASIC_PASSWORD`.

### 3. Run service

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

## Useful endpoints

- `POST /payments/create/{deal_id}`
- `GET /pay/{token}`
- `GET /payments/{token}`
- `POST /payments/refund/{deal_id}`
- `GET|POST /bcc/notify`
- `GET|POST /api/v1/payments/create`
- `GET|POST /api/v1/payments/status`
- `GET|POST /api/v1/payments/refund`


## Bank exchange logging

Все исходящие запросы в банк и входящие ответы банка пишутся в файл `BANK_LOG_FILE` (по умолчанию `bcc_bank_exchange.log`).

Для тестов, если нужен максимально подробный HTTP-лог, включите:

```env
BANK_LOG_FULL_HTTP=true
```

В этом режиме в bank log будут записываться:
- request URL и method,
- request headers,
- request body / form payload,
- response status code,
- response headers,
- parsed body или raw text ответа банка.

Удобно смотреть лог так:

```bash
tail -f bcc_bank_exchange.log
```

## Документация для не-программистов

Если сервис настраивает, тестирует или сопровождает человек без опыта разработки, используйте подробное описание: [docs/non_developer_guide_ru.md](docs/non_developer_guide_ru.md).

## State machine

См. отдельное описание жизненного цикла платежа: [docs/payment_state_machine.md](docs/payment_state_machine.md).
