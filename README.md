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
2. Configure environment

Создайте .env и задайте обязательные переменные сервиса:

BITRIX_BASE_URL=
BITRIX_WEBHOOK_TOKEN=
BRAND_LOGO_URL=https://pay.dionis-insurance.kz/static/dionis-logo.png
BCC_MERCHANT=
BCC_MERCH_NAME=
BCC_TERMINAL=
BCC_BACKREF=
BCC_NOTIFY_URL=
BCC_MERCH_URL=https://dionis-insurance.kz
BCC_MAC_KEY_HEX=
BCC_NOTIFY_BASIC_ENABLED=true
BCC_NOTIFY_BASIC_USERNAME=
BCC_NOTIFY_BASIC_PASSWORD=

BCC_NOTIFY_BASIC_USERNAME и BCC_NOTIFY_BASIC_PASSWORD обязательны только когда BCC_NOTIFY_BASIC_ENABLED=true.

Дополнительно можно настроить DB_PATH, PUBLIC_BASE_URL, Bitrix field codes и прочие параметры из app/settings.py.

BRAND_LOGO_URL опционален. Он позволяет показать логотип на redirect-странице перед отправкой в банк и на статусных HTML-страницах (оплачено, ссылка истекла, возврат и т.д.).

Если сервис публикуется за Nginx на домене:

https://pay.dionis-insurance.kz

то переменные нужно заполнять так:

PUBLIC_BASE_URL=https://pay.dionis-insurance.kz
BCC_BACKREF=https://pay.dionis-insurance.kz/bcc/backref
BCC_NOTIFY_URL=https://pay.dionis-insurance.kz/bcc/notify
BCC_MERCH_URL=https://dionis-insurance.kz

Не добавляйте :443 к HTTPS-адресу.

Для тестов можно отключить Basic Auth на /bcc/notify:

BCC_NOTIFY_BASIC_ENABLED=false
3. Run service
uvicorn app.main:app --host 0.0.0.0 --port 8080
Useful endpoints
POST /payments/create/{deal_id}
GET /pay/{token}
GET /payments/{token}
POST /payments/refund/{deal_id}
GET|POST /bcc/notify
GET|POST /api/v1/payments/create
GET|POST /api/v1/payments/status
GET|POST /api/v1/payments/refund
Bank exchange logging

Все исходящие запросы в банк и входящие ответы банка пишутся в файл:

bcc_bank_exchange.log

Для тестов можно включить подробный лог:

BANK_LOG_FULL_HTTP=true

Для диагностики (только тестовый мерчант):

BANK_LOG_INCLUDE_SENSITIVE=true

Важно:

использовать только в тесте,
не включать в продакшене.

В этом режиме логируются:

URL и метод,
headers,
payload,
ответ банка.
Просмотр логов
tail -f bcc_bank_exchange.log
Рабочие команды
systemctl restart bcc
systemctl stop bcc
systemctl start bcc
systemctl status bcc
journalctl -u bcc -f
Деплой
cd /opt/bcc
git pull origin main
systemctl restart bcc
systemctl status bcc --no-pager -l
curl -I http://127.0.0.1:8080/docs

(В проде /docs должен отдавать 404 — это нормально.)

Документация
Для не-программистов: docs/non_developer_guide_ru.md
State machine: docs/payment_state_machine.md

---

После вставки:

```bash
git add README.md
git status