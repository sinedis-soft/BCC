# Payment state machine

Этот документ описывает жизненный цикл платёжной сессии в сервисе BCC/Bitrix.

## Основной flow оплаты

```text
created -> pending -> paid
                \-> failed
created --------> expired
pending --------> expired
```

### Состояния

- `created` — локальная платёжная сессия создана, ссылка выпущена, но клиент ещё не был переведён на страницу банка.
- `pending` — клиент открыл ссылку, сервис сформировал и сохранил BCC payload, ожидается финальный callback/проверка банка.
- `paid` — банк подтвердил успешную оплату.
- `failed` — банк не подтвердил оплату.
- `expired` — срок действия ссылки истёк до успешной оплаты.

### Переходы

#### `created -> pending`
Триггер: пользователь открывает `/pay/{token}` и сервис успешно формирует redirect form в BCC.

#### `pending -> paid`
Триггер: `/bcc/notify` получает callback оплаты, затем сервис делает bank status check и подтверждает успешный результат.

#### `pending -> failed`
Триггер: `/bcc/notify` получает callback оплаты, затем bank status check не подтверждает успешную оплату.

#### `created -> expired`
Триггер: срок действия ссылки истёк до открытия/оплаты; сервис помечает сессию как `expired`.

#### `pending -> expired`
Триггер: пользователь открывает ссылку после TTL или активная pending-сессия устарела и помечается как `expired`.

## Flow возврата

```text
paid -> refund_pending -> refunded
                      \-> refund_failed
```

### Состояния возврата

- `refund_pending` — возврат инициирован, запрос в банк отправлен, ожидается подтверждение callback-ом или status check-ом.
- `refunded` — банк подтвердил успешный возврат или reversal.
- `refund_failed` — банк не подтвердил возврат.

### Переходы возврата

#### `paid -> refund_pending`
Триггер: вызывается ручной refund endpoint или Bitrix webhook, сервис валидирует сумму возврата и создаёт refund operation.

#### `refund_pending -> refunded`
Триггер: `/bcc/notify` получает callback на `TRTYPE=14` или `TRTYPE=22`, затем сервис подтверждает возврат через bank verification.

#### `refund_pending -> refund_failed`
Триггер: запрос возврата завершился ошибкой или callback/status check не подтвердил успешный возврат.

## Инварианты

- Одновременно активной для сделки может быть только одна сессия в состояниях `created`, `pending` или `refund_pending`.
- Callback банка не считается достаточным сам по себе: сервис дополнительно делает status check в BCC перед финальным переходом состояния.
- Для refund flow возврат допускается только для уже оплаченного платежа.
- Повторные notify/refund запросы обрабатываются идемпотентно через таблицу `payment_operations`.

## Соответствие коду

- Доменные статусы и переходы описаны в `app/domain/status_machine.py`.
- Создание/истечение/возвраты оркестрируются в `app/services/payment_service.py`.
- Проверка notify и BCC status check реализованы в `app/services/bcc_service.py`.
