import json
import logging
import sqlite3
from contextlib import contextmanager
from typing import Optional

from app.settings import settings


logger = logging.getLogger("bcc-payments.db")


class ConcurrencyError(Exception):
    """Raised when optimistic lock check fails."""


def _configure_connection(conn: sqlite3.Connection) -> None:
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute(f"PRAGMA busy_timeout = {int(settings.sqlite_busy_timeout_ms)}")


@contextmanager
def get_db():
    conn = sqlite3.connect(
        settings.db_path,
        check_same_thread=False,
        isolation_level=None,
    )
    _configure_connection(conn)
    try:
        yield conn
    finally:
        conn.close()


@contextmanager
def immediate_transaction():
    conn = sqlite3.connect(
        settings.db_path,
        check_same_thread=False,
        isolation_level=None,
    )
    _configure_connection(conn)
    try:
        conn.execute("BEGIN IMMEDIATE")
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row | None) -> Optional[dict]:
    if row is None:
        return None
    return dict(row)


def _column_exists(conn: sqlite3.Connection, table_name: str, column_name: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return any(row["name"] == column_name for row in rows)


def _index_exists(conn: sqlite3.Connection, index_name: str) -> bool:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND name = ?",
        (index_name,),
    ).fetchall()
    return bool(rows)


def _ensure_column(conn: sqlite3.Connection, table_name: str, column_name: str, column_def: str) -> None:
    if not _column_exists(conn, table_name, column_name):
        logger.info("Adding missing column | table=%s | column=%s", table_name, column_name)
        conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")


def init_db():
    logger.info("Initializing database | path=%s", settings.db_path)

    with immediate_transaction() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS payment_sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                token TEXT NOT NULL UNIQUE,
                deal_id INTEGER NOT NULL,
                contact_id INTEGER NOT NULL,
                order_id TEXT NOT NULL UNIQUE,
                merch_rn_id TEXT,
                amount TEXT NOT NULL,
                currency TEXT NOT NULL,
                invoice TEXT NOT NULL,
                product TEXT,
                policy_type TEXT,
                customer_name TEXT,
                customer_last_name TEXT,
                customer_email TEXT,
                customer_phone TEXT,
                description TEXT NOT NULL,
                status TEXT NOT NULL,
                bcc_payload_json TEXT,
                callback_json TEXT,
                refund_callback_json TEXT,
                status_checks_json TEXT,
                bank_status TEXT,
                result_code TEXT,
                rc_code TEXT,
                rrn TEXT,
                int_ref TEXT,
                callback_received_at TEXT,
                started_at TEXT,
                opened_at TEXT,
                paid_at TEXT,
                refunded_at TEXT,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                last_notify_trtype TEXT,
                version INTEGER NOT NULL DEFAULT 0
            )
            """
        )

        _ensure_column(conn, "payment_sessions", "merch_rn_id", "TEXT")
        _ensure_column(conn, "payment_sessions", "result_code", "TEXT")
        _ensure_column(conn, "payment_sessions", "rc_code", "TEXT")
        _ensure_column(conn, "payment_sessions", "rrn", "TEXT")
        _ensure_column(conn, "payment_sessions", "int_ref", "TEXT")
        _ensure_column(conn, "payment_sessions", "refund_callback_json", "TEXT")
        _ensure_column(conn, "payment_sessions", "status_checks_json", "TEXT")
        _ensure_column(conn, "payment_sessions", "last_notify_trtype", "TEXT")
        _ensure_column(conn, "payment_sessions", "version", "INTEGER NOT NULL DEFAULT 0")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS payment_operations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                operation_type TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                session_token TEXT,
                status TEXT NOT NULL,
                request_json TEXT,
                response_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(operation_type, idempotency_key),
                FOREIGN KEY (session_token) REFERENCES payment_sessions(token) ON DELETE CASCADE
            )
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_payment_sessions_deal_id
            ON payment_sessions(deal_id)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_payment_sessions_order_id
            ON payment_sessions(order_id)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_payment_sessions_token
            ON payment_sessions(token)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_payment_sessions_status
            ON payment_sessions(status)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_payment_sessions_expires_at
            ON payment_sessions(expires_at)
            """
        )

        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_payment_sessions_deal_active
            ON payment_sessions(deal_id)
            WHERE status IN ('created', 'pending', 'refund_pending')
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_payment_operations_session_token
            ON payment_operations(session_token)
            """
        )

        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_payment_operations_type_key
            ON payment_operations(operation_type, idempotency_key)
            """
        )

    logger.info("Database initialized")


def create_payment_session(data: dict, conn: sqlite3.Connection | None = None) -> dict:
    owns_conn = conn is None
    if owns_conn:
        ctx = immediate_transaction()
        conn = ctx.__enter__()

    try:
        logger.info(
            "Creating payment session in DB | deal_id=%s | order_id=%s | merch_rn_id=%s",
            data["deal_id"],
            data["order_id"],
            data.get("merch_rn_id"),
        )

        conn.execute(
            """
            INSERT INTO payment_sessions (
                token, deal_id, contact_id, order_id, merch_rn_id, amount, currency,
                invoice, product, policy_type, customer_name, customer_last_name,
                customer_email, customer_phone, description, status,
                expires_at, created_at, updated_at, version
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (
                data["token"],
                data["deal_id"],
                data["contact_id"],
                data["order_id"],
                data.get("merch_rn_id"),
                data["amount"],
                data["currency"],
                data["invoice"],
                data["product"],
                data["policy_type"],
                data["customer_name"],
                data["customer_last_name"],
                data["customer_email"],
                data["customer_phone"],
                data["description"],
                data["status"],
                data["expires_at"],
                data["created_at"],
                data["updated_at"],
            ),
        )
        row = conn.execute(
            "SELECT * FROM payment_sessions WHERE token = ?",
            (data["token"],),
        ).fetchone()
        result = row_to_dict(row)
        if owns_conn:
            ctx.__exit__(None, None, None)
        return result
    except Exception as exc:
        if owns_conn:
            ctx.__exit__(type(exc), exc, exc.__traceback__)
        raise


def get_payment_session_by_token(token: str, conn: sqlite3.Connection | None = None) -> Optional[dict]:
    if conn is None:
        with get_db() as local_conn:
            row = local_conn.execute(
                "SELECT * FROM payment_sessions WHERE token = ?",
                (token,),
            ).fetchone()
            return row_to_dict(row)

    row = conn.execute(
        "SELECT * FROM payment_sessions WHERE token = ?",
        (token,),
    ).fetchone()
    return row_to_dict(row)


def get_payment_session_by_order(order_id: str, conn: sqlite3.Connection | None = None) -> Optional[dict]:
    if conn is None:
        with get_db() as local_conn:
            row = local_conn.execute(
                "SELECT * FROM payment_sessions WHERE order_id = ?",
                (order_id,),
            ).fetchone()
            return row_to_dict(row)

    row = conn.execute(
        "SELECT * FROM payment_sessions WHERE order_id = ?",
        (order_id,),
    ).fetchone()
    return row_to_dict(row)


def get_latest_payment_session_by_deal_id(
    deal_id: int,
    conn: sqlite3.Connection | None = None,
) -> Optional[dict]:
    query = """
        SELECT *
        FROM payment_sessions
        WHERE deal_id = ?
        ORDER BY id DESC
        LIMIT 1
    """

    if conn is None:
        with get_db() as local_conn:
            row = local_conn.execute(query, (deal_id,)).fetchone()
            return row_to_dict(row)

    row = conn.execute(query, (deal_id,)).fetchone()
    return row_to_dict(row)


def expire_stale_sessions_for_deal(deal_id: int, now_iso: str, conn: sqlite3.Connection) -> int:
    cursor = conn.execute(
        """
        UPDATE payment_sessions
        SET status = 'expired',
            updated_at = ?,
            version = version + 1
        WHERE deal_id = ?
          AND status IN ('created', 'pending')
          AND expires_at < ?
        """,
        (now_iso, deal_id, now_iso),
    )
    return int(cursor.rowcount or 0)


def update_payment_session(
    token: str,
    fields: dict,
    *,
    expected_version: int | None = None,
    conn: sqlite3.Connection | None = None,
) -> Optional[dict]:
    if not fields:
        return get_payment_session_by_token(token, conn=conn)

    allowed_columns = {
        "status",
        "bank_status",
        "callback_json",
        "refund_callback_json",
        "status_checks_json",
        "callback_received_at",
        "paid_at",
        "refunded_at",
        "started_at",
        "opened_at",
        "bcc_payload_json",
        "updated_at",
        "result_code",
        "rc_code",
        "rrn",
        "int_ref",
        "last_notify_trtype",
    }

    payload = {k: v for k, v in fields.items() if k in allowed_columns}
    if not payload:
        return get_payment_session_by_token(token, conn=conn)

    owns_conn = conn is None
    if owns_conn:
        ctx = immediate_transaction()
        conn = ctx.__enter__()

    try:
        logger.info(
            "Updating payment session | token=%s | fields=%s | expected_version=%s",
            token,
            list(payload.keys()),
            expected_version,
        )

        set_clause = ", ".join([f"{key} = ?" for key in payload.keys()])
        values = list(payload.values())

        sql = f"UPDATE payment_sessions SET {set_clause}, version = version + 1 WHERE token = ?"
        params = values + [token]

        if expected_version is not None:
            sql += " AND version = ?"
            params.append(expected_version)

        cursor = conn.execute(sql, params)

        if expected_version is not None and int(cursor.rowcount or 0) == 0:
            raise ConcurrencyError(f"Concurrent update detected for token={token}")

        row = conn.execute(
            "SELECT * FROM payment_sessions WHERE token = ?",
            (token,),
        ).fetchone()
        result = row_to_dict(row)

        if owns_conn:
            ctx.__exit__(None, None, None)

        return result
    except Exception as exc:
        if owns_conn:
            ctx.__exit__(type(exc), exc, exc.__traceback__)
        raise


def get_payment_operation(
    operation_type: str,
    idempotency_key: str,
    conn: sqlite3.Connection | None = None,
) -> Optional[dict]:
    query = """
        SELECT *
        FROM payment_operations
        WHERE operation_type = ? AND idempotency_key = ?
        LIMIT 1
    """

    if conn is None:
        with get_db() as local_conn:
            row = local_conn.execute(query, (operation_type, idempotency_key)).fetchone()
            return row_to_dict(row)

    row = conn.execute(query, (operation_type, idempotency_key)).fetchone()
    return row_to_dict(row)


def create_payment_operation(
    *,
    operation_type: str,
    idempotency_key: str,
    session_token: str | None,
    status: str,
    request_json: str | None,
    created_at: str,
    updated_at: str,
    conn: sqlite3.Connection,
) -> tuple[dict, bool]:
    try:
        conn.execute(
            """
            INSERT INTO payment_operations (
                operation_type,
                idempotency_key,
                session_token,
                status,
                request_json,
                response_json,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, NULL, ?, ?)
            """,
            (
                operation_type,
                idempotency_key,
                session_token,
                status,
                request_json,
                created_at,
                updated_at,
            ),
        )
        created = True
    except sqlite3.IntegrityError:
        created = False

    row = conn.execute(
        """
        SELECT *
        FROM payment_operations
        WHERE operation_type = ? AND idempotency_key = ?
        LIMIT 1
        """,
        (operation_type, idempotency_key),
    ).fetchone()

    return row_to_dict(row), created


def finish_payment_operation(
    operation_id: int,
    *,
    status: str,
    response_json: str | None,
    updated_at: str,
    conn: sqlite3.Connection,
) -> dict:
    conn.execute(
        """
        UPDATE payment_operations
        SET status = ?, response_json = ?, updated_at = ?
        WHERE id = ?
        """,
        (status, response_json, updated_at, operation_id),
    )

    row = conn.execute(
        "SELECT * FROM payment_operations WHERE id = ?",
        (operation_id,),
    ).fetchone()
    return row_to_dict(row)


def list_recent_payments(limit: int) -> list[dict]:
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT token, deal_id, order_id, merch_rn_id, amount, currency, status,
                   bank_status, result_code, rc_code, rrn, int_ref,
                   created_at, opened_at, paid_at, refunded_at, expires_at,
                   last_notify_trtype, version
            FROM payment_sessions
            ORDER BY id DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]
