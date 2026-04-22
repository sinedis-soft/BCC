from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


BASE_DIR = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    app_name: str = Field("bcc-payments", alias="APP_NAME")
    public_base_url: str = Field("http://127.0.0.1:8080", alias="PUBLIC_BASE_URL")
    brand_logo_url: str = Field("", alias="BRAND_LOGO_URL")
    db_path: str = Field(str(BASE_DIR / "payments.db"), alias="DB_PATH")

    bitrix_base_url: str = Field(..., alias="BITRIX_BASE_URL")
    bitrix_user_id: str = Field("1", alias="BITRIX_USER_ID")
    bitrix_webhook_token: str = Field(..., alias="BITRIX_WEBHOOK_TOKEN")

    field_payment: str = Field("UF_CRM_1686311351324", alias="BITRIX_FIELD_PAYMENT")
    field_invoice: str = Field("UF_CRM_1746693537945", alias="BITRIX_FIELD_INVOICE")
    field_product: str = Field("UF_CRM_1686152485641", alias="BITRIX_FIELD_PRODUCT")
    field_policy_type: str = Field("UF_CRM_1690539097", alias="BITRIX_FIELD_POLICY_TYPE")

    field_payment_url: str = Field("", alias="BITRIX_FIELD_PAYMENT_URL")
    field_payment_status: str = Field("", alias="BITRIX_FIELD_PAYMENT_STATUS")
    field_payment_order: str = Field("", alias="BITRIX_FIELD_PAYMENT_ORDER")
    field_payment_refund_amount: str = Field("", alias="BITRIX_FIELD_PAYMENT_REFUND_AMOUNT")

    webhook_secret: str = Field("", alias="WEBHOOK_SECRET")

    bcc_trtype1_url: str = Field(
        "https://test3ds.bcc.kz:5445/cgi-bin/cgi_link",
        alias="BCC_TRTYPE1_URL",
    )

    merchant: str = Field(..., alias="BCC_MERCHANT")
    merch_name: str = Field(..., alias="BCC_MERCH_NAME")
    terminal: str = Field(..., alias="BCC_TERMINAL")
    merch_gmt: str = Field("0", alias="BCC_MERCH_GMT")
    trtype: str = Field("1", alias="BCC_TRTYPE")

    backref: str = Field(..., alias="BCC_BACKREF")
    lang: str = Field("ru", alias="BCC_LANG")
    mk_token: str = Field("MERCH", alias="BCC_MK_TOKEN")
    notify_url: str = Field(..., alias="BCC_NOTIFY_URL")

    country: str = Field("KZ", alias="BCC_COUNTRY")
    brands: str = Field("VISA, Mastercard", alias="BCC_BRANDS")
    merch_url: str = Field(..., alias="BCC_MERCH_URL")
    mac_key_hex: str = Field(..., alias="BCC_MAC_KEY_HEX")

    notify_basic_enabled: bool = Field(True, alias="BCC_NOTIFY_BASIC_ENABLED")
    notify_basic_username: str = Field("", alias="BCC_NOTIFY_BASIC_USERNAME")
    notify_basic_password: str = Field("", alias="BCC_NOTIFY_BASIC_PASSWORD")
    notify_basic_realm: str = Field("BCC Notify", alias="BCC_NOTIFY_BASIC_REALM")

    min_test_amount_kzt: float = Field(355.0, alias="BCC_MIN_TEST_AMOUNT_KZT")
    payment_link_ttl_minutes: int = Field(1440, alias="PAYMENT_LINK_TTL_MINUTES")

    log_level: str = Field("INFO", alias="LOG_LEVEL")
    bank_log_file: str = Field(str(BASE_DIR / "bcc_bank_exchange.log"), alias="BANK_LOG_FILE")
    bank_log_max_bytes: int = Field(10 * 1024 * 1024, alias="BANK_LOG_MAX_BYTES")
    bank_log_backup_count: int = Field(10, alias="BANK_LOG_BACKUP_COUNT")
    bank_log_full_http: bool = Field(False, alias="BANK_LOG_FULL_HTTP")

    sqlite_busy_timeout_ms: int = Field(30000, alias="SQLITE_BUSY_TIMEOUT_MS")

    @property
    def is_test_merchant(self) -> bool:
        return str(self.merchant or "").strip() == "00000001"

    @property
    def deal_get_url(self) -> str:
        return (
            f"{self.bitrix_base_url}/rest/"
            f"{self.bitrix_user_id}/"
            f"{self.bitrix_webhook_token}/"
            f"crm.deal.get.json"
        )

    @property
    def contact_get_url(self) -> str:
        return (
            f"{self.bitrix_base_url}/rest/"
            f"{self.bitrix_user_id}/"
            f"{self.bitrix_webhook_token}/"
            f"crm.contact.get.json"
        )

    @property
    def deal_update_url(self) -> str:
        return (
            f"{self.bitrix_base_url}/rest/"
            f"{self.bitrix_user_id}/"
            f"{self.bitrix_webhook_token}/"
            f"crm.deal.update.json"
        )


settings = Settings()
