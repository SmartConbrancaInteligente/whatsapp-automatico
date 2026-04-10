import os
from dataclasses import dataclass

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


@dataclass(frozen=True)
class Settings:
    zapi_instance_id: str
    zapi_token: str
    zapi_client_token: str
    pix_key: str
    payment_link: str
    spreadsheet_url: str
    mp_access_token: str
    app_base_url: str
    default_charge_amount: float
    google_spreadsheet_id: str
    google_worksheet_name: str
    google_service_account_file: str
    admin_username: str
    admin_password: str
    flask_secret_key: str
    mp_webhook_secret: str
    database_url: str

    @property
    def mercadopago_enabled(self) -> bool:
        return bool(self.mp_access_token.strip())


def load_settings() -> Settings:
    return Settings(
        zapi_instance_id=os.getenv("ZAPI_INSTANCE_ID", ""),
        zapi_token=os.getenv("ZAPI_TOKEN", ""),
        zapi_client_token=os.getenv("ZAPI_CLIENT_TOKEN", ""),
        pix_key=os.getenv("CHAVE_PIX", ""),
        payment_link=os.getenv("PAYMENT_LINK", ""),
        spreadsheet_url=os.getenv("URL_PLANILHA", ""),
        mp_access_token=os.getenv("MP_ACCESS_TOKEN", ""),
        app_base_url=os.getenv("APP_BASE_URL", "").rstrip("/"),
        default_charge_amount=float(os.getenv("DEFAULT_CHARGE_AMOUNT", "30.0")),
        google_spreadsheet_id=os.getenv("GOOGLE_SPREADSHEET_ID", ""),
        google_worksheet_name=os.getenv("GOOGLE_WORKSHEET_NAME", ""),
        google_service_account_file=os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", ""),
        admin_username=os.getenv("ADMIN_USERNAME", "adm"),
        admin_password=os.getenv("ADMIN_PASSWORD", "123455"),
        flask_secret_key=os.getenv("FLASK_SECRET_KEY", "trocar-esta-chave-em-producao"),
        mp_webhook_secret=os.getenv("MP_WEBHOOK_SECRET", ""),
        database_url=os.getenv("DATABASE_URL", ""),
    )
