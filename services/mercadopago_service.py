import uuid
from typing import Any, Dict, Optional, Tuple

import requests


class MercadoPagoClient:
    def __init__(self, access_token: str, app_base_url: str = "", timeout_seconds: int = 25) -> None:
        self.access_token = access_token
        self.app_base_url = app_base_url
        self.timeout_seconds = timeout_seconds

    @property
    def enabled(self) -> bool:
        return bool(self.access_token.strip())

    def create_checkout_preference(
        self,
        amount: float,
        description: str,
        number: str,
    ) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        if not self.enabled:
            return None, "MP_ACCESS_TOKEN nao configurado"

        external_reference = str(uuid.uuid4())
        payload: Dict[str, Any] = {
            "items": [
                {
                    "title": description,
                    "quantity": 1,
                    "currency_id": "BRL",
                    "unit_price": float(amount),
                }
            ],
            "external_reference": external_reference,
            "metadata": {"numero": number},
        }

        if self.app_base_url:
            base_url = self.app_base_url.rstrip("/")
            payload["notification_url"] = f"{base_url}/webhook/mercadopago"

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.post(
                "https://api.mercadopago.com/checkout/preferences",
                json=payload,
                headers=headers,
                timeout=self.timeout_seconds,
            )
            if response.status_code not in (200, 201):
                return None, f"Erro Mercado Pago: {response.status_code} - {response.text}"

            data = response.json()
            return {
                "id": data.get("id"),
                "init_point": data.get("init_point"),
                "external_reference": external_reference,
            }, None
        except requests.RequestException as exc:
            return None, str(exc)

    def get_payment(self, payment_id: str) -> Optional[Dict[str, Any]]:
        if not self.enabled:
            return None

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.get(
                f"https://api.mercadopago.com/v1/payments/{payment_id}",
                headers=headers,
                timeout=self.timeout_seconds,
            )
            if response.status_code != 200:
                return None
            return response.json()
        except requests.RequestException:
            return None

    def get_merchant_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Busca um merchant order do MP (usado em pagamentos via link fixo/QR Code)."""
        if not self.enabled:
            return None

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        try:
            response = requests.get(
                f"https://api.mercadopago.com/merchant_orders/{order_id}",
                headers=headers,
                timeout=self.timeout_seconds,
            )
            if response.status_code != 200:
                return None
            return response.json()
        except requests.RequestException:
            return None

    def search_payments(
        self,
        status: str = "approved",
        limit: int = 100,
    ) -> Tuple[list[Dict[str, Any]], Optional[str]]:
        if not self.enabled:
            return [], "MP_ACCESS_TOKEN nao configurado"

        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }
        params = {
            "sort": "date_created",
            "criteria": "desc",
            "limit": max(1, min(int(limit or 100), 100)),
        }
        if status:
            params["status"] = status

        try:
            response = requests.get(
                "https://api.mercadopago.com/v1/payments/search",
                headers=headers,
                params=params,
                timeout=self.timeout_seconds,
            )
            if response.status_code != 200:
                return [], f"Erro Mercado Pago: {response.status_code} - {response.text}"

            data = response.json()
            results = data.get("results") or []
            return [item for item in results if isinstance(item, dict)], None
        except requests.RequestException as exc:
            return [], str(exc)
