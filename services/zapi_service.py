from typing import Optional

import requests


class ZApiClient:
    def __init__(self, instance_id: str, token: str, client_token: str, timeout_seconds: int = 25) -> None:
        self.instance_id = instance_id
        self.token = token
        self.client_token = client_token
        self.timeout_seconds = timeout_seconds

    def send_text(self, number: str, message: str) -> int:
        if not number or not message:
            return 400

        url = f"https://api.z-api.io/instances/{self.instance_id}/token/{self.token}/send-text"
        payload = {"phone": number, "message": message}
        headers = {
            "Content-Type": "application/json",
            "client-token": self.client_token,
        }

        try:
            response = requests.post(url, json=payload, headers=headers, timeout=self.timeout_seconds)
            return response.status_code
        except requests.RequestException:
            return 500
