import time
from pathlib import Path

import jwt
import requests

APP_ID = "2330c837-db68-4541-b14e-19d56818fcb6"
ROOT = Path(__file__).resolve().parents[1]
PRIVATE_KEY = (ROOT / "secrets/private-prod.pem").read_text()

now = int(time.time())

token = jwt.encode(
    {
        "iss": "enablebanking.com",
        "aud": "api.enablebanking.com",
        "iat": now,
        "exp": now + 3600,
    },
    PRIVATE_KEY,
    algorithm="RS256",
    headers={"kid": APP_ID},
)

response = requests.get(
    "https://api.enablebanking.com/aspsps?country=ES",
    headers={"Authorization": f"Bearer {token}"},
    timeout=30,
)

print("HTTP:", response.status_code)
response.raise_for_status()

for bank in response.json().get("aspsps", []):
    if any(
        word in bank["name"].lower()
        for word in ("santander", "laboral")
    ):
        print("Banco:", bank["name"])
        print("País:", bank["country"])
        print(
            "Consentimiento máximo (días):",
            bank.get("maximum_consent_validity", 0) / 86400,
        )
        print()