import json
import os
import secrets
import time
from datetime import datetime, timedelta, timezone
from getpass import getpass
from pathlib import Path

import jwt
import requests

APP_ID = "2330c837-db68-4541-b14e-19d56818fcb6"
BANK_NAME = "Laboral Kutxa"
REDIRECT_URL = "https://www.graxiano.com/openbanking/callback"
BASE_URL = "https://api.enablebanking.com"

ROOT = Path(__file__).resolve().parents[1]

private_key = (ROOT / "secrets/private-prod.pem").read_text()


def headers():
    now = int(time.time())

    token = jwt.encode(
        {
            "iss": "enablebanking.com",
            "aud": "api.enablebanking.com",
            "iat": now,
            "exp": now + 3600,
        },
        private_key,
        algorithm="RS256",
        headers={"kid": APP_ID},
    )

    return {"Authorization": f"Bearer {token}"}


def post(endpoint, body):
    response = requests.post(
        BASE_URL + endpoint,
        headers=headers(),
        json=body,
        timeout=30,
    )

    if not response.ok:
        print("HTTP:", response.status_code)
        print(response.text)
        response.raise_for_status()

    return response.json()


state = secrets.token_urlsafe(32)

auth = post(
    "/auth",
    {
        "access": {
            "balances": True,
            "transactions": True,
            "valid_until": (
                datetime.now(timezone.utc) + timedelta(days=179)
            ).isoformat(),
        },
        "aspsp": {
            "name": BANK_NAME,
            "country": "ES",
        },
        "state": state,
        "redirect_url": REDIRECT_URL,
        "psu_type": "personal",
    },
)

print("\nBanco:", BANK_NAME)
print("\nAbre este enlace en tu navegador y autoriza el acceso:")
print(auth["url"])
print("\nMantén este script abierto.")

returned_state = input(
    "\nPega el STATE mostrado en la página: "
).strip()

if not secrets.compare_digest(returned_state, state):
    raise SystemExit("STATE incorrecto. Se cancela la autorización.")

code = getpass(
    "Pega el CODE mostrado en la página (no se verá): "
).strip()

if not code:
    raise SystemExit("No se ha recibido un código.")

session = post("/sessions", {"code": code})

destination = ROOT / "secrets/session-laboral.json"

fd = os.open(
    destination,
    os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
    0o600,
)

with os.fdopen(fd, "w") as file:
    json.dump(session, file, indent=2)

os.chmod(destination, 0o600)

print("\nSesión creada.")
print("Cuentas disponibles:", len(session.get("accounts", [])))
print("Sesión guardada en:", destination)