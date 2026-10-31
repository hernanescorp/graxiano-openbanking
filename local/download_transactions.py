import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import jwt
import requests

APP_ID = "2330c837-db68-4541-b14e-19d56818fcb6"
BASE_URL = "https://api.enablebanking.com"
ROOT = Path(__file__).resolve().parents[1]

PRIVATE_KEY = (ROOT / "secrets/private-prod.pem").read_text()

SESSIONS = {
    "santander": ROOT / "secrets/session-santander.json",
    "laboral": ROOT / "secrets/session-laboral.json",
}


def headers():
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
    return {"Authorization": f"Bearer {token}"}


def get_transactions(account_uid):
    transactions = []
    params = {}

    while True:
        response = requests.get(
            f"{BASE_URL}/accounts/{account_uid}/transactions",
            headers=headers(),
            params=params,
            timeout=60,
        )

        if not response.ok:
            print("HTTP:", response.status_code)
            print(response.text)
            response.raise_for_status()

        page = response.json()
        transactions.extend(page.get("transactions", []))

        print("  Movimientos descargados:", len(transactions))

        continuation_key = page.get("continuation_key")

        if not continuation_key:
            break

        params["continuation_key"] = continuation_key

    return transactions


def save_private_json(path, data):
    fd = os.open(
        path,
        os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
        0o600,
    )
    with os.fdopen(fd, "w", encoding="utf-8") as file:
        json.dump(data, file, ensure_ascii=False, indent=2)

    os.chmod(path, 0o600)


def main():
    os.umask(0o077)

    extracted_at = datetime.now(timezone.utc)
    run_id = extracted_at.strftime("%Y%m%dT%H%S%fZ")
    output_dir = ROOT / "data" / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    for bank, session_path in SESSIONS.items():
        print(f"\nBanco: {bank}")

        session = json.loads(session_path.read_text())
        accounts = session.get("accounts", [])

        if not accounts:
            print("  La sesión no contiene cuentas.")
            continue

        for index, account in enumerate(accounts, start=1):
            transactions = get_transactions(account["uid"])

            output_path = output_dir / f"{bank}_{index}.json"

            save_private_json(
                output_path,
                {
                    "bank": bank,
                    "extracted_at": extracted_at.isoformat(),
                    "account_uid": account["uid"],
                    "identification_hash": account.get(
                        "identification_hash"
                    ),
                    "transactions": transactions,
                },
            )

            print("  Archivo guardado:", output_path)
            print("  Total:", len(transactions))


if __name__ == "__main__":
    main()