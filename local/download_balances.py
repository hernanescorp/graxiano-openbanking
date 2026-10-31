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


def main():
    os.umask(0o077)

    extracted_at = datetime.now(timezone.utc)
    run_id = extracted_at.strftime("%Y%m%dT%H%M%S%fZ")
    output_dir = ROOT / "data" / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    for bank, session_path in SESSIONS.items():
        print(f"\nBanco: {bank}")

        session = json.loads(session_path.read_text())

        for index, account in enumerate(
            session.get("accounts", []), start=1
        ):
            response = requests.get(
                f"{BASE_URL}/accounts/{account['uid']}/balances",
                headers=headers(),
                timeout=60,
            )

            if not response.ok:
                print("HTTP:", response.status_code)
                print(response.text)
                response.raise_for_status()

            result = {
                "bank": bank,
                "extracted_at": extracted_at.isoformat(),
                "account_uid": account["uid"],
                "identification_hash": account.get(
                    "identification_hash"
                ),
                "response": response.json(),
            }

            destination = output_dir / f"{bank}_balances_{index}.json"

            fd = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_TRUNC,
                0o600,
            )

            with os.fdopen(fd, "w", encoding="utf-8") as file:
                json.dump(
                    result,
                    file,
                    ensure_ascii=False,
                    indent=2,
                )

            print("Archivo guardado:", destination)
            print(
                "Saldos recibidos:",
                len(result["response"].get("balances", [])),
            )


if __name__ == "__main__":
    main()