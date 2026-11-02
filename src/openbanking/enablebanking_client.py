"""Cliente de lectura para la API de Enable Banking."""

import time
from datetime import date
from typing import Iterator
from uuid import UUID

import jwt
import requests


class EnableBankingError(RuntimeError):
    """Error de API sin exponer credenciales ni datos bancarios."""

    def __init__(self, status_code: int, error_code: str):
        self.status_code = status_code
        self.error_code = error_code
        super().__init__(
            f"Enable Banking: HTTP {status_code}, error={error_code}"
        )


class EnableBankingClient:
    def __init__(
        self,
        application_id: str,
        private_key: str,
        base_url: str = "https://api.enablebanking.com",
        timeout: int = 60,
    ):
        if base_url.rstrip("/") != "https://api.enablebanking.com":
            raise ValueError("Endpoint de Enable Banking no permitido.")

        if timeout <= 0:
            raise ValueError("El timeout debe ser positivo.")

        self.application_id = application_id
        self._private_key = private_key
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._http = requests.Session()

    def close(self):
        self._http.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def _headers(self) -> dict:
        now = int(time.time())

        token = jwt.encode(
            {
                "iss": "enablebanking.com",
                "aud": "api.enablebanking.com",
                "iat": now,
                "exp": now + 3600,
            },
            self._private_key,
            algorithm="RS256",
            headers={"kid": self.application_id},
        )

        return {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
        }

    def _get(self, path: str, params: dict | None = None) -> dict:
        try:
            response = self._http.get(
                f"{self.base_url}{path}",
                headers=self._headers(),
                params=params,
                timeout=self.timeout,
                allow_redirects=False,
            )
        except requests.RequestException:
            raise RuntimeError(
                "No se pudo completar la conexión con Enable Banking."
            ) from None

        if response.status_code != 200:
            # No imprimimos el cuerpo completo ni la URL de la cuenta.
            try:
                error_code = response.json().get("error", "UNKNOWN")
            except (ValueError, AttributeError):
                error_code = "INVALID_ERROR_RESPONSE"

            raise EnableBankingError(
                response.status_code,
                str(error_code),
            )

        try:
            payload = response.json()
        except ValueError:
            raise RuntimeError(
                "Enable Banking devolvió una respuesta JSON inválida."
            ) from None

        if not isinstance(payload, dict):
            raise RuntimeError("Formato de respuesta inesperado.")

        return payload

    def get_balances(self, account_uid: str) -> dict:
        account_uid = str(UUID(account_uid))
        payload = self._get(f"/accounts/{account_uid}/balances")

        if not isinstance(payload.get("balances"), list):
            raise RuntimeError("Respuesta de saldos sin una lista válida.")

        return payload

    def iter_transaction_pages(
        self,
        account_uid: str,
        date_from: date,
        date_to: date,
    ) -> Iterator[dict]:
        """Devuelve cada página original sin acumular todo en memoria."""

        account_uid = str(UUID(account_uid))

        if date_from > date_to:
            raise ValueError("date_from debe ser anterior o igual a date_to.")

        params = {
            "date_from": date_from.isoformat(),
            "date_to": date_to.isoformat(),
        }
        seen_keys = set()

        while True:
            payload = self._get(
                f"/accounts/{account_uid}/transactions",
                params=params,
            )

            if not isinstance(payload.get("transactions"), list):
                raise RuntimeError(
                    "Respuesta de movimientos sin una lista válida."
                )

            continuation_key = payload.get("continuation_key")

            if continuation_key:
                if not isinstance(continuation_key, str):
                    raise RuntimeError("Clave de paginación inválida.")

                if continuation_key in seen_keys:
                    raise RuntimeError(
                        "Clave de paginación repetida; extracción incompleta."
                    )

                seen_keys.add(continuation_key)

            yield payload

            if not continuation_key:
                break

            params["continuation_key"] = continuation_key