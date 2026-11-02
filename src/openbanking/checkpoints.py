"""Puntos de control inmutables para movimientos bancarios."""

import hashlib
import json
from datetime import date, datetime, timedelta

from azure.core.exceptions import ResourceExistsError


def account_key(identification_hash: str) -> str:
    if not isinstance(identification_hash, str) or not identification_hash:
        raise ValueError("Falta identification_hash.")

    return hashlib.sha256(
        identification_hash.encode("utf-8")
    ).hexdigest()


class CheckpointStore:
    def __init__(self, storage, prefix: str):
        self.storage = storage
        self.prefix = prefix.strip("/") + "/_state/transactions"

    def _account_prefix(self, bank, identification_hash):
        if (
            not isinstance(bank, str)
            or not bank
            or any(
                char not in "abcdefghijklmnopqrstuvwxyz0123456789_-"
                for char in bank
            )
        ):
            raise ValueError("Nombre de banco no válido.")

        key = account_key(identification_hash)
        return f"{self.prefix}/{bank}/{key}/"

    def latest(self, bank, identification_hash):
        """Leer el estado más avanzado; no ocultar archivos corruptos."""

        prefix = self._account_prefix(bank, identification_hash)
        expected_key = account_key(identification_hash)
        states = []

        for path in self.storage.list_paths(prefix):
            if not path.endswith(".json"):
                continue

            state = json.loads(self.storage.download(path))

            if (
                state.get("schema_version") != 1
                or state.get("bank") != bank
                or state.get("account_key") != expected_key
            ):
                raise ValueError("Punto de control incompatible.")

            # Validar las fechas antes de utilizarlas.
            covered = date.fromisoformat(state["covered_through"])
            completed = datetime.fromisoformat(state["completed_at"])

            if completed.tzinfo is None:
                raise ValueError("completed_at debe incluir zona horaria.")

            states.append((covered, completed, state["run_id"], state))

        if not states:
            return None

        return max(states, key=lambda item: item[:3])[3]

    def plan(
        self,
        bank,
        identification_hash,
        date_to: date,
        overlap_days: int = 7,
    ):
        """Calcular el rango sin consultar la API bancaria."""

        if overlap_days < 0:
            raise ValueError("El solapamiento no puede ser negativo.")

        state = self.latest(bank, identification_hash)

        if state is None:
            raise RuntimeError(
                f"{bank}: falta la carga inicial de esta cuenta."
            )

        covered = date.fromisoformat(state["covered_through"])

        if covered > date_to:
            raise ValueError(
                "El punto de control es posterior a la fecha solicitada."
            )

        return {
            "strategy": "default",
            "date_from": covered - timedelta(days=overlap_days),
            "date_to": date_to,
            "previous_run_id": state["run_id"],
        }

    def save(
        self,
        bank,
        identification_hash,
        summary,
        account_index,
        covered_through: date,
    ):
        """Guardar estado solo para movimientos completados.

        Debe llamarse después de que ingest_banking haya retornado
        y guardado su manifiesto.
        """

        matches = [
            result
            for result in summary["results"]
            if result.get("bank") == bank
            and result.get("account_index") == account_index
            and result.get("endpoint") == "transactions"
        ]

        if len(matches) != 1 or matches[0]["status"] != "success":
            raise RuntimeError(
                "No se puede avanzar una extracción incompleta."
            )

        request = summary["request"]
        is_initial = request["strategy"] == "longest"

        expected_date = (
            summary["started_at"][:10]
            if is_initial
            else request["date_to"]
        )

        if covered_through.isoformat() != expected_date:
            raise ValueError(
                "La fecha del estado no coincide con la extracción."
            )

        state = {
            "schema_version": 1,
            "bank": bank,
            "account_key": account_key(identification_hash),
            "run_id": summary["run_id"],
            "mode": "initial" if is_initial else "incremental",
            "covered_through": covered_through.isoformat(),
            "completed_at": summary["finished_at"],
            "manifest_path": summary["manifest_path"],
        }

        path = (
            self._account_prefix(bank, identification_hash)
            + summary["run_id"]
            + ".json"
        )

        content = json.dumps(
            state,
            ensure_ascii=False,
            indent=2,
        ).encode("utf-8")

        try:
            self.storage.upload(path, content)
        except ResourceExistsError:
            existing = json.loads(self.storage.download(path))

            if existing != state:
                raise RuntimeError(
                    "El punto de control existente tiene otro contenido."
                ) from None

        return state