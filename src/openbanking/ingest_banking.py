"""Ingesta de respuestas originales de Enable Banking hacia ADLS."""

import json
from datetime import date, datetime, timezone
from uuid import uuid4


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _upload_json(storage, path: str, payload: dict):
    content = json.dumps(
        payload,
        ensure_ascii=False,
        indent=2,
        allow_nan=False,
    ).encode("utf-8")

    storage.upload(path, content)


def _safe_error(exc: Exception) -> dict:
    """Registrar el tipo de fallo sin cuerpos ni credenciales."""

    result = {"type": type(exc).__name__}

    if hasattr(exc, "status_code"):
        result["http_status"] = exc.status_code

    if hasattr(exc, "error_code"):
        result["error_code"] = exc.error_code

    return result


def ingest_banking(
    client,
    storage,
    sessions: dict,
    prefix: str,
    strategy: str = "longest",
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict:
    """Guardar una extracción y su manifiesto.

    sessions:
        Diccionario {nombre_banco: contenido_json_de_sesion}.

    Cada llamada crea una extracción independiente.
    No actualiza todavía ningún marcador incremental.
    """

    if strategy not in {"default", "longest"}:
        raise ValueError("Estrategia no válida.")

    if strategy == "longest":
        if date_from is not None or date_to is not None:
            raise ValueError("Usa longest sin fechas.")
    else:
        if date_from is None or date_to is None:
            raise ValueError("La extracción requiere ambas fechas.")

        if date_from > date_to:
            raise ValueError("Rango de fechas incorrecto.")

    if not sessions:
        raise ValueError("No se han proporcionado sesiones.")

    prefix = prefix.strip("/")
    if not prefix:
        raise ValueError("El prefijo de almacenamiento está vacío.")

    # Los nombres de banco forman parte de las rutas.
    for bank in sessions:
        if (
            not isinstance(bank, str)
            or not bank
            or any(
                character not in
                "abcdefghijklmnopqrstuvwxyz0123456789_-"
                for character in bank
            )
        ):
            raise ValueError("Nombre de banco no válido para la ruta.")

    started_at = datetime.now(timezone.utc)
    run_id = (
        started_at.strftime("%Y%m%dT%H%M%S%fZ")
        + "_"
        + uuid4().hex[:12]
    )

    run_prefix = (
        f"{prefix}/"
        f"extraction_date={started_at.date().isoformat()}/"
        f"run_id={run_id}"
    )

    summary = {
        "schema_version": 1,
        "run_id": run_id,
        "started_at": started_at.isoformat(),
        "finished_at": None,
        "status": "running",
        "request": {
            "strategy": strategy,
            "date_from": date_from.isoformat() if date_from else None,
            "date_to": date_to.isoformat() if date_to else None,
        },
        "results": [],
    }

    def save_response(path, bank, account, endpoint, response, page=None):
        _upload_json(
            storage,
            path,
            {
                "schema_version": 1,
                "run_id": run_id,
                "bank": bank,
                "account_uid": account["uid"],
                "identification_hash": account.get(
                    "identification_hash"
                ),
                "extracted_at": _utc_now(),
                "endpoint": endpoint,
                "page_number": page,
                "request": (
                    summary["request"]
                    if endpoint == "transactions"
                    else {}
                ),
                "response": response,
            },
        )

    for bank, session in sessions.items():
        accounts = (
            session.get("accounts")
            if isinstance(session, dict)
            else None
        )

        if not isinstance(accounts, list) or not accounts:
            summary["results"].append(
                {
                    "bank": bank,
                    "status": "failed",
                    "error": {"type": "MissingSessionAccounts"},
                }
            )
            continue

        for account_index, account in enumerate(accounts, start=1):
            if (
                not isinstance(account, dict)
                or not isinstance(account.get("uid"), str)
                or not account["uid"]
            ):
                summary["results"].append(
                    {
                        "bank": bank,
                        "account_index": account_index,
                        "status": "failed",
                        "error": {"type": "InvalidSessionAccount"},
                    }
                )
                continue

            # El índice evita introducir identificadores de cuenta
            # directamente en los nombres de archivo.
            account_prefix = (
                f"{run_prefix}/bank={bank}/"
                f"account={account_index:03d}"
            )

            balance_result = {
                "bank": bank,
                "account_index": account_index,
                "endpoint": "balances",
                "status": "running",
                "files": [],
            }

            try:
                response = client.get_balances(account["uid"])
                path = f"{account_prefix}/balances.json"

                save_response(
                    path, bank, account, "balances", response
                )

                balance_result["files"].append(path)
                balance_result["record_count"] = len(
                    response["balances"]
                )
                balance_result["status"] = "success"

            except Exception as exc:
                balance_result["status"] = "failed"
                balance_result["error"] = _safe_error(exc)

            summary["results"].append(balance_result)

            transaction_result = {
                "bank": bank,
                "account_index": account_index,
                "endpoint": "transactions",
                "status": "running",
                "files": [],
                "pages_saved": 0,
                "records_saved": 0,
            }

            try:
                pages = client.iter_transaction_pages(
                    account_uid=account["uid"],
                    date_from=date_from,
                    date_to=date_to,
                    strategy=strategy,
                )

                for page_number, response in enumerate(pages, start=1):
                    path = (
                        f"{account_prefix}/transactions/"
                        f"page_{page_number:06d}.json"
                    )

                    save_response(
                        path,
                        bank,
                        account,
                        "transactions",
                        response,
                        page=page_number,
                    )

                    transaction_result["files"].append(path)
                    transaction_result["pages_saved"] += 1
                    transaction_result["records_saved"] += len(
                        response["transactions"]
                    )

                transaction_result["status"] = "success"

            except Exception as exc:
                # Las páginas guardadas permanecen disponibles,
                # pero la extracción no se considera completa.
                transaction_result["status"] = "failed"
                transaction_result["error"] = _safe_error(exc)

            summary["results"].append(transaction_result)

    statuses = [item["status"] for item in summary["results"]]

    if all(status == "success" for status in statuses):
        summary["status"] = "success"
    elif any(status == "success" for status in statuses):
        summary["status"] = "partial_failure"
    else:
        summary["status"] = "failed"

    summary["finished_at"] = _utc_now()
    manifest_path = f"{run_prefix}/manifest.json"
    summary["manifest_path"] = manifest_path

    # Si falla esta escritura, propagamos el error al notebook.
    _upload_json(storage, manifest_path, summary)

    return summary