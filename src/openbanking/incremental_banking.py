"""Orquestación incremental por cuenta bancaria."""

from datetime import date

from openbanking.ingest_banking import ingest_banking


def run_incremental(
    client,
    storage,
    checkpoints,
    sessions: dict,
    prefix: str,
    date_to: date,
    overlap_days: int = 7,
) -> dict:
    results = []

    for bank, session in sessions.items():
        accounts = (
            session.get("accounts")
            if isinstance(session, dict)
            else None
        )

        if not isinstance(accounts, list) or not accounts:
            results.append({
                "bank": bank,
                "status": "failed",
                "error": {"type": "MissingSessionAccounts"},
            })
            continue

        for index, account in enumerate(accounts, start=1):
            result = {
                "bank": bank,
                "account_index": index,
                "status": "running",
                "checkpoint_saved": False,
            }

            try:
                identification_hash = account["identification_hash"]

                plan = checkpoints.plan(
                    bank=bank,
                    identification_hash=identification_hash,
                    date_to=date_to,
                    overlap_days=overlap_days,
                )

                result["date_from"] = plan["date_from"].isoformat()
                result["date_to"] = plan["date_to"].isoformat()

                # Una extracción por cuenta permite usar rangos distintos.
                summary = ingest_banking(
                    client=client,
                    storage=storage,
                    sessions={
                        bank: {"accounts": [account]}
                    },
                    prefix=prefix,
                    strategy=plan["strategy"],
                    date_from=plan["date_from"],
                    date_to=plan["date_to"],
                )

                result["manifest_path"] = summary["manifest_path"]
                result["extraction_status"] = summary["status"]

                transaction_result = next(
                    item
                    for item in summary["results"]
                    if item.get("endpoint") == "transactions"
                )

                # En esta extracción individual, el índice interno es 1.
                if transaction_result["status"] == "success":
                    checkpoints.save(
                        bank=bank,
                        identification_hash=identification_hash,
                        summary=summary,
                        account_index=1,
                        covered_through=date_to,
                    )
                    result["checkpoint_saved"] = True

                result["status"] = summary["status"]

                result["endpoints"] = [
                    {
                        "endpoint": item.get("endpoint"),
                        "status": item["status"],
                        "records": item.get(
                            "records_saved",
                            item.get("record_count", 0),
                        ),
                        **(
                            {"error": item["error"]}
                            if "error" in item
                            else {}
                        ),
                    }
                    for item in summary["results"]
                ]

            except Exception as exc:
                result["status"] = "failed"
                result["error"] = {"type": type(exc).__name__}

                if hasattr(exc, "status_code"):
                    result["error"]["http_status"] = exc.status_code

                if hasattr(exc, "error_code"):
                    result["error"]["error_code"] = exc.error_code

            results.append(result)

    statuses = [item["status"] for item in results]

    if not statuses:
        raise ValueError("No se han proporcionado cuentas.")

    if all(status == "success" for status in statuses):
        status = "success"
    elif any(
        item.get("checkpoint_saved")
        or item.get("extraction_status") in {"success", "partial_failure"}
        for item in results
    ):
        status = "partial_failure"
    else:
        status = "failed"

    return {
        "status": status,
        "accounts": results,
    }