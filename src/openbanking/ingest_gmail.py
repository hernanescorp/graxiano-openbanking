import hashlib
import json
from datetime import datetime, timezone

from azure.core.exceptions import ResourceExistsError


def ingest_gmail(gmail_client, storage, prefix):
    uploaded = 0
    existing = 0

    for pdf in gmail_client.iter_visa_pdfs():
        content = pdf["content"]
        digest = hashlib.sha256(content).hexdigest()
        pdf_path = f"{prefix}/{digest}.pdf"

        metadata = {
            "message_id": pdf["message_id"],
            "message_internal_date_ms": pdf["message_internal_date_ms"],
            "original_filename": pdf["original_filename"],
            "sha256": digest,
            "size_bytes": len(content),
            "extracted_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            storage.upload(pdf_path, content)
            uploaded += 1
        except ResourceExistsError:
            if storage.download(pdf_path) != content:
                raise RuntimeError("El PDF existente no coincide.")
            existing += 1

        # Completar el JSON aunque el PDF ya exista.
        try:
            storage.upload(
                f"{prefix}/{digest}.json",
                json.dumps(
                    metadata,
                    ensure_ascii=False,
                    indent=2,
                ).encode("utf-8"),
            )
        except ResourceExistsError:
            pass

    return {
        "uploaded": uploaded,
        "existing": existing,
    }