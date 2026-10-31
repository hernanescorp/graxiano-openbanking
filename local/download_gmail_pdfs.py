import base64
import hashlib
import json
import os
from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

ROOT = Path(__file__).resolve().parents[1]
TOKEN_FILE = ROOT / "secrets/gmail-token.json"
OUTPUT_DIR = ROOT / "data/gmail/laboral_kutxa"

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

QUERY = (
    'from:postamail@laboralkutxa.com '
    'subject:"POSTAMAIL - LABORAL KUTXA" '
    'has:attachment filename:pdf'
)


def walk_parts(part):
    yield part
    for child in part.get("parts", []):
        yield from walk_parts(child)


def main():
    os.umask(0o077)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    credentials = Credentials.from_authorized_user_file(
        str(TOKEN_FILE), SCOPES
    )

    if not credentials.valid:
        if not credentials.refresh_token:
            raise SystemExit("Ejecuta connect_gmail.py para autorizar Gmail.")

        credentials.refresh(Request())
        TOKEN_FILE.write_text(
            credentials.to_json(), encoding="utf-8"
        )
        TOKEN_FILE.chmod(0o600)

    gmail = build("gmail", "v1", credentials=credentials)

    page_token = None
    downloaded = 0
    skipped = 0
    messages_found = 0

    while True:
        page = gmail.users().messages().list(
            userId="me",
            q=QUERY,
            maxResults=100,
            pageToken=page_token,
        ).execute()

        for item in page.get("messages", []):
            messages_found += 1
            message_id = item["id"]

            message = gmail.users().messages().get(
                userId="me",
                id=message_id,
                format="full",
            ).execute()

            for part in walk_parts(message.get("payload", {})):
                filename = part.get("filename", "")

                if not (
                    filename.lower().endswith(".pdf")
                    and "EXTRACTO_TARJETA_VISA_CLASICA_GO" in filename.upper()
                    ):
                    continue

                body = part.get("body", {})
                attachment_id = body.get("attachmentId")

                if attachment_id:
                    attachment = (
                        gmail.users().messages().attachments().get(
                            userId="me",
                            messageId=message_id,
                            id=attachment_id,
                        ).execute()
                    )
                    encoded = attachment.get("data", "")
                else:
                    encoded = body.get("data", "")

                if not encoded:
                    print("Adjunto vacío; se omite.")
                    continue

                content = base64.urlsafe_b64decode(
                    encoded + "=" * (-len(encoded) % 4)
                )

                if b"%PDF-" not in content[:1024]:
                    print("Adjunto sin cabecera PDF; se omite.")
                    continue

                digest = hashlib.sha256(content).hexdigest()
                destination = OUTPUT_DIR / f"{digest}.pdf"

                if destination.exists():
                    if hashlib.sha256(
                        destination.read_bytes()
                    ).hexdigest() != digest:
                        raise RuntimeError(
                            f"El archivo existente está alterado: {destination}"
                        )

                    skipped += 1
                    continue

                destination.write_bytes(content)

                metadata = {
                    "message_id": message_id,
                    "message_internal_date_ms": message.get("internalDate"),
                    "original_filename": filename,
                    "sha256": digest,
                    "size_bytes": len(content),
                }

                destination.with_suffix(".json").write_text(
                    json.dumps(
                        metadata,
                        ensure_ascii=False,
                        indent=2,
                    ),
                    encoding="utf-8",
                )

                downloaded += 1
                print(
                    f"PDF guardado: {digest[:12]}… "
                    f"({len(content)} bytes)"
                )

        page_token = page.get("nextPageToken")
        if not page_token:
            break

    print("\nCorreos encontrados:", messages_found)
    print("PDF nuevos descargados:", downloaded)
    print("PDF ya existentes:", skipped)
    print("Carpeta:", OUTPUT_DIR)


if __name__ == "__main__":
    main()