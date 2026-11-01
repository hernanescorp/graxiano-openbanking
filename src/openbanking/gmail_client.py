import base64

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build


QUERY = (
    'from:postamail@laboralkutxa.com '
    'subject:"POSTAMAIL - LABORAL KUTXA" '
    'has:attachment filename:pdf'
)


def walk_parts(part):
    yield part
    for child in part.get("parts", []):
        yield from walk_parts(child)


class GmailClient:
    def __init__(self, credentials_info):
        credentials = Credentials.from_authorized_user_info(
            credentials_info,
            scopes=["https://www.googleapis.com/auth/gmail.readonly"],
        )

        credentials.refresh(Request())

        self.service = build(
            "gmail",
            "v1",
            credentials=credentials,
            cache_discovery=False,
        )

    def iter_visa_pdfs(self):
        """Recorrer los correos y devolver cada extracto VISA."""
        page_token = None

        while True:
            page = self.service.users().messages().list(
                userId="me",
                q=QUERY,
                maxResults=100,
                pageToken=page_token,
            ).execute()

            for item in page.get("messages", []):
                message = self.service.users().messages().get(
                    userId="me",
                    id=item["id"],
                    format="full",
                ).execute()

                for part in walk_parts(message.get("payload", {})):
                    filename = part.get("filename", "")

                    if not (
                        filename.lower().endswith(".pdf")
                        and "EXTRACTO_TARJETA_VISA_CLASICA_GO"
                        in filename.upper()
                    ):
                        continue

                    body = part.get("body", {})

                    if body.get("attachmentId"):
                        body = (
                            self.service.users()
                            .messages()
                            .attachments()
                            .get(
                                userId="me",
                                messageId=item["id"],
                                id=body["attachmentId"],
                            )
                            .execute()
                        )

                    encoded = body.get("data", "")
                    if not encoded:
                        raise ValueError("El adjunto está vacío.")

                    content = base64.urlsafe_b64decode(
                        encoded + "=" * (-len(encoded) % 4)
                    )

                    if not content.startswith(b"%PDF-"):
                        raise ValueError("Cabecera PDF no válida.")

                    yield {
                        "content": content,
                        "message_id": item["id"],
                        "message_internal_date_ms": message.get(
                            "internalDate"
                        ),
                        "original_filename": filename,
                    }

            page_token = page.get("nextPageToken")
            if not page_token:
                break