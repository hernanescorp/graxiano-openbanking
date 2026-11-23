"""Enviar un correo de prueba para validar Gmail."""

import base64
from email.message import EmailMessage
from pathlib import Path

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

ROOT = Path(__file__).resolve().parents[1]
TOKEN_FILE = ROOT / "secrets/gmail-send-token.json"
SCOPES = ["https://www.googleapis.com/auth/gmail.send"]


def main():
    credentials = Credentials.from_authorized_user_file(
        str(TOKEN_FILE),
        scopes=SCOPES,
    )

    message = EmailMessage()
    message["From"] = "admin@graxiano.com"
    message["To"] = "hernan081097@gmail.com"
    message["Subject"] = "[Graxiano] Prueba de notificaciones de pedidos"
    message.set_content(
        "Conexión con Gmail API validada.\n\n"
        "Este correo comprueba el envío que utilizaremos para "
        "notificar los pedidos recibidos por WhatsApp.\n\n"
        "Todavía no se ha activado el envío automático."
    )

    raw = base64.urlsafe_b64encode(message.as_bytes()).decode("ascii")

    with build("gmail", "v1", credentials=credentials) as gmail:
        result = (
            gmail.users()
            .messages()
            .send(userId="me", body={"raw": raw})
            .execute(num_retries=0)
        )

    print("Correo enviado. ID:", result["id"])


if __name__ == "__main__":
    main()