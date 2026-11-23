"""Autorizar Gmail para enviar notificaciones de pedidos."""

import os
import secrets
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from google_auth_oauthlib.flow import InstalledAppFlow

ROOT = Path(__file__).resolve().parents[1]

CLIENT_FILE = ROOT / "secrets/gmail-client.json"
TOKEN_FILE = ROOT / "secrets/gmail-send-token.json"

SCOPES = ["https://www.googleapis.com/auth/gmail.send"]
SENDER = "admin@graxiano.com"

PORT = 8766
REDIRECT_URI = f"http://127.0.0.1:{PORT}/callback"


def main():
    os.umask(0o077)

    if not CLIENT_FILE.is_file():
        raise SystemExit(f"No existe el cliente OAuth: {CLIENT_FILE}")

    if TOKEN_FILE.exists():
        raise SystemExit(
            f"Ya existe {TOKEN_FILE.name}. "
            "No se sobrescribirá. Renómbralo si necesitas autorizar de nuevo."
        )

    flow = InstalledAppFlow.from_client_secrets_file(
        str(CLIENT_FILE),
        SCOPES,
        autogenerate_code_verifier=True,
    )
    flow.redirect_uri = REDIRECT_URI

    auth_url, expected_state = flow.authorization_url(
        access_type="offline",
        prompt="consent",
        login_hint=SENDER,
    )

    result = {}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            # Evitar registrar códigos y parámetros de autorización.
            pass

        def reply(self, status, message):
            body = message.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):
            parsed = urlsplit(self.path)

            if parsed.path != "/callback":
                self.reply(404, "Ruta no válida.")
                return

            params = parse_qs(parsed.query)
            returned_state = params.get("state", [""])[0]

            if not secrets.compare_digest(returned_state, expected_state):
                self.reply(
                    400,
                    "Autorización no válida. Usa el enlace de la terminal.",
                )
                return

            if "error" in params:
                result["error"] = params["error"][0]
                self.reply(400, "Google no ha autorizado el acceso.")
                return

            code = params.get("code", [""])[0]

            if not code:
                self.reply(400, "No se ha recibido el código.")
                return

            result["code"] = code
            self.reply(
                200,
                "Respuesta recibida. Comprueba el resultado en la terminal.",
            )

    with HTTPServer(("127.0.0.1", PORT), Handler) as server:
        server.timeout = 1

        print(f"\nSelecciona la cuenta {SENDER}.")
        print("Abre este enlace en tu navegador:\n")
        print(auth_url)
        print("\nEsperando autorización durante 5 minutos...")

        deadline = time.monotonic() + 300

        while not result and time.monotonic() < deadline:
            server.handle_request()

    if not result:
        raise SystemExit("Tiempo agotado. Ejecuta el script otra vez.")

    if "error" in result:
        raise SystemExit(f"Autorización rechazada: {result['error']}")

    flow.fetch_token(code=result["code"])
    credentials = flow.credentials

    if not credentials.refresh_token:
        raise SystemExit(
            "No se recibió refresh_token. No se guardaron credenciales."
        )

    # Creación exclusiva: evita sobrescribir un token existente.
    with TOKEN_FILE.open("x", encoding="utf-8") as token_file:
        token_file.write(credentials.to_json())

    TOKEN_FILE.chmod(0o600)

    print("\nAutorización de envío guardada.")
    print("Archivo:", TOKEN_FILE)
    print("No se ha enviado ningún correo.")


if __name__ == "__main__":
    main()