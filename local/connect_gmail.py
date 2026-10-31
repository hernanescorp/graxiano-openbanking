import os
import secrets
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

ROOT = Path(__file__).resolve().parent
CLIENT_FILE = ROOT / "secrets/gmail-client.json"
TOKEN_FILE = ROOT / "secrets/gmail-token.json"

SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]
PORT = 8766
REDIRECT_URI = f"http://127.0.0.1:{PORT}/callback"


def main():
    os.umask(0o077)

    flow = InstalledAppFlow.from_client_secrets_file(
        str(CLIENT_FILE),
        SCOPES,
        autogenerate_code_verifier=True,
    )
    flow.redirect_uri = REDIRECT_URI

    auth_url, expected_state = flow.authorization_url(
        access_type="offline",
        prompt="consent",
    )

    result = {}

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            # No registrar códigos ni parámetros de autorización.
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

            if not secrets.compare_digest(
                returned_state, expected_state
            ):
                print("Petición ignorada: state ausente o distinto.")
                self.reply(
                    400,
                    "Autorización no válida. Usa el enlace nuevo "
                    "de la terminal.",
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
                "Respuesta recibida. Comprueba el resultado "
                "en la terminal.",
            )

    with HTTPServer(("127.0.0.1", PORT), Handler) as server:
        server.timeout = 1

        print("\nAbre este NUEVO enlace en tu navegador:\n")
        print(auth_url)
        print("\nEsperando autorización durante 5 minutos...")

        deadline = time.monotonic() + 300

        while not result and time.monotonic() < deadline:
            server.handle_request()

    if not result:
        raise SystemExit("Tiempo agotado. Ejecuta el script otra vez.")

    if "error" in result:
        raise SystemExit(f"Autorización rechazada: {result['error']}")

    # State ya validado; se conserva PKCE en el mismo objeto flow.
    flow.fetch_token(code=result["code"])
    credentials = flow.credentials

    gmail = build("gmail", "v1", credentials=credentials)
    profile = gmail.users().getProfile(userId="me").execute()

    TOKEN_FILE.write_text(
        credentials.to_json(),
        encoding="utf-8",
    )
    TOKEN_FILE.chmod(0o600)

    print("\nConexión correcta.")
    print("Buzón autorizado:", profile["emailAddress"])
    print("Credenciales guardadas en:", TOKEN_FILE)


if __name__ == "__main__":
    main()