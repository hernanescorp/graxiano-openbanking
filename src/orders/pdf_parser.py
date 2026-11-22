"""Extractor del formato estándar de pedidos PDF."""

import re
from datetime import datetime
from io import BytesIO

import pdfplumber


PARSER_VERSION = "1.0.0"


def _required_field(text, pattern, field_name):
    match = re.search(pattern, text, flags=re.MULTILINE)

    if not match:
        raise ValueError(f"No se encontró el campo: {field_name}")

    return match.group(1).strip()


def _extract_lines(page, page_number):
    words = page.extract_words()

    quantity_headers = [
        word for word in words
        if word["text"] == "Cantidad"
    ]

    if len(quantity_headers) != 1:
        raise ValueError(
            f"Página {page_number}: cabecera de cantidades no reconocida."
        )

    quantity_header = quantity_headers[0]

    observations_headers = [
        word for word in words
        if word["text"] == "Observaciones"
        and word["top"] > quantity_header["bottom"]
    ]

    if len(observations_headers) != 1:
        raise ValueError(
            f"Página {page_number}: final de tabla no reconocido."
        )

    observations_header = observations_headers[0]
    column_boundary = quantity_header["x0"] - 5

    body_words = [
        word for word in words
        if word["top"] > quantity_header["bottom"]
        and word["bottom"] < observations_header["top"]
    ]

    grouped_rows = []

    for word in sorted(
        body_words,
        key=lambda item: (item["top"], item["x0"]),
    ):
        if (
            not grouped_rows
            or abs(
                word["top"] - grouped_rows[-1][0]["top"]
            ) > 3
        ):
            grouped_rows.append([word])
        else:
            grouped_rows[-1].append(word)

    lines = []

    for row in grouped_rows:
        row = sorted(row, key=lambda item: item["x0"])

        left = [
            word["text"]
            for word in row
            if word["x0"] < column_boundary
        ]

        right = [
            word["text"]
            for word in row
            if word["x0"] >= column_boundary
        ]

        if (
            len(left) < 2
            or not re.fullmatch(r"\d+", left[0])
            or not right
        ):
            raise ValueError(
                f"Página {page_number}: fila con formato inesperado."
            )

        lines.append({
            "page_number": page_number,
            "line_number": int(left[0]),
            "product_raw": " ".join(left[1:]),
            "quantity_raw": " ".join(right),
        })

    if not lines:
        raise ValueError("El pedido no contiene líneas reconocibles.")

    expected_numbers = list(range(1, len(lines) + 1))
    observed_numbers = [line["line_number"] for line in lines]

    if observed_numbers != expected_numbers:
        raise ValueError(
            "La numeración de líneas no es consecutiva desde 1."
        )

    return lines


def extract_order(pdf_bytes):
    """Extraer un pedido de la plantilla validada de una página.

    Devuelve datos originales y metadatos del extractor.
    Rechaza documentos incompatibles para evitar cargas incompletas.
    """

    if not isinstance(pdf_bytes, (bytes, bytearray)):
        raise TypeError("pdf_bytes debe contener los bytes del PDF.")

    with pdfplumber.open(BytesIO(bytes(pdf_bytes))) as pdf:
        # Esta versión solo admite la plantilla que hemos validado.
        if len(pdf.pages) != 1:
            raise ValueError(
                "La plantilla actual requiere exactamente una página."
            )

        page = pdf.pages[0]
        text = page.extract_text() or ""

        if not text.strip():
            raise ValueError(
                "PDF sin texto extraíble; requiere revisión u OCR."
            )

        if not re.search(
            r"^PEDIDO A DISTRIBUIDORA ALIMENTARIA\s*$",
            text,
            flags=re.MULTILINE,
        ):
            raise ValueError("El documento no corresponde a la plantilla.")

        customer_raw = _required_field(
            text,
            r"^Cliente:\s*([^\r\n]+)$",
            "cliente",
        )

        order_date_raw = _required_field(
            text,
            r"^Fecha del pedido:\s*(\d{2}/\d{2}/\d{4})[ \t]*$",
            "fecha del pedido",
        )

        delivery_date_raw = _required_field(
            text,
            r"^Entrega solicitada:\s*(\d{2}/\d{2}/\d{4})[ \t]*$",
            "entrega solicitada",
        )

        for date_value in (order_date_raw, delivery_date_raw):
            datetime.strptime(date_value, "%d/%m/%Y")

        notes_match = re.search(
            r"^Observaciones de entrega[ \t]*\r?\n"
            r"(.*?)"
            r"(?=^DOCUMENTO DE EJEMPLO|\Z)",
            text,
            flags=re.MULTILINE | re.DOTALL,
        )

        header = {
            "customer_raw": customer_raw,
            "order_date_raw": order_date_raw,
            "delivery_date_raw": delivery_date_raw,
            "delivery_notes_raw": (
                notes_match.group(1).strip()
                if notes_match else None
            ),
        }

        lines = _extract_lines(page, page_number=1)

        return {
            "parser_version": PARSER_VERSION,
            "header": header,
            "lines": lines,
        }