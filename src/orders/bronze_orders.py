"""Ingesta de pedidos PDF recibidos por WhatsApp."""

LINES_TABLE = "pedidos.bronze.whatsapp_order_lines"
CONTROL_TABLE = "pedidos.ops.document_processing"


def ensure_tables(spark):
    """Crear catálogo, esquemas y tablas sin sobrescribir datos."""

    spark.sql("""
        CREATE CATALOG IF NOT EXISTS pedidos
        MANAGED LOCATION
        'abfss://databricks@graxianodatalake.dfs.core.windows.net/pedidos/'
    """)

    spark.sql("CREATE SCHEMA IF NOT EXISTS pedidos.bronze")
    spark.sql("CREATE SCHEMA IF NOT EXISTS pedidos.ops")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {LINES_TABLE} (
            observation_id STRING NOT NULL,
            source_file STRING NOT NULL,
            document_sha256 STRING NOT NULL,
            media_id STRING,
            parser_version STRING NOT NULL,
            page_number INT,
            line_number INT,
            customer_raw STRING,
            order_date_raw STRING,
            delivery_date_raw STRING,
            delivery_notes_raw STRING,
            product_raw STRING,
            quantity_raw STRING,
            ingested_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Líneas extraídas de pedidos PDF recibidos por WhatsApp'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
            source_file STRING NOT NULL,
            document_sha256 STRING NOT NULL,
            parser_version STRING NOT NULL,
            status STRING,
            line_count BIGINT,
            error_type STRING,
            error_message STRING,
            processed_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Resultado del procesamiento de documentos PDF'
    """)

def ingest_orders(spark, dbutils, documents_path):
    """Procesar PDF pendientes y escribir líneas y control en Delta."""
    import hashlib
    import json
    from datetime import datetime, timezone
    from pathlib import PurePosixPath

    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    from orders.pdf_parser import PARSER_VERSION, extract_order

    ensure_tables(spark)

    files = [
        item.path
        for item in dbutils.fs.ls(documents_path)
        if not item.isDir() and item.name.lower().endswith(".pdf")
    ]

    if not files:
        return {"documents_processed": 0, "lines": 0, "errors": 0}

    candidates = spark.createDataFrame(
        [(path,) for path in files],
        "source_file STRING",
    )

    processed = (
        spark.table(CONTROL_TABLE)
        .where(
            (F.col("status") == "success")
            & (F.col("parser_version") == PARSER_VERSION)
        )
        .select("source_file")
        .distinct()
    )

    pending = [
        row.source_file
        for row in candidates.join(
            processed, "source_file", "left_anti"
        ).collect()
    ]

    if not pending:
        return {"documents_processed": 0, "lines": 0, "errors": 0}

    summary = {"documents_processed": 0, "lines": 0, "errors": 0}

    # Lotes pequeños: no descargar todos los PDF a la memoria del driver.
    for offset in range(0, len(pending), 10):
        batch_paths = pending[offset:offset + 10]

        documents = (
            spark.read
            .format("binaryFile")
            .load(batch_paths)
            .select("path", "length", "content")
            .collect()
        )

        line_rows = []
        control_rows = []

        for document in documents:
            source_file = document.path
            content = bytes(document.content)
            document_hash = hashlib.sha256(content).hexdigest()
            processed_at = datetime.now(timezone.utc)

            try:
                order = extract_order(content)
            except Exception as exc:
                # El error del documento queda registrado.
                # Los errores de lectura o escritura de infraestructura
                # se propagan y hacen fallar la ejecución.
                control_rows.append({
                    "source_file": source_file,
                    "document_sha256": document_hash,
                    "parser_version": PARSER_VERSION,
                    "status": "error",
                    "line_count": 0,
                    "error_type": type(exc).__name__,
                    "error_message": str(exc)[:1000],
                    "processed_at": processed_at,
                })
                summary["errors"] += 1
                continue

            media_id = PurePosixPath(source_file).stem

            for line in order["lines"]:
                identity = json.dumps(
                    [
                        source_file,
                        document_hash,
                        PARSER_VERSION,
                        line["page_number"],
                        line["line_number"],
                    ],
                    ensure_ascii=False,
                    separators=(",", ":"),
                )

                line_rows.append({
                    "observation_id": hashlib.sha256(
                        identity.encode("utf-8")
                    ).hexdigest(),
                    "source_file": source_file,
                    "document_sha256": document_hash,
                    "media_id": media_id,
                    "parser_version": PARSER_VERSION,
                    **order["header"],
                    **line,
                    "ingested_at": processed_at,
                })

            control_rows.append({
                "source_file": source_file,
                "document_sha256": document_hash,
                "parser_version": PARSER_VERSION,
                "status": "success",
                "line_count": len(order["lines"]),
                "error_type": None,
                "error_message": None,
                "processed_at": processed_at,
            })

            summary["documents_processed"] += 1
            summary["lines"] += len(order["lines"])

        # Primero guardar las líneas; después marcar los documentos.
        if line_rows:
            lines_df = spark.createDataFrame(
                line_rows,
                schema=spark.table(LINES_TABLE).schema,
            )

            (
                DeltaTable.forName(spark, LINES_TABLE)
                .alias("target")
                .merge(
                    lines_df.alias("source"),
                    "target.observation_id = source.observation_id",
                )
                .whenNotMatchedInsertAll()
                .execute()
            )

        if control_rows:
            control_df = spark.createDataFrame(
                control_rows,
                schema=spark.table(CONTROL_TABLE).schema,
            )

            (
                DeltaTable.forName(spark, CONTROL_TABLE)
                .alias("target")
                .merge(
                    control_df.alias("source"),
                    """
                    target.source_file = source.source_file
                    AND target.document_sha256 = source.document_sha256
                    AND target.parser_version = source.parser_version
                    """,
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )

    return summary