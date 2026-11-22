"""Carga de transacciones bancarias desde RAW a Bronze."""

from pyspark.sql import functions as F

from openbanking.bronze_schemas import (
    INGESTION_FILES_TABLE,
    TRANSACTIONS_TABLE,
)


def ensure_tables(spark):
    """Crear las tablas si no existen, conservando los datos existentes."""

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TRANSACTIONS_TABLE} (
            observation_id STRING NOT NULL,
            source_file STRING NOT NULL,
            transaction_position INT NOT NULL,
            run_id STRING,
            bank STRING,
            account_uid STRING,
            identification_hash STRING,
            extracted_at TIMESTAMP,
            page_number BIGINT,
            schema_version BIGINT,
            transaction_json STRING,
            ingested_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Observaciones de transacciones de Enable Banking'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {INGESTION_FILES_TABLE} (
            source_file STRING NOT NULL,
            target_table STRING NOT NULL,
            run_id STRING,
            bank STRING,
            record_count BIGINT,
            processed_at TIMESTAMP
        )
        USING DELTA
        COMMENT 'Archivos RAW cargados correctamente en Bronze'
    """)


def find_pending_transaction_files(spark, dbutils, raw_path):
    """Seleccionar páginas de extracciones satisfactorias aún no cargadas."""

    raw_path = raw_path.rstrip("/") + "/"
    container_root = raw_path.split(
        "/personal/finanzas/openbanking/", 1
    )[0] + "/"

    manifest_paths = []

    def visit(path):
        for item in dbutils.fs.ls(path):
            if item.isDir():
                visit(item.path)
            elif item.name == "manifest.json":
                manifest_paths.append(item.path)

    for item in dbutils.fs.ls(raw_path):
        if item.isDir() and item.name.startswith("extraction_date="):
            visit(item.path)

    if not manifest_paths:
        raise RuntimeError("No se encontraron manifiestos de extracción.")

    manifest_schema = """
        run_id STRING,
        results ARRAY<STRUCT<
            bank: STRING,
            account_index: BIGINT,
            endpoint: STRING,
            status: STRING,
            files: ARRAY<STRING>,
            pages_saved: BIGINT,
            records_saved: BIGINT
        >>
    """

    declared = (
        spark.read
        .schema(manifest_schema)
        .option("multiLine", "true")
        .option("mode", "FAILFAST")
        .json(manifest_paths)
        .select(
            "run_id",
            F.explode("results").alias("result"),
        )
        .where(
            (F.col("result.endpoint") == "transactions")
            & (F.col("result.status") == "success")
        )
        .select(
            "run_id",
            F.col("result.bank").alias("bank"),
            F.col("result.account_index").alias("account_index"),
            F.explode("result.files").alias("declared_path"),
        )
    )

    # Convertir las rutas declaradas a rutas completas de ADLS.
    declared = declared.withColumn(
        "source_file",
        F.when(
            F.col("declared_path").startswith(container_root),
            F.col("declared_path"),
        )
        .when(
            F.col("declared_path").startswith(
                "personal/finanzas/openbanking/enablebanking/"
            ),
            F.concat(
                F.lit(container_root),
                F.col("declared_path"),
            ),
        )
        .when(
            F.col("declared_path").startswith("extraction_date="),
            F.concat(
                F.lit(raw_path),
                F.col("declared_path"),
            ),
        ),
    )

    invalid_paths = declared.where(
        F.col("source_file").isNull()
        | ~F.col("source_file").startswith(raw_path)
        | ~F.col("source_file").rlike(
            r"/transactions/page_[0-9]+\.json$"
        )
    )

    if invalid_paths.limit(1).count():
        raise ValueError(
            "Un manifiesto contiene una ruta no admitida."
        )

    files = (
        declared
        .select(
            "source_file",
            "run_id",
            "bank",
            "account_index",
        )
        .distinct()
    )

    conflicting_files = (
        files
        .groupBy("source_file")
        .count()
        .where(F.col("count") > 1)
    )

    if conflicting_files.limit(1).count():
        raise ValueError(
            "Hay archivos con metadatos contradictorios."
        )

    processed = (
        spark.table(INGESTION_FILES_TABLE)
        .where(F.col("target_table") == TRANSACTIONS_TABLE)
        .select("source_file")
        .distinct()
    )

    return files.join(
        processed,
        on="source_file",
        how="left_anti",
    )

def ingest_transactions(spark, dbutils, raw_path):
    """Cargar archivos pendientes de forma idempotente."""
    from delta.tables import DeltaTable

    from openbanking.bronze_schemas import TRANSACTION_PAGE_SCHEMA

    ensure_tables(spark)

    pending = find_pending_transaction_files(
        spark, dbutils, raw_path
    )

    pending_rows = pending.collect()
    paths = [row.source_file for row in pending_rows]

    if not paths:
        return {"files_processed": 0, "observations_processed": 0}

    # Fijar la selección para que no cambie al actualizar el control.
    pending = spark.createDataFrame(
        pending_rows,
        schema=pending.schema,
    )

    pages = (
        spark.read
        .schema(TRANSACTION_PAGE_SCHEMA)
        .option("multiLine", "true")
        .option("mode", "FAILFAST")
        .json(paths)
        .select(
            F.col("_metadata.file_path").alias("source_file"),
            "*",
        )
    )

    # Comparar los metadatos del JSON con los del manifiesto.
    checked = (
        pending.alias("expected")
        .join(
            pages.alias("page"),
            on="source_file",
            how="left",
        )
    )

    invalid = checked.where(
        ~F.col("page.run_id").eqNullSafe(
            F.col("expected.run_id")
        )
        | ~F.col("page.bank").eqNullSafe(
            F.col("expected.bank")
        )
        | ~F.col("page.endpoint").eqNullSafe(
            F.lit("transactions")
        )
        | F.col("page.response.transactions").isNull()
        | F.col("page.account_uid").isNull()
        | (F.length(F.trim(F.col("page.account_uid"))) == 0)
        | F.expr("try_cast(page.extracted_at AS TIMESTAMP)").isNull()
    )

    if invalid.limit(1).count():
        raise ValueError(
            "Página ausente o inválida, o metadatos distintos "
            "a los del manifiesto. No se ha escrito en Bronze."
        )

    # Una página debe producir exactamente una fila antes de expandir.
    if (
        pages.groupBy("source_file")
        .count()
        .where(F.col("count") != 1)
        .limit(1)
        .count()
    ):
        raise ValueError("Se esperaba un objeto JSON por archivo.")

    records = (
        pages
        .select(
            "source_file",
            "run_id",
            "bank",
            "account_uid",
            "identification_hash",
            "extracted_at",
            "page_number",
            "schema_version",
            F.posexplode("response.transactions").alias(
                "transaction_position",
                "transaction_json",
            ),
        )
        .withColumn(
            "observation_id",
            F.sha2(
                F.to_json(
                    F.struct(
                        "source_file",
                        "transaction_position",
                    )
                ),
                256,
            ),
        )
        .withColumn(
            "extracted_at",
            F.col("extracted_at").cast("timestamp"),
        )
        .withColumn("ingested_at", F.current_timestamp())
        .select(
            "observation_id",
            "source_file",
            "transaction_position",
            "run_id",
            "bank",
            "account_uid",
            "identification_hash",
            "extracted_at",
            "page_number",
            "schema_version",
            "transaction_json",
            "ingested_at",
        )
    )

    # Exigir un objeto JSON por movimiento, conservando su contenido.
    invalid_records = records.where(
        F.col("transaction_json").isNull()
        | ~F.trim(F.col("transaction_json")).startswith("{")
        | F.get_json_object(
            F.col("transaction_json"), "$"
        ).isNull()
    )

    if invalid_records.limit(1).count():
        raise ValueError(
            "Hay movimientos que no son objetos JSON válidos."
        )

    # No modifica observaciones existentes ni duplica un reintento.
    (
        DeltaTable.forName(spark, TRANSACTIONS_TABLE)
        .alias("target")
        .merge(
            records.alias("source"),
            "target.observation_id = source.observation_id",
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    file_counts = pages.select(
        "source_file",
        "run_id",
        "bank",
        F.size("response.transactions")
        .cast("long")
        .alias("record_count"),
    )

    # Confirmar que todas las observaciones llegaron antes de marcar
    # los archivos. También admite páginas con cero movimientos.
    stored_counts = (
        spark.table(TRANSACTIONS_TABLE)
        .join(pending.select("source_file"), "source_file", "inner")
        .groupBy("source_file")
        .agg(F.count("*").alias("stored_count"))
    )

    mismatches = (
        file_counts
        .join(stored_counts, "source_file", "left")
        .where(
            F.col("record_count")
            != F.coalesce(F.col("stored_count"), F.lit(0))
        )
    )

    if mismatches.limit(1).count():
        raise RuntimeError(
            "El recuento escrito no coincide con las páginas. "
            "Los archivos no se marcarán como procesados."
        )

    completed = (
        file_counts
        .withColumn("target_table", F.lit(TRANSACTIONS_TABLE))
        .withColumn("processed_at", F.current_timestamp())
    )

    (
        DeltaTable.forName(spark, INGESTION_FILES_TABLE)
        .alias("target")
        .merge(
            completed.alias("source"),
            """
            target.source_file = source.source_file
            AND target.target_table = source.target_table
            """,
        )
        .whenNotMatchedInsertAll()
        .execute()
    )

    total = file_counts.agg(
        F.sum("record_count").alias("observations")
    ).first().observations

    return {
        "files_processed": len(paths),
        "observations_processed": total or 0,
    }