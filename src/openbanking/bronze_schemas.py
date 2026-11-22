"""Esquemas de entrada y nombres de tablas para transacciones Bronze."""

from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
)

TRANSACTIONS_TABLE = "openbanking.bronze.enablebanking_transactions"
INGESTION_FILES_TABLE = "openbanking.ops.banking_ingestion_files"

# Conservamos cada movimiento completo como JSON.
# Su normalización a columnas de negocio se realizará en Silver.
TRANSACTION_PAGE_SCHEMA = StructType([
    StructField("schema_version", LongType(), True),
    StructField("run_id", StringType(), True),
    StructField("bank", StringType(), True),
    StructField("account_uid", StringType(), True),
    StructField("identification_hash", StringType(), True),
    StructField("extracted_at", StringType(), True),
    StructField("endpoint", StringType(), True),
    StructField("page_number", LongType(), True),
    StructField(
        "response",
        StructType([
            StructField(
                "transactions",
                ArrayType(StringType(), containsNull=True),
                True,
            ),
            StructField("continuation_key", StringType(), True),
        ]),
        True,
    ),
])