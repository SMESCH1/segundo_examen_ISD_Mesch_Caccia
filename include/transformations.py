# módulo para la limpieza y normalización de los datos crudos
# valifa los datos y genera un archivo parquet para el downstream dbt (Silver/Gold)


from __future__ import annotations

from datetime import date
from pathlib import Path
import pandas as pd

RAW_FILE_TEMPLATE = "transactions_{ds_nodash}.csv"
CLEAN_FILE_TEMPLATE = "transactions_{ds_nodash}_clean.parquet"


def _coerce_amount(value: pd.Series) -> pd.Series:
    """Normalizar campos numéricos y eliminar entradas no-coercibe"""
    return pd.to_numeric(value, errors="coerce")


def _normalize_status(value: pd.Series) -> pd.Series:
    """
    Normalizar status a minúsculas.
    Unknown statuses a NaN, se eliminan en la validación.
    """
    normalized = value.fillna("").str.strip().str.lower()
    mapping = {
        "completed": "completed",
        "pending": "pending",
        "failed": "failed",
    }
    return normalized.map(mapping)


def clean_daily_transactions(
    execution_date: date,
    raw_dir: Path,
    clean_dir: Path,
    raw_template: str = RAW_FILE_TEMPLATE,
    clean_template: str = CLEAN_FILE_TEMPLATE,
) -> Path:
    """
    Etapa de limpieza de los datos crudos.

    Incluye:
    - Validación de las columnas requeridas
    - Normalización de campos numéricos y categóricos
    - Eliminación de statuses inválidos
    - Parsing de timestamps y eliminación de timestamps futuros
    - Eliminación de duplicados
    - Mínimo de filas válidas antes de escribir la salida
    """

    ds_nodash = execution_date.strftime("%Y%m%d")
    input_path = raw_dir / raw_template.format(ds_nodash=ds_nodash)
    output_path = clean_dir / clean_template.format(ds_nodash=ds_nodash)

    # Validación de la existencia del archivo
    
    if not input_path.exists():
        if skip_if_missing:
            logger.warning(
                "Datos crudos no encontrados para %s: %s. Saltando el procesamiento (skip_if_missing=True).",
                execution_date, input_path
            )
            return None
    raise FileNotFoundError(f"Raw data not found for {execution_date}: {input_path}")

    clean_dir.mkdir(parents=True, exist_ok=True)

    # Lectura y normalización de las columnas
    df = pd.read_csv(input_path)
    df.columns = [col.strip().lower() for col in df.columns]
    df = df.drop_duplicates()

    # Validación de las columnas
    required_cols = {
        "transaction_id",
        "customer_id",
        "amount",
        "status",
        "transaction_ts",
    }

    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas en el archivo crudo: {missing}")

    # Limpieza: normalización de los campos
    df["amount"] = _coerce_amount(df["amount"])
    df["status"] = _normalize_status(df["status"])

    # Eliminación de filas con campos críticos inválidos
    df = df.dropna(subset=["transaction_id", "customer_id", "amount", "status"])

    
    # Parsing y validación de timestamps
    df["transaction_ts"] = pd.to_datetime(df["transaction_ts"], errors="coerce", utc=True)
    df = df.dropna(subset=["transaction_ts"])

    # Convertir a datetime naive (remover la información de la zona horaria)
    df["transaction_ts"] = df["transaction_ts"].dt.tz_localize(None)

    # Eliminar timestamps futuros
    now_utc = pd.Timestamp.utcnow().replace(tzinfo=None)
    df = df[df["transaction_ts"] <= now_utc]

    df["transaction_date"] = df["transaction_ts"].dt.date

    # Validación final: asegurar al menos una fila válida
    if df.empty:
        raise ValueError(
            f"After Bronze cleaning, no valid rows remain for {execution_date}. "
            "Raw file may contain only invalid or future-dated rows."
        )

    # output en formato parquet
    df.to_parquet(output_path, index=False)
    return output_path
