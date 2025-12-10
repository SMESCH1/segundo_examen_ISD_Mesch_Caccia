from __future__ import annotations
import logging
from datetime import date
from pathlib import Path
import pandas as pd

# Plantillas de archivos: 
# Estas son las plantillas para los nombres de los archivos crudos y los archivos limpios.
RAW_FILE_TEMPLATE = "transactions_{ds_nodash}.csv"
CLEAN_FILE_TEMPLATE = "transactions_{ds_nodash}_clean.parquet"

# Configuración básica de logging
# Esto configura el registro de logs para que podamos hacer un seguimiento de lo que sucede.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Función para normalizar los valores en la columna "amount" (monto)
def _coerce_amount(value: pd.Series) -> pd.Series:
    """
    Normaliza los valores de la columna 'amount' a numéricos. Si algún valor no es numérico,
    lo convierte a NaN.
    """
    return pd.to_numeric(value, errors="coerce")

# Función para normalizar los valores en la columna "status" (estado de la transacción)
def _normalize_status(value: pd.Series) -> pd.Series:
    """
    Normaliza los valores en la columna 'status' a minúsculas.
    Si hay estados desconocidos, los convierte a NaN (estos serán eliminados más adelante).
    """
    # Elimina espacios en blanco, convierte a minúsculas y reemplaza valores desconocidos
    normalized = value.fillna("").str.strip().str.lower()
    mapping = {
        "completed": "completed",
        "pending": "pending",
        "failed": "failed",
    }
    # Mapea los valores conocidos y convierte los desconocidos a NaN
    return normalized.map(mapping)

# Función principal para limpiar los datos crudos
def clean_daily_transactions(
    execution_date: date,  # Fecha de ejecución para la cual se procesan los datos
    raw_dir: Path,  # Directorio donde están los archivos crudos
    clean_dir: Path,  # Directorio donde se guardarán los archivos limpios
    raw_template: str = RAW_FILE_TEMPLATE,  # Plantilla para los archivos crudos
    clean_template: str = CLEAN_FILE_TEMPLATE,  # Plantilla para los archivos limpios
    skip_if_missing: bool = False,  # Si es True, se omite el procesamiento si falta el archivo crudo
) -> Path:
    """
    Esta función realiza la limpieza y normalización de los datos crudos.
    """

    # Resolución de rutas relativas a absolutas
    # Asegura que las rutas proporcionadas se conviertan en rutas absolutas
    raw_dir = raw_dir.resolve()  # Convierte ruta relativa de raw_dir a absoluta
    clean_dir = clean_dir.resolve()  # Convierte ruta relativa de clean_dir a absoluta

    # Generación de la fecha sin guiones (por ejemplo, "20251210")
    ds_nodash = execution_date.strftime("%Y%m%d")
    
    # Construcción de las rutas de los archivos de entrada y salida
    input_path = raw_dir / raw_template.format(ds_nodash=ds_nodash)
    output_path = clean_dir / clean_template.format(ds_nodash=ds_nodash)

    # Verificar si el directorio de datos crudos existe
    if not raw_dir.exists():
        raise FileNotFoundError(f"Directorio de datos crudos no encontrado: {raw_dir}")

    # Validación de la existencia del archivo crudo
    # Si no existe el archivo crudo, se puede omitir si skip_if_missing es True
    if not input_path.exists():
        if skip_if_missing:
            logger.warning(
                "Datos crudos no encontrados para %s: %s. Saltando el procesamiento (skip_if_missing=True).",
                execution_date, input_path
            )
            return None  # Si se salta, no se realiza ninguna acción
        raise FileNotFoundError(f"Raw data not found for {execution_date}: {input_path}")

    # Crear el directorio de salida si no existe
    clean_dir.mkdir(parents=True, exist_ok=True)

    # Leer el archivo CSV y procesarlo
    logger.info(f"Procesando archivo: {input_path}")
    df = pd.read_csv(input_path)
    
    # Convertir todos los nombres de las columnas a minúsculas y quitar espacios
    df.columns = [col.strip().lower() for col in df.columns]
    
    # Eliminar duplicados del DataFrame
    df = df.drop_duplicates()

    # Validación de que todas las columnas requeridas estén presentes
    required_cols = {"transaction_id", "customer_id", "amount", "status", "transaction_ts"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Faltan columnas requeridas en el archivo crudo: {missing}")

    # Limpieza de los datos:
    # Normalización del monto y el estado
    df["amount"] = _coerce_amount(df["amount"])  # Normalización de la columna 'amount'
    df["status"] = _normalize_status(df["status"])  # Normalización de la columna 'status'
    
    # Eliminar filas con valores NaN en columnas críticas (transaction_id, customer_id, amount, status)
    df = df.dropna(subset=["transaction_id", "customer_id", "amount", "status"])

    # Parsing de los timestamps: conversion de 'transaction_ts' a formato datetime
    df["transaction_ts"] = pd.to_datetime(df["transaction_ts"], errors="coerce", utc=True)
    
    # Eliminar filas con timestamps inválidos
    df = df.dropna(subset=["transaction_ts"])

    # Convertir la fecha 'now_utc' a naive (sin zona horaria) para la comparación
    now_utc = pd.Timestamp.utcnow().replace(tzinfo=None)

    # Eliminar la zona horaria de los timestamps de la columna 'transaction_ts'
    df["transaction_ts"] = df["transaction_ts"].dt.tz_localize(None)

    # Eliminar filas con timestamps futuros
    df = df[df["transaction_ts"] <= now_utc]

    # Crear una nueva columna 'transaction_date' con la fecha de la transacción (sin la hora)
    df["transaction_date"] = df["transaction_ts"].dt.date

    # Validación final: verificar que el DataFrame no esté vacío después de la limpieza
    if df.empty:
        raise ValueError(
            f"After Bronze cleaning, no valid rows remain for {execution_date}. "
            "Raw file may contain only invalid or future-dated rows."
        )

    # Guardar el DataFrame limpio en formato Parquet
    logger.info(f"Guardando archivo limpio: {output_path}")
    df.to_parquet(output_path, index=False)  # Guardar como archivo Parquet sin índices

    # Devolver la ruta del archivo limpio generado
    return output_path
