from __future__ import annotations

import json
import os
import subprocess
import sys
import logging
from pathlib import Path
from datetime import datetime

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator

logger = logging.getLogger(__name__)

BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from include.transformations import clean_daily_transactions  # noqa: E402

RAW_DIR = BASE_DIR / "data/raw"
CLEAN_DIR = BASE_DIR / "data/clean"
QUALITY_DIR = BASE_DIR / "data/quality"
QUALITY_LOG_DIR = QUALITY_DIR / "logs"
DBT_DIR = BASE_DIR / "dbt"
PROFILES_DIR = BASE_DIR / "profiles"
WAREHOUSE_PATH = BASE_DIR / "warehouse/medallion.duckdb"


# funciones auxiliares

def _build_env(ds_nodash: str) -> dict[str, str]:
    """
    función que crea las variables de entorno de dbt
    """
    env = os.environ.copy()
    env.update(
        {
            "DBT_PROFILES_DIR": str(PROFILES_DIR),
            "CLEAN_DIR": str(CLEAN_DIR),
            "DS_NODASH": ds_nodash,
            "DUCKDB_PATH": str(WAREHOUSE_PATH),
        }
    )
    return env


def _run_dbt_command(command: str, ds_nodash: str) -> subprocess.CompletedProcess:
    """
    función para ejecturar los comandos de dbt (`run` y `test`).
    permite hacer un seguimiento de los stdout/stderr para debugging y logging.
    """
    env = _build_env(ds_nodash)
    logger.info("Running dbt %s for ds_nodash=%s", command, ds_nodash)

    return subprocess.run(
        ["dbt", command, "--project-dir", str(DBT_DIR)],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,  
    )


# limpieza y validación de los datos

def bronze_clean(**context):
    """
    Etapa bronze de limpieza y validación de los datos
    Limpia los datos crudos y almacena los datos en un archivo normalizado en formato parquet
    Verifica que el archivo parquet existe y no esté vacío 
    """
    logical_date = context["logical_date"].in_timezone("America/Argentina/Buenos_Aires")
    execution_date = logical_date.date()
    ds_nodash = logical_date.strftime("%Y%m%d")

    logger.info(
        "Bronze stage — execution_date=%s ds_nodash=%s logical_date=%s",
        execution_date, ds_nodash, logical_date,
    )

    try:
        parquet_path = clean_daily_transactions(execution_date, RAW_DIR, CLEAN_DIR)
    except (FileNotFoundError, ValueError) as exc:
        raise AirflowFailException(str(exc)) from exc

    # Verificaciones adicionales de la etapa bronze
    if not parquet_path.exists():
        raise AirflowFailException(f"Bronze parquet missing: {parquet_path}")

    if parquet_path.stat().st_size < 500:  # arbitrary threshold; prevents empty files
        logger.warning("Bronze parquet may be too small (<500 bytes): %s", parquet_path)

    logger.info("Bronze OK — parquet generated: %s (%d bytes)", parquet_path, parquet_path.stat().st_size)
    return str(parquet_path)


# Etapa Silver: modelo de transformación de los datos
# dbt run (Transforms → Silver models)

def silver_dbt_run(**context):
    """
    Etapa Silver: ejecuta `dbt run` para cargar y transformar los datos limpios.

    - Confirma que el archivo parquet para ds_nodash existe antes de ejecutar dbt
    - Escribe logs mejorados en QUALITY_LOG_DIR
    - Lanza una excepción explícita cuando dbt returncode != 0
    """
    logical_date = context["logical_date"].in_timezone("America/Argentina/Buenos_Aires") # evitar errires de timezone
    ds_nodash = logical_date.strftime("%Y%m%d")

    parquet_expected = CLEAN_DIR / f"transactions_{ds_nodash}_clean.parquet"
    if not parquet_expected.exists():
        raise AirflowFailException(
            f"Etapa Silver fallida, falta el archivo parquet: {parquet_expected}"
        )

    # Tracking del tiempo de ejecución
    start_time = pendulum.now("UTC")
    result = _run_dbt_command("run", ds_nodash)
    end_time = pendulum.now("UTC")
    duration = (end_time - start_time).total_seconds()

    QUALITY_LOG_DIR.mkdir(parents=True, exist_ok=True)
    log_path = QUALITY_LOG_DIR / f"dbt_run_{ds_nodash}.log"

    with open(log_path, "w", encoding="utf-8") as f:
        f.write(f"DBT RUN LOG — {ds_nodash}\n")
        f.write(f"Duration: {duration:.2f}s\n\n")
        f.write("STDOUT:\n" + (result.stdout or "") + "\n\n")
        f.write("STDERR:\n" + (result.stderr or ""))

    if result.returncode != 0:
        raise AirflowException(
            f"Etapa Silver fallida, dbt run fallido para {ds_nodash}. Ver logs: {log_path}"
        )

    logger.info("Etapa Silver OK — dbt run completada en %.2fs", duration)
    return True


# Etapa Gold: validación de los datos
# dbt test (Final validation + DQ artifact)

def gold_dbt_test(**context):
    """
    Etapa Gold: ejecuta dbt test y escribe un archivo JSON que resume la calidad de los datos

    - Verifica que el warehouse de DuckDB existe antes de ejecutar dbt test
    - Escribe un archivo JSON que resume la calidad de los datos
    - Lanza una excepción explícita cuando dbt tests fallan
    """
    #ds_nodash = context["ds_nodash"] 
    logical_date = context["logical_date"].in_timezone("America/Argentina/Buenos_Aires")
    execution_date = logical_date.date()
    ds_nodash = logical_date.strftime("%Y%m%d")

    if not WAREHOUSE_PATH.exists():
        raise AirflowFailException(
            f"Etapa Gold fallida, el warehouse de DuckDB no existe: {WAREHOUSE_PATH}"
        )

    start_time = pendulum.now("UTC") # tracking del tiempo de ejecución
    result = _run_dbt_command("test", ds_nodash)
    end_time = pendulum.now("UTC")
    duration = (end_time - start_time).total_seconds()

    QUALITY_DIR.mkdir(parents=True, exist_ok=True)

    dq_file = QUALITY_DIR / f"dq_results_{ds_nodash}.json"
    dq_file.write_text(
        json.dumps(
            {
                "ds_nodash": ds_nodash,
                "status": "passed" if result.returncode == 0 else "failed",
                "duration_seconds": duration,
                "stdout": result.stdout,
                "stderr": result.stderr,
                "warehouse_exists": WAREHOUSE_PATH.exists(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    if result.returncode != 0:
        raise AirflowException(f"Etapa Gold fallida, dbt test fallido para {ds_nodash}")

    logger.info("Etapa Gold OK — dbt test completado para %s", ds_nodash)
    return True


# Definimos el DAG

def build_dag() -> DAG:
    """
    DAG para el pipeline de Medallion (Bronze → Silver → Gold) con validaciones

    Pipeline:
        Bronze → Silver → Gold
    """
    with DAG(
        dag_id="medallion_pipeline",
        schedule="0 6 * * *",
        start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        description="Pipeline de Medallion (Bronze → Silver → Gold) con validaciones",
    ) as dag:

        bronze = PythonOperator(task_id="bronze_clean", python_callable=bronze_clean)
        silver = PythonOperator(task_id="silver_dbt_run", python_callable=silver_dbt_run)
        gold = PythonOperator(task_id="gold_dbt_test", python_callable=gold_dbt_test)

        bronze >> silver >> gold

    return dag


dag = build_dag()
