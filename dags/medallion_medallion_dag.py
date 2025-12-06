"""Airflow DAG that orchestrates the medallion pipeline."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
# from airflow.operators.python import PythonOperator

#modificamos para atajar warning que nos salta
from airflow.providers.standard.operators.python import PythonOperator


# pylint: disable=import-error,wrong-import-position


BASE_DIR = Path(__file__).resolve().parents[1]
if str(BASE_DIR) not in sys.path:
    sys.path.append(str(BASE_DIR))

from include.transformations import (
    clean_daily_transactions,
)  # pylint: disable=wrong-import-position

RAW_DIR = BASE_DIR / "data/raw"
CLEAN_DIR = BASE_DIR / "data/clean"
QUALITY_DIR = BASE_DIR / "data/quality"
DBT_DIR = BASE_DIR / "dbt"
PROFILES_DIR = BASE_DIR / "profiles"
WAREHOUSE_PATH = BASE_DIR / "warehouse/medallion.duckdb"


def _build_env(ds_nodash: str) -> dict[str, str]:
    """Build environment variables needed by dbt commands."""
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
    """Execute a dbt command and return the completed process."""
    env = _build_env(ds_nodash)
    return subprocess.run(
        [
            "dbt",
            command,
            "--project-dir",
            str(DBT_DIR),
        ],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )


# TODO: Definir las funciones necesarias para cada etapa del pipeline
#  (bronze, silver, gold) usando las funciones de transformación y
#  los comandos de dbt.


# funcion limpieza de datos en capa bronze
def bronze_clean(**context): # usamos **context para poder acceder a ds_nodash desde Airflow
    """Limpia los datos en capa bronze
    Agarra los datos crudos y los limpia utilizando la funcion clean_daily_transactions
    """
    ds_nodash = context["ds_nodash"]
    execution_date = datetime.strptime(ds_nodash, "%Y%m%d").date()
    clean_daily_transactions(execution_date, RAW_DIR, CLEAN_DIR)

# funcion para carga de datos en capa silver
def silver_dbt_run(**context):
    """Carga los datos en capa silver"""
    ds_nodash = context["ds_nodash"]
    result = _run_dbt_command("run", ds_nodash)
    if result.returncode != 0:
        # agregamos logs detallados para debug
        error_log = f"dbt run failed (returncode: {result.returncode})\n"
        error_log += f"STDOUT:\n{result.stdout}\n"
        error_log += f"STDERR:\n{result.stderr}\n" 
        raise AirflowException(error_log)
        
    return True

# funcion test en capa gold
def gold_dbt_test(**context):
    """Testea los datos en capa gold y escribe el archivo JSON de resultados"""
    ds_nodash = context["ds_nodash"]
    result = _run_dbt_command("test", ds_nodash)
    
    # Crear el directorio si no existe
    QUALITY_DIR.mkdir(parents=True, exist_ok=True)
    
    # Escribir el archivo JSON con los resultados
    output_file = QUALITY_DIR / f"dq_results_{ds_nodash}.json"
    output_data = {
        "ds_nodash": ds_nodash,
        "status": "passed" if result.returncode == 0 else "failed",
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
    
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2)
    
    # Si los tests fallaron, lanzar excepción para que Airflow lo marque como fallido
    if result.returncode != 0:
        raise AirflowException(
            f"dbt test failed: {result.stderr}"
        )
    
    return True




def build_dag() -> DAG:
    """Construct the medallion pipeline DAG with bronze/silver/gold tasks."""
    with DAG(
        description="Bronze/Silver/Gold medallion demo with pandas, dbt, and DuckDB",
        dag_id="medallion_pipeline",
        schedule="0 6 * * *",
        start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
        catchup=True,
        max_active_runs=1,
    ) as medallion_dag:

        # TODO:
        # * Agregar las tasks necesarias del pipeline para completar lo pedido por el enunciado.
        # * Usar PythonOperator con el argumento op_kwargs para pasar ds_nodash a las funciones.
        #   De modo que cada task pueda trabajar con la fecha de ejecución correspondiente.
        # Recomendaciones:
        #  * Pasar el argumento ds_nodash a las funciones definidas arriba.
        #    ds_nodash contiene la fecha de ejecución en formato YYYYMMDD sin guiones.
        #    Utilizarlo para que cada task procese los datos del dia correcto y los archivos
        #    de salida tengan nombres únicos por fecha.
        #  * Asegurarse de que los paths usados en las funciones sean relativos a BASE_DIR.
        #  * Usar las funciones definidas arriba para cada etapa del pipeline.



        # task para limpieza de datos en capa bronze
        bronze_clean_task = PythonOperator(
            task_id="bronze_clean",
            python_callable=bronze_clean,
            # op_kwargs={"ds_nodash": "{{ ds_nodash }}"}, # si usamos **context no hace falta
        )
        # task para carga de datos en capa silver
        silver_dbt_run_task = PythonOperator(
            task_id="silver_dbt_run",
            python_callable=silver_dbt_run,
            # op_kwargs={"ds_nodash": "{{ ds_nodash }}"}, 
        )
        # task para test en capa gold
        gold_dbt_test_task = PythonOperator(
            task_id="gold_dbt_test",
            python_callable=gold_dbt_test,
            #op_kwargs={"ds_nodash": "{{ ds_nodash }}"},
        )

        # dependencias del flujo del dag
        bronze_clean_task >> silver_dbt_run_task >> gold_dbt_test_task



    return medallion_dag


dag = build_dag()
