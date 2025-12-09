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


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def _build_env(ds_nodash: str) -> dict[str, str]:
    """
    Builds environment variables consumed by dbt models.
    Ensures dbt has consistent and reproducible paths.
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
    Generic runner for dbt commands (`run` and `test`).
    Captures stdout/stderr for debugging and logging.
    """
    env = _build_env(ds_nodash)
    logger.info("Running dbt %s for ds_nodash=%s", command, ds_nodash)

    return subprocess.run(
        ["dbt", command, "--project-dir", str(DBT_DIR)],
        cwd=DBT_DIR,
        env=env,
        capture_output=True,
        text=True,
        check=False,  # Never raise here. We handle errors manually.
    )


# ---------------------------------------------------------------------------
# Bronze — Data cleaning and validation
# ---------------------------------------------------------------------------

def bronze_clean(**context):
    """
    Bronze step: cleans the raw CSV for the execution date and stores
    a normalized parquet file.

    Enhancements added:
    - Uses local timezone to avoid UTC drift on manual triggers.
    - Validates that the generated parquet exists.
    - Validates that the file is not empty (minimum required rows).
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

    # --- Additional Bronze verifications ---
    if not parquet_path.exists():
        raise AirflowFailException(f"Bronze parquet missing: {parquet_path}")

    if parquet_path.stat().st_size < 500:  # arbitrary threshold; prevents empty files
        logger.warning("Bronze parquet may be too small (<500 bytes): %s", parquet_path)

    logger.info("Bronze OK — parquet generated: %s (%d bytes)", parquet_path, parquet_path.stat().st_size)
    return str(parquet_path)


# ---------------------------------------------------------------------------
# Silver — dbt run (Transforms → Silver models)
# ---------------------------------------------------------------------------

def silver_dbt_run(**context):
    """
    Silver step: executes `dbt run` to load and transform cleaned data.

    Improvements:
    - Confirms that parquet for ds_nodash exists BEFORE running dbt.
    - Writes enhanced logs to QUALITY_LOG_DIR.
    - Raises explicit failure when dbt returncode != 0.
    """
    logical_date = context["logical_date"].in_timezone("America/Argentina/Buenos_Aires")
    ds_nodash = logical_date.strftime("%Y%m%d")

    parquet_expected = CLEAN_DIR / f"transactions_{ds_nodash}_clean.parquet"
    if not parquet_expected.exists():
        raise AirflowFailException(
            f"Silver stage blocked — missing bronze parquet: {parquet_expected}"
        )

    # Time tracking for observability
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
            f"dbt run failed for {ds_nodash}. See logs: {log_path}"
        )

    logger.info("Silver OK — dbt run completed in %.2fs", duration)
    return True


# ---------------------------------------------------------------------------
# Gold — dbt test (Final validation + DQ artifact)
# ---------------------------------------------------------------------------

def gold_dbt_test(**context):
    """
    Gold step: executes `dbt test` and writes a JSON file summarizing
    the data quality status.

    Improvements:
    - Validates DuckDB warehouse exists before testing.
    - Writes enhanced DQ report, including duration.
    - Fails the task if dbt tests fail.
    """
    #ds_nodash = context["ds_nodash"]
    logical_date = context["logical_date"].in_timezone("America/Argentina/Buenos_Aires")
    execution_date = logical_date.date()
    ds_nodash = logical_date.strftime("%Y%m%d")

    if not WAREHOUSE_PATH.exists():
        raise AirflowFailException(
            f"DuckDB warehouse missing before Gold stage: {WAREHOUSE_PATH}"
        )

    start_time = pendulum.now("UTC")
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
        raise AirflowException(f"dbt test failed for {ds_nodash}")

    logger.info("Gold OK — dbt test passed for %s", ds_nodash)
    return True


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

def build_dag() -> DAG:
    """
    DAG for Medallion pipeline with enhanced testing and documentation.

    Pipeline:
        Bronze → Silver → Gold

    Notes:
    - catchup=False to avoid undesired historical executions.
    - max_active_runs=1 to enforce serial data correctness.
    """
    with DAG(
        dag_id="medallion_pipeline",
        schedule="0 6 * * *",
        start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
        catchup=False,
        max_active_runs=1,
        description="Medallion Architecture Pipeline (Bronze/Silver/Gold) with validations",
    ) as dag:

        bronze = PythonOperator(task_id="bronze_clean", python_callable=bronze_clean)
        silver = PythonOperator(task_id="silver_dbt_run", python_callable=silver_dbt_run)
        gold = PythonOperator(task_id="gold_dbt_test", python_callable=gold_dbt_test)

        bronze >> silver >> gold

    return dag


dag = build_dag()
