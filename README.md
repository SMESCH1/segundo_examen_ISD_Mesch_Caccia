# Medallion Architecture Pipeline (Airflow + dbt + DuckDB)

Integrantes:

* **Matías Caccia**
* **Sebastián Mesch Henriques**

Este proyecto implementa un pipeline de datos siguiendo la **arquitectura Medallion (Bronze → Silver → Gold)** utilizando:

* **Airflow** como orquestador
* **Pandas** para la etapa Bronze
* **dbt + DuckDB** para modelado Silver y Gold
* **Tests personalizados** de calidad de datos
* **Reportes automáticos de Data Quality (DQ)**

El objetivo es demostrar un pipeline moderno, limpio y escalable que aplica prácticas reales de ingeniería de datos:

* Buenas prácticas de ingestión
* Normalización y validación robusta en Bronze
* Modelado en dbt en capas Silver/Gold
* Data Quality sólido con tests genéricos y singulares
* Observabilidad y trazabilidad completa del pipeline

---

# Estructura del proyecto

```
├── dags/
│   └── medallion_medallion_dag.py        <- DAG completo Bronze/Silver/Gold
├── data/
│   ├── raw/                              <- Archivos CSV crudos
│   ├── clean/                            <- Parquet Bronze
│   └── quality/                          <- Resultados Gold (DQ)
├── dbt/
│   ├── dbt_project.yml
│   ├── models/
│   │   ├── staging/                      <- stg_transactions.sql + schema.yml
│   │   └── marts/                        <- fct_customer_transactions.sql + schema.yml
│   └── tests/                            <- Tests genéricos y singulares (Silver/Gold)
├── include/
│   └── transformations.py                <- Limpieza Bronze robusta
├── profiles/
│   └── profiles.yml                      <- Perfil DuckDB para dbt
├── warehouse/
│   └── medallion.duckdb                  <- Base creada en runtime
└── requirements.txt
```

---

# Arquitectura Medallion Implementada

## BRONZE — Limpieza mínima pero confiable con Pandas

Airflow ejecuta:

```python
clean_daily_transactions(execution_date, RAW_DIR, CLEAN_DIR)
```

### Mejoras implementadas en Bronze

✔ Validación de columnas requeridas
✔ Normalización de `amount` → numérico robusto
✔ Normalización de `status` → lowercase + mapping estricto
✔ Parsing seguro de timestamps
✔ Eliminación de fechas futuras
✔ Eliminación de duplicados
✔ Validación de que existan filas válidas
✔ Escritura en Parquet eficiente y estándar

Salida:

```
data/clean/transactions_<ds>_clean.parquet
```

## SILVER — Modelado con dbt

Airflow ejecuta:

```
dbt run
```

Silver produce un modelo limpio, tipado y listo para análisis:

* `stg_transactions.sql`
* Derivación de `transaction_date`
* Cast de tipos según `schema.yml`
* Validaciones complejas a nivel de dominio

### Tests implementados en Silver y su razón de ser

A continuación, se describen todos los tests incluidos, **por qué existen**, y **cuándo fallarían** en un sistema real.

#### 1. `not_null`

Asegura que columnas esenciales no estén vacías.

**Fallaría si:**

* El archivo crudo viene truncado
* El upstream omite campos
* Se rompe el mapeo de status o amounts

#### 2. `unique` (transaction_id)

Cada transacción debe ser única.

**Fallaría si:**

* El proveedor envía duplicados
* Se concatenan archivos accidentalmente
* El sistema upstream reenvía transacciones

#### 3. `non_negative` (amount)

Un monto nunca debe ser negativo.

**Fallaría si:**

* Se invierte el signo por error
* Se registran reversas mal representadas
* Hay fallas del ETL upstream

#### 4. `accepted_status`

Sólo se aceptan:

```
completed | pending | failed
```

**Fallaría si:**

* Aparece un nuevo estado no documentado
* Existen inconsistencias (Completed, completed , UNKNOWN)
* Hay problemas de input manual

#### 5. `valid_timestamp`

Verifica que `transaction_ts` sea un timestamp válido.

**Fallaría si:**

* Aparecen fechas inválidas (“2025/13/99”)
* Hay strings corruptas
* El upstream envía formatos distintos

#### 6. `not_in_future`

Protege contra timestamps futuros.

**Fallaría si:**

* El reloj del upstream está adelantado
* Existen datos simulados colados en producción
* Hay errores en la conversión de zona horaria

#### 7. `unique_transaction_id`

Segunda barrera contra duplicados, valida lógica de negocio.

**Fallaría si:**

* Un día se carga dos veces el mismo archivo
* Se incorporan filas repetidas después del Bronze

#### 8. `amount_not_outlier`

Detecta outliers usando IQR.

**Fallaría si:**

* Hay transacciones fraudulentas
* Hay errores manuales de carga
* Se recibe “5000000” por error humano o de sistema


#### 9. `no_duplicate_rows`

Evita duplicados completos de todas las columnas.

**Fallaría si:**

* Se adjunta el mismo dataset dos veces
* Hay un join mal aplicado en Bronze
* Pandas concatena archivos sin cuidado

## GOLD — Métricas finales + Validación avanzada

Airflow ejecuta:

```
dbt test
```

El modelo:

```
fct_customer_transactions.sql
```

Produce:

* `transaction_count`
* `total_amount_completed`
* `total_amount_all`
* `first_transaction_ts`
* `last_transaction_ts`

### Tests GOLD y su valor analítico

#### 1. `transaction_count_positive`

Cada cliente debe tener al menos una transacción.

**Fallaría si:**

* El join Silver→Gold se rompe
* Se filtran filas accidentalmente
* Se incorporan clientes sin información factual

---

#### 2. `total_amount_non_negative`

Asegura que las sumas nunca sean negativas.

**Fallaría si:**

* Escapó un valor negativo desde Silver
* Las agregaciones están mal definidas
* Se resta accidentalmente en vez de sumar

---

#### 3. `customer_exists_in_silver`

Valida integridad referencial.

**Fallaría si:**

* Hay clientes agregados sin base factual
* Se rompe el join
* Hay llaves mal tipadas

---

#### 4. `unique_customer_rows`

Cada cliente debe aparecer una sola vez.

**Fallaría si:**

* Se agrupa incorrectamente
* Existen duplicados en la lógica Gold
* Se mezclan dimensiones con hechos

### Reportes GOLD de Data Quality

La tarea `gold_dbt_test()` genera:

```
data/quality/dq_results_<ds>.json
```

Incluye:

* Estado global (`passed` / `failed`)
* Duración de la corrida
* stdout y stderr de dbt
* Existencia del warehouse
* Resumen completo para auditoría

---

# Instalación

```bash
python -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
```

---

# Configuración de entorno

```bash
export AIRFLOW_HOME=$(pwd)/airflow_home
export DBT_PROFILES_DIR=$(pwd)/profiles
export DUCKDB_PATH=$(pwd)/warehouse/medallion.duckdb
export AIRFLOW__CORE__DAGS_FOLDER=$(pwd)/dags
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```

---

# Inicializar Airflow

```bash
airflow standalone
```

---

# Ejecutar el DAG

1. Colocar un archivo CSV:

```
data/raw/transactions_YYYYMMDD.csv
```

2. Ejecutar:

```bash
airflow dags trigger medallion_pipeline --run-id manual_$(date +%s)
```

---

# Observabilidad por capa

## Bronze

```
duckdb -c "
  select * from read_parquet('data/clean/transactions_<ds>_clean.parquet')
  limit 5;
"
```

## Silver

```
duckdb warehouse/medallion.duckdb -c ".tables"
duckdb warehouse/medallion.duckdb -c "select * from stg_transactions limit 10;"
```

## Gold

```
cat data/quality/dq_results_<ds>.json | jq
```

---

# Ejecutar dbt manualmente

```bash
cd dbt
dbt run
DBT_PROFILES_DIR=../profiles dbt test
```

---

# Conclusión: Escalabilidad y propuesta arquitectónica

Este pipeline es **perfecto para entornos educativos, prototipos y equipos pequeños**, pero su diseño también permite visualizar claramente el camino de evolución hacia un sistema de nivel industria.

## ✔ Límites del diseño actual

| Componente         | Limitación                            |
| ------------------ | ------------------------------------- |
| Pandas             | No escala horizontalmente             |
| Parquet local      | No soporta concurrencia ni versionado |
| DuckDB             | No distribuido, memoria local         |
| Airflow standalone | No altamente disponible               |

---

# Evolución

## 1. Migración del almacenamiento

Del local a la nube:

* Amazon S3
* GCS
* Azure Blob

Incluye versionado, gobernanza y escalabilidad.

---

## 2. Migración del warehouse

De DuckDB a sistemas distribuidos:

* BigQuery
* Snowflake
* Redshift Serverless
* Databricks + Delta Lake

Beneficios:

* Escalabilidad automática
* Costos optimizados
* Gran concurrencia en consultas

---

# Casos concretos que justificarían migrar

* Retail procesando **millones de transacciones diarias**
* Empresas que requieren **auditoría estricta y versionado**
* Equipos de BI con decenas de usuarios simultáneos
* Casos de Machine Learning donde se requieren años de histórico

---

Este pipeline implementa un ejemplo sólido, profesional y completamente funcional de arquitectura Medallion moderna, integrando:

* Airflow
* dbt
* DuckDB
* Validaciones Silver y Gold avanzadas
* Orquestación y observabilidad
* Diseño modular y extensible

Además, muestra cómo este diseño puede evolucionar hacia arquitecturas empresariales de gran escala sin reescribir la lógica esencial del negocio.