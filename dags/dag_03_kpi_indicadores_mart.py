"""
DAG 3: Cálculo de los 6 Indicadores de Gestión (KPIs) en SQL sobre el Data Warehouse
(esquema dw) y materialización idempotente en el esquema mart.
"""
import os
import time
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from etl_utils import callback_falla_tarea, registrar_auditoria_etl

POSTGRES_CONN_ID = "postgres_dw_conn"
SQL_KPI_PATH = os.path.join(
    os.path.dirname(os.path.dirname(__file__)), "sql", "02_kpi_mart_queries.sql"
)


def verificar_datos_base_dw(**context: Any) -> int:
    """
    Tarea 1: Verifica que existan territorios en dw.dim_territorio y hechos cargados
    para la fecha de análisis (o toma la última fecha disponible en dw.fact_operacion_farmacia).
    """
    dag_run = context.get("dag_run")
    conf_fecha = dag_run.conf.get("fecha_proceso") if dag_run and dag_run.conf else None
    ds = conf_fecha or context["ds"]
    fecha_id_objetivo = int(ds.replace("-", ""))

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM dw.dim_territorio WHERE territorio_sk <> -1;")
        total_comunas = cur.fetchone()[0]
        if total_comunas == 0:
            raise AirflowException(
                "No existen comunas cargadas en dw.dim_territorio. Ejecute primero dag_01_etl_subdere_dpa."
            )

        cur.execute(
            "SELECT COUNT(*) FROM dw.fact_operacion_farmacia WHERE fecha_id = %s;",
            (fecha_id_objetivo,),
        )
        hechos_fecha = cur.fetchone()[0]

        if hechos_fecha == 0:
            cur.execute("SELECT MAX(fecha_id) FROM dw.fact_operacion_farmacia;")
            max_fecha_id = cur.fetchone()[0]
            if not max_fecha_id:
                raise AirflowException("La tabla dw.fact_operacion_farmacia no tiene registros cargados.")
            fecha_id_objetivo = int(max_fecha_id)

    return fecha_id_objetivo


def calcular_kpis_en_mart(**context: Any) -> None:
    """
    Tarea 2: Ejecuta las consultas SQL analíticas que calculan los 6 indicadores de gestión
    y los inserta de forma idempotente en mart.kpi_detalle_comunal_diario y
    mart.kpi_resumen_regional_diario.
    """
    t_inicio = time.time()
    ti = context["ti"]
    fecha_id = ti.xcom_pull(task_ids="verificar_datos_base_dw")
    ds = context["ds"]
    run_id = context["run_id"]

    with open(SQL_KPI_PATH, "r", encoding="utf-8") as f:
        sql_script = f.read()

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    with conn.cursor() as cur:
        cur.execute(sql_script, {"fecha_id": fecha_id})
        cur.execute(
            "SELECT COUNT(*) FROM mart.kpi_resumen_regional_diario WHERE fecha_id = %s;",
            (fecha_id,),
        )
        regiones_calculadas = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM mart.kpi_detalle_comunal_diario WHERE fecha_id = %s;",
            (fecha_id,),
        )
        comunas_calculadas = cur.fetchone()[0]
    conn.commit()

    duracion = time.time() - t_inicio
    registrar_auditoria_etl(
        postgres_conn_id=POSTGRES_CONN_ID,
        dag_id=context["dag"].dag_id,
        task_id=context["task"].task_id,
        run_id=run_id,
        execution_date=ds,
        fuente_datos="CALCULO_SQL_MART_KPIS",
        filas_leidas=comunas_calculadas,
        filas_cargadas=regiones_calculadas + comunas_calculadas,
        porcentaje_nulos=0.0,
        duracion_segundos=duracion,
        estado="SUCCESS",
    )


default_args = {
    "owner": "equipo_analitica_ucm",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
    "on_failure_callback": callback_falla_tarea,
}

with DAG(
    dag_id="dag_03_kpi_indicadores_mart",
    default_args=default_args,
    description="Cálculo SQL de Indicadores de Gestión sobre el esquema DW hacia MART",
    schedule_interval="30 6 * * *",  # 06:30 AM (o disparado por dag_02)
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["inf522", "kpis", "mart", "sql"],
) as dag:

    t_verificar_dw = PythonOperator(
        task_id="verificar_datos_base_dw",
        python_callable=verificar_datos_base_dw,
    )

    t_calcular_kpis = PythonOperator(
        task_id="calcular_kpis_en_mart",
        python_callable=calcular_kpis_en_mart,
    )

    t_verificar_dw >> t_calcular_kpis