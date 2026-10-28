"""
DAG 1: Extracción, Transformación y Carga de la División Político Administrativa (DPA)
desde la API pública de SUBDERE / Gobierno Digital (apis.digital.gob.cl).
"""
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from etl_utils import (
    callback_falla_tarea,
    consumir_api_paginada,
    crear_sesion_http,
    normalizar_texto_comuna,
    parsear_coordenada_chile,
    registrar_auditoria_etl,
)

POSTGRES_CONN_ID = "postgres_dw_conn"
DEFAULT_DPA_BASE_URL = "https://apis.digital.gob.cl/dpa"


def validar_api_subdere(**context: Any) -> None:
    """Tarea 1: Comprueba disponibilidad HTTP 200 de la API DPA antes de extraer."""
    base_url = Variable.get("SUBDERE_API_BASE_URL", default_var=DEFAULT_DPA_BASE_URL)
    session = crear_sesion_http(max_retries=2, backoff_factor=1.0)
    resp = session.get(f"{base_url}/regiones", timeout=15)
    resp.raise_for_status()


def extraer_raw_subdere(**context: Any) -> Dict[str, int]:
    """Tarea 2: Consume regiones, provincias y comunas y almacena el JSON crudo en raw."""
    t_inicio = time.time()
    base_url = Variable.get("SUBDERE_API_BASE_URL", default_var=DEFAULT_DPA_BASE_URL)
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    entidades = ["regiones", "provincias", "comunas"]
    conteo: Dict[str, int] = {}

    for entidad in entidades:
        url = f"{base_url}/{entidad}"
        datos, status_code = consumir_api_paginada(url=url, timeout=30)
        conteo[entidad] = len(datos)

        hook.run(
            """
            INSERT INTO raw.raw_subdere_dpa (tipo_entidad, endpoint_url, http_status, payload)
            VALUES (%s, %s, %s, %s::jsonb);
            """,
            parameters=(entidad, url, status_code, json.dumps(datos)),
        )

    conteo["duracion_extract"] = round(time.time() - t_inicio, 2)
    return conteo


def transformar_y_cargar_dw_territorio(**context: Any) -> None:
    """
    Tarea 3: Limpia y cruza jerárquicamente Regiones -> Provincias -> Comunas desde raw,
    carga staging.stg_territorio_dpa y actualiza idempotentemente dw.dim_territorio.
    """
    t_inicio = time.time()
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    ds = context["ds"]
    run_id = context["run_id"]

    with conn.cursor() as cur:
        def obtener_ultimo_payload(entidad: str):
            cur.execute(
                """
                SELECT payload FROM raw.raw_subdere_dpa
                WHERE tipo_entidad = %s
                ORDER BY extracted_at DESC LIMIT 1;
                """,
                (entidad,),
            )
            fila = cur.fetchone()
            return fila[0] if fila else []

        regiones = obtener_ultimo_payload("regiones")
        provincias = obtener_ultimo_payload("provincias")
        comunas = obtener_ultimo_payload("comunas")

        mapa_regiones = {
            str(r.get("codigo", "")).strip(): str(r.get("nombre", "")).strip().upper()
            for r in regiones
        }
        mapa_provincias = {
            str(p.get("codigo", "")).strip(): {
                "nombre": str(p.get("nombre", "")).strip().upper(),
                "codigo_region": str(p.get("codigo_padre", "")).strip(),
            }
            for p in provincias
        }

        registros_limpios = []
        nulos_coord = 0

        for c in comunas:
            cod_comuna = str(c.get("codigo", "")).strip()
            nom_comuna = str(c.get("nombre", "")).strip()
            cod_prov = str(c.get("codigo_padre", "")).strip()
            if not cod_comuna or not nom_comuna:
                continue

            info_prov = mapa_provincias.get(cod_prov, {})
            nom_prov = info_prov.get("nombre", "SIN PROVINCIA")
            cod_reg = info_prov.get("codigo_region", cod_comuna[:2])
            nom_reg = mapa_regiones.get(cod_reg, "SIN REGION")

            lat, lng, coord_ok = parsear_coordenada_chile(c.get("lat"), c.get("lng"))
            if not coord_ok:
                nulos_coord += 1

            registros_limpios.append((
                cod_comuna,
                nom_comuna.upper(),
                normalizar_texto_comuna(nom_comuna),
                cod_prov,
                nom_prov,
                cod_reg,
                nom_reg,
                lat,
                lng,
            ))

        # 1. Carga idempotente en STAGING
        sql_stg = """
            INSERT INTO staging.stg_territorio_dpa (
                codigo_comuna_cut, nombre_comuna_oficial, nombre_comuna_norm,
                codigo_provincia, nombre_provincia, codigo_region, nombre_region,
                latitud_comuna, longitud_comuna, updated_at
            ) VALUES %s
            ON CONFLICT (codigo_comuna_cut) DO UPDATE SET
                nombre_comuna_oficial = EXCLUDED.nombre_comuna_oficial,
                nombre_comuna_norm = EXCLUDED.nombre_comuna_norm,
                codigo_provincia = EXCLUDED.codigo_provincia,
                nombre_provincia = EXCLUDED.nombre_provincia,
                codigo_region = EXCLUDED.codigo_region,
                nombre_region = EXCLUDED.nombre_region,
                latitud_comuna = EXCLUDED.latitud_comuna,
                longitud_comuna = EXCLUDED.longitud_comuna,
                updated_at = CURRENT_TIMESTAMP;
        """
        execute_values(cur, sql_stg, registros_limpios)

        # 2. Carga idempotente hacia DW (dw.dim_territorio)
        cur.execute("""
            INSERT INTO dw.dim_territorio (
                codigo_comuna_cut, nombre_comuna_oficial, nombre_comuna_norm,
                codigo_provincia, nombre_provincia, codigo_region, nombre_region,
                latitud_comuna, longitud_comuna, updated_at
            )
            SELECT
                codigo_comuna_cut, nombre_comuna_oficial, nombre_comuna_norm,
                codigo_provincia, nombre_provincia, codigo_region, nombre_region,
                latitud_comuna, longitud_comuna, CURRENT_TIMESTAMP
            FROM staging.stg_territorio_dpa
            ON CONFLICT (codigo_comuna_cut) DO UPDATE SET
                nombre_comuna_oficial = EXCLUDED.nombre_comuna_oficial,
                nombre_comuna_norm = EXCLUDED.nombre_comuna_norm,
                codigo_provincia = EXCLUDED.codigo_provincia,
                nombre_provincia = EXCLUDED.nombre_provincia,
                codigo_region = EXCLUDED.codigo_region,
                nombre_region = EXCLUDED.nombre_region,
                latitud_comuna = EXCLUDED.latitud_comuna,
                longitud_comuna = EXCLUDED.longitud_comuna,
                updated_at = CURRENT_TIMESTAMP;
        """)
    conn.commit()

    total_leidas = len(comunas)
    total_cargadas = len(registros_limpios)
    pct_nulos = (nulos_coord / total_leidas * 100.0) if total_leidas > 0 else 0.0
    duracion = time.time() - t_inicio

    registrar_auditoria_etl(
        postgres_conn_id=POSTGRES_CONN_ID,
        dag_id=context["dag"].dag_id,
        task_id=context["task"].task_id,
        run_id=run_id,
        execution_date=ds,
        fuente_datos="API_SUBDERE_DPA_COMUNAS",
        filas_leidas=total_leidas,
        filas_cargadas=total_cargadas,
        porcentaje_nulos=pct_nulos,
        duracion_segundos=duracion,
        estado="SUCCESS",
    )


default_args = {
    "owner": "equipo_analitica_ucm",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=20),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=10),
    "on_failure_callback": callback_falla_tarea,
}

with DAG(
    dag_id="dag_01_etl_subdere_dpa",
    default_args=default_args,
    description="ETL Independiente para Catálogo Territorial DPA (SUBDERE)",
    schedule_interval="0 5 * * 1",  # Todos los lunes a las 05:00 AM
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["inf522", "subdere", "dpa", "dimension"],
) as dag:

    t_validar = PythonOperator(
        task_id="validar_api_subdere",
        python_callable=validar_api_subdere,
    )

    t_extraer = PythonOperator(
        task_id="extraer_raw_subdere",
        python_callable=extraer_raw_subdere,
    )

    t_transformar_cargar = PythonOperator(
        task_id="transformar_y_cargar_dw_territorio",
        python_callable=transformar_y_cargar_dw_territorio,
    )

    t_validar >> t_extraer >> t_transformar_cargar