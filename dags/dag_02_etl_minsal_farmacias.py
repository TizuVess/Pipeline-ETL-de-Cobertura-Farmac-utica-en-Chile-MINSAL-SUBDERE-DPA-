"""
DAG 2: Extracción, Transformación y Carga de Locales y Farmacias de Turno desde MINSAL
(midas.minsal.cl). Incluye interruptor de Falla Controlada mediante Airflow Variable.
"""
import json
import time
from datetime import datetime, timedelta
from typing import Any, Dict

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

from etl_utils import (
    calcular_horas_operacion,
    callback_falla_tarea,
    clasificar_cadena_farmaceutica,
    consumir_api_paginada,
    crear_sesion_http,
    normalizar_texto_comuna,
    parsear_coordenada_chile,
    registrar_auditoria_etl,
)

POSTGRES_CONN_ID = "postgres_dw_conn"
DEFAULT_MINSAL_BASE_URL = "https://midas.minsal.cl/farmacia_v2/WS"


def validar_api_minsal(**context: Any) -> None:
    """
    Tarea 1: Comprueba disponibilidad de la API MINSAL antes de extraer.
    Permite simular una falla controlada cambiando la Variable 'SIMULAR_FALLA_MINSAL' a 'true'.
    """
    simular_falla = Variable.get("SIMULAR_FALLA_MINSAL", default_var="false").lower() == "true"
    base_url = Variable.get("MINSAL_API_BASE_URL", default_var=DEFAULT_MINSAL_BASE_URL)

    if simular_falla:
        url_erronea = f"{base_url}/endpoint_inexistente_falla_controlada_500.php"
        session = crear_sesion_http(max_retries=1, backoff_factor=0.5)
        resp = session.get(url_erronea, timeout=10)
        resp.raise_for_status()
        raise AirflowException(f"Falla controlada forzada en URL: {url_erronea}")

    session = crear_sesion_http(max_retries=2, backoff_factor=1.0)
    resp = session.get(f"{base_url}/getLocalesTurnos.php", timeout=20)
    resp.raise_for_status()


def extraer_raw_minsal(**context: Any) -> Dict[str, int]:
    """Tarea 2: Extrae el catálogo completo de farmacias y el listado de farmacias de turno."""
    base_url = Variable.get("MINSAL_API_BASE_URL", default_var=DEFAULT_MINSAL_BASE_URL)
    ds = context["ds"]
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    endpoints = {
        "locales_catalogo": f"{base_url}/getLocales.php",
        "locales_turnos": f"{base_url}/getLocalesTurnos.php",
    }

    resumen: Dict[str, int] = {}
    for tipo, url in endpoints.items():
        datos, status = consumir_api_paginada(url=url, timeout=45)
        resumen[tipo] = len(datos)
        hook.run(
            """
            INSERT INTO raw.raw_minsal_farmacias (
                tipo_endpoint, endpoint_url, fecha_proceso, http_status, payload
            ) VALUES (%s, %s, %s, %s, %s::jsonb);
            """,
            parameters=(tipo, url, ds, status, json.dumps(datos)),
        )
    return resumen


def transformar_y_cargar_dw_minsal(**context: Any) -> None:
    """
    Tarea 3: Depura datos crudos de MINSAL, homologa comunas con el código CUT de SUBDERE,
    valida coordenadas GPS, calcula horas de operación y carga idempotentemente
    staging y el esquema dimensional dw.
    """
    t_inicio = time.time()
    ds = context["ds"]
    run_id = context["run_id"]
    fecha_dt = datetime.strptime(ds, "%Y-%m-%d").date()
    fecha_id = int(fecha_dt.strftime("%Y%m%d"))

    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()

    with conn.cursor() as cur:
        # 1. Obtener diccionario de homologación comunal desde staging.stg_territorio_dpa
        cur.execute("""
            SELECT nombre_comuna_norm, codigo_comuna_cut, territorio_sk
            FROM dw.dim_territorio
            WHERE territorio_sk <> -1;
        """)
        mapa_cut = {fila[0]: (fila[1], fila[2]) for fila in cur.fetchall()}

        # 2. Leer últimos payloads de la fecha de proceso desde raw
        def leer_payload_minsal(tipo: str):
            cur.execute(
                """
                SELECT payload FROM raw.raw_minsal_farmacias
                WHERE tipo_endpoint = %s AND fecha_proceso = %s
                ORDER BY extracted_at DESC LIMIT 1;
                """,
                (tipo, ds),
            )
            row = cur.fetchone()
            return row[0] if row else []

        catalogo_raw = leer_payload_minsal("locales_catalogo")
        turnos_raw = leer_payload_minsal("locales_turnos")

        ids_en_turno = {
            str(item.get("local_id", "")).strip()
            for item in turnos_raw
            if item.get("local_id")
        }

        # Combinar registros priorizando locales únicos por local_id
        locales_por_id: Dict[str, Dict[str, Any]] = {}
        for item in catalogo_raw + turnos_raw:
            lid = str(item.get("local_id", "")).strip()
            if lid:
                locales_por_id[lid] = item

        filas_stg = []
        nulos_detectados = 0

        for lid, loc in locales_por_id.items():
            nombre_local = str(loc.get("local_nombre", "")).strip().upper() or "FARMACIA SIN NOMBRE"
            cadena = clasificar_cadena_farmaceutica(nombre_local)
            direccion = str(loc.get("local_direccion", "")).strip().upper() or None
            telefono = str(loc.get("local_telefono", "")).strip() or None
            fk_reg = str(loc.get("fk_region", "")).strip() or None
            fk_com = str(loc.get("fk_comuna", "")).strip() or None
            comuna_raw = str(loc.get("comuna_nombre", "")).strip()
            comuna_norm = normalizar_texto_comuna(comuna_raw)

            cod_cut, _ = mapa_cut.get(comuna_norm, (None, -1))
            lat, lng, gps_valido = parsear_coordenada_chile(
                loc.get("local_lat"), loc.get("local_lng")
            )
            h_ap, h_ci, hrs_op, es_24h = calcular_horas_operacion(
                loc.get("funcionamiento_hora_apertura"),
                loc.get("funcionamiento_hora_cierre"),
            )
            es_turno = lid in ids_en_turno

            if not gps_valido or not cod_cut or not direccion:
                nulos_detectados += 1

            filas_stg.append((
                ds,
                lid,
                nombre_local,
                cadena,
                direccion,
                telefono,
                fk_reg,
                fk_com,
                comuna_raw,
                comuna_norm,
                cod_cut,
                lat,
                lng,
                gps_valido,
                h_ap,
                h_ci,
                hrs_op,
                es_24h,
                es_turno,
            ))

        # 3. Carga Idempotente en staging.stg_minsal_farmacias
        sql_stg = """
            INSERT INTO staging.stg_minsal_farmacias (
                fecha_registro, local_id, nombre_local, cadena_farmaceutica,
                direccion, telefono, fk_region_minsal, fk_comuna_minsal,
                nombre_comuna_raw, nombre_comuna_norm, codigo_comuna_cut,
                latitud, longitud, georreferencia_valida,
                hora_apertura, hora_cierre, horas_operacion, es_apertura_24h, es_turno, updated_at
            ) VALUES %s
            ON CONFLICT (fecha_registro, local_id) DO UPDATE SET
                nombre_local = EXCLUDED.nombre_local,
                cadena_farmaceutica = EXCLUDED.cadena_farmaceutica,
                direccion = EXCLUDED.direccion,
                telefono = EXCLUDED.telefono,
                nombre_comuna_norm = EXCLUDED.nombre_comuna_norm,
                codigo_comuna_cut = EXCLUDED.codigo_comuna_cut,
                latitud = EXCLUDED.latitud,
                longitud = EXCLUDED.longitud,
                georreferencia_valida = EXCLUDED.georreferencia_valida,
                hora_apertura = EXCLUDED.hora_apertura,
                hora_cierre = EXCLUDED.hora_cierre,
                horas_operacion = EXCLUDED.horas_operacion,
                es_apertura_24h = EXCLUDED.es_apertura_24h,
                es_turno = EXCLUDED.es_turno,
                updated_at = CURRENT_TIMESTAMP;
        """
        execute_values(cur, sql_stg, filas_stg)

        # 4. Carga Idempotente de dw.dim_tiempo
        nombres_mes = [
            "", "ENERO", "FEBRERO", "MARZO", "ABRIL", "MAYO", "JUNIO",
            "JULIO", "AGOSTO", "SEPTIEMBRE", "OCTUBRE", "NOVIEMBRE", "DICIEMBRE"
        ]
        nombres_dia = [
            "LUNES", "MARTES", "MIERCOLES", "JUEVES", "VIERNES", "SABADO", "DOMINGO"
        ]
        dia_sem = fecha_dt.isoweekday()
        trimestre = (fecha_dt.month - 1) // 3 + 1

        cur.execute(
            """
            INSERT INTO dw.dim_tiempo (
                fecha_id, fecha, anio, mes, nombre_mes, dia,
                trimestre, dia_semana, nombre_dia, es_fin_de_semana
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (fecha_id) DO NOTHING;
            """,
            (
                fecha_id,
                fecha_dt,
                fecha_dt.year,
                fecha_dt.month,
                nombres_mes[fecha_dt.month],
                fecha_dt.day,
                trimestre,
                dia_sem,
                nombres_dia[dia_sem - 1],
                dia_sem in (6, 7),
            ),
        )

        # 5. Carga Idempotente de dw.dim_establecimiento
        cur.execute(
            """
            INSERT INTO dw.dim_establecimiento (
                local_id, nombre_local, cadena_farmaceutica, direccion,
                telefono, latitud, longitud, georreferencia_valida, updated_at
            )
            SELECT
                local_id, nombre_local, cadena_farmaceutica, direccion,
                telefono, latitud, longitud, georreferencia_valida, CURRENT_TIMESTAMP
            FROM staging.stg_minsal_farmacias
            WHERE fecha_registro = %s
            ON CONFLICT (local_id) DO UPDATE SET
                nombre_local = EXCLUDED.nombre_local,
                cadena_farmaceutica = EXCLUDED.cadena_farmaceutica,
                direccion = EXCLUDED.direccion,
                telefono = EXCLUDED.telefono,
                latitud = EXCLUDED.latitud,
                longitud = EXCLUDED.longitud,
                georreferencia_valida = EXCLUDED.georreferencia_valida,
                updated_at = CURRENT_TIMESTAMP;
            """,
            (ds,),
        )

        # 6. Carga Idempotente de la Tabla de Hechos (dw.fact_operacion_farmacia)
        cur.execute(
            """
            INSERT INTO dw.fact_operacion_farmacia (
                fecha_id, establecimiento_sk, territorio_sk,
                es_turno, hora_apertura, hora_cierre,
                horas_operacion, es_apertura_24h, created_at
            )
            SELECT
                %s AS fecha_id,
                de.establecimiento_sk,
                COALESCE(dt.territorio_sk, -1) AS territorio_sk,
                stg.es_turno,
                stg.hora_apertura,
                stg.hora_cierre,
                COALESCE(stg.horas_operacion, 0.00),
                stg.es_apertura_24h,
                CURRENT_TIMESTAMP
            FROM staging.stg_minsal_farmacias stg
            INNER JOIN dw.dim_establecimiento de
                ON stg.local_id = de.local_id
            LEFT JOIN dw.dim_territorio dt
                ON stg.codigo_comuna_cut = dt.codigo_comuna_cut
            WHERE stg.fecha_registro = %s
            ON CONFLICT (fecha_id, establecimiento_sk) DO UPDATE SET
                territorio_sk = EXCLUDED.territorio_sk,
                es_turno = EXCLUDED.es_turno,
                hora_apertura = EXCLUDED.hora_apertura,
                hora_cierre = EXCLUDED.hora_cierre,
                horas_operacion = EXCLUDED.horas_operacion,
                es_apertura_24h = EXCLUDED.es_apertura_24h;
            """,
            (fecha_id, ds),
        )

    conn.commit()

    total_leidos = len(catalogo_raw) + len(turnos_raw)
    total_cargados = len(filas_stg)
    pct_nulos = (nulos_detectados / total_cargados * 100.0) if total_cargados > 0 else 0.0
    duracion = time.time() - t_inicio

    registrar_auditoria_etl(
        postgres_conn_id=POSTGRES_CONN_ID,
        dag_id=context["dag"].dag_id,
        task_id=context["task"].task_id,
        run_id=run_id,
        execution_date=ds,
        fuente_datos="API_MINSAL_FARMACIAS_Y_TURNOS",
        filas_leidas=total_leidos,
        filas_cargadas=total_cargados,
        porcentaje_nulos=pct_nulos,
        duracion_segundos=duracion,
        estado="SUCCESS",
    )


default_args = {
    "owner": "equipo_analitica_ucm",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=15),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=15),
    "on_failure_callback": callback_falla_tarea,
}

with DAG(
    dag_id="dag_02_etl_minsal_farmacias",
    default_args=default_args,
    description="ETL Diario para Catálogo y Turnos de Farmacias MINSAL",
    schedule_interval="0 6 * * *",  # Todos los días a las 06:00 AM
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    tags=["inf522", "minsal", "farmacias", "hechos"],
) as dag:

    t_validar = PythonOperator(
        task_id="validar_api_minsal",
        python_callable=validar_api_minsal,
    )

    t_extraer = PythonOperator(
        task_id="extraer_raw_minsal",
        python_callable=extraer_raw_minsal,
    )

    t_transformar_cargar = PythonOperator(
        task_id="transformar_y_cargar_dw_minsal",
        python_callable=transformar_y_cargar_dw_minsal,
    )

    t_disparar_kpis = TriggerDagRunOperator(
        task_id="disparar_dag_indicadores_mart",
        trigger_dag_id="dag_03_kpi_indicadores_mart",
        conf={"fecha_proceso": "{{ ds }}"},
        wait_for_completion=False,
    )

    t_validar >> t_extraer >> t_transformar_cargar >> t_disparar_kpis