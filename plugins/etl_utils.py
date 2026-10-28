"""
Módulo transversal de utilidades ETL para consumo de APIs públicas de Chile,
normalización de datos, cálculo de métricas de calidad y auditoría en PostgreSQL.
"""
import logging
import re
import time
import unicodedata
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

# Diccionario de homologación de topónimos con diferencias históricas entre MINSAL y SUBDERE
HOMOLOGACION_COMUNAS: Dict[str, str] = {
    "LLAY LLAY": "LLAILLAY",
    "LLAY-LLAY": "LLAILLAY",
    "PAIHUANO": "PAIGUANO",
    "COYHAIQUE": "COIHAIQUE",
    "MARCHIGUE": "MARCHIHUE",
    "TREHUACO": "TREGUACO",
    "OHIGGINS": "O'HIGGINS",
    "SAN VICENTE DE TAGUA TAGUA": "SAN VICENTE",
    "CALERA": "LA CALERA",
    "TIL TIL": "TILTIL",
    "CABO DE HORNOS": "CABO DE HORNOS (EX - NAVARINO)",
}


def crear_sesion_http(max_retries: int = 3, backoff_factor: float = 1.0) -> requests.Session:
    """
    Crea una sesión HTTP robusta con User-Agent institucional, timeout y manejo de
    límites de tasa (HTTP 429) y errores de servidor (500, 502, 503, 504).
    """
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        read=max_retries,
        connect=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "UCM-INF522-DataEngineering-Pipeline/1.0 (Academic Project)",
        "Accept": "application/json",
    })
    return session


def consumir_api_paginada(
    url: str,
    params: Optional[Dict[str, Any]] = None,
    timeout: int = 30,
    page_size: Optional[int] = None,
    rate_limit_sleep: float = 0.2,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Consume una API pública manejando paginación opcional, pausas por límite de consultas
    y validación de estructura JSON.
    """
    session = crear_sesion_http()
    resultados_totales: List[Dict[str, Any]] = []
    params_actuales = dict(params) if params else {}
    pagina = 1
    ultimo_status = 200

    while True:
        if page_size:
            params_actuales["limit"] = page_size
            params_actuales["offset"] = (pagina - 1) * page_size

        logger.info("Consultando endpoint: %s | params=%s", url, params_actuales)
        response = session.get(url, params=params_actuales, timeout=timeout)
        ultimo_status = response.status_code
        response.raise_for_status()

        data = response.json()
        if isinstance(data, dict) and "data" in data:
            lote = data["data"]
        elif isinstance(data, list):
            lote = data
        else:
            lote = [data]

        resultados_totales.extend(lote)

        # Si la API no usa paginación o devolvió menos registros que el tamaño de página, terminamos
        if not page_size or len(lote) < page_size:
            break

        pagina += 1
        time.sleep(rate_limit_sleep)

    return resultados_totales, ultimo_status


def normalizar_texto_comuna(nombre: Optional[str]) -> str:
    """
    Limpia tildes, caracteres especiales, espacios múltiples y aplica el diccionario
    de homologación oficial entre MINSAL y SUBDERE DPA.
    """
    if not nombre or not str(nombre).strip():
        return "SIN CLASIFICAR"

    texto = str(nombre).upper().strip()
    # Preservar apóstrofe en O'HIGGINS antes de limpiar diacríticos
    texto = unicodedata.normalize("NFKD", texto)
    texto = "".join(c for c in texto if not unicodedata.combining(c))
    texto = re.sub(r"[^A-Z0-9\s\'\-\(\)]", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()

    return HOMOLOGACION_COMUNAS.get(texto, texto)


def clasificar_cadena_farmaceutica(nombre_local: Optional[str]) -> str:
    """
    Recodifica el nombre comercial del establecimiento en categorías analíticas
    para evaluar concentración de mercado vs. farmacias independientes/comunales.
    """
    if not nombre_local:
        return "INDEPENDIENTE / OTRA"
    nombre_up = str(nombre_local).upper().strip()

    if "CRUZ VERDE" in nombre_up:
        return "CRUZ VERDE"
    if "SALCOBRAND" in nombre_up:
        return "SALCOBRAND"
    if "AHUMADA" in nombre_up:
        return "FARMACIAS AHUMADA"
    if "SIMI" in nombre_up:
        return "DR. SIMI"
    if any(k in nombre_up for k in ["POPULAR", "COMUNAL", "MUNICIPAL", "SOLIDARIA"]):
        return "FARMACIA MUNICIPAL / POPULAR"
    return "INDEPENDIENTE / OTRA"


def parsear_coordenada_chile(lat_raw: Any, lng_raw: Any) -> Tuple[Optional[float], Optional[float], bool]:
    """
    Convierte coordenadas a float y valida si se encuentran dentro del bounding box
    geográfico de Chile (-56.5° a -17.4° Latitud; -109.5° a -66.0° Longitud).
    """
    try:
        if lat_raw in (None, "", "0", 0) or lng_raw in (None, "", "0", 0):
            return None, None, False
        lat_str = str(lat_raw).replace(",", ".").strip()
        lng_str = str(lng_raw).replace(",", ".").strip()
        lat = float(lat_str)
        lng = float(lng_str)

        es_valida = (-56.5 <= lat <= -17.4) and (-109.5 <= lng <= -66.0)
        if not es_valida:
            return None, None, False
        return round(lat, 6), round(lng, 6), True
    except (ValueError, TypeError):
        return None, None, False


def calcular_horas_operacion(
    hora_apertura_str: Any, hora_cierre_str: Any
) -> Tuple[Optional[str], Optional[str], float, bool]:
    """
    Normaliza horarios de apertura y cierre y calcula la duración continua de atención
    en horas decimales, detectando atención de 24 horas o cruces de medianoche.
    """
    fmt = "%H:%M:%S"
    try:
        ap_str = str(hora_apertura_str or "").strip()
        ci_str = str(hora_cierre_str or "").strip()
        if not ap_str or not ci_str:
            return None, None, 0.0, False

        if len(ap_str) == 5:
            ap_str += ":00"
        if len(ci_str) == 5:
            ci_str += ":00"

        t_ap = datetime.strptime(ap_str[:8], fmt)
        t_ci = datetime.strptime(ci_str[:8], fmt)

        segundos = (t_ci - t_ap).total_seconds()
        if segundos <= 0:
            segundos += 24 * 3600

        horas = round(segundos / 3600.0, 2)
        es_24h = horas >= 23.5
        return ap_str[:8], ci_str[:8], min(horas, 24.0), es_24h
    except ValueError:
        return None, None, 0.0, False


def registrar_auditoria_etl(
    postgres_conn_id: str,
    dag_id: str,
    task_id: str,
    run_id: str,
    execution_date: str,
    fuente_datos: str,
    filas_leidas: int,
    filas_cargadas: int,
    porcentaje_nulos: float,
    duracion_segundos: float,
    estado: str,
    mensaje_error: Optional[str] = None,
) -> None:
    """
    Inserta un registro de trazabilidad y métricas de calidad en raw.etl_audit.
    """
    hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    sql = """
        INSERT INTO raw.etl_audit (
            dag_id, task_id, run_id, execution_date, fuente_datos,
            filas_leidas, filas_cargadas, porcentaje_nulos,
            duracion_segundos, estado, mensaje_error
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    hook.run(
        sql,
        parameters=(
            dag_id,
            task_id,
            run_id,
            execution_date,
            fuente_datos,
            filas_leidas,
            filas_cargadas,
            round(porcentaje_nulos, 2),
            round(duracion_segundos, 2),
            estado,
            mensaje_error,
        ),
    )


def callback_falla_tarea(context: Dict[str, Any]) -> None:
    """
    Callback invocado automáticamente por Apache Airflow (on_failure_callback)
    cuando una tarea agota sus reintentos o falla, dejando evidencia en raw.etl_audit.
    """
    try:
        ti = context.get("task_instance")
        dag_id = context.get("dag").dag_id if context.get("dag") else "unknown_dag"
        task_id = ti.task_id if ti else "unknown_task"
        run_id = context.get("run_id", "manual_run")
        ds = context.get("ds", datetime.utcnow().strftime("%Y-%m-%d"))
        exception = str(context.get("exception", "Error no especificado en ejecución de tarea"))

        duracion = 0.0
        if ti and ti.start_date and ti.end_date:
            duracion = (ti.end_date - ti.start_date).total_seconds()

        registrar_auditoria_etl(
            postgres_conn_id="postgres_dw_conn",
            dag_id=dag_id,
            task_id=task_id,
            run_id=run_id,
            execution_date=ds,
            fuente_datos="AIRFLOW_ON_FAILURE_CALLBACK",
            filas_leidas=0,
            filas_cargadas=0,
            porcentaje_nulos=0.0,
            duracion_segundos=duracion,
            estado="FAILED",
            mensaje_error=exception[:1000],
        )
        logger.error("Falla registrada exitosamente en raw.etl_audit para %s.%s", dag_id, task_id)
    except Exception as exc:
        logger.error("No fue posible registrar la falla en raw.etl_audit: %s", exc)