-- DDL: ESQUEMAS RAW, STAGING, DW, MART Y AUDITORÍA ETL
-- =============================================================================

-- 1. Creación de Esquemas
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS dw;
CREATE SCHEMA IF NOT EXISTS mart;

-- =============================================================================
-- 2. TABLA DE AUDITORÍA Y CONTROL DE FALLAS (raw.etl_audit)
-- =============================================================================
CREATE TABLE IF NOT EXISTS raw.etl_audit (
    audit_id SERIAL PRIMARY KEY,
    dag_id VARCHAR(100) NOT NULL,
    task_id VARCHAR(100) NOT NULL,
    run_id VARCHAR(150) NOT NULL,
    execution_date DATE NOT NULL,
    fuente_datos VARCHAR(100) NOT NULL,
    filas_leidas INTEGER DEFAULT 0,
    filas_cargadas INTEGER DEFAULT 0,
    porcentaje_nulos NUMERIC(5,2) DEFAULT 0.00,
    duracion_segundos NUMERIC(10,2) DEFAULT 0.00,
    estado VARCHAR(30) NOT NULL, -- 'SUCCESS', 'FAILED', 'RETRY'
    mensaje_error TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- 3. CAPA RAW (Datos crudos tal como llegan de la API en formato JSONB)
-- =============================================================================
CREATE TABLE IF NOT EXISTS raw.raw_subdere_dpa (
    raw_id BIGSERIAL PRIMARY KEY,
    tipo_entidad VARCHAR(30) NOT NULL, -- 'regiones', 'provincias', 'comunas'
    endpoint_url TEXT NOT NULL,
    http_status INTEGER NOT NULL,
    payload JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS raw.raw_minsal_farmacias (
    raw_id BIGSERIAL PRIMARY KEY,
    tipo_endpoint VARCHAR(30) NOT NULL, -- 'locales_catalogo', 'locales_turnos'
    endpoint_url TEXT NOT NULL,
    fecha_proceso DATE NOT NULL,
    http_status INTEGER NOT NULL,
    payload JSONB NOT NULL,
    extracted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- =============================================================================
-- 4. CAPA STAGING (Datos limpios, tipados, normalizados y deduplicados)
-- =============================================================================
CREATE TABLE IF NOT EXISTS staging.stg_territorio_dpa (
    codigo_comuna_cut VARCHAR(10) PRIMARY KEY,
    nombre_comuna_oficial VARCHAR(120) NOT NULL,
    nombre_comuna_norm VARCHAR(120) NOT NULL,
    codigo_provincia VARCHAR(10) NOT NULL,
    nombre_provincia VARCHAR(120) NOT NULL,
    codigo_region VARCHAR(10) NOT NULL,
    nombre_region VARCHAR(150) NOT NULL,
    latitud_comuna NUMERIC(10,6),
    longitud_comuna NUMERIC(10,6),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stg_territorio_norm
    ON staging.stg_territorio_dpa(nombre_comuna_norm);

CREATE TABLE IF NOT EXISTS staging.stg_minsal_farmacias (
    fecha_registro DATE NOT NULL,
    local_id VARCHAR(50) NOT NULL,
    nombre_local VARCHAR(200) NOT NULL,
    cadena_farmaceutica VARCHAR(100) NOT NULL,
    direccion VARCHAR(250),
    telefono VARCHAR(80),
    fk_region_minsal VARCHAR(20),
    fk_comuna_minsal VARCHAR(20),
    nombre_comuna_raw VARCHAR(120),
    nombre_comuna_norm VARCHAR(120) NOT NULL,
    codigo_comuna_cut VARCHAR(10),
    latitud NUMERIC(10,6),
    longitud NUMERIC(10,6),
    georreferencia_valida BOOLEAN DEFAULT FALSE,
    hora_apertura TIME,
    hora_cierre TIME,
    horas_operacion NUMERIC(5,2),
    es_apertura_24h BOOLEAN DEFAULT FALSE,
    es_turno BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_registro, local_id)
);

-- =============================================================================
-- 5. CAPA DATA WAREHOUSE (dw - Modelo Dimensional en Estrella)
-- =============================================================================

-- Dimensión 1: Tiempo
CREATE TABLE IF NOT EXISTS dw.dim_tiempo (
    fecha_id INTEGER PRIMARY KEY, -- Formato YYYYMMDD
    fecha DATE NOT NULL UNIQUE,
    anio SMALLINT NOT NULL,
    mes SMALLINT NOT NULL,
    nombre_mes VARCHAR(20) NOT NULL,
    dia SMALLINT NOT NULL,
    trimestre SMALLINT NOT NULL,
    dia_semana SMALLINT NOT NULL, -- 1=Lunes ... 7=Domingo
    nombre_dia VARCHAR(20) NOT NULL,
    es_fin_de_semana BOOLEAN NOT NULL
);

-- Dimensión 2: Territorio (DPA SUBDERE)
CREATE TABLE IF NOT EXISTS dw.dim_territorio (
    territorio_sk SERIAL PRIMARY KEY,
    codigo_comuna_cut VARCHAR(10) NOT NULL UNIQUE,
    nombre_comuna_oficial VARCHAR(120) NOT NULL,
    nombre_comuna_norm VARCHAR(120) NOT NULL,
    codigo_provincia VARCHAR(10) NOT NULL,
    nombre_provincia VARCHAR(120) NOT NULL,
    codigo_region VARCHAR(10) NOT NULL,
    nombre_region VARCHAR(150) NOT NULL,
    latitud_comuna NUMERIC(10,6),
    longitud_comuna NUMERIC(10,6),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Registro por defecto para comunas no homologadas (Integridad Referencial)
INSERT INTO dw.dim_territorio (
    territorio_sk, codigo_comuna_cut, nombre_comuna_oficial, nombre_comuna_norm,
    codigo_provincia, nombre_provincia, codigo_region, nombre_region
) VALUES (
    -1, '00000', 'SIN CLASIFICAR', 'SIN CLASIFICAR',
    '000', 'SIN CLASIFICAR', '00', 'SIN CLASIFICAR'
) ON CONFLICT (codigo_comuna_cut) DO NOTHING;

-- Dimensión 3: Establecimiento Farmacéutico (MINSAL)
CREATE TABLE IF NOT EXISTS dw.dim_establecimiento (
    establecimiento_sk SERIAL PRIMARY KEY,
    local_id VARCHAR(50) NOT NULL UNIQUE,
    nombre_local VARCHAR(200) NOT NULL,
    cadena_farmaceutica VARCHAR(100) NOT NULL,
    direccion VARCHAR(250),
    telefono VARCHAR(80),
    latitud NUMERIC(10,6),
    longitud NUMERIC(10,6),
    georreferencia_valida BOOLEAN DEFAULT FALSE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de Hechos: Operación Diaria y Turnos de Farmacias
CREATE TABLE IF NOT EXISTS dw.fact_operacion_farmacia (
    operacion_id BIGSERIAL PRIMARY KEY,
    fecha_id INTEGER NOT NULL REFERENCES dw.dim_tiempo(fecha_id),
    establecimiento_sk INTEGER NOT NULL REFERENCES dw.dim_establecimiento(establecimiento_sk),
    territorio_sk INTEGER NOT NULL REFERENCES dw.dim_territorio(territorio_sk),
    es_turno BOOLEAN NOT NULL DEFAULT FALSE,
    hora_apertura TIME,
    hora_cierre TIME,
    horas_operacion NUMERIC(5,2) NOT NULL DEFAULT 0.00,
    es_apertura_24h BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_fact_fecha_establecimiento UNIQUE (fecha_id, establecimiento_sk)
);

-- =============================================================================
-- 6. CAPA DATA MART (mart - Tablas de Indicadores de Gestión / KPIs)
-- =============================================================================

-- Tabla consolidada de KPIs por Región y Fecha
CREATE TABLE IF NOT EXISTS mart.kpi_resumen_regional_diario (
    fecha_id INTEGER NOT NULL,
    codigo_region VARCHAR(10) NOT NULL,
    nombre_region VARCHAR(150) NOT NULL,
    total_comunas_dpa INTEGER NOT NULL,
    comunas_con_farmacia INTEGER NOT NULL,
    comunas_desierto_farmaceutico INTEGER NOT NULL,
    comunas_con_turno_activo INTEGER NOT NULL,
    kpi1_cobertura_comunal_turno_pct NUMERIC(6,2) NOT NULL,
    kpi2_desierto_farmaceutico_pct NUMERIC(6,2) NOT NULL,
    total_farmacias_activas INTEGER NOT NULL,
    total_farmacias_turno INTEGER NOT NULL,
    kpi3_proporcion_turno_sobre_catalogo_pct NUMERIC(6,2) NOT NULL,
    kpi4_promedio_horas_atencion NUMERIC(5,2) NOT NULL,
    kpi4_farmacias_24h_pct NUMERIC(6,2) NOT NULL,
    kpi5_participacion_cadenas_pct NUMERIC(6,2) NOT NULL,
    kpi6_integridad_geografica_gps_pct NUMERIC(6,2) NOT NULL,
    calculated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_id, codigo_region)
);

-- Tabla de Detalle Comunal para detección focalizada de brechas territoriales
CREATE TABLE IF NOT EXISTS mart.kpi_detalle_comunal_diario (
    fecha_id INTEGER NOT NULL,
    codigo_comuna_cut VARCHAR(10) NOT NULL,
    nombre_comuna_oficial VARCHAR(120) NOT NULL,
    nombre_region VARCHAR(150) NOT NULL,
    total_farmacias INTEGER NOT NULL,
    farmacias_de_turno INTEGER NOT NULL,
    farmacias_24h INTEGER NOT NULL,
    promedio_horas_apertura NUMERIC(5,2) NOT NULL,
    es_desierto_farmaceutico BOOLEAN NOT NULL,
    cumple_meta_turno BOOLEAN NOT NULL,
    calculated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (fecha_id, codigo_comuna_cut)
);