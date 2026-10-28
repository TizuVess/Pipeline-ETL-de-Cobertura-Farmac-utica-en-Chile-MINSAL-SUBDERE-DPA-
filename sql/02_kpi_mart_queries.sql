-- =============================================================================
-- CÁLCULO IDEMPOTENTE DE LOS 6 INDICADORES DE GESTIÓN EN ESQUEMA MART
-- =============================================================================

-- 1. Carga Idempotente del Detalle Comunal (Cruce catálogo completo DPA vs Operación MINSAL)
INSERT INTO mart.kpi_detalle_comunal_diario (
    fecha_id,
    codigo_comuna_cut,
    nombre_comuna_oficial,
    nombre_region,
    total_farmacias,
    farmacias_de_turno,
    farmacias_24h,
    promedio_horas_apertura,
    es_desierto_farmaceutico,
    cumple_meta_turno,
    calculated_at
)
SELECT
    %(fecha_id)s::INTEGER AS fecha_id,
    dt.codigo_comuna_cut,
    dt.nombre_comuna_oficial,
    dt.nombre_region,
    COUNT(f.operacion_id) AS total_farmacias,
    COUNT(f.operacion_id) FILTER (WHERE f.es_turno = TRUE) AS farmacias_de_turno,
    COUNT(f.operacion_id) FILTER (WHERE f.es_apertura_24h = TRUE) AS farmacias_24h,
    COALESCE(ROUND(AVG(f.horas_operacion), 2), 0.00) AS promedio_horas_apertura,
    CASE WHEN COUNT(f.operacion_id) = 0 THEN TRUE ELSE FALSE END AS es_desierto_farmaceutico,
    CASE WHEN COUNT(f.operacion_id) FILTER (WHERE f.es_turno = TRUE) >= 1 THEN TRUE ELSE FALSE END AS cumple_meta_turno,
    CURRENT_TIMESTAMP
FROM dw.dim_territorio dt
LEFT JOIN dw.fact_operacion_farmacia f
    ON dt.territorio_sk = f.territorio_sk
    AND f.fecha_id = %(fecha_id)s::INTEGER
WHERE dt.territorio_sk <> -1
GROUP BY
    dt.codigo_comuna_cut,
    dt.nombre_comuna_oficial,
    dt.nombre_region
ON CONFLICT (fecha_id, codigo_comuna_cut) DO UPDATE SET
    total_farmacias = EXCLUDED.total_farmacias,
    farmacias_de_turno = EXCLUDED.farmacias_de_turno,
    farmacias_24h = EXCLUDED.farmacias_24h,
    promedio_horas_apertura = EXCLUDED.promedio_horas_apertura,
    es_desierto_farmaceutico = EXCLUDED.es_desierto_farmaceutico,
    cumple_meta_turno = EXCLUDED.cumple_meta_turno,
    calculated_at = CURRENT_TIMESTAMP;

-- 2. Carga Idempotente de los 6 KPIs Consolidados por Región
WITH base_comunal AS (
    SELECT
        dt.codigo_region,
        dt.nombre_region,
        dt.territorio_sk,
        COUNT(f.operacion_id) AS cant_farmacias,
        COUNT(f.operacion_id) FILTER (WHERE f.es_turno = TRUE) AS cant_turnos,
        COUNT(f.operacion_id) FILTER (WHERE f.es_apertura_24h = TRUE) AS cant_24h,
        AVG(f.horas_operacion) AS avg_horas,
        COUNT(f.operacion_id) FILTER (
            WHERE de.cadena_farmaceutica IN ('CRUZ VERDE', 'SALCOBRAND', 'FARMACIAS AHUMADA', 'DR. SIMI')
        ) AS cant_cadenas,
        COUNT(f.operacion_id) FILTER (WHERE de.georreferencia_valida = TRUE) AS cant_gps_valido
    FROM dw.dim_territorio dt
    LEFT JOIN dw.fact_operacion_farmacia f
        ON dt.territorio_sk = f.territorio_sk
        AND f.fecha_id = %(fecha_id)s::INTEGER
    LEFT JOIN dw.dim_establecimiento de
        ON f.establecimiento_sk = de.establecimiento_sk
    WHERE dt.territorio_sk <> -1
    GROUP BY dt.codigo_region, dt.nombre_region, dt.territorio_sk
)
INSERT INTO mart.kpi_resumen_regional_diario (
    fecha_id,
    codigo_region,
    nombre_region,
    total_comunas_dpa,
    comunas_con_farmacia,
    comunas_desierto_farmaceutico,
    comunas_con_turno_activo,
    kpi1_cobertura_comunal_turno_pct,
    kpi2_desierto_farmaceutico_pct,
    total_farmacias_activas,
    total_farmacias_turno,
    kpi3_proporcion_turno_sobre_catalogo_pct,
    kpi4_promedio_horas_atencion,
    kpi4_farmacias_24h_pct,
    kpi5_participacion_cadenas_pct,
    kpi6_integridad_geografica_gps_pct,
    calculated_at
)
SELECT
    %(fecha_id)s::INTEGER AS fecha_id,
    codigo_region,
    nombre_region,
    COUNT(territorio_sk) AS total_comunas_dpa,
    COUNT(territorio_sk) FILTER (WHERE cant_farmacias > 0) AS comunas_con_farmacia,
    COUNT(territorio_sk) FILTER (WHERE cant_farmacias = 0) AS comunas_desierto_farmaceutico,
    COUNT(territorio_sk) FILTER (WHERE cant_turnos > 0) AS comunas_con_turno_activo,
    -- KPI 1: % de Comunas de la región con al menos 1 farmacia de turno activa
    ROUND((COUNT(territorio_sk) FILTER (WHERE cant_turnos > 0)::NUMERIC / NULLIF(COUNT(territorio_sk), 0)) * 100, 2) AS kpi1_cobertura_comunal_turno_pct,
    -- KPI 2: % de Comunas en condición de Desierto Farmacéutico (0 farmacias registradas)
    ROUND((COUNT(territorio_sk) FILTER (WHERE cant_farmacias = 0)::NUMERIC / NULLIF(COUNT(territorio_sk), 0)) * 100, 2) AS kpi2_desierto_farmaceutico_pct,
    SUM(cant_farmacias) AS total_farmacias_activas,
    SUM(cant_turnos) AS total_farmacias_turno,
    -- KPI 3: Proporción de Farmacias de Turno respecto al Parque Total Regional
    COALESCE(ROUND((SUM(cant_turnos)::NUMERIC / NULLIF(SUM(cant_farmacias), 0)) * 100, 2), 0.00) AS kpi3_proporcion_turno_sobre_catalogo_pct,
    -- KPI 4: Promedio de Horas Diarias de Atención y % de Farmacias 24 Horas
    COALESCE(ROUND(AVG(avg_horas), 2), 0.00) AS kpi4_promedio_horas_atencion,
    COALESCE(ROUND((SUM(cant_24h)::NUMERIC / NULLIF(SUM(cant_farmacias), 0)) * 100, 2), 0.00) AS kpi4_farmacias_24h_pct,
    -- KPI 5: Índice de Concentración en Grandes Cadenas Farmacéuticas
    COALESCE(ROUND((SUM(cant_cadenas)::NUMERIC / NULLIF(SUM(cant_farmacias), 0)) * 100, 2), 0.00) AS kpi5_participacion_cadenas_pct,
    -- KPI 6: Tasa de Integridad Geográfica (Coordenadas GPS válidas en territorio nacional)
    COALESCE(ROUND((SUM(cant_gps_valido)::NUMERIC / NULLIF(SUM(cant_farmacias), 0)) * 100, 2), 0.00) AS kpi6_integridad_geografica_gps_pct,
    CURRENT_TIMESTAMP
FROM base_comunal
GROUP BY codigo_region, nombre_region
ON CONFLICT (fecha_id, codigo_region) DO UPDATE SET
    total_comunas_dpa = EXCLUDED.total_comunas_dpa,
    comunas_con_farmacia = EXCLUDED.comunas_con_farmacia,
    comunas_desierto_farmaceutico = EXCLUDED.comunas_desierto_farmaceutico,
    comunas_con_turno_activo = EXCLUDED.comunas_con_turno_activo,
    kpi1_cobertura_comunal_turno_pct = EXCLUDED.kpi1_cobertura_comunal_turno_pct,
    kpi2_desierto_farmaceutico_pct = EXCLUDED.kpi2_desierto_farmaceutico_pct,
    total_farmacias_activas = EXCLUDED.total_farmacias_activas,
    total_farmacias_turno = EXCLUDED.total_farmacias_turno,
    kpi3_proporcion_turno_sobre_catalogo_pct = EXCLUDED.kpi3_proporcion_turno_sobre_catalogo_pct,
    kpi4_promedio_horas_atencion = EXCLUDED.kpi4_promedio_horas_atencion,
    kpi4_farmacias_24h_pct = EXCLUDED.kpi4_farmacias_24h_pct,
    kpi5_participacion_cadenas_pct = EXCLUDED.kpi5_participacion_cadenas_pct,
    kpi6_integridad_geografica_gps_pct = EXCLUDED.kpi6_integridad_geografica_gps_pct,
    calculated_at = CURRENT_TIMESTAMP;