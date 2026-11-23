-- ============================================================
-- CARGA IDEMPOTENTE: staging (stg_*) -> modelo final
-- Ejecutado por etl/transformacion_entrega2.py dentro de UNA transacción.
-- Usa UPSERT (ON CONFLICT DO UPDATE) para preservar ids y evitar duplicados.
-- ============================================================

-- 1. Código DANE y coordenadas en dim_municipio
UPDATE dim_municipio m
SET codigo_dane = s.codigo_dane,
    latitud     = s.latitud,
    longitud    = s.longitud
FROM dim_departamento d, stg_codigo_dane s
WHERE m.id_departamento = d.id_departamento
  AND d.nombre_departamento = s.nombre_departamento
  AND m.nombre_municipio = s.nombre_municipio;

-- 2. Áreas de Bucaramanga que usan la API o la ECV y que SIVIGILA no trae
INSERT INTO dim_municipio (id_departamento, nombre_municipio, area, codigo_dane, latitud, longitud)
SELECT b.id_departamento, b.nombre_municipio, a.area, b.codigo_dane, b.latitud, b.longitud
FROM (SELECT area FROM stg_hechos_api UNION SELECT area FROM stg_hechos_analisis) a
CROSS JOIN (SELECT id_departamento, nombre_municipio, codigo_dane, latitud, longitud
            FROM dim_municipio WHERE codigo_dane = '68001' ORDER BY id_municipio LIMIT 1) b
ON CONFLICT ON CONSTRAINT uq_municipio DO NOTHING;

-- 3. Meses que usan la API y el OSEC (semana 0 = la fuente no trae semana)
INSERT INTO dim_tiempo (year, mes, semana)
SELECT anio, mes, 0 FROM stg_hechos_api
UNION
SELECT anio, mes, 0 FROM stg_hechos_osec
ON CONFLICT ON CONSTRAINT uq_tiempo DO NOTHING;

-- 4. API Medicina Legal (UPSERT idempotente por id_caso_api, conserva id_hecho_api)
INSERT INTO hechos_api_violencia (
    id_caso_api, id_municipio, id_tiempo, ciclo_vida, grupo_agresor, presunto_agresor,
    sexo_agresor, tipo_violencia, contexto, escenario, turno, dias_incapacidad
)
SELECT s.id_caso_api, m.id_municipio, t.id_tiempo, s.ciclo_vida, s.grupo_agresor, s.presunto_agresor,
       s.sexo_agresor, s.tipo_violencia, s.contexto, s.escenario, s.turno, s.dias_incapacidad
FROM stg_hechos_api s
JOIN dim_municipio m ON m.codigo_dane = '68001' AND m.area = s.area
JOIN dim_tiempo t    ON t.year = s.anio AND t.mes = s.mes AND t.semana = 0
ON CONFLICT (id_caso_api) DO UPDATE SET
    id_municipio     = EXCLUDED.id_municipio,
    id_tiempo        = EXCLUDED.id_tiempo,
    ciclo_vida       = EXCLUDED.ciclo_vida,
    grupo_agresor    = EXCLUDED.grupo_agresor,
    presunto_agresor = EXCLUDED.presunto_agresor,
    sexo_agresor     = EXCLUDED.sexo_agresor,
    tipo_violencia   = EXCLUDED.tipo_violencia,
    contexto         = EXCLUDED.contexto,
    escenario        = EXCLUDED.escenario,
    turno            = EXCLUDED.turno,
    dias_incapacidad = EXCLUDED.dias_incapacidad;

-- 5. Boletines OSEC (UPSERT idempotente por comuna, tiempo y delito)
INSERT INTO hechos_osec_comuna (id_comuna, id_tiempo, delito, casos, publicado_en)
SELECT s.id_comuna, t.id_tiempo, s.delito, s.casos, s.publicado_en
FROM stg_hechos_osec s
JOIN dim_tiempo t ON t.year = s.anio AND t.mes = s.mes AND t.semana = 0
ON CONFLICT ON CONSTRAINT uq_osec DO UPDATE SET
    casos        = EXCLUDED.casos,
    publicado_en = EXCLUDED.publicado_en;

-- 6. Vivienda ECV 2024 (reemplazo controlado por municipio Bucaramanga)
DELETE FROM hechos_analisis
WHERE id_municipio IN (SELECT id_municipio FROM dim_municipio WHERE codigo_dane = '68001');

INSERT INTO hechos_analisis (id_departamento, id_municipio, zona_vivienda, tipo_vivienda, tiene_energia_electrica, id_estrato)
SELECT m.id_departamento, m.id_municipio, s.zona_vivienda, s.tipo_vivienda, s.tiene_energia_electrica, s.id_estrato
FROM stg_hechos_analisis s
JOIN dim_municipio m ON m.codigo_dane = '68001' AND m.area = s.area;

-- 7. Conjunto unificado (UPSERT idempotente por anio y mes)
INSERT INTO dataset_unificado (
    anio, mes, fecha_mes, siv_casos_total, siv_casos_mujeres, siv_mujeres_agresor_pareja,
    siv_mujeres_agresor_familiar, siv_mujeres_turno_noche, siv_mujeres_fallecidas,
    api_casos_mujeres, api_mujeres_incapacidad_grave, osec_denuncias_vif, osec_denuncias_sexuales,
    ecv_viviendas_bucaramanga, ecv_pct_estrato_1_2, ecv_pct_estrato_5_6, fuentes_con_dato
)
SELECT
    anio, mes, fecha_mes, siv_casos_total, siv_casos_mujeres, siv_mujeres_agresor_pareja,
    siv_mujeres_agresor_familiar, siv_mujeres_turno_noche, siv_mujeres_fallecidas,
    api_casos_mujeres, api_mujeres_incapacidad_grave, osec_denuncias_vif, osec_denuncias_sexuales,
    ecv_viviendas_bucaramanga, ecv_pct_estrato_1_2, ecv_pct_estrato_5_6, fuentes_con_dato
FROM stg_dataset_unificado
ON CONFLICT (anio, mes) DO UPDATE SET
    fecha_mes                     = EXCLUDED.fecha_mes,
    siv_casos_total               = EXCLUDED.siv_casos_total,
    siv_casos_mujeres             = EXCLUDED.siv_casos_mujeres,
    siv_mujeres_agresor_pareja    = EXCLUDED.siv_mujeres_agresor_pareja,
    siv_mujeres_agresor_familiar  = EXCLUDED.siv_mujeres_agresor_familiar,
    siv_mujeres_turno_noche       = EXCLUDED.siv_mujeres_turno_noche,
    siv_mujeres_fallecidas        = EXCLUDED.siv_mujeres_fallecidas,
    api_casos_mujeres             = EXCLUDED.api_casos_mujeres,
    api_mujeres_incapacidad_grave = EXCLUDED.api_mujeres_incapacidad_grave,
    osec_denuncias_vif            = EXCLUDED.osec_denuncias_vif,
    osec_denuncias_sexuales       = EXCLUDED.osec_denuncias_sexuales,
    ecv_viviendas_bucaramanga     = EXCLUDED.ecv_viviendas_bucaramanga,
    ecv_pct_estrato_1_2           = EXCLUDED.ecv_pct_estrato_1_2,
    ecv_pct_estrato_5_6           = EXCLUDED.ecv_pct_estrato_5_6,
    fuentes_con_dato              = EXCLUDED.fuentes_con_dato;

-- 8. Control: verificar que al menos existen todos los registros de staging
DO $$
DECLARE
    faltan TEXT := '';
BEGIN
    IF (SELECT COUNT(*) FROM hechos_api_violencia) < (SELECT COUNT(*) FROM stg_hechos_api) THEN
        faltan := faltan || ' hechos_api_violencia';
    END IF;
    IF (SELECT COUNT(*) FROM hechos_osec_comuna) < (SELECT COUNT(*) FROM stg_hechos_osec) THEN
        faltan := faltan || ' hechos_osec_comuna';
    END IF;
    IF (SELECT COUNT(*) FROM hechos_analisis) < (SELECT COUNT(*) FROM stg_hechos_analisis) THEN
        faltan := faltan || ' hechos_analisis';
    END IF;
    IF (SELECT COUNT(*) FROM dataset_unificado) < (SELECT COUNT(*) FROM stg_dataset_unificado) THEN
        faltan := faltan || ' dataset_unificado';
    END IF;
    IF faltan <> '' THEN
        RAISE EXCEPTION USING MESSAGE = 'Carga incompleta (filas de staging sin municipio o tiempo):' || faltan;
    END IF;
END $$;

-- ------------------------------------------------------------
-- Garantizar creación/actualización de vista_ml_hechos_enriquecidos
-- sin imputaciones artificiales (NULL preservado para ML Pipeline)
-- ------------------------------------------------------------
DROP VIEW IF EXISTS vista_ml_hechos_enriquecidos;
CREATE OR REPLACE VIEW vista_ml_hechos_enriquecidos AS
WITH ecv_area AS (
    SELECT DISTINCT ON (ha.id_municipio)
           ha.id_municipio,
           ha.id_estrato   AS nivel_estrato,
           ha.zona_vivienda
    FROM hechos_analisis ha
    GROUP BY ha.id_municipio, ha.id_estrato, ha.zona_vivienda
    ORDER BY ha.id_municipio, COUNT(*) DESC
)
SELECT
    h.id_hecho_api,
    h.escenario,
    h.ciclo_vida,
    h.contexto,
    h.turno,
    h.dias_incapacidad,
    h.tipo_violencia,
    m.nombre_municipio,
    m.area,
    t.mes,
    t.year AS anio,
    du.osec_denuncias_sexuales,
    ecv.nivel_estrato,
    ecv.zona_vivienda,
    h.grupo_agresor,
    h.presunto_agresor
FROM hechos_api_violencia h
JOIN dim_municipio m ON m.id_municipio = h.id_municipio
JOIN dim_tiempo t    ON t.id_tiempo = h.id_tiempo
LEFT JOIN dataset_unificado du ON du.anio = t.year AND du.mes = t.mes
LEFT JOIN ecv_area ecv         ON ecv.id_municipio = h.id_municipio;
