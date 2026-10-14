-- ============================================================
-- CARGA: staging (stg_*) -> modelo final
-- Lo ejecuta etl/transformacion_entrega2.py (cargar_fuentes_nuevas) dentro
-- de UNA transacción: si cualquier paso falla, no se carga nada.
-- Se ejecuta SIEMPRE después de etl/transformacion.py, porque aquel vuelve a
-- crear dim_municipio y dim_tiempo (y vacía en cascada las tablas que las usan).
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

-- 4. Vaciar las tablas finales de las fuentes nuevas
TRUNCATE hechos_api_violencia, hechos_osec_comuna, hechos_analisis, dataset_unificado RESTART IDENTITY;

-- 5. API Medicina Legal
INSERT INTO hechos_api_violencia (id_caso_api, id_municipio, id_tiempo, ciclo_vida, grupo_agresor, presunto_agresor,
                                  sexo_agresor, tipo_violencia, contexto, escenario, turno, dias_incapacidad)
SELECT s.id_caso_api, m.id_municipio, t.id_tiempo, s.ciclo_vida, s.grupo_agresor, s.presunto_agresor,
       s.sexo_agresor, s.tipo_violencia, s.contexto, s.escenario, s.turno, s.dias_incapacidad
FROM stg_hechos_api s
JOIN dim_municipio m ON m.codigo_dane = '68001' AND m.area = s.area
JOIN dim_tiempo t    ON t.year = s.anio AND t.mes = s.mes AND t.semana = 0;

-- 6. Boletines OSEC
INSERT INTO hechos_osec_comuna (id_comuna, id_tiempo, delito, casos, publicado_en)
SELECT s.id_comuna, t.id_tiempo, s.delito, s.casos, s.publicado_en
FROM stg_hechos_osec s
JOIN dim_tiempo t ON t.year = s.anio AND t.mes = s.mes AND t.semana = 0;

-- 7. Vivienda ECV 2024
INSERT INTO hechos_analisis (id_departamento, id_municipio, zona_vivienda, tipo_vivienda, tiene_energia_electrica, id_estrato)
SELECT m.id_departamento, m.id_municipio, s.zona_vivienda, s.tipo_vivienda, s.tiene_energia_electrica, s.id_estrato
FROM stg_hechos_analisis s
JOIN dim_municipio m ON m.codigo_dane = '68001' AND m.area = s.area;

-- 8. Conjunto unificado
INSERT INTO dataset_unificado SELECT * FROM stg_dataset_unificado;

-- 9. Control: ninguna fila de staging se puede quedar sin cargar
DO $$
DECLARE
    faltan TEXT := '';
BEGIN
    IF (SELECT COUNT(*) FROM hechos_api_violencia) <> (SELECT COUNT(*) FROM stg_hechos_api) THEN
        faltan := faltan || ' hechos_api_violencia';
    END IF;
    IF (SELECT COUNT(*) FROM hechos_osec_comuna) <> (SELECT COUNT(*) FROM stg_hechos_osec) THEN
        faltan := faltan || ' hechos_osec_comuna';
    END IF;
    IF (SELECT COUNT(*) FROM hechos_analisis) <> (SELECT COUNT(*) FROM stg_hechos_analisis) THEN
        faltan := faltan || ' hechos_analisis';
    END IF;
    IF (SELECT COUNT(*) FROM dataset_unificado) <> (SELECT COUNT(*) FROM stg_dataset_unificado) THEN
        faltan := faltan || ' dataset_unificado';
    END IF;
    IF faltan <> '' THEN
        RAISE EXCEPTION USING MESSAGE = 'Carga incompleta (filas de staging sin municipio o tiempo):' || faltan;
    END IF;
END $$;
