-- ============================================================
-- VISTAS PARA EL DASHBOARD (Power BI)
-- Proyecto ETL - Violencia contra las mujeres en Bucaramanga (ODS 5)
--
-- Todas leen SOLO del modelo final (datos validados por Great Expectations).
-- Power BI se conecta a estas vistas: cada una ya viene preparada para un
-- tipo de gráfico, con nombres en español y categorías homologadas.
--
-- Ejecutar DESPUÉS de sql/esquema_entrega2.sql. Es re-ejecutable.
-- ============================================================

-- Se borran primero (en orden de dependencias) para poder recrearlas sin errores
DROP VIEW IF EXISTS vw_pbi_embudo;
DROP VIEW IF EXISTS vw_pbi_pareto_comunas;
DROP VIEW IF EXISTS vw_pbi_comparacion_agresor;
DROP VIEW IF EXISTS vw_pbi_sivigila_mujeres;
DROP VIEW IF EXISTS vw_pbi_medicina_legal;
DROP VIEW IF EXISTS vw_pbi_indicadores_vivienda;
DROP VIEW IF EXISTS vw_pbi_vivienda;
DROP VIEW IF EXISTS vw_pbi_tendencia;
DROP VIEW IF EXISTS vw_pbi_comunas;
DROP VIEW IF EXISTS vw_pbi_calidad;
DROP VIEW IF EXISTS vw_pbi_calidad_historial;


-- 1. Tendencia mensual (página "Tendencia")
CREATE OR REPLACE VIEW vw_pbi_tendencia AS
SELECT fecha_mes,
       anio,
       mes,
       siv_casos_total               AS sivigila_casos_total,
       siv_casos_mujeres             AS sivigila_casos_mujeres,
       siv_mujeres_agresor_pareja    AS sivigila_mujeres_agresor_pareja,
       siv_mujeres_agresor_familiar  AS sivigila_mujeres_agresor_familiar,
       siv_mujeres_turno_noche       AS sivigila_mujeres_turno_noche,
       siv_mujeres_fallecidas        AS sivigila_mujeres_fallecidas,
       api_casos_mujeres             AS medicina_legal_casos_mujeres,
       api_mujeres_incapacidad_grave AS medicina_legal_incapacidad_grave,
       osec_denuncias_vif            AS osec_denuncias_violencia_intrafamiliar,
       osec_denuncias_sexuales       AS osec_denuncias_delitos_sexuales,
       fuentes_con_dato,
       CONCAT_WS(' + ',
                 CASE WHEN siv_casos_total    IS NOT NULL THEN 'SIVIGILA' END,
                 CASE WHEN api_casos_mujeres  IS NOT NULL THEN 'Medicina Legal' END,
                 CASE WHEN osec_denuncias_vif IS NOT NULL THEN 'OSEC' END) AS fuentes_disponibles
FROM dataset_unificado;


-- 2. Comunas por año (página "Territorio")
CREATE OR REPLACE VIEW vw_pbi_comunas AS
SELECT LPAD(numero_comuna::TEXT, 2, '0') || '. ' || nombre_comuna AS comuna,
       numero_comuna,
       anio,
       COALESCE(siv_casos_mujeres, 0)       AS sivigila_casos_mujeres,
       COALESCE(osec_denuncias_vif, 0)      AS osec_denuncias_violencia_intrafamiliar,
       COALESCE(osec_denuncias_sexuales, 0) AS osec_denuncias_delitos_sexuales,
       osec_meses_con_dato
FROM vista_comuna_anual
WHERE tipo = 'COMUNA';


-- 3. Casos SIVIGILA de mujeres en Bucaramanga, uno por fila (página "Perfil")
CREATE VIEW vw_pbi_sivigila_mujeres AS
SELECT h.id_hecho,
       t.year AS anio,
       t.mes,
       CASE h.turno WHEN 'MAÑANA' THEN '1. MAÑANA (7-12 h)' WHEN 'TARDE' THEN '2. TARDE (12-22 h)'
                    WHEN 'NOCHE' THEN '3. NOCHE (22-7 h)' ELSE '4. NO REPORTADO' END AS turno,
       CASE h.con_fin WHEN '1' THEN 'VIVA' WHEN '2' THEN 'FALLECIDA' ELSE 'NO REPORTADO' END AS condicion_final,
       CASE WHEN h.sust_vict THEN 'SI' WHEN NOT h.sust_vict THEN 'NO' ELSE 'NO REPORTADO' END AS consumo_sustancias_victima,
       CASE WHEN h.pac_hos THEN 'SI' WHEN NOT h.pac_hos THEN 'NO' ELSE 'NO REPORTADO' END AS hospitalizada,
       CASE
           WHEN v.ciclo_vida LIKE 'PRIMERA INFANCIA%' THEN '1. PRIMERA INFANCIA'
           WHEN v.ciclo_vida LIKE 'INFANCIA%'         THEN '2. INFANCIA'
           WHEN v.ciclo_vida LIKE 'ADOLESCENCIA%'     THEN '3. ADOLESCENCIA'
           WHEN v.ciclo_vida LIKE 'JOVEN%' OR v.ciclo_vida LIKE 'JUVENTUD%' THEN '4. JUVENTUD'
           WHEN v.ciclo_vida LIKE 'ADULTEZ%'          THEN '5. ADULTEZ'
           WHEN v.ciclo_vida LIKE '%MAYOR%'           THEN '6. PERSONA MAYOR'
           ELSE '7. NO REPORTADO'
       END AS ciclo_vida,
       a.parentezco_vict AS agresor_detalle,
       CASE
           WHEN a.parentezco_vict ~ '(PAREJA|ESPOSO|COMPANERO|COMANERO|NOVIO|AMANTE)' THEN 'PAREJA O EXPAREJA'
           WHEN a.parentezco_vict ~ '^(PADRE|MADRE|PADRAST|MADRAST)'                 THEN 'PADRE, MADRE O PADRASTROS'
           WHEN a.parentezco_vict ~ '^(HERMANO|HIJO|TIO|ABUELO|PRIMO|CUNADO|SUEGRO|SOBRINO|NIETO|YERNO|NUERA)'
                OR a.parentezco_vict LIKE '%FAMILIAR%'                                THEN 'OTRO FAMILIAR'
           WHEN a.parentezco_vict ~ '(ENCARGADO|PROFESOR|CUSTODIA)' OR a.parentezco_vict = 'OTROS'
                                                                                      THEN 'CUIDADOR U OTRO CONOCIDO'
           WHEN a.parentezco_vict = 'NINGUNO'                                         THEN 'SIN VINCULO'
           ELSE 'NO REPORTADO'
       END AS grupo_agresor,
       tv.def_naturaleza AS tipo_violencia,
       CASE WHEN tv.naturaleza = 1 THEN '1. FISICA'
            WHEN tv.naturaleza = 2 THEN '2. PSICOLOGICA'
            WHEN tv.naturaleza IN (4, 5, 6, 7, 10, 12, 14) THEN '3. SEXUAL'
            WHEN tv.naturaleza = 3 THEN '4. NEGLIGENCIA Y ABANDONO'
            ELSE '5. SIN INFORMACION' END AS tipo_violencia_grupo,
       c.nombre_comuna   AS comuna,
       m.area
FROM hechos_violencia h
JOIN dim_tiempo t          ON t.id_tiempo = h.id_tiempo
JOIN dim_victima v         ON v.id_victima = h.id_victima
JOIN dim_agresor a         ON a.id_agresor = h.id_agresor
JOIN dim_tipo_violencia tv ON tv.id_tipo_violencia = h.id_tipo_violencia
JOIN dim_municipio m       ON m.id_municipio = h.id_municipio
LEFT JOIN dim_comuna c     ON c.id_comuna = h.id_comuna
WHERE m.codigo_dane = '68001' AND h.sexo_victima = 'FEMENINO';


-- 4. Casos de Medicina Legal (mujeres), uno por fila (página "Perfil")
CREATE OR REPLACE VIEW vw_pbi_medicina_legal AS
SELECT a.id_caso_api,
       t.year AS anio,
       t.mes,
       a.ciclo_vida,
       a.grupo_agresor,
       a.presunto_agresor AS agresor_detalle,
       a.tipo_violencia,
       a.escenario,
       a.turno,
       a.dias_incapacidad
FROM hechos_api_violencia a
JOIN dim_tiempo t ON t.id_tiempo = a.id_tiempo;


-- 5. Comparación SIVIGILA vs Medicina Legal por agresor familiar, 2015-2024
--    (solo categorías familiares: la API no incluye violencia de pareja)
CREATE VIEW vw_pbi_comparacion_agresor AS
WITH casos AS (
    SELECT 'SIVIGILA' AS fuente, grupo_agresor FROM vw_pbi_sivigila_mujeres
    WHERE anio BETWEEN 2015 AND 2024
    UNION ALL
    SELECT 'MEDICINA LEGAL', grupo_agresor FROM vw_pbi_medicina_legal
)
SELECT fuente,
       grupo_agresor,
       COUNT(*) AS casos,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (PARTITION BY fuente), 1) AS porcentaje
FROM casos
WHERE grupo_agresor IN ('PADRE, MADRE O PADRASTROS', 'OTRO FAMILIAR')
GROUP BY fuente, grupo_agresor;


-- 6. Viviendas ECV 2024 (página "Contexto")
CREATE VIEW vw_pbi_vivienda AS
SELECT m.nombre_municipio AS municipio,
       m.codigo_dane,
       e.id_estrato,
       e.nombre_estrato AS estrato,
       e.nivel_estrato,
       CASE WHEN e.id_estrato <= 2 THEN '1. BAJO (estratos 1-2)'
            WHEN e.id_estrato <= 4 THEN '2. MEDIO (estratos 3-4)'
            ELSE '3. ALTO (estratos 5-6)' END AS nivel_socioeconomico,
       CASE h.tipo_vivienda WHEN 1 THEN 'CASA' WHEN 2 THEN 'APARTAMENTO' WHEN 3 THEN 'CUARTO(S)'
                            WHEN 4 THEN 'VIVIENDA INDIGENA' ELSE 'OTRO' END AS tipo_vivienda,
       CASE h.zona_vivienda WHEN 1 THEN 'CABECERA' WHEN 2 THEN 'CENTRO POBLADO' ELSE 'RURAL DISPERSO' END AS zona
FROM hechos_analisis h
JOIN dim_estrato e   ON e.id_estrato = h.id_estrato
JOIN dim_municipio m ON m.id_municipio = h.id_municipio;


-- 6b. Indicadores de condiciones de vida por municipio (una fila por municipio).
--     Hoy solo Bucaramanga; si se carga otro municipio, aparece como otra fila
--     y se puede comparar directamente.
CREATE VIEW vw_pbi_indicadores_vivienda AS
SELECT municipio,
       codigo_dane,
       COUNT(*) AS viviendas_encuestadas,
       ROUND(100.0 * COUNT(*) FILTER (WHERE id_estrato <= 2) / COUNT(*), 1)          AS pct_estrato_bajo_1_2,
       ROUND(100.0 * COUNT(*) FILTER (WHERE id_estrato = 1) / COUNT(*), 1)           AS pct_estrato_1,
       ROUND(100.0 * COUNT(*) FILTER (WHERE id_estrato >= 5) / COUNT(*), 1)          AS pct_estrato_alto_5_6,
       ROUND(100.0 * COUNT(*) FILTER (WHERE tipo_vivienda = 'CUARTO(S)') / COUNT(*), 1) AS pct_vive_en_cuartos,
       ROUND(100.0 * COUNT(*) FILTER (WHERE tipo_vivienda = 'APARTAMENTO') / COUNT(*), 1) AS pct_apartamento,
       ROUND(100.0 * COUNT(*) FILTER (WHERE zona <> 'CABECERA') / COUNT(*), 1)      AS pct_fuera_de_cabecera
FROM vw_pbi_vivienda
GROUP BY municipio, codigo_dane;


-- 7. Calidad de datos: última validación de Great Expectations (página "Calidad")
CREATE OR REPLACE VIEW vw_pbi_calidad AS
SELECT fecha_ejecucion,
       suite,
       tabla,
       expectativa,
       columna,
       CASE WHEN exito THEN 'CUMPLIDA' ELSE 'FALLIDA' END AS estado,
       CASE WHEN exito THEN 1 ELSE 0 END AS cumplida,
       filas_inesperadas,
       descripcion
FROM log_validaciones
WHERE fecha_ejecucion = (SELECT MAX(fecha_ejecucion) FROM log_validaciones);


-- 8. Historial de validaciones (una fila por ejecución)
CREATE OR REPLACE VIEW vw_pbi_calidad_historial AS
SELECT fecha_ejecucion,
       COUNT(*)                     AS expectativas,
       COUNT(*) FILTER (WHERE exito) AS cumplidas,
       COUNT(*) FILTER (WHERE NOT exito) AS fallidas,
       ROUND(100.0 * COUNT(*) FILTER (WHERE exito) / COUNT(*), 1) AS porcentaje_cumplimiento
FROM log_validaciones
GROUP BY fecha_ejecucion;


-- 9. Embudo de integración: cuántos registros quedan en cada etapa y por qué
CREATE OR REPLACE VIEW vw_pbi_embudo AS
SELECT * FROM (
    SELECT 'SIVIGILA' AS fuente, 1 AS orden, '1. Registros extraídos' AS etapa,
           (SELECT COUNT(*) FROM raw_violencia_genero) AS registros, 'CSV de la primera entrega' AS motivo
    UNION ALL
    SELECT 'SIVIGILA', 2, '2. Casos en Bucaramanga',
           (SELECT COUNT(*) FROM hechos_violencia h JOIN dim_municipio m ON m.id_municipio = h.id_municipio WHERE m.codigo_dane = '68001'),
           'Se excluyen casos de otros municipios'
    UNION ALL
    SELECT 'SIVIGILA', 3, '3. Víctimas mujeres',
           (SELECT COUNT(*) FROM vw_pbi_sivigila_mujeres), 'Enfoque ODS 5'
    UNION ALL
    SELECT 'MEDICINA LEGAL', 1, '1. Registros extraídos',
           (SELECT COUNT(*) FROM raw_api_violencia), 'API: Bucaramanga + muestra nacional de 50.000'
    UNION ALL
    SELECT 'MEDICINA LEGAL', 2, '2. Casos en Bucaramanga',
           (SELECT COUNT(*) FROM raw_api_violencia WHERE alcance = 'BUCARAMANGA'), 'La muestra nacional solo se usa para comparar'
    UNION ALL
    SELECT 'MEDICINA LEGAL', 3, '3. Víctimas mujeres',
           (SELECT COUNT(*) FROM hechos_api_violencia), 'Enfoque ODS 5'
    UNION ALL
    SELECT 'ECV 2024', 1, '1. Registros extraídos',
           (SELECT COUNT(*) FROM raw_vivienda), 'Archivo nacional de viviendas'
    UNION ALL
    SELECT 'ECV 2024', 2, '2. Viviendas en Bucaramanga',
           (SELECT COUNT(*) FROM raw_vivienda WHERE LPAD(p1_departamento, 2, '0') = '68' AND LPAD(p1_municipio, 3, '0') = '001'),
           'Departamento 68, municipio 001'
    UNION ALL
    SELECT 'ECV 2024', 3, '3. Con estrato válido (1 a 6)',
           (SELECT COUNT(*) FROM hechos_analisis), 'Los códigos 0, 8 y 9 no son estratos'
    UNION ALL
    SELECT 'OSEC', 1, '1. Registros extraídos',
           (SELECT COUNT(*) FROM raw_scraping_comunas), '16 boletines x 2 delitos x 2 bloques x 21 comunas'
    UNION ALL
    SELECT 'OSEC', 2, '2. Bloque mensual',
           (SELECT COUNT(*) FROM raw_scraping_comunas WHERE bloque = 'MES'), 'El acumulado no se usa (duplicaría casos)'
    UNION ALL
    SELECT 'OSEC', 3, '3. Serie comuna-mes-delito',
           (SELECT COUNT(*) FROM hechos_osec_comuna), 'Cada fila da 2 meses; se deja la cifra del boletín más reciente'
) e;


-- 10. Pareto de comunas: casos SIVIGILA de mujeres por comuna y % acumulado
CREATE VIEW vw_pbi_pareto_comunas AS
WITH c AS (
    SELECT comuna, COUNT(*) AS casos
    FROM vw_pbi_sivigila_mujeres
    WHERE comuna NOT IN ('SIN COMUNA') AND comuna NOT LIKE 'CORREGIMIENTO%'
    GROUP BY comuna
)
SELECT comuna,
       casos,
       RANK() OVER (ORDER BY casos DESC) AS posicion,
       ROUND(100.0 * casos / SUM(casos) OVER (), 1) AS porcentaje,
       ROUND(100.0 * SUM(casos) OVER (ORDER BY casos DESC, comuna ROWS UNBOUNDED PRECEDING) / SUM(casos) OVER (), 1) AS porcentaje_acumulado
FROM c;