-- ============================================================
-- ZONA CRUDA (RAW) - Segunda entrega
-- Proyecto ETL - Violencia contra las mujeres en Bucaramanga (ODS 5)
--
-- Una tabla cruda POR FUENTE. Las tablas crudas guardan los datos
-- tal como llegan (todo TEXT), sin limpiar ni filtrar, más columnas de
-- trazabilidad (fuente y fecha_carga). La limpieza se hace después,
-- en etl/transformacion.py.
--
--   Fuente 1  raw_violencia_genero     CSV SIVIGILA (ya existe, entrega 1)
--   Fuente 2  raw_api_violencia        API Medicina Legal (datos.gov.co, ers2-kerr)
--   Fuente 3  raw_vivienda             CSV ECV 2024 - DANE (6 columnas)
--   Fuente 4  raw_scraping_boletines   Scraping OSEC: un registro por boletín
--             raw_scraping_comunas     Scraping OSEC: filas por comuna
--
-- REQUISITO: ejecutar después de sql/raw_violencia_genero.sql.
-- Es RE-EJECUTABLE: usa CREATE TABLE IF NOT EXISTS, así que NO borra
-- datos que ya estén cargados.
--
-- Ejecutar:  psql -U postgres -d violencia_genero_db -f sql/raw_fuentes_entrega2.sql
-- (en Windows, si ves caracteres raros, antes: \encoding UTF8)
-- ============================================================


-- ------------------------------------------------------------
-- LIMPIEZA de la ingesta anterior
-- La versión anterior de etl/ingesta.py guardaba la API y el scraping
-- del Geoportal DENTRO de raw_violencia_genero, con columnas que
-- significaban otra cosa (ej. escolaridad -> tipo_seguridad_social) y
-- una fila falsa "COLOMBIA / NACIONAL". Esas filas se eliminan: ahora
-- cada fuente tiene su propia tabla. Después de esto hay que volver a
-- correr etl/transformacion.py para que hechos_violencia quede limpia.
-- ------------------------------------------------------------
DELETE FROM raw_violencia_genero
WHERE fuente IN ('API_Datos_Gov_Socrata', 'Scraping_Geoportal_DANE');


-- ------------------------------------------------------------
-- FUENTE 2: API Medicina Legal
-- Los nombres de columna son los mismos de la API (Socrata), así
-- no hay que traducir nada al cargar.
-- alcance = 'BUCARAMANGA'      -> todos los casos de 68001 (análisis principal)
--           'MUESTRA_NACIONAL' -> 50.000 casos recientes del país (comparación)
-- Un mismo caso de Bucaramanga puede estar en los dos alcances,
-- por eso la llave única es (id, alcance).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_api_violencia (
    id_raw                                      SERIAL PRIMARY KEY,
    id                                          TEXT NOT NULL,
    a_o_del_hecho                               TEXT,
    sexo_de_la_victima                          TEXT,
    grupo_de_edad_quinquenal                    TEXT,
    grupo_mayor_menor_de_edad                   TEXT,
    grupo_de_edad_judicial                      TEXT,
    ciclo_vital                                 TEXT,
    pais_de_nacimiento                          TEXT,
    escolaridad                                 TEXT,
    estado_civil                                TEXT,
    tipo_de_discapacidad                        TEXT,
    pertenencia_etnica                          TEXT,
    orientacion_sexual                          TEXT,
    identidad_de_genero                         TEXT,
    transgenero                                 TEXT,
    pertenencia_grupal                          TEXT,
    mes_del_hecho                               TEXT,
    dia_del_hecho                               TEXT,
    rango_de_hora_del_hecho_x_3_horas           TEXT,
    codigo_dane_municipio                       TEXT,
    municipio_del_hecho_dane                    TEXT,
    departamento_del_hecho_dane                 TEXT,
    codigo_dane_departamento                    TEXT,
    localidad_del_hecho                         TEXT,
    zona_del_hecho                              TEXT,
    escenario_del_hecho                         TEXT,
    actividad_durante_el_hecho                  TEXT,
    circunstancia_del_hecho_detallada           TEXT,
    contexto_del_hecho                          TEXT,
    mecanismo_causal_de_la_lesion_no_fatal      TEXT,
    diagnostico_topografico_de_la_lesion_no_fatal TEXT,
    sexo_del_agresor                            TEXT,
    presunto_agresor_detallado                  TEXT,
    factor_desencadenante_de_la_agresion        TEXT,
    dias_de_incapacidad_medicolegal             TEXT,
    pueblo_indigena                             TEXT,
    alcance                                     VARCHAR(20) NOT NULL
                                                CHECK (alcance IN ('BUCARAMANGA', 'MUESTRA_NACIONAL')),
    fuente                                      VARCHAR(30) NOT NULL DEFAULT 'API_Medicina_Legal',
    fecha_carga                                 TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_raw_api UNIQUE (id, alcance)
);

CREATE INDEX IF NOT EXISTS idx_raw_api_alcance   ON raw_api_violencia(alcance);
CREATE INDEX IF NOT EXISTS idx_raw_api_municipio ON raw_api_violencia(codigo_dane_municipio);

COMMENT ON TABLE raw_api_violencia IS
    'Cruda API Medicina Legal (datos.gov.co ers2-kerr). Todo TEXT, tal cual llega. alcance separa Bucaramanga completa de la muestra nacional.';


-- ------------------------------------------------------------
-- FUENTE 3: ECV 2024 - Datos de la vivienda (DANE)
-- Solo las 6 columnas del alcance, con el archivo NACIONAL completo
-- (la capa cruda no filtra). El filtro 68 / 001 / estrato 1-6 se
-- aplica al cargar hechos_analisis.
-- Nombres en minúscula (PostgreSQL convierte a minúscula los nombres
-- sin comillas).
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_vivienda (
    id_raw            SERIAL PRIMARY KEY,
    p1_departamento   TEXT,
    p1_municipio      TEXT,
    clase             TEXT,
    p1070             TEXT,
    p8520s1           TEXT,
    p8520s1a1         TEXT,
    archivo           TEXT,
    fuente            VARCHAR(30) NOT NULL DEFAULT 'ECV_2024_DANE',
    fecha_carga       TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_raw_viv_municipio ON raw_vivienda(p1_departamento, p1_municipio);

COMMENT ON TABLE raw_vivienda IS
    'Cruda ECV 2024 (DANE), tabla de vivienda, archivo nacional. Solo 6 columnas: P1_DEPARTAMENTO, P1_MUNICIPIO, CLASE, P1070, P8520S1, P8520S1A1.';


-- ------------------------------------------------------------
-- FUENTE 4: Scraping OSEC - Alcaldía de Bucaramanga
-- raw_scraping_boletines: un registro por boletín descargado con éxito.
--   Sirve también de "memoria" para la extracción incremental: el
--   pipeline solo descarga los periodos que NO están aquí.
-- raw_scraping_comunas: las filas de las tablas por comuna del PDF.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw_scraping_boletines (
    periodo            VARCHAR(6)  PRIMARY KEY,     -- AAAAMM
    nombre_boletin     TEXT,
    url_pdf            TEXT,
    estado_extraccion  TEXT,
    fecha_extraccion   TEXT,
    fuente             VARCHAR(30) NOT NULL DEFAULT 'Scraping_OSEC_Bucaramanga',
    fecha_carga        TIMESTAMP   NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_scraping_comunas (
    id_raw               SERIAL PRIMARY KEY,
    periodo              VARCHAR(6) NOT NULL REFERENCES raw_scraping_boletines(periodo),
    delito               TEXT NOT NULL,
    bloque               TEXT NOT NULL,
    comuna               TEXT NOT NULL,
    casos_anio_anterior  TEXT,
    casos_anio_actual    TEXT,
    diferencia           TEXT,
    variacion_pct        TEXT,
    anio_anterior        TEXT,
    anio_actual          TEXT,
    pagina_pdf           TEXT,
    url_pdf              TEXT,
    fecha_extraccion     TEXT,
    fuente               VARCHAR(30) NOT NULL DEFAULT 'Scraping_OSEC_Bucaramanga',
    fecha_carga          TIMESTAMP   NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_raw_scraping UNIQUE (periodo, delito, bloque, comuna)
);

COMMENT ON TABLE raw_scraping_comunas IS
    'Cruda scraping OSEC: tablas "Violencia Intrafamiliar por Comuna" y "Delitos Sexuales por Comuna" de los boletines mensuales. Cifras como TEXT con formato colombiano.';


-- ------------------------------------------------------------
-- CONSULTAS DE CONTROL (después de correr etl/ingesta.py)
-- ------------------------------------------------------------
-- SELECT fuente, COUNT(*) FROM raw_violencia_genero GROUP BY fuente;
-- SELECT alcance, COUNT(*) FROM raw_api_violencia GROUP BY alcance;
-- SELECT COUNT(*) AS viviendas, COUNT(*) FILTER (WHERE p1_departamento = '68' AND p1_municipio = '001') AS bucaramanga FROM raw_vivienda;
-- SELECT COUNT(*) AS boletines, MIN(periodo), MAX(periodo) FROM raw_scraping_boletines;
-- SELECT periodo, delito, bloque, COUNT(*) FROM raw_scraping_comunas GROUP BY 1, 2, 3 ORDER BY 1, 2, 3;   -- deben ser 21