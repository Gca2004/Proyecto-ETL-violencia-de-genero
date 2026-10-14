-- ============================================================
-- MODELO DE DATOS - Segunda entrega (constelación de hechos)
-- Proyecto ETL - Violencia contra las mujeres en Bucaramanga (ODS 5)
--
-- Agrega al esquema estrella de la primera entrega:
--   * dim_municipio      + codigo_dane, latitud, longitud
--   * dim_comuna         (nueva, fija): 17 comunas + 3 corregimientos + SIN COMUNA
--   * hechos_violencia   + sexo_victima, id_comuna
--   * hechos_api_violencia   (nueva) casos Medicina Legal, víctimas mujeres
--   * hechos_osec_comuna     (nueva) denuncias por comuna y mes (OSEC)
--   * hechos_analisis        (ya existía) viviendas ECV 2024
--   * dataset_unificado      (nueva) conjunto único y enriquecido por año-mes
--   * vista_comuna_anual     (vista) comparación SIVIGILA vs OSEC por comuna
--   * tablas stg_*           zona de staging: lo que produce la transformación
--                            ANTES de validarlo y cargarlo al modelo final
--   * log_validaciones       registro de cada validación de Great Expectations
--
-- ORDEN DE EJECUCIÓN:
--   1. sql/raw_violencia_genero.sql
--   2. sql/esquema_violencia_genero.sql
--   3. script de hechos_analisis / dim_estrato
--   4. sql/raw_fuentes_entrega2.sql
--   5. ESTE ARCHIVO
-- Si se vuelve a ejecutar esquema_violencia_genero.sql, hay que volver a
-- ejecutar este archivo (aquel recrea hechos_violencia y dim_municipio).
--
-- Es RE-EJECUTABLE: usa IF NOT EXISTS / OR REPLACE.
-- ============================================================


-- ------------------------------------------------------------
-- 1. dim_municipio: código DANE y coordenadas (fuente: DIVIPOLA - DANE)
-- ------------------------------------------------------------
ALTER TABLE dim_municipio ADD COLUMN IF NOT EXISTS codigo_dane VARCHAR(5);
ALTER TABLE dim_municipio ADD COLUMN IF NOT EXISTS latitud     NUMERIC(9, 6);
ALTER TABLE dim_municipio ADD COLUMN IF NOT EXISTS longitud    NUMERIC(9, 6);
CREATE INDEX IF NOT EXISTS idx_municipio_codigo_dane ON dim_municipio(codigo_dane);

COMMENT ON COLUMN dim_municipio.codigo_dane IS
    'Código DIVIPOLA de 5 dígitos (texto, conserva el cero inicial). Lo llena etl/transformacion_entrega2.py.';


-- ------------------------------------------------------------
-- 2. dim_comuna (fija, no la vacía ninguna transformación)
--    id 1..17 = número oficial de la comuna
--    id 101..103 = corregimientos 1, 2 y 3
--    id 0 = SIN COMUNA (sin información, no disponible u otro municipio)
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_comuna (
    id_comuna       SMALLINT    PRIMARY KEY,
    numero_comuna   SMALLINT    CHECK (numero_comuna BETWEEN 1 AND 17),
    nombre_comuna   VARCHAR(40) NOT NULL UNIQUE,
    nombre_alterno  VARCHAR(40),
    tipo            VARCHAR(20) NOT NULL CHECK (tipo IN ('COMUNA', 'CORREGIMIENTO', 'SIN UBICACION'))
);

INSERT INTO dim_comuna (id_comuna, numero_comuna, nombre_comuna, nombre_alterno, tipo) VALUES
    (1, 1, 'NORTE', NULL, 'COMUNA'),
    (2, 2, 'NORORIENTAL', NULL, 'COMUNA'),
    (3, 3, 'SAN FRANCISCO', NULL, 'COMUNA'),
    (4, 4, 'OCCIDENTAL', NULL, 'COMUNA'),
    (5, 5, 'GARCIA ROVIRA', NULL, 'COMUNA'),
    (6, 6, 'LA CONCORDIA', NULL, 'COMUNA'),
    (7, 7, 'CIUDADELA REAL DE MINAS', 'LA CIUDADELA', 'COMUNA'),
    (8, 8, 'SUROCCIDENTE', 'SUR OCCIDENTE', 'COMUNA'),
    (9, 9, 'LA PEDREGOSA', NULL, 'COMUNA'),
    (10, 10, 'PROVENZA', NULL, 'COMUNA'),
    (11, 11, 'SUR', NULL, 'COMUNA'),
    (12, 12, 'CABECERA DEL LLANO', NULL, 'COMUNA'),
    (13, 13, 'ORIENTAL', NULL, 'COMUNA'),
    (14, 14, 'MORRORICO', NULL, 'COMUNA'),
    (15, 15, 'CENTRO', NULL, 'COMUNA'),
    (16, 16, 'TEJAR', 'LAGOS DEL CACIQUE', 'COMUNA'),
    (17, 17, 'MUTIS', NULL, 'COMUNA'),
    (101, NULL, 'CORREGIMIENTO 1', NULL, 'CORREGIMIENTO'),
    (102, NULL, 'CORREGIMIENTO 2', NULL, 'CORREGIMIENTO'),
    (103, NULL, 'CORREGIMIENTO 3', NULL, 'CORREGIMIENTO'),
    (0, NULL, 'SIN COMUNA', NULL, 'SIN UBICACION')
ON CONFLICT (id_comuna) DO NOTHING;

COMMENT ON TABLE dim_comuna IS
    'Comunas y corregimientos de Bucaramanga. Llave común entre SIVIGILA y los boletines OSEC (se une por número, no por nombre).';


-- ------------------------------------------------------------
-- 3. hechos_violencia: sexo de la víctima y comuna
-- ------------------------------------------------------------
ALTER TABLE hechos_violencia ADD COLUMN IF NOT EXISTS sexo_victima VARCHAR(20);
ALTER TABLE hechos_violencia ADD COLUMN IF NOT EXISTS id_comuna    SMALLINT;
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_hechos_violencia_comuna') THEN
        ALTER TABLE hechos_violencia
            ADD CONSTRAINT fk_hechos_violencia_comuna FOREIGN KEY (id_comuna) REFERENCES dim_comuna(id_comuna);
    END IF;
END $$;
CREATE INDEX IF NOT EXISTS idx_hechos_comuna ON hechos_violencia(id_comuna);
CREATE INDEX IF NOT EXISTS idx_hechos_sexo   ON hechos_violencia(sexo_victima);


-- ------------------------------------------------------------
-- 4. hechos_api_violencia: un caso de Medicina Legal en Bucaramanga
--    (solo víctimas mujeres). Comparte dim_municipio y dim_tiempo.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hechos_api_violencia (
    id_hecho_api       SERIAL       PRIMARY KEY,
    id_caso_api        TEXT         NOT NULL UNIQUE,      -- "id" de la API
    id_municipio       INT          NOT NULL REFERENCES dim_municipio(id_municipio),
    id_tiempo          INT          NOT NULL REFERENCES dim_tiempo(id_tiempo),
    ciclo_vida         VARCHAR(30)  NOT NULL,             -- homologado con SIVIGILA
    grupo_agresor      VARCHAR(40)  NOT NULL,             -- homologado con SIVIGILA
    presunto_agresor   VARCHAR(80)  NOT NULL,             -- valor original normalizado
    sexo_agresor       VARCHAR(20)  NOT NULL,
    tipo_violencia     VARCHAR(80)  NOT NULL,             -- circunstancia del hecho
    contexto           VARCHAR(120) NOT NULL,
    escenario          VARCHAR(150) NOT NULL,
    turno              VARCHAR(15)  NOT NULL CHECK (turno IN ('MAÑANA', 'TARDE', 'NOCHE', 'NO REPORTADO')),
    dias_incapacidad   VARCHAR(15)  NOT NULL CHECK (dias_incapacidad IN ('CERO DIAS', '1 A 30', '31 A 90', 'MAS DE 90', 'NO REPORTADO')),
    fuente             VARCHAR(30)  NOT NULL DEFAULT 'API_Medicina_Legal'
);
CREATE INDEX IF NOT EXISTS idx_api_tiempo ON hechos_api_violencia(id_tiempo);


-- ------------------------------------------------------------
-- 5. hechos_osec_comuna: denuncias por comuna, mes y delito (OSEC).
--    Una fila = cifra más reciente publicada para ese mes.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS hechos_osec_comuna (
    id_hecho_osec  SERIAL      PRIMARY KEY,
    id_comuna      SMALLINT    NOT NULL REFERENCES dim_comuna(id_comuna),
    id_tiempo      INT         NOT NULL REFERENCES dim_tiempo(id_tiempo),
    delito         VARCHAR(30) NOT NULL CHECK (delito IN ('VIOLENCIA INTRAFAMILIAR', 'DELITOS SEXUALES')),
    casos          INT         NOT NULL CHECK (casos >= 0),
    publicado_en   VARCHAR(6)  NOT NULL,                  -- periodo del boletín de donde sale la cifra
    fuente         VARCHAR(30) NOT NULL DEFAULT 'Scraping_OSEC_Bucaramanga',
    CONSTRAINT uq_osec UNIQUE (id_comuna, id_tiempo, delito)
);


-- ------------------------------------------------------------
-- 6. dataset_unificado: el conjunto único y enriquecido.
--    Una fila = un mes en Bucaramanga, con las 4 fuentes lado a lado.
--    NULL = la fuente no cubre ese mes ; 0 = la fuente lo cubre y no hubo casos.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dataset_unificado (
    anio                            SMALLINT NOT NULL,
    mes                             SMALLINT NOT NULL CHECK (mes BETWEEN 1 AND 12),
    fecha_mes                       DATE     NOT NULL,
    -- SIVIGILA (casos atendidos en salud)
    siv_casos_total                 INT,
    siv_casos_mujeres               INT,
    siv_mujeres_agresor_pareja      INT,
    siv_mujeres_agresor_familiar    INT,
    siv_mujeres_turno_noche         INT,
    siv_mujeres_fallecidas          INT,
    -- API Medicina Legal (violencia entre familiares, víctimas mujeres)
    api_casos_mujeres               INT,
    api_mujeres_incapacidad_grave   INT,      -- más de 30 días
    -- OSEC (denuncias ante la Policía, toda la población)
    osec_denuncias_vif              INT,
    osec_denuncias_sexuales         INT,
    -- ECV 2024 (contexto de vivienda de la ciudad, constante)
    ecv_viviendas_bucaramanga       INT,
    ecv_pct_estrato_1_2             NUMERIC(5, 2),
    ecv_pct_estrato_5_6             NUMERIC(5, 2),
    fuentes_con_dato                SMALLINT NOT NULL,
    PRIMARY KEY (anio, mes)
);

COMMENT ON TABLE dataset_unificado IS
    'Conjunto unificado y enriquecido: Bucaramanga por año y mes con SIVIGILA, Medicina Legal, OSEC y contexto ECV 2024. Las fuentes se comparan, no se suman.';


-- ------------------------------------------------------------
-- 7. STAGING: salida de la transformación antes de validar y cargar.
--    Llaves naturales (año, mes, área), sin ids: los ids se resuelven al cargar.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS stg_hechos_api (
    id_caso_api TEXT, anio SMALLINT, mes SMALLINT, area VARCHAR(30), ciclo_vida VARCHAR(30),
    grupo_agresor VARCHAR(40), presunto_agresor VARCHAR(80), sexo_agresor VARCHAR(20),
    tipo_violencia VARCHAR(80), contexto VARCHAR(120), escenario VARCHAR(150), turno VARCHAR(15),
    dias_incapacidad VARCHAR(15)
);
CREATE TABLE IF NOT EXISTS stg_hechos_osec (
    id_comuna SMALLINT, anio SMALLINT, mes SMALLINT, delito VARCHAR(30), casos INT, publicado_en VARCHAR(6)
);
CREATE TABLE IF NOT EXISTS stg_hechos_analisis (
    area VARCHAR(30), zona_vivienda SMALLINT, tipo_vivienda SMALLINT, tiene_energia_electrica SMALLINT, id_estrato SMALLINT
);
CREATE TABLE IF NOT EXISTS stg_codigo_dane (
    nombre_departamento VARCHAR(60), nombre_municipio VARCHAR(60), codigo_dane VARCHAR(5),
    latitud NUMERIC(9, 6), longitud NUMERIC(9, 6)
);
CREATE TABLE IF NOT EXISTS stg_dataset_unificado (LIKE dataset_unificado INCLUDING DEFAULTS);


-- ------------------------------------------------------------
-- 8. VISTA para el dashboard: comuna por año, SIVIGILA vs OSEC
-- ------------------------------------------------------------
CREATE OR REPLACE VIEW vista_comuna_anual AS
WITH siv AS (
    SELECT h.id_comuna, t.year AS anio,
           COUNT(*) FILTER (WHERE h.sexo_victima = 'FEMENINO') AS siv_casos_mujeres,
           COUNT(*) AS siv_casos_total
    FROM hechos_violencia h
    JOIN dim_tiempo t    ON t.id_tiempo = h.id_tiempo
    JOIN dim_municipio m ON m.id_municipio = h.id_municipio
    WHERE m.codigo_dane = '68001'
    GROUP BY h.id_comuna, t.year
),
osec AS (
    SELECT o.id_comuna, t.year AS anio,
           SUM(o.casos) FILTER (WHERE o.delito = 'VIOLENCIA INTRAFAMILIAR') AS osec_denuncias_vif,
           SUM(o.casos) FILTER (WHERE o.delito = 'DELITOS SEXUALES')        AS osec_denuncias_sexuales,
           COUNT(DISTINCT t.mes) AS osec_meses_con_dato
    FROM hechos_osec_comuna o
    JOIN dim_tiempo t ON t.id_tiempo = o.id_tiempo
    GROUP BY o.id_comuna, t.year
)
SELECT c.id_comuna, c.numero_comuna, c.nombre_comuna, c.tipo,
       COALESCE(siv.anio, osec.anio) AS anio,
       siv.siv_casos_mujeres, siv.siv_casos_total,
       osec.osec_denuncias_vif, osec.osec_denuncias_sexuales, osec.osec_meses_con_dato
FROM dim_comuna c
JOIN (siv FULL OUTER JOIN osec ON siv.id_comuna = osec.id_comuna AND siv.anio = osec.anio)
     ON c.id_comuna = COALESCE(siv.id_comuna, osec.id_comuna);


-- ------------------------------------------------------------
-- 9. Registro de validaciones de calidad (Great Expectations)
--    Una fila = una expectativa evaluada en una ejecución.
--    La escribe etl/validacion.py en cada corrida del DAG.
-- ------------------------------------------------------------
CREATE TABLE IF NOT EXISTS log_validaciones (
    id_log                 SERIAL       PRIMARY KEY,
    fecha_ejecucion        TIMESTAMP    NOT NULL,
    suite                  VARCHAR(60)  NOT NULL,
    tabla                  VARCHAR(60)  NOT NULL,
    expectativa            VARCHAR(80)  NOT NULL,
    columna                VARCHAR(120),
    exito                  BOOLEAN      NOT NULL,
    valor_observado        TEXT,
    filas_inesperadas      INT,
    porcentaje_inesperado  NUMERIC(7, 3),
    ejemplos_inesperados   TEXT,
    descripcion            TEXT
);
CREATE INDEX IF NOT EXISTS idx_log_validaciones_fecha ON log_validaciones(fecha_ejecucion);

-- Última ejecución, resumida por suite:
-- SELECT suite, COUNT(*) AS expectativas, COUNT(*) FILTER (WHERE exito) AS cumplidas
-- FROM log_validaciones
-- WHERE fecha_ejecucion = (SELECT MAX(fecha_ejecucion) FROM log_validaciones)
-- GROUP BY suite ORDER BY suite;