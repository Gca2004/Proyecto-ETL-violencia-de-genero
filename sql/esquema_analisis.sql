-- ============================================================
-- ESQUEMA: Condiciones de vivienda en Bucaramanga (ECV 2024 - DANE)
-- Proyecto ETL - segunda entrega
--
-- Fuente : CSV "Datos de la vivienda" (Encuesta Nacional de Calidad de
--          Vida 2024, DANE). Separador ';'. Una fila = una vivienda.
-- Alcance: P1_DEPARTAMENTO = '68' (Santander)
--          P1_MUNICIPIO    = '001' (Bucaramanga; código completo 68001)
--          P8520S1A1 entre 1 y 6 (estrato de la tarifa de energía)
--          (el filtro 68 / 001 / estrato 1-6 se aplica AL CARGAR)
--
-- REQUISITO: ejecutar ANTES sql/esquema.sql, porque hechos_analisis
-- apunta a dim_municipio y dim_departamento (las mismas dimensiones
-- que usa hechos_violencia).
--
-- SOLO ESTRUCTURA: este script no carga datos de la encuesta (solo las
-- 6 filas fijas de dim_estrato). La carga/transformación viene después.
--
-- UNIÓN CON VIOLENCIA:
-- hechos_analisis.id_municipio es el MISMO tipo de llave que
-- hechos_violencia.id_municipio.
--
-- Ahora también se almacena id_departamento directamente en hechos_analisis.
-- Las relaciones son:
--   hechos_* -> dim_departamento
--   hechos_* -> dim_municipio -> dim_departamento
--
-- Ojo: dim_municipio tiene una fila por (departamento, municipio, área),
-- así que para unir a nivel MUNICIPIO se agrupa por
-- (id_departamento, nombre_municipio).
--
-- RENOMBRADO de columnas (original -> hechos_analisis):
--   P1_DEPARTAMENTO               -> id_departamento (FK a dim_departamento)
--   P1_DEPARTAMENTO + P1_MUNICIPIO -> id_municipio  (FK a dim_municipio)
--   CLASE                         -> zona_vivienda
--   P1070                         -> tipo_vivienda
--   P8520S1                       -> tiene_energia_electrica
--   P8520S1A1                     -> id_estrato     (FK a dim_estrato)
--
-- ORDEN DE CARGA: primero violencia, después hechos_analisis.
--   * Recargar violencia (etl/transformacion.py usa TRUNCATE ... CASCADE)
--     también VACÍA hechos_analisis, porque esta tabla depende de dim_municipio.
--   * Re-ejecutar sql/esquema.sql recrea dim_municipio: hechos_analisis
--     pierde su llave foránea y sus ids quedan obsoletos.
-- En ambos casos: vuelve a correr este script (si hace falta) y recarga la tabla.
--
-- IMPORTANTE PARA EL ETL:
--   La carga de hechos_analisis debe proporcionar tanto id_departamento
--   como id_municipio. Ambos deben corresponder a las dimensiones.
--
-- Este script es RE-EJECUTABLE, pero elimina y recrea hechos_analisis
-- y dim_estrato, por lo que se pierden los datos que contengan.
-- Si lo corres con psql en Windows y ves caracteres raros, ejecuta antes:
--   \encoding UTF8
-- ============================================================

DROP TABLE IF EXISTS hechos_analisis CASCADE;
DROP TABLE IF EXISTS dim_estrato     CASCADE;

-- ------------------------------------------------------------
-- DIMENSIÓN: Estrato
-- id_estrato = número del estrato (1..6), igual al código de P8520S1A1,
-- así que la carga no necesita ninguna traducción de claves.
-- Los códigos 0, 8 y 9 de P8520S1A1 NO son estratos y no entran aquí.
-- ------------------------------------------------------------
CREATE TABLE dim_estrato (
    id_estrato      SMALLINT    PRIMARY KEY CHECK (id_estrato BETWEEN 1 AND 6),
    nombre_estrato  VARCHAR(20) NOT NULL UNIQUE,
    nivel_estrato   VARCHAR(20) NOT NULL
                    -- Clasificación usual de los estratos en Colombia
                    -- (no viene en el dataset): BAJO-BAJO, BAJO, MEDIO-BAJO,
                    -- MEDIO, MEDIO-ALTO, ALTO
);

INSERT INTO dim_estrato (id_estrato, nombre_estrato, nivel_estrato) VALUES
    (1, 'ESTRATO 1', 'BAJO-BAJO'),
    (2, 'ESTRATO 2', 'BAJO'),
    (3, 'ESTRATO 3', 'MEDIO-BAJO'),
    (4, 'ESTRATO 4', 'MEDIO'),
    (5, 'ESTRATO 5', 'MEDIO-ALTO'),
    (6, 'ESTRATO 6', 'ALTO');

-- ------------------------------------------------------------
-- TABLA DE HECHOS: una fila = una vivienda de Bucaramanga con estrato 1-6
-- Los códigos de zona_vivienda, tipo_vivienda y tiene_energia_electrica se
-- guardan tal como vienen en la fuente (ver COMMENT ON COLUMN).
--
-- NUEVO: id_departamento almacena la llave foránea al departamento
-- correspondiente a la vivienda, obtenida de P1_DEPARTAMENTO.
-- ------------------------------------------------------------
CREATE TABLE hechos_analisis (
    id_hecho_analisis        SERIAL   PRIMARY KEY,
    id_departamento          INT      NOT NULL REFERENCES dim_departamento(id_departamento),
    id_municipio             INT      NOT NULL REFERENCES dim_municipio(id_municipio),
    zona_vivienda            SMALLINT NOT NULL,
    tipo_vivienda            SMALLINT NOT NULL,
    tiene_energia_electrica  SMALLINT NOT NULL,
    id_estrato               SMALLINT NOT NULL REFERENCES dim_estrato(id_estrato),

    CONSTRAINT chk_zona_vivienda
        CHECK (zona_vivienda IN (1, 2, 3)),
    CONSTRAINT chk_tipo_vivienda
        CHECK (tipo_vivienda BETWEEN 1 AND 5),
    CONSTRAINT chk_tiene_energia
        CHECK (tiene_energia_electrica IN (1, 2))
);

COMMENT ON TABLE hechos_analisis IS
    'ECV 2024 (DANE) - Datos de la vivienda. Una fila = una vivienda de Bucaramanga con estrato 1-6.';

COMMENT ON COLUMN hechos_analisis.id_departamento IS
    'Original: P1_DEPARTAMENTO. Llave foránea a dim_departamento. Debe corresponder al departamento del municipio asociado.';

COMMENT ON COLUMN hechos_analisis.id_municipio IS
    'Originales: P1_DEPARTAMENTO + P1_MUNICIPIO (68 + 001 = Bucaramanga). Mismo id_municipio de hechos_violencia (dim_municipio -> dim_departamento): sirve para unir ambos hechos.';

COMMENT ON COLUMN hechos_analisis.zona_vivienda IS
    'Original: CLASE. Zona de la vivienda según el DANE: 1 = cabecera; 2 = centro poblado / corregimiento; 3 = rural disperso.';

COMMENT ON COLUMN hechos_analisis.tipo_vivienda IS
    'Original: P1070. 1 = Casa; 2 = Apartamento; 3 = Cuarto(s); 4 = Vivienda tradicional indígena; 5 = Otro.';

COMMENT ON COLUMN hechos_analisis.tiene_energia_electrica IS
    'Original: P8520S1. 1 = Sí cuenta; 2 = No cuenta (significado deducido, no verificado individualmente en el catálogo DANE).';

COMMENT ON COLUMN hechos_analisis.id_estrato IS
    'Original: P8520S1A1. Estrato 1-6 del recibo de energía (FK a dim_estrato). Los códigos 0, 8, 9 y vacío no se cargan.';

-- ------------------------------------------------------------
-- ÍNDICES para consultas y gráficos
-- ------------------------------------------------------------
CREATE INDEX idx_analisis_departamento ON hechos_analisis(id_departamento);
CREATE INDEX idx_analisis_municipio    ON hechos_analisis(id_municipio);
CREATE INDEX idx_analisis_estrato      ON hechos_analisis(id_estrato);
CREATE INDEX idx_analisis_tipo         ON hechos_analisis(tipo_vivienda);
CREATE INDEX idx_analisis_zona         ON hechos_analisis(zona_vivienda);

-- ------------------------------------------------------------
-- EJEMPLOS (para cuando la tabla tenga datos)
-- ------------------------------------------------------------

-- Viviendas por estrato
-- SELECT e.id_estrato, e.nombre_estrato, COUNT(*) AS viviendas
-- FROM hechos_analisis h
-- JOIN dim_estrato e ON h.id_estrato = e.id_estrato
-- GROUP BY e.id_estrato, e.nombre_estrato
-- ORDER BY e.id_estrato;

-- Tipo de vivienda por nivel de estrato
-- SELECT e.nivel_estrato, h.tipo_vivienda, COUNT(*) AS viviendas
-- FROM hechos_analisis h
-- JOIN dim_estrato e ON h.id_estrato = e.id_estrato
-- GROUP BY e.nivel_estrato, h.tipo_vivienda
-- ORDER BY e.nivel_estrato, h.tipo_vivienda;

-- Departamento y municipio de las viviendas
-- SELECT d.nombre_departamento, m.nombre_municipio, m.area, COUNT(*) AS viviendas
-- FROM hechos_analisis h
-- JOIN dim_departamento d ON h.id_departamento = d.id_departamento
-- JOIN dim_municipio m ON h.id_municipio = m.id_municipio
-- GROUP BY d.nombre_departamento, m.nombre_municipio, m.area;

-- UNIÓN CON VIOLENCIA a nivel municipio (sin importar el área):
-- viviendas con estrato vs. casos de violencia en el mismo municipio
-- WITH viv AS (
--     SELECT h.id_departamento, m.nombre_municipio, COUNT(*) AS viviendas
--     FROM hechos_analisis h
--     JOIN dim_municipio m ON h.id_municipio = m.id_municipio
--     GROUP BY h.id_departamento, m.nombre_municipio
-- ),
-- vio AS (
--     SELECT m.id_departamento, m.nombre_municipio, COUNT(*) AS casos
--     FROM hechos_violencia h
--     JOIN dim_municipio m ON h.id_municipio = m.id_municipio
--     GROUP BY m.id_departamento, m.nombre_municipio
-- )
-- SELECT d.nombre_departamento, viv.nombre_municipio, viv.viviendas, vio.casos
-- FROM viv
-- JOIN vio USING (id_departamento, nombre_municipio)
-- JOIN dim_departamento d ON d.id_departamento = viv.id_departamento;
