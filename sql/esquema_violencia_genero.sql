-- ============================================================
-- ESQUEMA (estrella + jerarquía geográfica normalizada)
-- Violencia de género contra mujeres (Colombia)
-- Proyecto ETL - ODS 5 (Igualdad de Género)
-- Jerarquía: pais -> departamento -> municipio
-- ============================================================

-- ------------------------------------------------------------
-- NIVEL 1: País
-- ------------------------------------------------------------
CREATE TABLE dim_pais (
    id_pais         SERIAL PRIMARY KEY,
    nombre_pais     VARCHAR(60) NOT NULL UNIQUE DEFAULT 'Colombia'
);

-- ------------------------------------------------------------
-- NIVEL 2: Departamento
-- ------------------------------------------------------------
CREATE TABLE dim_departamento (
    id_departamento     SERIAL PRIMARY KEY,
    id_pais             INT NOT NULL REFERENCES dim_pais(id_pais),
    nombre_departamento VARCHAR(60) NOT NULL,
    CONSTRAINT uq_departamento UNIQUE (id_pais, nombre_departamento)
);

-- ------------------------------------------------------------
-- NIVEL 3: Municipio
-- ------------------------------------------------------------
CREATE TABLE dim_municipio (
    id_municipio        SERIAL PRIMARY KEY,
    id_departamento     INT NOT NULL REFERENCES dim_departamento(id_departamento),
    nombre_municipio    VARCHAR(60) NOT NULL,
    area                VARCHAR(30),          -- Cabecera municipal / Centro poblado / Rural disperso
    CONSTRAINT uq_municipio UNIQUE (id_departamento, nombre_municipio, area)
);

-- ------------------------------------------------------------
-- DIMENSIÓN: Tiempo
-- ------------------------------------------------------------
CREATE TABLE dim_tiempo (
    id_tiempo   SERIAL PRIMARY KEY,
    year        INT NOT NULL,
    mes         INT NOT NULL,
    semana      INT NOT NULL,
    CONSTRAINT uq_tiempo UNIQUE (year, mes, semana)
);

-- ------------------------------------------------------------
-- DIMENSIÓN: Víctima
-- ------------------------------------------------------------
CREATE TABLE dim_victima (
    id_victima              SERIAL PRIMARY KEY,
    grupo_edad              VARCHAR(60),
    ciclo_vida              VARCHAR(60),
    tipo_seguridad_social   VARCHAR(60),        -- C, S, P, E, N, I
    actividad               VARCHAR(60),
    CONSTRAINT uq_victima UNIQUE (grupo_edad, ciclo_vida, tipo_seguridad_social, actividad)
);

-- ------------------------------------------------------------
-- DIMENSIÓN: Agresor
-- ------------------------------------------------------------
CREATE TABLE dim_agresor (
    id_agresor          SERIAL PRIMARY KEY,
    edad_agre           INT CHECK (edad_agre BETWEEN 10 AND 99),
    sexo_agre           VARCHAR(20),
    parentezco_vict     VARCHAR(60),
    CONSTRAINT uq_agresor UNIQUE (edad_agre, sexo_agre, parentezco_vict)
);

-- ------------------------------------------------------------
-- DIMENSIÓN: Tipo de violencia
-- ------------------------------------------------------------
CREATE TABLE dim_tipo_violencia (
    id_tipo_violencia  SERIAL PRIMARY KEY,
    naturaleza         INT NOT NULL,
    def_naturaleza     VARCHAR(60) NOT NULL,
    CONSTRAINT uq_tipo_violencia UNIQUE (naturaleza)
);

-- ------------------------------------------------------------
-- TABLA DE HECHOS (se conecta directo a municipio;
-- departamento y pais se obtienen por JOIN en cascada)
-- ------------------------------------------------------------
CREATE TABLE hechos_violencia (
    id_hecho            SERIAL PRIMARY KEY,
    id_municipio        INT NOT NULL REFERENCES dim_municipio(id_municipio),
    id_tiempo           INT NOT NULL REFERENCES dim_tiempo(id_tiempo),
    id_victima          INT NOT NULL REFERENCES dim_victima(id_victima),
    id_agresor          INT NOT NULL REFERENCES dim_agresor(id_agresor),
    id_tipo_violencia   INT NOT NULL REFERENCES dim_tipo_violencia(id_tipo_violencia),
    escenario           VARCHAR(60),
    zona_conf           VARCHAR(60),
    con_fin             VARCHAR(20),          -- Vivo / Muerto / No sabe (detecta feminicidios)
    pac_hos             BOOLEAN,
    sust_vict           BOOLEAN,
    fecha_hecho         DATE,
    hora_hecho          TIME,
    fuente              VARCHAR(30) NOT NULL DEFAULT 'SIVIGILA_Colombia'
                        -- 'SIVIGILA_Colombia' o 'Historico_Bucaramanga'
);

-- ------------------------------------------------------------
-- ÍNDICES para consultas futuras rápidas
-- ------------------------------------------------------------
CREATE INDEX idx_hechos_municipio  ON hechos_violencia(id_municipio);
CREATE INDEX idx_hechos_tiempo     ON hechos_violencia(id_tiempo);
CREATE INDEX idx_hechos_fuente     ON hechos_violencia(fuente);
CREATE INDEX idx_departamento_pais ON dim_departamento(id_pais);
CREATE INDEX idx_municipio_depto   ON dim_municipio(id_departamento);
CREATE INDEX idx_tiempo_year       ON dim_tiempo(year);

-- ------------------------------------------------------------
-- EJEMPLO: filtro Santander / Bucaramanga con este modelo
-- ------------------------------------------------------------
-- SELECT h.*
-- FROM hechos_violencia h
-- JOIN dim_municipio m ON h.id_municipio = m.id_municipio
-- JOIN dim_departamento d ON m.id_departamento = d.id_departamento
-- WHERE d.nombre_departamento = 'Santander';

-- SELECT h.*
-- FROM hechos_violencia h
-- JOIN dim_municipio m ON h.id_municipio = m.id_municipio
-- WHERE m.nombre_municipio = 'Bucaramanga';