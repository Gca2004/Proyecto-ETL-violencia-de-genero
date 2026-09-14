

CREATE TABLE raw_violencia_genero (
    id_raw                  SERIAL PRIMARY KEY,
    orden                   TEXT,
    departamento            TEXT,
    municipio               TEXT,
    semana                  TEXT,
    year                    TEXT,
    grupo_edad              TEXT,
    ciclo_vida              TEXT,
    sexo                    TEXT,
    area                    TEXT,
    barrio                  TEXT,
    comuna                  TEXT,
    tipo_seguridad_social   TEXT,
    pac_hos                 TEXT,
    con_fin                 TEXT,
    version                 TEXT,
    naturaleza              TEXT,
    def_naturaleza          TEXT,
    actividad               TEXT,
    nom_actividad           TEXT,
    edad_agre               TEXT,
    sexo_agre               TEXT,
    parentezco_vict         TEXT,
    sust_vict               TEXT,
    fec_hecho               TEXT,
    hora_hecho              TEXT,
    escenario               TEXT,
    zona_conf               TEXT,
    nom_eve                 TEXT,
    nom_upgd                TEXT,
    ndep_resi               TEXT,
    nmun_resi               TEXT,
    mes                     TEXT,
    fuente                  VARCHAR(30) NOT NULL DEFAULT 'SIVIGILA_Colombia',
                            -- 'SIVIGILA_Colombia' o 'Historico_Bucaramanga'
    fecha_carga             TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_raw_fuente ON raw_violencia_genero(fuente);

-- ------------------------------------------------------------
-- MAPEO DE COLUMNAS: encabezado original del CSV -> columna raw
-- (Pásale esto a Leidy para su script de ingesta)
-- ------------------------------------------------------------
-- Orden                    -> orden
-- Departamento             -> departamento
-- Municipio                -> municipio
-- semana                   -> semana
-- año                      -> year
-- Grupo edad               -> grupo_edad
-- Ciclo de vida            -> ciclo_vida
-- sexo_                    -> sexo
-- area_                    -> area
-- Barrio                   -> barrio
-- Comuna                   -> comuna
-- Tipo de Seguridad Social -> tipo_seguridad_social
-- pac_hos_                 -> pac_hos
-- con_fin_                 -> con_fin
-- version                  -> version
-- naturaleza               -> naturaleza
-- def_naturaleza           -> def_naturaleza
-- actividad                -> actividad
-- nom_actividad            -> nom_actividad
-- edad_agre                -> edad_agre
-- sexo_agre                -> sexo_agre
-- parentezco_vict          -> parentezco_vict
-- sust_vict                -> sust_vict
-- fec_hecho                -> fec_hecho
-- hora_hecho               -> hora_hecho
-- escenario                -> escenario
-- zona_conf                -> zona_conf
-- nom_eve                  -> nom_eve
-- nom_upgd                 -> nom_upgd
-- ndep_resi                -> ndep_resi
-- nmun_resi                -> nmun_resi
-- MES                      -> mes
-- (fuente y fecha_carga las agrega Leidy en el DataFrame antes de subir,
--  o se dejan con su valor DEFAULT si todo el CSV es de la misma fuente)