"""
etl/validacion.py
Validación de calidad con Great Expectations (GX Core 1.x) en dos puntos del DAG:
  1. validar_extraccion_raw(): justo después de la extracción (sobre raw_*),
     antes de ejecutar cualquier transformación.
  2. validar_staging(): sobre la zona de staging (stg_*), antes de cargar al
     modelo final.

Flujo en el DAG:
    extracciones -> validar_raw -> transformaciones -> validar_calidad -> cargar_fuentes_nuevas

- Cada resultado se registra en:
  - la tabla log_validaciones de PostgreSQL (una fila por expectativa), y
  - un archivo JSON en data/validaciones/ (uno por ejecución).
- Si alguna expectativa falla, lanza ValueError: la tarea de Airflow queda en
  rojo y las tareas posteriores NO se ejecutan.

Uso manual (desde la raíz del proyecto):
    python etl/validacion.py
"""

import json
import logging
import os
import re
from datetime import datetime
from pathlib import Path

os.environ.setdefault("GX_ANALYTICS_ENABLED", "false")   # sin envío de estadísticas de uso

import great_expectations as gx
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

try:
    from etl import homologacion as hom
except ImportError:
    import homologacion as hom

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s")
logger = logging.getLogger("ETL_Validacion")

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ_PROYECTO / ".env")
CARPETA_RESULTADOS = RAIZ_PROYECTO / "data" / "validaciones"

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "violencia_genero_db")

ANIO_ACTUAL = datetime.now().year
TURNOS = ["MAÑANA", "TARDE", "NOCHE", "NO REPORTADO"]
AREAS = hom.AREAS + [hom.NO_REPORTADO]
IDS_COMUNA = [0] + list(range(1, 18)) + [101, 102, 103]


# ---------------------------------------------------------------------------
# Helpers de definición de reglas
# ---------------------------------------------------------------------------
def _no_nulos(columnas):
    return [("ExpectColumnValuesToNotBeNull", {"column": c}, f"{c} siempre tiene valor") for c in columnas]


def _existen_columnas(columnas):
    return [("ExpectColumnToExist", {"column": c}, f"La columna {c} existe en el esquema crudo") for c in columnas]


# ---------------------------------------------------------------------------
# 1. Suites de Validación Temprana (Post-Extracción / Zona RAW)
# ---------------------------------------------------------------------------
SUITES_RAW = {
    "raw_suite_sivigila": {
        "tabla": "raw_violencia_genero",
        "expectativas": [
            ("ExpectTableRowCountToBeBetween", {"min_value": 1000, "max_value": 200000},
             "raw_violencia_genero debe tener registros cargados de SIVIGILA/Histórico"),
            *_existen_columnas(["departamento", "municipio", "year", "mes", "sexo", "ciclo_vida", "parentezco_vict", "fuente"]),
            *_no_nulos(["fuente"]),
            ("ExpectColumnValuesToBeInSet", {"column": "fuente", "value_set": ["SIVIGILA_Colombia", "Historico_Bucaramanga"]},
             "Solo fuentes válidas de SIVIGILA en raw_violencia_genero"),
        ],
    },
    "raw_suite_api": {
        "tabla": "raw_api_violencia",
        "expectativas": [
            ("ExpectTableRowCountToBeBetween", {"min_value": 1000, "max_value": 100000},
             "La API Medicina Legal debe traer registros de Bucaramanga y muestra nacional"),
            *_existen_columnas([
                "id", "a_o_del_hecho", "mes_del_hecho", "sexo_de_la_victima", "ciclo_vital",
                "codigo_dane_municipio", "escenario_del_hecho", "contexto_del_hecho",
                "rango_de_hora_del_hecho_x_3_horas", "dias_de_incapacidad_medicolegal",
                "presunto_agresor_detallado", "alcance"
            ]),
            *_no_nulos(["id", "alcance"]),
            ("ExpectColumnValuesToBeInSet", {"column": "alcance", "value_set": ["BUCARAMANGA", "MUESTRA_NACIONAL"]},
             "El alcance crudo debe ser BUCARAMANGA o MUESTRA_NACIONAL"),
            ("ExpectColumnValuesToMatchRegex", {"column": "a_o_del_hecho", "regex": r"^\d{4}$"},
             "El año crudo de la API debe venir en formato de 4 dígitos"),
        ],
    },
    "raw_suite_vivienda": {
        "tabla": "raw_vivienda",
        "expectativas": [
            ("ExpectTableRowCountToBeBetween", {"min_value": 500, "max_value": 500000},
             "raw_vivienda debe contener el archivo de ECV 2024"),
            *_existen_columnas(["p1_departamento", "p1_municipio", "clase", "p1070", "p8520s1", "p8520s1a1"]),
            *_no_nulos(["p1_departamento", "p1_municipio"]),
            ("ExpectColumnDistinctValuesToContainSet", {"column": "p1_departamento", "value_set": ["68"]},
             "Debe existir Santander (68) en raw_vivienda"),
        ],
    },
    "raw_suite_scraping_boletines": {
        "tabla": "raw_scraping_boletines",
        "expectativas": [
            ("ExpectTableRowCountToBeBetween", {"min_value": 1, "max_value": 500},
             "Debe existir al menos un boletín descargado de OSEC"),
            *_existen_columnas(["periodo", "nombre_boletin", "url_pdf", "estado_extraccion"]),
            *_no_nulos(["periodo"]),
            ("ExpectColumnValuesToBeUnique", {"column": "periodo"}, "Un registro único por periodo de boletín"),
            ("ExpectColumnValuesToMatchRegex", {"column": "periodo", "regex": r"^\d{6}$"},
             "El periodo del boletín debe tener formato AAAAMM"),
        ],
    },
    "raw_suite_scraping_comunas": {
        "tabla": "raw_scraping_comunas",
        "expectativas": [
            ("ExpectTableRowCountToBeBetween", {"min_value": 42, "max_value": 50000},
             "Al menos un boletín completo con 21 comunas x 2 delitos"),
            *_existen_columnas(["periodo", "delito", "bloque", "comuna", "casos_anio_actual", "anio_actual"]),
            *_no_nulos(["periodo", "delito", "bloque", "comuna"]),
            ("ExpectColumnValuesToMatchRegex", {"column": "periodo", "regex": r"^\d{6}$"},
             "Periodo en formato AAAAMM"),
        ],
    },
}


# ---------------------------------------------------------------------------
# 2. Suites de Validación Final (Pre-Carga DW / Zona STAGING)
# ---------------------------------------------------------------------------
SUITES = {
    "suite_hechos_api": {
        "tabla": "stg_hechos_api",
        "expectativas": [
            ("ExpectTableRowCountToBeBetween", {"min_value": 500, "max_value": 20000},
             "La API trae del orden de 2.000 casos de mujeres en Bucaramanga; muy pocos indicaría una consulta fallida"),
            *_no_nulos(["id_caso_api", "anio", "mes", "area", "ciclo_vida", "grupo_agresor", "turno", "dias_incapacidad"]),
            ("ExpectColumnValuesToBeUnique", {"column": "id_caso_api"}, "Cada caso de la API aparece una sola vez"),
            ("ExpectColumnValuesToBeBetween", {"column": "anio", "min_value": 2015, "max_value": ANIO_ACTUAL},
             "El conjunto cubre desde 2015 y no puede tener años futuros"),
            ("ExpectColumnValuesToBeBetween", {"column": "mes", "min_value": 1, "max_value": 12},
             "El mes homologado debe ser un mes válido"),
            ("ExpectColumnValuesToBeInSet", {"column": "area", "value_set": AREAS},
             "El área debe coincidir con las áreas de dim_municipio"),
            ("ExpectColumnValuesToBeInSet", {"column": "ciclo_vida", "value_set": hom.CICLOS_VIDA + [hom.NO_REPORTADO]},
             "Ciclo de vida homologado con SIVIGILA"),
            ("ExpectColumnValuesToBeInSet", {"column": "grupo_agresor", "value_set": hom.GRUPOS_AGRESOR},
             "Grupo de agresor homologado con SIVIGILA"),
            ("ExpectColumnValuesToNotBeInSet", {"column": "grupo_agresor", "value_set": [hom.NO_REPORTADO], "mostly": 0.9},
             "Al menos el 90 % de los casos debe tener agresor clasificado: si no, la homologación dejó de reconocer categorías"),
            ("ExpectColumnValuesToBeInSet", {"column": "turno", "value_set": TURNOS},
             "Turno con los mismos 4 valores del CHECK de la base de datos"),
            ("ExpectColumnValuesToBeInSet", {"column": "dias_incapacidad", "value_set": hom.RANGOS_INCAPACIDAD},
             "Días de incapacidad en los 5 rangos definidos"),
        ],
    },
    "suite_hechos_osec": {
        "tabla": "stg_hechos_osec",
        "expectativas": [
            ("ExpectTableRowCountToBeBetween", {"min_value": 42, "max_value": 20000},
             "Al menos un mes completo: 21 comunas/corregimientos x 2 delitos"),
            *_no_nulos(["id_comuna", "anio", "mes", "delito", "casos", "publicado_en"]),
            ("ExpectCompoundColumnsToBeUnique", {"column_list": ["id_comuna", "anio", "mes", "delito"]},
             "Un solo valor por comuna, mes y delito (se usa la cifra del boletín más reciente)"),
            ("ExpectColumnValuesToBeInSet", {"column": "id_comuna", "value_set": IDS_COMUNA},
             "Toda comuna existe en dim_comuna"),
            ("ExpectColumnValuesToBeInSet", {"column": "delito", "value_set": ["VIOLENCIA INTRAFAMILIAR", "DELITOS SEXUALES"]},
             "Solo los dos delitos que se extraen de los boletines"),
            ("ExpectColumnValuesToBeBetween", {"column": "casos", "min_value": 0},
             "No existen casos negativos"),
            ("ExpectColumnValuesToBeBetween", {"column": "mes", "min_value": 1, "max_value": 12},
             "Mes válido"),
            ("ExpectColumnValuesToBeBetween", {"column": "anio", "min_value": 2024, "max_value": ANIO_ACTUAL},
             "Los boletines empiezan en 2025 y traen el año anterior (2024)"),
            ("ExpectColumnValuesToMatchRegex", {"column": "publicado_en", "regex": r"^\d{6}$"},
             "Periodo del boletín en formato AAAAMM"),
        ],
    },
    "suite_hechos_analisis": {
        "tabla": "stg_hechos_analisis",
        "expectativas": [
            ("ExpectTableRowCountToBeBetween", {"min_value": 100, "max_value": 5000},
             "La ECV 2024 trae del orden de 400 viviendas de Bucaramanga"),
            *_no_nulos(["area", "zona_vivienda", "tipo_vivienda", "tiene_energia_electrica", "id_estrato"]),
            ("ExpectColumnValuesToBeBetween", {"column": "id_estrato", "min_value": 1, "max_value": 6},
             "Solo estratos 1 a 6 (los códigos 0, 8 y 9 no son estratos)"),
            ("ExpectColumnValuesToBeInSet", {"column": "zona_vivienda", "value_set": [1, 2, 3]},
             "CLASE del DANE: 1 cabecera, 2 centro poblado, 3 rural disperso"),
            ("ExpectColumnValuesToBeBetween", {"column": "tipo_vivienda", "min_value": 1, "max_value": 5},
             "P1070: tipos de vivienda 1 a 5"),
            ("ExpectColumnValuesToBeInSet", {"column": "tiene_energia_electrica", "value_set": [1]},
             "Toda vivienda con estrato tiene energía (el estrato sale del recibo de luz)"),
            ("ExpectColumnValuesToBeInSet", {"column": "area", "value_set": hom.AREAS},
             "El área debe existir en dim_municipio"),
        ],
    },
    "suite_codigo_dane": {
        "tabla": "stg_codigo_dane",
        "expectativas": [
            ("ExpectTableRowCountToBeBetween", {"min_value": 1},
             "Al menos Bucaramanga tiene código DANE"),
            ("ExpectColumnValuesToMatchRegex", {"column": "codigo_dane", "regex": r"^\d{5}$"},
             "Código DANE de 5 dígitos como texto (conserva el cero inicial)"),
            ("ExpectColumnDistinctValuesToContainSet", {"column": "codigo_dane", "value_set": ["68001"]},
             "Bucaramanga (68001) siempre debe quedar identificada: es la llave del merge"),
        ],
    },
    "suite_dataset_unificado": {
        "tabla": "stg_dataset_unificado",
        "expectativas": [
            ("ExpectTableRowCountToBeBetween", {"min_value": 100, "max_value": 400},
             "Serie mensual desde 2015: alrededor de 140 meses"),
            *_no_nulos(["anio", "mes", "fecha_mes", "fuentes_con_dato"]),
            ("ExpectCompoundColumnsToBeUnique", {"column_list": ["anio", "mes"]},
             "Una sola fila por mes"),
            ("ExpectColumnValuesToBeBetween", {"column": "anio", "min_value": 2015, "max_value": ANIO_ACTUAL},
             "El conjunto empieza en 2015"),
            ("ExpectColumnValuesToBeBetween", {"column": "fuentes_con_dato", "min_value": 1, "max_value": 3},
             "Todo mes tiene al menos una fuente de casos"),
            *[("ExpectColumnValuesToBeBetween", {"column": c, "min_value": 0}, f"{c} no puede ser negativo")
              for c in ["siv_casos_total", "siv_casos_mujeres", "api_casos_mujeres", "osec_denuncias_vif", "osec_denuncias_sexuales"]],
            ("ExpectColumnPairValuesAToBeGreaterThanB",
             {"column_A": "siv_casos_total", "column_B": "siv_casos_mujeres", "or_equal": True,
              "ignore_row_if": "either_value_is_missing"},
             "Los casos de mujeres no pueden superar el total de casos del mes"),
            ("ExpectColumnPairValuesAToBeGreaterThanB",
             {"column_A": "siv_casos_mujeres", "column_B": "siv_mujeres_agresor_pareja", "or_equal": True,
              "ignore_row_if": "either_value_is_missing"},
             "Los casos con agresor pareja son una parte de los casos de mujeres"),
            ("ExpectColumnValuesToBeBetween", {"column": "ecv_pct_estrato_1_2", "min_value": 0, "max_value": 100},
             "Porcentaje válido"),
        ],
    },
}


# ---------------------------------------------------------------------------
# Base de datos
# ---------------------------------------------------------------------------
def obtener_motor_db():
    if not DB_PASSWORD:
        raise RuntimeError(f"Falta DB_PASSWORD en {RAIZ_PROYECTO / '.env'}")
    url = URL.create("postgresql+psycopg2", username=DB_USER, password=DB_PASSWORD,
                     host=DB_HOST, port=int(DB_PORT), database=DB_NAME)
    return create_engine(url)


# ---------------------------------------------------------------------------
# Great Expectations
# ---------------------------------------------------------------------------
def _resumir(valor):
    """Valor observado en texto corto (para el log)."""
    if valor is None:
        return None
    texto = json.dumps(valor, default=str, ensure_ascii=False)
    return texto if len(texto) <= 300 else texto[:297] + "..."


def _snake(nombre: str) -> str:
    """ExpectColumnValuesToBeInSet -> expect_column_values_to_be_in_set"""
    return re.sub(r"(?<!^)(?=[A-Z])", "_", nombre).lower()


def _emparejar(resultados, reglas):
    """
    Devuelve [(resultado, regla)] emparejando por tipo de expectativa y columna,
    para no depender del orden en que GX entrega los resultados.
    """
    pendientes = list(reglas)
    pares = []
    for i, r in enumerate(resultados):
        cfg = getattr(r, "expectation_config", None)
        elegida = None
        if cfg is not None:
            tipo = getattr(cfg, "type", None) or getattr(cfg, "expectation_type", None)
            kw = dict(getattr(cfg, "kwargs", {}) or {})
            for regla in pendientes:
                t, args, _ = regla
                if _snake(t) == tipo and all(kw.get(k) == args.get(k) for k in ("column", "column_list", "column_A", "column_B")
                                             if k in args):
                    elegida = regla
                    break
        if elegida is None:                      # respaldo: mismo orden en que se agregaron
            elegida = pendientes[0]
        pendientes.remove(elegida)
        pares.append((r, elegida))
    return pares


def validar_tabla(contexto, nombre_suite: str, definicion: dict, df: pd.DataFrame) -> list:
    """Ejecuta una suite de GX sobre un DataFrame y devuelve una fila de log por expectativa."""
    fuente = contexto.data_sources.add_pandas(name=f"fuente_{nombre_suite}")
    activo = fuente.add_dataframe_asset(name=definicion["tabla"])
    lote = activo.add_batch_definition_whole_dataframe(name=f"lote_{nombre_suite}")

    suite = contexto.suites.add(gx.ExpectationSuite(name=nombre_suite))
    for tipo, argumentos, _ in definicion["expectativas"]:
        suite.add_expectation(getattr(gx.expectations, tipo)(**argumentos))

    resultado = lote.get_batch(batch_parameters={"dataframe": df}).validate(suite)

    filas = []
    for r, (tipo, argumentos, descripcion) in _emparejar(resultado.results, definicion["expectativas"]):
        detalle = r.result or {}
        filas.append({
            "suite": nombre_suite,
            "tabla": definicion["tabla"],
            "expectativa": tipo,
            "columna": argumentos.get("column") or ", ".join(argumentos.get("column_list", [])) or
                       " vs ".join(filter(None, [argumentos.get("column_A"), argumentos.get("column_B")])) or None,
            "exito": bool(r.success),
            "valor_observado": _resumir(detalle.get("observed_value")),
            "filas_inesperadas": detalle.get("unexpected_count"),
            "porcentaje_inesperado": detalle.get("unexpected_percent"),
            "ejemplos_inesperados": _resumir(detalle.get("partial_unexpected_list")),
            "descripcion": descripcion,
        })
    return filas


def _ejecutar_suites(suites_dict: dict, prefijo_archivo: str, fase: str) -> dict:
    """Motor común de ejecución, persistencia de logs y detención en caso de fallo."""
    inicio = datetime.now()
    contexto = gx.get_context(mode="ephemeral")
    engine = obtener_motor_db()
    try:
        filas = []
        for nombre_suite, definicion in suites_dict.items():
            with engine.connect() as conn:
                df = pd.read_sql(text(f"SELECT * FROM {definicion['tabla']}"), conn)
            filas_suite = validar_tabla(contexto, nombre_suite, definicion, df)
            ok = sum(f["exito"] for f in filas_suite)
            logger.info("[%s] %-28s %-24s %d filas  -> %d/%d expectativas cumplidas",
                        fase, nombre_suite, definicion["tabla"], len(df), ok, len(filas_suite))
            for f in filas_suite:
                if not f["exito"]:
                    logger.error("   FALLA %s (%s): %s | observado=%s | inesperadas=%s | ej=%s",
                                 f["expectativa"], f["columna"], f["descripcion"], f["valor_observado"],
                                 f["filas_inesperadas"], f["ejemplos_inesperados"])
            filas.extend(filas_suite)

        log = pd.DataFrame(filas)
        log.insert(0, "fecha_ejecucion", inicio)
        log["filas_inesperadas"] = pd.to_numeric(log["filas_inesperadas"], errors="coerce").round().astype("Int64")
        log["porcentaje_inesperado"] = pd.to_numeric(log["porcentaje_inesperado"], errors="coerce").round(3)
        exito_total = bool(log["exito"].all())

        # Registro 1: tabla en PostgreSQL
        with engine.begin() as conn:
            log.to_sql("log_validaciones", conn, if_exists="append", index=False, method="multi", chunksize=200)

        # Registro 2: archivo JSON
        CARPETA_RESULTADOS.mkdir(parents=True, exist_ok=True)
        archivo = CARPETA_RESULTADOS / f"{prefijo_archivo}_{inicio:%Y%m%d_%H%M%S_%f}.json"
        archivo.write_text(json.dumps({
            "fase": fase,
            "fecha_ejecucion": inicio.isoformat(),
            "exito": exito_total,
            "expectativas": int(len(log)),
            "fallidas": int((~log["exito"]).sum()),
            "resultados": log.drop(columns="fecha_ejecucion").to_dict("records"),
        }, ensure_ascii=False, indent=2, default=str), encoding="utf-8")

        resumen = {
            "fase": fase,
            "exito": exito_total,
            "expectativas": int(len(log)),
            "cumplidas": int(log["exito"].sum()),
            "fallidas": int((~log["exito"]).sum()),
            "archivo": archivo.name,
        }
        logger.info("Resultado de la validación [%s]: %s", fase, resumen)
        if not exito_total:
            fallidas = log.loc[~log["exito"], ["suite", "expectativa", "columna"]].to_dict("records")
            raise ValueError(f"Validación [{fase}] FALLIDA ({resumen['fallidas']} expectativas). "
                             f"Se detiene el pipeline. Detalle: {fallidas}")
        return resumen
    finally:
        engine.dispose()


def validar_extraccion_raw() -> dict:
    """Valida tablas raw_* post-extracción; falla inmediatamente si hay datos corruptos."""
    return _ejecutar_suites(SUITES_RAW, prefijo_archivo="validacion_raw", fase="RAW_POST_EXTRACCION")


def validar_staging() -> dict:
    """Valida todas las tablas stg_*; registra los resultados y falla si alguna regla no se cumple."""
    return _ejecutar_suites(SUITES, prefijo_archivo="validacion_stg", fase="STAGING_PRE_CARGA")


if __name__ == "__main__":
    print("--- Validando RAW ---")
    print(validar_extraccion_raw())
    print("--- Validando STAGING ---")
    print(validar_staging())