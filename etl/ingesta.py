"""
etl/ingesta.py  (segunda entrega)
Extracción de las 4 fuentes y carga a sus tablas CRUDAS en PostgreSQL.

    Fuente 1  CSV SIVIGILA (entrega 1)      -> raw_violencia_genero
    Fuente 2  API Medicina Legal            -> raw_api_violencia
    Fuente 3  CSV vivienda ECV 2024 (DANE)  -> raw_vivienda
    Fuente 4  Scraping OSEC Bucaramanga     -> raw_scraping_boletines + raw_scraping_comunas

Reglas
------
* La capa cruda guarda los datos TAL CUAL llegan (todo como texto): no se
  limpia, no se filtra por sexo ni por estrato. Eso lo hace la transformación.
* Cada carga es IDEMPOTENTE: si se ejecuta dos veces el mismo día no duplica
  datos. Las fuentes 1, 2 y 3 se reemplazan completas (borrar + insertar en UNA
  transacción); la fuente 4 es incremental (solo agrega boletines nuevos).
* Cada fuente tiene su propia función "tarea_*". Esas son las funciones que
  llamará Airflow, una por cada tarea del DAG.

Requisito: haber ejecutado sql/raw_violencia_genero.sql y sql/raw_fuentes_entrega2.sql

Uso:
    python etl/ingesta.py                      (las 4 fuentes)
    python etl/ingesta.py --fuente api         (solo una: sivigila | api | vivienda | scraping)
"""

import argparse
import logging
import os
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

try:  # Airflow / python -m etl.ingesta (raíz del proyecto en el PYTHONPATH)
    from etl.extraer_scraping import extraer_scraping_osec
except ImportError:  # python etl/ingesta.py
    from extraer_scraping import extraer_scraping_osec

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s")
logger = logging.getLogger("ETL_Ingesta")

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
CARPETA_DATOS = RAIZ_PROYECTO / "data"
load_dotenv(RAIZ_PROYECTO / ".env")

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "violencia_genero_db")

# ---------------------------------------------------------------------------
# Configuración de las fuentes
# ---------------------------------------------------------------------------
# Fuente 1: CSV de la primera entrega
NOMBRES_CSV_SIVIGILA = ["bucaramanga_violencia.csv.csv", "bucaramanga_violencia.csv"]
FUENTE_SIVIGILA = "Historico_Bucaramanga"
COLUMN_MAP_SIVIGILA = {
    "Orden": "orden", "Departamento": "departamento", "Municipio": "municipio", "semana": "semana",
    "año": "year", "Grupo edad": "grupo_edad", "Ciclo de vida": "ciclo_vida", "sexo_": "sexo",
    "area_": "area", "Barrio": "barrio", "Comuna": "comuna", "Tipo de Seguridad Social": "tipo_seguridad_social",
    "pac_hos_": "pac_hos", "con_fin_": "con_fin", "version": "version", "naturaleza": "naturaleza",
    "def_naturaleza": "def_naturaleza", "actividad": "actividad", "nom_actividad": "nom_actividad",
    "edad_agre": "edad_agre", "sexo_agre": "sexo_agre", "parentezco_vict": "parentezco_vict",
    "sust_vict": "sust_vict", "fec_hecho": "fec_hecho", "hora_hecho": "hora_hecho", "escenario": "escenario",
    "zona_conf": "zona_conf", "nom_eve": "nom_eve", "nom_upgd": "nom_upgd", "ndep_resi": "ndep_resi",
    "nmun_resi": "nmun_resi", "MES": "mes",
}

# Fuente 2: API Medicina Legal (datos.gov.co)
URL_API = "https://www.datos.gov.co/resource/ers2-kerr.json"
CODIGO_BUCARAMANGA = "68001"
LIMITE_API = 50000          # máximo por consulta / tamaño de la muestra nacional
TAMANO_PAGINA = 10000
FUENTE_API = "API_Medicina_Legal"
COLUMNAS_API = [            # las 36 columnas del conjunto ers2-kerr (mismo orden que la tabla raw)
    "id", "a_o_del_hecho", "sexo_de_la_victima", "grupo_de_edad_quinquenal", "grupo_mayor_menor_de_edad",
    "grupo_de_edad_judicial", "ciclo_vital", "pais_de_nacimiento", "escolaridad", "estado_civil",
    "tipo_de_discapacidad", "pertenencia_etnica", "orientacion_sexual", "identidad_de_genero", "transgenero",
    "pertenencia_grupal", "mes_del_hecho", "dia_del_hecho", "rango_de_hora_del_hecho_x_3_horas",
    "codigo_dane_municipio", "municipio_del_hecho_dane", "departamento_del_hecho_dane",
    "codigo_dane_departamento", "localidad_del_hecho", "zona_del_hecho", "escenario_del_hecho",
    "actividad_durante_el_hecho", "circunstancia_del_hecho_detallada", "contexto_del_hecho",
    "mecanismo_causal_de_la_lesion_no_fatal", "diagnostico_topografico_de_la_lesion_no_fatal",
    "sexo_del_agresor", "presunto_agresor_detallado", "factor_desencadenante_de_la_agresion",
    "dias_de_incapacidad_medicolegal", "pueblo_indigena",
]

# Fuente 3: CSV de vivienda
COLUMNAS_VIVIENDA = ["P1_DEPARTAMENTO", "P1_MUNICIPIO", "CLASE", "P1070", "P8520S1", "P8520S1A1"]
FUENTE_VIVIENDA = "ECV_2024_DANE"

HEADERS = {"User-Agent": "Mozilla/5.0 (proyecto academico ETL)"}


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
def obtener_motor_db():
    if not DB_PASSWORD:
        raise RuntimeError(f"Falta DB_PASSWORD en {RAIZ_PROYECTO / '.env'}")
    url = URL.create("postgresql+psycopg2", username=DB_USER, password=DB_PASSWORD,
                     host=DB_HOST, port=int(DB_PORT), database=DB_NAME)
    return create_engine(url)


def buscar_archivo(nombres=None, contiene=None) -> Path:
    """Busca un CSV en data/ por nombre exacto o por una palabra (sin importar mayúsculas)."""
    for p in sorted(CARPETA_DATOS.rglob("*")):
        if not p.is_file() or "eda_cache" in p.parts:
            continue
        if nombres and p.name.lower() in [n.lower() for n in nombres]:
            return p
        if contiene and contiene in p.name.lower() and p.suffix.lower() == ".csv":
            return p
    raise FileNotFoundError(f"No se encontró en {CARPETA_DATOS} el archivo {nombres or ('*' + contiene + '*.csv')}")


def pedir(url, params=None, intentos=3, espera=5, timeout=120):
    """GET con reintentos."""
    ultimo_error = None
    for i in range(1, intentos + 1):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            ultimo_error = e
            logger.warning("Intento %d/%d falló (%s): %s", i, intentos, url, e)
            if i < intentos:
                time.sleep(espera)
    raise ultimo_error


def reemplazar_en_raw(df: pd.DataFrame, tabla: str, condicion: str, parametros: dict) -> int:
    """
    Borra las filas de 'tabla' que cumplen 'condicion' e inserta df, todo en UNA
    transacción: si algo falla, la tabla queda como estaba (no se pierden datos).
    """
    if df is None or df.empty:
        raise ValueError(f"No llegaron datos para '{tabla}'. No se borra nada.")
    engine = obtener_motor_db()
    try:
        with engine.begin() as conn:
            borradas = conn.execute(text(f"DELETE FROM {tabla} WHERE {condicion}"), parametros).rowcount
            df.to_sql(tabla, conn, if_exists="append", index=False, method="multi", chunksize=500)
            total = conn.execute(text(f"SELECT COUNT(*) FROM {tabla} WHERE {condicion}"), parametros).scalar()
        logger.info("'%s': %d filas anteriores reemplazadas por %d nuevas.", tabla, borradas, total)
        if total != len(df):
            raise RuntimeError(f"'{tabla}': se insertaron {len(df)} filas pero hay {total}")
        return int(total)
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Fuente 1: CSV SIVIGILA (primera entrega)
# ---------------------------------------------------------------------------
def extraer_sivigila() -> pd.DataFrame:
    ruta = buscar_archivo(nombres=NOMBRES_CSV_SIVIGILA)
    logger.info("Leyendo CSV SIVIGILA: %s", ruta.name)
    df = pd.read_csv(ruta, sep=",", encoding="utf-8-sig", dtype=str, keep_default_na=False, na_values=[""])
    faltantes = [c for c in COLUMN_MAP_SIVIGILA if c not in df.columns]
    if faltantes:
        raise ValueError(f"Columnas faltantes en el CSV SIVIGILA: {faltantes}")
    df = df.rename(columns=COLUMN_MAP_SIVIGILA)[list(COLUMN_MAP_SIVIGILA.values())]
    df = df.astype(object).where(df.notna(), None)
    df["fuente"] = FUENTE_SIVIGILA
    logger.info("SIVIGILA: %d filas leídas.", len(df))
    return df


def tarea_sivigila() -> int:
    df = extraer_sivigila()
    return reemplazar_en_raw(df, "raw_violencia_genero", "fuente = :f", {"f": FUENTE_SIVIGILA})


# ---------------------------------------------------------------------------
# Fuente 2: API Medicina Legal
# ---------------------------------------------------------------------------
def _a_tabla_api(registros: list, alcance: str) -> pd.DataFrame:
    df = pd.DataFrame(registros)
    extra = [c for c in df.columns if c not in COLUMNAS_API]
    if extra:
        logger.warning("La API trajo columnas nuevas que no están en la tabla raw (se ignoran): %s", extra)
    df = df.reindex(columns=COLUMNAS_API)          # columnas que no vinieron quedan vacías
    df = df.astype(object).where(df.notna(), None)
    df["alcance"] = alcance
    df["fuente"] = FUENTE_API
    return df


def extraer_api_bucaramanga() -> pd.DataFrame:
    """Todos los casos de Bucaramanga (filtro desde la consulta SoQL)."""
    params = {"$where": f"codigo_dane_municipio='{CODIGO_BUCARAMANGA}'", "$limit": LIMITE_API, "$order": "id"}
    registros = pedir(URL_API, params=params).json()
    if len(registros) >= LIMITE_API:
        logger.warning("Bucaramanga llegó al límite de %d registros: puede haber más casos.", LIMITE_API)
    logger.info("API Bucaramanga: %d registros.", len(registros))
    return _a_tabla_api(registros, "BUCARAMANGA")


def extraer_api_muestra_nacional() -> pd.DataFrame:
    """50.000 registros del país, años más recientes, en páginas de 10.000."""
    registros = []
    for offset in range(0, LIMITE_API, TAMANO_PAGINA):
        params = {"$limit": TAMANO_PAGINA, "$offset": offset, "$order": "a_o_del_hecho DESC, id"}
        lote = pedir(URL_API, params=params).json()
        logger.info("API nacional: página con offset %d -> %d registros", offset, len(lote))
        if not lote:
            break
        registros.extend(lote)
    return _a_tabla_api(registros, "MUESTRA_NACIONAL")


def tarea_api(incluir_muestra_nacional: bool = True) -> int:
    total = reemplazar_en_raw(extraer_api_bucaramanga(), "raw_api_violencia", "alcance = :a", {"a": "BUCARAMANGA"})
    if incluir_muestra_nacional:
        total += reemplazar_en_raw(extraer_api_muestra_nacional(), "raw_api_violencia", "alcance = :a",
                                   {"a": "MUESTRA_NACIONAL"})
    return total


# ---------------------------------------------------------------------------
# Fuente 3: CSV vivienda ECV 2024
# ---------------------------------------------------------------------------
def extraer_vivienda() -> pd.DataFrame:
    ruta = buscar_archivo(contiene="vivienda")
    logger.info("Leyendo CSV de vivienda: %s", ruta.name)
    df = pd.read_csv(ruta, sep=";", dtype=str, encoding="utf-8-sig", usecols=COLUMNAS_VIVIENDA)[COLUMNAS_VIVIENDA]
    df.columns = [c.lower() for c in df.columns]   # PostgreSQL usa minúsculas
    df = df.astype(object).where(df.notna(), None)
    df["archivo"] = ruta.name
    df["fuente"] = FUENTE_VIVIENDA
    logger.info("Vivienda: %d filas leídas (archivo nacional).", len(df))
    return df


def tarea_vivienda() -> int:
    return reemplazar_en_raw(extraer_vivienda(), "raw_vivienda", "fuente = :f", {"f": FUENTE_VIVIENDA})


# ---------------------------------------------------------------------------
# Fuente 4: Scraping OSEC (incremental)
# ---------------------------------------------------------------------------
def tarea_scraping() -> int:
    engine = obtener_motor_db()
    try:
        with engine.connect() as conn:
            ya_cargados = {r[0] for r in conn.execute(text("SELECT periodo FROM raw_scraping_boletines"))}
        logger.info("Boletines que ya están en la base de datos: %d", len(ya_cargados))

        boletines, comunas = extraer_scraping_osec(periodos_ya_cargados=ya_cargados)

        ok = boletines[boletines["estado_extraccion"] == "OK"]
        fallidos = boletines[boletines["estado_extraccion"] != "OK"]
        if len(fallidos):
            # No se guardan: así el próximo día se vuelven a intentar
            logger.warning("Boletines con problemas (se reintentarán): %s",
                           fallidos[["periodo", "estado_extraccion"]].to_dict("records"))
        if ok.empty:
            logger.info("No hay boletines nuevos para cargar.")
            return 0

        comunas = comunas[comunas["periodo"].isin(ok["periodo"])].astype(str)
        cols_bol = ["periodo", "nombre_boletin", "url_pdf", "estado_extraccion", "fecha_extraccion", "fuente"]
        with engine.begin() as conn:   # boletines y comunas en la misma transacción
            ok[cols_bol].to_sql("raw_scraping_boletines", conn, if_exists="append", index=False, method="multi")
            comunas.to_sql("raw_scraping_comunas", conn, if_exists="append", index=False,
                           method="multi", chunksize=500)
        logger.info("Scraping: %d boletines nuevos y %d filas por comuna cargadas.", len(ok), len(comunas))
        return int(len(comunas))
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# Ejecución manual
# ---------------------------------------------------------------------------
TAREAS = {
    "sivigila": tarea_sivigila,
    "api": tarea_api,
    "vivienda": tarea_vivienda,
    "scraping": tarea_scraping,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingesta de las 4 fuentes a las tablas crudas")
    parser.add_argument("--fuente", choices=["todas", *TAREAS], default="todas")
    args = parser.parse_args()

    elegidas = TAREAS if args.fuente == "todas" else {args.fuente: TAREAS[args.fuente]}
    resumen = {}
    for nombre, tarea in elegidas.items():
        logger.info("========== Ingesta: %s ==========", nombre.upper())
        resumen[nombre] = tarea()

    print("\nRESUMEN DE LA INGESTA (filas en la tabla cruda de cada fuente):")
    for nombre, n in resumen.items():
        print(f"  {nombre:10s} {n:>8,}")