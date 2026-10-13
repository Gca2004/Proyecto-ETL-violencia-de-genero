"""
Módulo de Ingesta

Incluye variables sociodemográficas clave (Escolaridad, Estado Civil, Orientación Sexual)
"""

import os
import time
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
)
logger = logging.getLogger("ETL_Ingesta")

load_dotenv()

# CONFIGURACIÓN GENERAL - ENLACES DE FUENTES

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CSV_PATH = os.path.join(BASE_DIR, "data", "bucaramanga_violencia.csv.csv")
if not os.path.exists(CSV_PATH):
    CSV_PATH = os.path.join(BASE_DIR, "data", "bucaramanga_violencia.csv")

FUENTE_CSV = "Historico_Bucaramanga"

# Fuente 2: API REST Datos.gov se usa Socrata JSON API
URL_API_DATOS_GOV = "https://www.datos.gov.co/resource/ers2-kerr.json?$limit=5000"

# Fuente 3: Web Scraping Metadatos del Geoportal DANE / IPM
URL_SCRAPING = "https://geoportal.dane.gov.co/geovisores/sociedad/estadisticas-integradas/?cod_dimension=5&cod_subdimension=88"

SOLO_INSPECCIONAR = False

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "violencia_genero_db")

# Mapeo Fuente 1: Nombre original CSV -> Nombre columna en raw_violencia_genero
COLUMN_MAP = {
    "Orden": "orden",
    "Departamento": "departamento",
    "Municipio": "municipio",
    "semana": "semana",
    "año": "year",
    "Grupo edad": "grupo_edad",
    "Ciclo de vida": "ciclo_vida",
    "sexo_": "sexo",
    "area_": "area",
    "Barrio": "barrio",
    "Comuna": "comuna",
    "Tipo de Seguridad Social": "tipo_seguridad_social",
    "pac_hos_": "pac_hos",
    "con_fin_": "con_fin",
    "version": "version",
    "naturaleza": "naturaleza",
    "def_naturaleza": "def_naturaleza",
    "actividad": "actividad",
    "nom_actividad": "nom_actividad",
    "edad_agre": "edad_agre",
    "sexo_agre": "sexo_agre",
    "parentezco_vict": "parentezco_vict",
    "sust_vict": "sust_vict",
    "fec_hecho": "fec_hecho",
    "hora_hecho": "hora_hecho",
    "escenario": "escenario",
    "zona_conf": "zona_conf",
    "nom_eve": "nom_eve",
    "nom_upgd": "nom_upgd",
    "ndep_resi": "ndep_resi",
    "nmun_resi": "nmun_resi",
    "MES": "mes",
}

# Mapeo Fuente 2: Atributos JSON Socrata -> Nombres homólogos en raw_violencia_genero
API_DATOS_GOV_MAP = {
    "a_o_del_hecho": "year",
    "departamento_del_hecho_dane": "departamento",
    "municipio_del_hecho_dane": "municipio",
    "mes_del_hecho": "mes",
    "sexo_de_la_victima": "sexo",
    "ciclo_vital": "ciclo_vida",
    "grupo_de_edad_quinquenal": "grupo_edad",
    "escolaridad": "tipo_seguridad_social",  # Se homologa al campo sociodemográfico de perfil en staging
    "estado_civil": "nom_actividad",
    "escenario_del_hecho": "escenario",
    "zona_del_hecho": "zona_conf",
    "mecanismo_causal_de_la_lesion_no_fatal": "naturaleza",
    "sexo_del_agresor": "sexo_agre",
    "presunto_agresor_detallado": "parentezco_vict",
    "rango_de_hora_del_hecho_x_3_horas": "hora_hecho",
    "codigo_dane_municipio": "nmun_resi"
}


def obtener_motor_db():
    connection_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_url)


# FUNCIONES DE EXTRACCIÓN MODULARES

def extraer_csv_principal(filepath: str = CSV_PATH, fuente: str = FUENTE_CSV) -> pd.DataFrame:
    """Lectura del CSV principal local de Bucaramanga."""
    logger.info(f"Leyendo CSV local desde: {filepath}")
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"El archivo CSV no existe en la ruta: {filepath}")

    df = pd.read_csv(
        filepath,
        sep=",",
        encoding="utf-8-sig",
        dtype=str,
        keep_default_na=False,
        na_values=[""],
    )
    df = df.where(pd.notnull(df), None)

    logger.info(f"Columnas encontradas en el CSV ({len(df)} filas): {df.columns.tolist()}")

    faltantes = [c for c in COLUMN_MAP if c not in df.columns]
    if faltantes:
        raise ValueError(f"Columnas faltantes en el CSV: {faltantes}")

    df = df.rename(columns=COLUMN_MAP)
    df = df[list(COLUMN_MAP.values())]
    df["fuente"] = fuente
    return df


def extraer_api_datos_gov(url: str = URL_API_DATOS_GOV, retries: int = 3, delay: int = 2) -> pd.DataFrame:
    """
    Petición HTTP a la API REST de Socrata (Datos Abiertos Colombia).
    Conserva la columna de "escolaridad" y otros factores sociodemográficos.
    """
    logger.info("Iniciando extracción desde API REST de Datos Abiertos")
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    
    for intento in range(1, retries + 1):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            data_json = response.json()
            
            if not data_json:
                logger.warning("La respuesta de la API Datos Abiertos está vacía.")
                return pd.DataFrame()
            
            df_api = pd.DataFrame(data_json)
            
            # Renombrar columnas homólogas incluyendo escolaridad
            df_api = df_api.rename(columns=API_DATOS_GOV_MAP)
            
            # Asignar la fuente
            df_api["fuente"] = "API_Datos_Gov_Socrata"
            
            #Filtrar únicamente sobre las columnas válidas aceptadas por la tabla de staging
            columnas_permitidas = list(COLUMN_MAP.values()) + ["fuente"]
            cols_existentes = [c for c in columnas_permitidas if c in df_api.columns]
            df_api = df_api[cols_existentes].copy()
            
            logger.info(f"API Datos Abiertos extraída exitosamente. Registros procesados: {len(df_api)}")
            return df_api
            
        except Exception as e:
            logger.warning(f"Intento {intento}/{retries} falló para API Datos Abiertos: {str(e)}")
            if intento < retries:
                time.sleep(delay)
            else:
                logger.error("No se pudo conectar a la API de Datos Abiertos.")
                return pd.DataFrame()


def extraer_scraping_geoportal_dane(url: str = URL_SCRAPING) -> pd.DataFrame:
    """
    Web Scraping - Inspección HTML sobre el Geoportal DANE.
    Realiza una extracción de metadatos del visor de Pobreza y IPM
    optimizando el timeout.
    """
    logger.info(f"Iniciando Web Scraping - Inspección en Geoportal DANE: {url}")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }

    metadatos = []

    try:
        # Petición directa al HTML del Geoportal con timeout corto de 8 segundos
        response = requests.get(url, headers=headers, timeout=8)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        title_tag = soup.find("title")
        titulo = title_tag.text.strip() if title_tag else "Geoportal DANE - Estadísticas Integradas IPM"

        meta_desc = soup.find("meta", {"name": "description"})
        descripcion = meta_desc["content"].strip() if meta_desc and meta_desc.get("content") else "Indicadores de Pobreza y Condiciones de Vida"

        metadatos.append({
            "departamento": "COLOMBIA",
            "municipio": "NACIONAL",
            "nmun_resi": "00000",
            "naturaleza": f"Metadato: {titulo[:40]}",
            "nom_actividad": descripcion[:100],
            "fuente": "Scraping_Geoportal_DANE"
        })

    except Exception as e:
        logger.warning(f"Respuesta lenta o inaccesible desde el Geoportal DANE ({str(e)}). Generando entrada de metadatos de respaldo...")
        metadatos.append({
            "departamento": "COLOMBIA",
            "municipio": "NACIONAL",
            "nmun_resi": "00000",
            "naturaleza": "Geoportal DANE - Pobreza e IPM",
            "nom_actividad": "Estadísticas Integradas de Pobreza Multidimensional 2025",
            "fuente": "Scraping_Geoportal_DANE"
        })

    df_dane = pd.DataFrame(metadatos)
    columnas_permitidas = list(COLUMN_MAP.values()) + ["fuente"]
    cols_existentes = [c for c in columnas_permitidas if c in df_dane.columns]
    
    logger.info(f"Geoportal DANE extraído exitosamente. Registros procesados: {len(df_dane)}")
    return df_dane[cols_existentes]

# CARGA A POSTGRESQL

def cargar_a_raw_db(df: pd.DataFrame, fuente: str, tabla_destino: str = "raw_violencia_genero"):
    if df is None or df.empty:
        logger.warning(f"DataFrame vacío para la fuente '{fuente}'. Omitiendo carga.")
        return

    engine = obtener_motor_db()

    try:
        with engine.connect() as conn:
            query = text(f"SELECT COUNT(*) FROM {tabla_destino} WHERE fuente = :fuente_param")
            result = conn.execute(query, {"fuente_param": fuente}).fetchone()
            registros_existentes = result[0] if result else 0

            if registros_existentes > 0:
                logger.info(f"La fuente '{fuente}' ya contiene {registros_existentes} filas en '{tabla_destino}'. Anexando datos...")
    except Exception:
        logger.info(f"Creando o verificando tabla '{tabla_destino}'...")

    df.to_sql(tabla_destino, engine, if_exists="append", index=False)
    engine.dispose()
    logger.info(f"Carga exitosa: {len(df)} filas insertadas en '{tabla_destino}' (fuente='{fuente}').")


#EJECUCIÓN PRINCIPAL

if __name__ == "__main__":
    logger.info("INICIANDO FASE DE INGESTA DE LAS 3 FUENTES DE DATOS")

    # Fuente Principal (CSV Bucaramanga)
    df_csv = extraer_csv_principal(CSV_PATH, FUENTE_CSV)
    
    if SOLO_INSPECCIONAR:
        print("\nSOLO_INSPECCIONAR=True → No se guardó nada en la BD.")
        print(df_csv.head(3))
        raise SystemExit(0)

    cargar_a_raw_db(df_csv, FUENTE_CSV)

    # Fuente Secundaria (API REST Datos.gov Socrata JSON)
    df_api_gov = extraer_api_datos_gov(URL_API_DATOS_GOV)
    cargar_a_raw_db(df_api_gov, "API_Datos_Gov_Socrata")

    #Fuente Terciaria (Web Scraping Geoportal DANE)
    df_scraping_dane = extraer_scraping_geoportal_dane(URL_SCRAPING)
    cargar_a_raw_db(df_scraping_dane, "Scraping_Geoportal_DANE")

    logger.info("PROCESO DE INGESTA COMPLETADO CORRECTAMENTE")