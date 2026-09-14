"""

Requisitos (instalar una sola vez) desde la carpeta raíz del proyecto:
    pip install pandas sqlalchemy psycopg2-binary
"""
import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv 

load_dotenv()

#configuración de la ruta del archivo CSV y la fuente de datos

CSV_PATH = "data/bucaramanga_violencia.csv.csv"
FUENTE = "Historico_Bucaramanga" 

SOLO_INSPECCIONAR = False

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")  
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "violencia_genero_db")

#nombre de las columnas en el CSV y su mapeo a los nombres de columnas en la tabla raw_violencia_genero

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

#leer el CSV con pandas, asegurándose de que todos los campos se traten como texto y que los vacíos reales se conviertan en NULL

df = pd.read_csv(
    CSV_PATH,
    sep=",",
    encoding="utf-8-sig",
    dtype=str,              # todo como texto, sin que pandas adivine tipos
    keep_default_na=False,  # no tratar "NA", "NULL", etc. como vacío
    na_values=[""],         # solo un campo realmente vacío cuenta como vacío
)
df = df.where(pd.notnull(df), None) 
 
print("Columnas encontradas en el CSV:")
print(df.columns.tolist())
print(f"\nTotal de filas: {len(df)}")
print("\nPrimeras 3 filas:")
print(df.head(3))

if SOLO_INSPECCIONAR:
    print("\nSOLO_INSPECCIONAR=True → no se cargó nada a la base de datos.")
    print("Compara la lista de columnas de arriba con las claves (izquierda)")
    print("de COLUMN_MAP. Si coinciden, pon SOLO_INSPECCIONAR=False y corre de nuevo.")
    raise SystemExit(0)

#renombrar las columnas según COLUMN_MAP y verificar que todas las columnas esperadas estén presentes

faltantes = [c for c in COLUMN_MAP if c not in df.columns]
if faltantes:
    raise ValueError(
        f"Estas columnas esperadas no aparecen en el CSV: {faltantes}\n"
        "Revisa el nombre exacto en el CSV (mayúsculas, espacios, tildes) "
        "y ajústalo en COLUMN_MAP."
    )

df = df.rename(columns=COLUMN_MAP)
df = df[list(COLUMN_MAP.values())]   # solo las columnas que la tabla raw espera
df["fuente"] = FUENTE

#Cargar a la base de datos PostgreSQL 

connection_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_url)

df.to_sql("raw_violencia_genero", engine, if_exists="append", index=False)
engine.dispose() 

print(f"\nListo: se cargaron {len(df)} filas en raw_violencia_genero (fuente='{FUENTE}').")

