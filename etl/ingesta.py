"""

Requisitos (instalar una sola vez) desde la carpeta raíz del proyecto:
    pip install pandas sqlalchemy psycopg2-binary
"""

import pandas as pd
from sqlalchemy import create_engine

# ============================================================
# 1. CONFIGURACIÓN 
# ============================================================
CSV_PATH = "../data/bucaramanga_violencia.csv.csv"
FUENTE = "Historico_Bucaramanga"   #  "SIVIGILA_Colombia" para el otro CSV

# True la primera vez: solo lee el CSV e imprime columnas/filas,
# NO escribe nada en la base de datos. Cuando se confirme que las columnas
# cambiar a False para cargar de verdad.
SOLO_INSPECCIONAR = False

DB_USER = "postgres"
DB_PASSWORD = "Tu_clave_postgresql"    
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "violencia_genero_db"

# ============================================================
# 2. MAPEO: nombre de columna original del CSV -> nombre en raw
# ============================================================
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

# ============================================================
# 3. LEER EL CSV
# ============================================================
df = pd.read_csv(
    CSV_PATH,
    sep=",",
    encoding="utf-8-sig",
    dtype=str,              # todo como texto, sin que pandas adivine tipos
    keep_default_na=False,  # no tratar "NA", "NULL", etc. como vacío
    na_values=[""],         # solo un campo realmente vacío cuenta como vacío
)
df = df.where(pd.notnull(df), None)  # vacíos reales -> NULL de verdad, no "nan"
 
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

# ============================================================
# 4. RENOMBRAR COLUMNAS Y AGREGAR LA FUENTE
# ============================================================
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

# ============================================================
# 5. CARGAR A POSTGRESQL
# ============================================================
engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

df.to_sql("raw_violencia_genero", engine, if_exists="append", index=False)
print(f"\nListo: se cargaron {len(df)} filas en raw_violencia_genero (fuente='{FUENTE}').")

