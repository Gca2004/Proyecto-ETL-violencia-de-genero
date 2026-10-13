"""
Módulo de Transformación y Enrutado Dimensional
"""

import os
import logging
import unicodedata
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s'
)
logger = logging.getLogger("ETL_Transformacion")

load_dotenv()

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "violencia_genero_db")


def obtener_motor_db():
    connection_url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(connection_url)


def normalizar_cadena(texto) -> str:
    if pd.isna(texto) or texto is None:
        return "NO REPORTADO"
    val_str = str(texto).strip().upper()
    val_norm = unicodedata.normalize('NFKD', val_str).encode('ASCII', 'ignore').decode('utf-8')
    return val_norm if val_norm != "" else "NO REPORTADO"


def calcular_turno(hora_raw) -> str:
    """1. Derivación de hora a turno (Mañana, Tarde, Noche, Madrugada)."""
    if pd.isna(hora_raw) or hora_raw is None:
        return "NO REPORTADO"
    
    val = str(hora_raw).strip().lower()
    
    if '18:' in val or '19:' in val or '20:' in val or '21:' in val or '22:' in val or '23:' in val or 'noche' in val:
        return "NOCHE"
    elif '06:' in val or '07:' in val or '08:' in val or '09:' in val or '10:' in val or '11:' in val or 'mañana' in val:
        return "MAÑANA"
    elif '12:' in val or '13:' in val or '14:' in val or '15:' in val or '16:' in val or '17:' in val or 'tarde' in val:
        return "TARDE"
    elif '00:' in val or '01:' in val or '02:' in val or '03:' in val or '04:' in val or '05:' in val or 'madrugada' in val:
        return "MADRUGADA"
    
    try:
        hora_int = int(val.split(':')[0])
        if 0 <= hora_int < 6:
            return "MADRUGADA"
        elif 6 <= hora_int < 12:
            return "MAÑANA"
        elif 12 <= hora_int < 18:
            return "TARDE"
        elif 18 <= hora_int <= 23:
            return "NOCHE"
    except Exception:
        pass
        
    return "NO REPORTADO"


def ejecutar_transformacion_y_enrutado() -> dict:
    logger.info("Iniciando proceso de Transformación y Enrutado Dimensional...")
    engine = obtener_motor_db()
    
    df_raw = pd.read_sql("SELECT * FROM raw_violencia_genero;", engine)
    if df_raw.empty:
        logger.error("La tabla 'raw_violencia_genero' está vacía.")
        return {}

    df = df_raw.copy()

    # Estandarización de Código DANE
    if 'nmun_resi' in df.columns:
        df['codigo_dane_clean'] = df['nmun_resi'].astype(str).str.replace(r'\.0$', '', regex=True).str.zfill(5)
    else:
        df['codigo_dane_clean'] = "00000"

    cols_a_limpiar = ['departamento', 'municipio', 'sexo', 'ciclo_vida', 'grupo_edad', 
                       'tipo_seguridad_social', 'parentezco_vict', 'naturaleza', 'escenario']
    for col in cols_a_limpiar:
        if col in df.columns:
            df[col] = df[col].apply(normalizar_cadena)
        else:
            df[col] = "NO REPORTADO"

    if 'fec_hecho' in df.columns and df['fec_hecho'].notnull().any():
        df['fecha_dt'] = pd.to_datetime(df['fec_hecho'], errors='coerce')
    else:
        df['year_clean'] = pd.to_numeric(df['year'], errors='coerce').fillna(2023).astype(int)
        df['fecha_dt'] = pd.to_datetime(df['year_clean'].astype(str) + '-01-01', errors='coerce')

    if 'hora_hecho' in df.columns:
        df['turno'] = df['hora_hecho'].apply(calcular_turno)
        df.drop(columns=['hora_hecho'], inplace=True, errors='ignore')
    else:
        df['turno'] = "NO REPORTADO"

    # Dim_Ubicacion
    Dim_Ubicacion = df[['codigo_dane_clean', 'departamento', 'municipio']].drop_duplicates().reset_index(drop=True)
    Dim_Ubicacion['id_ubicacion_sk'] = Dim_Ubicacion.index + 1
    Dim_Ubicacion.rename(columns={
        'codigo_dane_clean': 'codigo_dane',
        'departamento': 'nombre_departamento',
        'municipio': 'nombre_municipio'
    }, inplace=True)

    # Dim_Jornada
    Dim_Jornada = df[['fecha_dt', 'turno']].drop_duplicates().reset_index(drop=True)
    Dim_Jornada['id_jornada_sk'] = Dim_Jornada.index + 1
    Dim_Jornada['anio'] = Dim_Jornada['fecha_dt'].dt.year.fillna(2023).astype(int)
    Dim_Jornada['mes'] = Dim_Jornada['fecha_dt'].dt.month.fillna(1).astype(int)
    Dim_Jornada['nombre_mes'] = Dim_Jornada['fecha_dt'].dt.month_name().fillna("ENERO")
    Dim_Jornada['dia'] = Dim_Jornada['fecha_dt'].dt.day.fillna(1).astype(int)

    # Dim_TipoViolencia
    Dim_TipoViolencia = df[['parentezco_vict', 'naturaleza', 'escenario']].drop_duplicates().reset_index(drop=True)
    Dim_TipoViolencia['id_tipo_violencia_sk'] = Dim_TipoViolencia.index + 1
    Dim_TipoViolencia.rename(columns={'parentezco_vict': 'parentesco_agresor'}, inplace=True)

    # Dim_Victima
    Dim_Victima = df[['sexo', 'ciclo_vida', 'grupo_edad', 'tipo_seguridad_social']].drop_duplicates().reset_index(drop=True)
    Dim_Victima['id_victima_sk'] = Dim_Victima.index + 1
    Dim_Victima.rename(columns={'tipo_seguridad_social': 'escolaridad_o_seguridad'}, inplace=True)

    # Fact_Violencia (MERGES UNÍVOCOS POR CLAVES COMPLETAS)
    fact_df = df.merge(
        Dim_Ubicacion, 
        left_on=['codigo_dane_clean', 'departamento', 'municipio'], 
        right_on=['codigo_dane', 'nombre_departamento', 'nombre_municipio'], 
        how='left'
    )
    fact_df = fact_df.merge(
        Dim_Jornada, 
        left_on=['fecha_dt', 'turno'], 
        right_on=['fecha_dt', 'turno'], 
        how='left'
    )
    fact_df = fact_df.merge(
        Dim_TipoViolencia, 
        left_on=['parentezco_vict', 'naturaleza', 'escenario'], 
        right_on=['parentesco_agresor', 'naturaleza', 'escenario'], 
        how='left'
    )
    fact_df = fact_df.merge(
        Dim_Victima, 
        left_on=['sexo', 'ciclo_vida', 'grupo_edad', 'tipo_seguridad_social'], 
        right_on=['sexo', 'ciclo_vida', 'grupo_edad', 'escolaridad_o_seguridad'], 
        how='left'
    )

    Fact_Violencia = fact_df[[
        'id_ubicacion_sk',
        'id_jornada_sk',
        'id_tipo_violencia_sk',
        'id_victima_sk'
    ]].copy()

    Fact_Violencia['cantidad_casos'] = 1
    Fact_Violencia.reset_index(drop=True, inplace=True)
    Fact_Violencia['id_fact_sk'] = Fact_Violencia.index + 1
    Fact_Violencia.fillna(1, inplace=True)

    logger.info(f"Transformación completada sin duplicaciones. Total registros en Fact_Violencia: {len(Fact_Violencia)}")

    return {
        "Dim_Ubicacion": Dim_Ubicacion,
        "Dim_Jornada": Dim_Jornada,
        "Dim_TipoViolencia": Dim_TipoViolencia,
        "Dim_Victima": Dim_Victima,
        "Fact_Violencia": Fact_Violencia
    }

def persistir_esquema_dimensional(tablas: dict):
    if not tablas:
        return

    engine = obtener_motor_db()
    tablas_a_limpiar = ["Fact_Violencia", "fact_violencia", "hechos_violencia", "Dim_Ubicacion", "dim_municipio", 
                        "Dim_Jornada", "dim_tiempo", "Dim_TipoViolencia", "dim_tipo_violencia", "Dim_Victima", "dim_victima"]
    
    with engine.begin() as conn:
        for t in tablas_a_limpiar:
            conn.execute(text(f"DROP TABLE IF EXISTS {t} CASCADE;"))

    for nombre_tabla, df_tabla in tablas.items():
        df_tabla.to_sql(nombre_tabla, engine, if_exists="replace", index=False)
        logger.info(f"Tabla '{nombre_tabla}' persistida ({len(df_tabla)} filas).")

    engine.dispose()


if __name__ == "__main__":
    tablas_dw = ejecutar_transformacion_y_enrutado()
    persistir_esquema_dimensional(tablas_dw)
    print("\n¡Transformación actualizada con los nombres de tablas requeridos!")