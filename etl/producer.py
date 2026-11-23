"""
etl/producer.py
Kafka Producer para streaming de hechos validados hacia el consumidor ML.

Lee   : vista_ml_hechos_enriquecidos (PostgreSQL)
Envia : Topico Kafka 'violencia-genero-features' (configurable por env)

Regla anti-fuga (Data Leakage):
  Excluye estrictamente 'grupo_agresor', 'presunto_agresor', 'sexo_agresor',
  'edad_agre' y 'parentezco_vict' del payload JSON.

Uso manual (desde la raiz del proyecto):
    python etl/producer.py
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaProducer
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
)
logger = logging.getLogger("ETL_Kafka_Producer")

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ_PROYECTO / ".env")

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "violencia_genero_db")

# En contenedor Docker de Airflow usa 'kafka:29092'; en ejecucion local usa 'localhost:9092'
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:29092" if DB_HOST == "host.docker.internal" else "localhost:9092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "violencia-genero-features")
STREAM_DELAY_SEC = float(os.getenv("KAFKA_STREAM_DELAY_SEC", "0.02"))
MAX_RECORDS = int(os.getenv("KAFKA_MAX_RECORDS", "0"))  # 0 = envia todos los registros

# Contrato estricto de features permitidas (sin variables del agresor / target)
FEATURES_CONTRATO = [
    "id_hecho_api",
    "escenario",
    "ciclo_vida",
    "contexto",
    "turno",
    "dias_incapacidad",
    "tipo_violencia",
    "nombre_municipio",
    "area",
    "mes",
    "anio",
    "osec_denuncias_sexuales",
    "nivel_estrato",
    "zona_vivienda",
]

COLUMNAS_PROHIBIDAS_LEAKAGE = {
    "grupo_agresor",
    "presunto_agresor",
    "sexo_agresor",
    "edad_agre",
    "parentezco_vict",
    "parentesco_vict",
}


def obtener_motor_db():
    if not DB_PASSWORD:
        raise RuntimeError(f"Falta DB_PASSWORD en {RAIZ_PROYECTO / '.env'}")
    url = URL.create(
        "postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=int(DB_PORT),
        database=DB_NAME,
    )
    return create_engine(url)


def cargar_registros_validados() -> pd.DataFrame:
    """Consulta la vista enriquecida seleccionando exclusivamente las columnas del contrato."""
    columnas_sql = ", ".join(FEATURES_CONTRATO)
    query = f"""
        SELECT {columnas_sql}
        FROM vista_ml_hechos_enriquecidos
        ORDER BY id_hecho_api ASC
    """
    if MAX_RECORDS > 0:
        query += f" LIMIT {MAX_RECORDS}"

    engine = obtener_motor_db()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
    finally:
        engine.dispose()

    # Verificacion defensiva anti-fuga de datos
    fuga = COLUMNAS_PROHIBIDAS_LEAKAGE.intersection(df.columns)
    if fuga:
        raise RuntimeError(f"ALERTA DE DATA LEAKAGE: columnas prohibidas presentes: {fuga}")

    return df


def _a_entero_o_none(valor):
    """Convierte a int nativo de Python o devuelve None (null en JSON) si viene vacío/NaN."""
    if valor is None or pd.isna(valor):
        return None
    return int(valor)


def _a_texto_o_none(valor):
    """Convierte a str nativo de Python o devuelve None (null en JSON) si viene vacío/NaN."""
    if valor is None or pd.isna(valor):
        return None
    texto = str(valor).strip()
    return texto if texto else None


def construir_mensaje(fila: dict) -> dict:
    """Construye el diccionario JSON respetando el contrato y preservando nulls reales."""
    return {
        "id_hecho_api": int(fila["id_hecho_api"]),
        "timestamp_evento": datetime.now(timezone.utc).isoformat(),
        "escenario": _a_texto_o_none(fila.get("escenario")),
        "ciclo_vida": _a_texto_o_none(fila.get("ciclo_vida")),
        "contexto": _a_texto_o_none(fila.get("contexto")),
        "turno": _a_texto_o_none(fila.get("turno")),
        "dias_incapacidad": _a_texto_o_none(fila.get("dias_incapacidad")),
        "tipo_violencia": _a_texto_o_none(fila.get("tipo_violencia")),
        "nombre_municipio": _a_texto_o_none(fila.get("nombre_municipio")),
        "area": _a_texto_o_none(fila.get("area")),
        "mes": _a_entero_o_none(fila.get("mes")),
        "anio": _a_entero_o_none(fila.get("anio")),
        "osec_denuncias_sexuales": _a_entero_o_none(fila.get("osec_denuncias_sexuales")),
        "nivel_estrato": _a_entero_o_none(fila.get("nivel_estrato")),
        "zona_vivienda": _a_entero_o_none(fila.get("zona_vivienda")),
    }


def ejecutar_producer(delay: float = STREAM_DELAY_SEC) -> dict:
    """Lee los hechos enriquecidos y los publica uno a uno al topico de Kafka."""
    df = cargar_registros_validados()
    if df.empty:
        logger.warning("No hay registros en vista_ml_hechos_enriquecidos para publicar.")
        return {"topic": KAFKA_TOPIC, "enviados": 0}

    logger.info(
        "Conectando a Kafka (%s) -> topico '%s' (%d registros)...",
        KAFKA_BOOTSTRAP_SERVERS,
        KAFKA_TOPIC,
        len(df),
    )

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        acks="all",
        retries=3,
    )

    enviados = 0
    try:
        for registro in df.to_dict(orient="records"):
            payload = construir_mensaje(registro)
            producer.send(
                KAFKA_TOPIC,
                key=payload["id_hecho_api"],
                value=payload,
            )
            enviados += 1
            if delay > 0:
                time.sleep(delay)

        producer.flush()
    finally:
        producer.close()

    resumen = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "topic": KAFKA_TOPIC,
        "enviados": enviados,
    }
    logger.info("Publicacion en Kafka finalizada: %s", resumen)
    return resumen


if __name__ == "__main__":
    print(ejecutar_producer())