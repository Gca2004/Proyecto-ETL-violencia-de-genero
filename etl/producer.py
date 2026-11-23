"""
etl/producer.py
Kafka Producer INCREMENTAL: publica solo los casos validados que aún no se han enviado.

Lee   : vista_ml_hechos_enriquecidos (PostgreSQL, datos ya validados por GE)
        + hechos_api_violencia (para la llave natural id_caso_api de la API)
Envía : tópico Kafka 'violencia-genero-features' (configurable por env)
Anota : kafka_publicaciones -> "libreta de control" con cada caso ya publicado

Cómo funciona (micro-lotes por eventos):
  1. El DAG baja la API cada día, valida, limpia y carga el modelo dimensional.
  2. Este producer compara la vista contra kafka_publicaciones y envía SOLO
     los casos nuevos (los que no están anotados). Si no hay nuevos, no envía nada.
  3. Cada caso se anota apenas Kafka confirma que lo recibió (acks=all).
     Así un caso viaja por Kafka -> consumer -> dashboard una sola vez.

Se usa id_caso_api (id de la API de Medicina Legal) como llave de control porque
no cambia entre ejecuciones; id_hecho_api es un SERIAL interno que puede cambiar
si las tablas se recargan.

Regla anti-fuga (Data Leakage):
  Nunca viajan 'grupo_agresor', 'presunto_agresor', 'sexo_agresor', 'edad_agre'
  ni 'parentezco_vict' en el payload JSON.

Variables de entorno útiles:
  KAFKA_STREAM_DELAY_SEC  segundos entre mensajes (default 0.02; 1 para la demo)
  KAFKA_MAX_RECORDS       máximo de casos por ejecución (0 = todos los pendientes)
  KAFKA_REPUBLICAR        1 = ignora la libreta y reenvía (solo para demostraciones)

Uso manual (desde la raíz del proyecto):
    python etl/producer.py
"""

import json
import logging
import math
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
    "kafka:29092" if DB_HOST in ("host.docker.internal", "postgres_violencia_db") else "localhost:9092",
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "violencia-genero-features")
STREAM_DELAY_SEC = float(os.getenv("KAFKA_STREAM_DELAY_SEC", "0.02"))
MAX_RECORDS = int(os.getenv("KAFKA_MAX_RECORDS", "0"))  # 0 = todos los pendientes
REPUBLICAR = os.getenv("KAFKA_REPUBLICAR", "0") == "1"

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
CAMPOS_TEXTO = ["escenario", "ciclo_vida", "contexto", "turno", "dias_incapacidad",
                "tipo_violencia", "nombre_municipio", "area"]
CAMPOS_ENTEROS = ["mes", "anio", "osec_denuncias_sexuales", "nivel_estrato", "zona_vivienda"]

COLUMNAS_PROHIBIDAS_LEAKAGE = {
    "grupo_agresor",
    "presunto_agresor",
    "sexo_agresor",
    "edad_agre",
    "parentezco_vict",
    "parentesco_vict",
}

DDL_CONTROL = """
CREATE TABLE IF NOT EXISTS kafka_publicaciones (
    id_caso_api    TEXT        PRIMARY KEY,
    id_hecho_api   INT         NOT NULL,
    topico         VARCHAR(80) NOT NULL,
    publicado_en   TIMESTAMPTZ NOT NULL
)
"""

REGISTRAR_PUBLICACION = text("""
INSERT INTO kafka_publicaciones (id_caso_api, id_hecho_api, topico, publicado_en)
VALUES (:id_caso_api, :id_hecho_api, :topico, :publicado_en)
ON CONFLICT (id_caso_api) DO UPDATE
SET id_hecho_api = EXCLUDED.id_hecho_api, publicado_en = EXCLUDED.publicado_en
""")


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


def cargar_registros_pendientes(engine) -> tuple[pd.DataFrame, int]:
    """Casos de la vista que aún no están en kafka_publicaciones (o todos si REPUBLICAR)."""
    columnas_sql = ", ".join(f"v.{c}" for c in FEATURES_CONTRATO)
    filtro = "" if REPUBLICAR else (
        "WHERE NOT EXISTS (SELECT 1 FROM kafka_publicaciones k WHERE k.id_caso_api = h.id_caso_api)"
    )
    query = f"""
        SELECT {columnas_sql}, h.id_caso_api
        FROM vista_ml_hechos_enriquecidos v
        JOIN hechos_api_violencia h ON h.id_hecho_api = v.id_hecho_api
        {filtro}
        ORDER BY v.anio, v.mes, v.id_hecho_api
    """
    if MAX_RECORDS > 0:
        query += f" LIMIT {MAX_RECORDS}"

    with engine.begin() as conn:
        conn.execute(text(DDL_CONTROL))
        ya_publicados = conn.execute(text("SELECT COUNT(*) FROM kafka_publicaciones")).scalar_one()
        df = pd.read_sql(text(query), conn)

    # Verificacion defensiva anti-fuga de datos
    fuga = COLUMNAS_PROHIBIDAS_LEAKAGE.intersection(df.columns)
    if fuga:
        raise RuntimeError(f"ALERTA DE DATA LEAKAGE: columnas prohibidas presentes: {fuga}")

    return df, int(ya_publicados)


def _es_vacio(valor) -> bool:
    return valor is None or (isinstance(valor, float) and math.isnan(valor)) or valor is pd.NA


def _texto(valor):
    return None if _es_vacio(valor) else str(valor)


def _entero(valor):
    return None if _es_vacio(valor) else int(valor)


def construir_mensaje(fila: dict) -> dict:
    """Construye el JSON del contrato; los vacíos viajan como null (no se inventan valores)."""
    mensaje = {
        "id_hecho_api": int(fila["id_hecho_api"]),
        "id_caso_api": str(fila["id_caso_api"]),
        "timestamp_evento": datetime.now(timezone.utc).isoformat(),
        **{c: _texto(fila[c]) for c in CAMPOS_TEXTO},
        **{c: _entero(fila[c]) for c in CAMPOS_ENTEROS},
    }

    # Garantizar que ninguna llave prohibida viaje en el payload
    if COLUMNAS_PROHIBIDAS_LEAKAGE.intersection(mensaje.keys()):
        raise RuntimeError("Se detecto fuga de informacion en el mensaje JSON.")

    return mensaje


def ejecutar_producer(delay: float = STREAM_DELAY_SEC) -> dict:
    """Publica en Kafka solo los casos nuevos y los anota en kafka_publicaciones."""
    engine = obtener_motor_db()
    try:
        df, ya_publicados = cargar_registros_pendientes(engine)
        modo = "REPUBLICAR (demo)" if REPUBLICAR else "incremental"
        logger.info("Modo %s | casos ya publicados antes: %d | pendientes por enviar: %d",
                    modo, ya_publicados, len(df))

        if df.empty:
            logger.info("No hay casos nuevos para publicar. Nada que enviar.")
            return {"topic": KAFKA_TOPIC, "modo": modo, "enviados": 0,
                    "ya_publicados_antes": ya_publicados}

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
                # .get() espera la confirmación de Kafka antes de anotar el caso
                producer.send(KAFKA_TOPIC, key=payload["id_caso_api"], value=payload).get(timeout=30)
                with engine.begin() as conn:
                    conn.execute(REGISTRAR_PUBLICACION, {
                        "id_caso_api": payload["id_caso_api"],
                        "id_hecho_api": payload["id_hecho_api"],
                        "topico": KAFKA_TOPIC,
                        "publicado_en": payload["timestamp_evento"],
                    })
                enviados += 1
                if delay > 0:
                    time.sleep(delay)
            producer.flush()
        finally:
            producer.close()
    finally:
        engine.dispose()

    resumen = {
        "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
        "topic": KAFKA_TOPIC,
        "modo": modo,
        "ya_publicados_antes": ya_publicados,
        "enviados": enviados,
    }
    logger.info("Publicacion en Kafka finalizada: %s", resumen)
    return resumen


if __name__ == "__main__":
    print(ejecutar_producer())