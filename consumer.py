"""
consumer.py
Kafka Consumer + inferencia en tiempo real.

Escucha : tópico 'violencia-genero-features' (lo publica etl/producer.py)
Carga   : models/model_pipeline.pkl + models/metadata_modelo.json (una sola vez, no reentrena)
Hace    : por cada mensaje válido -> predict_proba -> guarda la predicción
Escribe : tabla predicciones_agresor (PostgreSQL), que alimenta el dashboard app.py

Manejo de mensajes:
  * JSON ilegible o sin los campos requeridos -> se descarta, se registra en el log
    y se cuenta como inválido (no tumba el consumer).
  * Campos vacíos (null) -> se aceptan; el pipeline los trata como "NO REPORTADO".
  * Mensaje repetido (Kafka puede reentregar) -> no se duplica gracias a la
    llave única (id_hecho_api, timestamp_evento).
  * El offset se confirma DESPUÉS de guardar la predicción (al menos una vez).
  * Se guarda id_caso_api (llave de la API) para trazabilidad del caso.

Uso (desde la raíz del proyecto, con Kafka y PostgreSQL arriba):
    python consumer.py
Detener: Ctrl + C
"""

import json
import logging
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

import joblib
import pandas as pd
from dotenv import load_dotenv
from kafka import KafkaConsumer
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(message)s")
log = logging.getLogger("Consumer_ML")

RAIZ = Path(__file__).resolve().parent
load_dotenv(RAIZ / ".env")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS_LOCAL", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "violencia-genero-features")
KAFKA_GROUP = os.getenv("KAFKA_GROUP_ID", "consumer-ml-violencia")

RUTA_MODELO = RAIZ / "models" / "model_pipeline.pkl"
RUTA_METADATA = RAIZ / "models" / "metadata_modelo.json"

CAMPOS_OBLIGATORIOS = {"id_hecho_api", "timestamp_evento"}

DDL_PREDICCIONES = """
CREATE TABLE IF NOT EXISTS predicciones_agresor (
    id_prediccion        SERIAL PRIMARY KEY,
    id_hecho_api         INT          NOT NULL,
    timestamp_evento     TIMESTAMPTZ  NOT NULL,
    procesado_en         TIMESTAMPTZ  NOT NULL,
    ciclo_vida           VARCHAR(30),
    escenario            VARCHAR(150),
    turno                VARCHAR(15),
    dias_incapacidad     VARCHAR(15),
    area                 VARCHAR(30),
    mes                  SMALLINT,
    prob_padre_madre     NUMERIC(6, 4) NOT NULL,
    prob_otro_familiar   NUMERIC(6, 4) NOT NULL,
    prediccion           VARCHAR(40)   NOT NULL,
    alerta               BOOLEAN       NOT NULL,
    umbral_alerta        NUMERIC(6, 4) NOT NULL,
    modelo               VARCHAR(40)   NOT NULL,
    UNIQUE (id_hecho_api, timestamp_evento)
);
ALTER TABLE predicciones_agresor ADD COLUMN IF NOT EXISTS id_caso_api TEXT;
ALTER TABLE predicciones_agresor ADD COLUMN IF NOT EXISTS nombre_municipio VARCHAR(80);
-- Índices para que el dashboard siga rápido con miles o millones de eventos
CREATE INDEX IF NOT EXISTS idx_pred_procesado ON predicciones_agresor (procesado_en DESC);
CREATE INDEX IF NOT EXISTS idx_pred_caso ON predicciones_agresor
    ((COALESCE(id_caso_api, id_hecho_api::TEXT)), procesado_en DESC);
COMMENT ON TABLE predicciones_agresor IS
    'Una fila = una predicción hecha por consumer.py a un mensaje de Kafka. Alimenta el dashboard en tiempo real.';
"""

INSERT_PREDICCION = text("""
INSERT INTO predicciones_agresor (
    id_hecho_api, id_caso_api, timestamp_evento, procesado_en, ciclo_vida, escenario, turno,
    dias_incapacidad, area, mes, nombre_municipio, prob_padre_madre, prob_otro_familiar,
    prediccion, alerta, umbral_alerta, modelo)
VALUES (
    :id_hecho_api, :id_caso_api, :timestamp_evento, :procesado_en, :ciclo_vida, :escenario, :turno,
    :dias_incapacidad, :area, :mes, :nombre_municipio, :prob_padre_madre, :prob_otro_familiar,
    :prediccion, :alerta, :umbral_alerta, :modelo)
ON CONFLICT (id_hecho_api, timestamp_evento) DO NOTHING
""")


# ---------------------------------------------------------------------------
# Conexiones y modelo
# ---------------------------------------------------------------------------
def motor_db():
    host = os.getenv("DB_HOST", "localhost")
    if host in ("host.docker.internal", "postgres_violencia_db"):
        host = "localhost"  # el consumer corre en el PC, no dentro de Docker
    url = URL.create(
        "postgresql+psycopg2",
        username=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
        host=host,
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "violencia_genero_db"),
    )
    return create_engine(url, pool_pre_ping=True)


def cargar_modelo():
    modelo = joblib.load(RUTA_MODELO)
    with open(RUTA_METADATA, encoding="utf-8") as f:
        metadata = json.load(f)
    log.info("Modelo cargado: %s | features=%s | umbral=%.3f",
             metadata["modelo"], metadata["features"], metadata["umbral_alerta"])
    return modelo, metadata


# ---------------------------------------------------------------------------
# Validación del mensaje
# ---------------------------------------------------------------------------
def validar_mensaje(msg, features: list[str]) -> str | None:
    """Devuelve None si el mensaje es válido, o el motivo por el que se descarta."""
    if not isinstance(msg, dict):
        return "el mensaje no es un objeto JSON"
    faltantes = (CAMPOS_OBLIGATORIOS | set(features)) - msg.keys()
    if faltantes:
        return f"faltan campos: {sorted(faltantes)}"
    try:
        int(msg["id_hecho_api"])
        datetime.fromisoformat(str(msg["timestamp_evento"]))
    except (TypeError, ValueError):
        return "id_hecho_api o timestamp_evento con formato inválido"
    mes = msg.get("mes")
    if mes is not None and (not isinstance(mes, int) or not 1 <= mes <= 12):
        return f"mes fuera de rango: {mes}"
    for campo in features:
        if campo != "mes" and msg[campo] is not None and not isinstance(msg[campo], str):
            return f"'{campo}' debe ser texto o null"
    return None


# ---------------------------------------------------------------------------
# Inferencia
# ---------------------------------------------------------------------------
def predecir(modelo, metadata: dict, msg: dict) -> dict:
    features = metadata["features"]
    X = pd.DataFrame([{f: msg.get(f) for f in features}])  # solo las features del modelo
    prob_padre = float(modelo.predict_proba(X)[0, 1])
    umbral = float(metadata["umbral_alerta"])
    alerta = prob_padre >= umbral
    return {
        "id_hecho_api": int(msg["id_hecho_api"]),
        "id_caso_api": None if msg.get("id_caso_api") is None else str(msg["id_caso_api"]),
        "timestamp_evento": msg["timestamp_evento"],
        "procesado_en": datetime.now(timezone.utc).isoformat(),
        **{f: msg.get(f) for f in features},
        "nombre_municipio": msg.get("nombre_municipio"),  # solo para el mapa, no es feature
        "prob_padre_madre": round(prob_padre, 4),
        "prob_otro_familiar": round(1 - prob_padre, 4),
        "prediccion": metadata["clases"]["1"] if alerta else metadata["clases"]["0"],
        "alerta": alerta,
        "umbral_alerta": round(umbral, 4),
        "modelo": metadata["modelo"],
    }


def deserializar(valor: bytes):
    try:
        return json.loads(valor.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None  # se maneja como mensaje inválido


# ---------------------------------------------------------------------------
# Bucle principal
# ---------------------------------------------------------------------------
def ejecutar():
    modelo, metadata = cargar_modelo()
    engine = motor_db()
    with engine.begin() as conn:
        for sentencia in DDL_PREDICCIONES.strip().split(";"):
            if sentencia.strip():
                conn.execute(text(sentencia))

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        group_id=KAFKA_GROUP,
        auto_offset_reset="earliest",   # la primera vez lee el tópico desde el inicio
        enable_auto_commit=False,       # el offset se confirma después de guardar
        value_deserializer=deserializar,
    )
    log.info("Escuchando '%s' en %s (grupo %s). Ctrl + C para detener.",
             KAFKA_TOPIC, KAFKA_BOOTSTRAP, KAFKA_GROUP)

    detener = {"si": False}
    signal.signal(signal.SIGINT, lambda *_: detener.update(si=True))

    procesados = invalidos = alertas = 0
    try:
        while not detener["si"]:
            lotes = consumer.poll(timeout_ms=1000)
            for registros in lotes.values():
                for registro in registros:
                    msg = registro.value
                    motivo = validar_mensaje(msg, metadata["features"])
                    if motivo:
                        invalidos += 1
                        log.warning("Mensaje descartado (offset %s): %s", registro.offset, motivo)
                        continue
                    pred = predecir(modelo, metadata, msg)
                    with engine.begin() as conn:
                        conn.execute(INSERT_PREDICCION, pred)
                    procesados += 1
                    alertas += int(pred["alerta"])
                    log.info("id=%s | P(padre/madre)=%.1f%% | %s",
                             pred["id_hecho_api"], pred["prob_padre_madre"] * 100,
                             "ALERTA" if pred["alerta"] else "sin alerta")
            if lotes:
                consumer.commit()
    finally:
        consumer.close()
        engine.dispose()
        log.info("Consumer detenido. Procesados=%d | alertas=%d | inválidos=%d",
                 procesados, alertas, invalidos)


if __name__ == "__main__":
    ejecutar()