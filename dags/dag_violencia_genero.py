"""
dags/dag_violencia_genero.py
DAG del pipeline ETL - Violencia contra las mujeres en Bucaramanga (ODS 5)

VERSIÓN 4 (Entrega Final):
- Validación temprana con Great Expectations sobre tablas raw_* post-extracción.
- Transformación, homologación y validación final sobre stg_*.
- Carga idempotente al modelo dimensional/analítico.
- Publicación de features sin data leakage a Apache Kafka.

Flujo:
    extraer_sivigila ─┐
    extraer_api ──────┤
    extraer_vivienda ─┼──> validar_raw ──> transformar_violencia ──> transformar_fuentes_nuevas ──> validar_calidad ──> cargar_fuentes_nuevas ──> publicar_kafka
    extraer_scraping ─┘   (GE temprano)                                                            (GE staging)        (UPSERT / DW)             (Streaming ML)

Programación: todos los días a las 6:00 a. m. (hora de Colombia).
"""

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

ARGUMENTOS = {
    "owner": "equipo_etl",
    "retries": 2,                          # si una tarea falla, se reintenta 2 veces
    "retry_delay": timedelta(minutes=5),   # esperando 5 minutos entre intentos
}


@dag(
    dag_id="etl_violencia_genero_bucaramanga",
    description="Ingesta de 4 fuentes, validaciones GE, carga idempotente al DW y streaming Kafka",
    schedule="0 6 * * *",                  # diario a las 6:00 a. m.
    start_date=pendulum.datetime(2026, 10, 1, tz="America/Bogota"),
    catchup=False,                         # no ejecuta los días pasados desde start_date
    max_active_runs=1,                     # nunca dos ejecuciones al mismo tiempo
    default_args=ARGUMENTOS,
    tags=["etl", "ods5", "bucaramanga", "kafka"],
)
def etl_violencia_genero():

    @task
    def extraer_sivigila() -> int:
        """Fuente 1: CSV de la primera entrega -> raw_violencia_genero."""
        from etl.ingesta import tarea_sivigila
        return tarea_sivigila()

    @task
    def extraer_api() -> int:
        """Fuente 2: API Medicina Legal (Bucaramanga + muestra nacional) -> raw_api_violencia."""
        from etl.ingesta import tarea_api
        return tarea_api()

    @task
    def extraer_vivienda() -> int:
        """Fuente 3: CSV vivienda ECV 2024 -> raw_vivienda."""
        from etl.ingesta import tarea_vivienda
        return tarea_vivienda()

    @task(execution_timeout=timedelta(minutes=45))
    def extraer_scraping() -> int:
        """Fuente 4: boletines OSEC (incremental) -> raw_scraping_boletines / raw_scraping_comunas."""
        from etl.ingesta import tarea_scraping
        return tarea_scraping()

    @task(retries=0)
    def validar_raw() -> dict:
        """Gate temprano de Great Expectations sobre tablas raw_* antes de transformar."""
        from etl.validacion import validar_extraccion_raw
        return validar_extraccion_raw()

    @task
    def transformar_violencia() -> dict:
        """Transformación de la entrega 1: raw_violencia_genero -> modelo estrella."""
        from etl.transformacion import ejecutar_transformacion_y_enrutado, persistir_esquema_dimensional
        tablas = ejecutar_transformacion_y_enrutado()
        persistir_esquema_dimensional(tablas)
        return {nombre: len(df) for nombre, df in tablas.items()}

    @task
    def transformar_fuentes_nuevas() -> dict:
        """API, OSEC y vivienda: limpieza + homologación + conjunto unificado -> tablas stg_*."""
        from etl.transformacion_entrega2 import transformar_fuentes_nuevas as transformar
        return transformar()

    @task(retries=0)
    def validar_calidad() -> dict:
        """Great Expectations sobre stg_*: registra en log_validaciones y falla si algo no se cumple."""
        from etl.validacion import validar_staging
        return validar_staging()

    @task
    def cargar_fuentes_nuevas() -> dict:
        """stg_* -> hechos_api_violencia, hechos_osec_comuna, hechos_analisis, dataset_unificado (idempotente)."""
        from etl.transformacion_entrega2 import cargar_fuentes_nuevas as cargar
        return cargar()

    @task
    def publicar_kafka() -> dict:
        """Publica registros validados (solo features, sin data leakage) al tópico de Kafka."""
        from etl.producer import ejecutar_producer
        return ejecutar_producer()

    # ---- Orden de ejecución ----
    extracciones = [extraer_sivigila(), extraer_api(), extraer_vivienda(), extraer_scraping()]
    (
        extracciones
        >> validar_raw()
        >> transformar_violencia()
        >> transformar_fuentes_nuevas()
        >> validar_calidad()
        >> cargar_fuentes_nuevas()
        >> publicar_kafka()
    )


etl_violencia_genero()