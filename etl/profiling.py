"""
etl/profiling.py
Paso previo a la transformación: mirar qué valores reales trae cada columna
"problemática" de la raw, para diseñar bien las reglas de limpieza.
No modifica nada, solo lee e imprime.
"""

import pandas as pd
from sqlalchemy import create_engine

DB_USER = "postgres"
DB_PASSWORD = "Tu_clave_postgresql"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "violencia_genero_db"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

consultas = {
    "sexo (víctima)": "SELECT DISTINCT sexo, COUNT(*) FROM raw_violencia_genero GROUP BY sexo ORDER BY 1",
    "con_fin (¿vivo/muerto?)": "SELECT DISTINCT con_fin, COUNT(*) FROM raw_violencia_genero GROUP BY con_fin ORDER BY 1",
    "naturaleza + def_naturaleza": "SELECT DISTINCT naturaleza, def_naturaleza, COUNT(*) FROM raw_violencia_genero GROUP BY naturaleza, def_naturaleza ORDER BY 1",
    "escenario": "SELECT DISTINCT escenario, COUNT(*) FROM raw_violencia_genero GROUP BY escenario ORDER BY 1",
    "zona_conf": "SELECT DISTINCT zona_conf, COUNT(*) FROM raw_violencia_genero GROUP BY zona_conf ORDER BY 1",
    "sexo_agre": "SELECT DISTINCT sexo_agre, COUNT(*) FROM raw_violencia_genero GROUP BY sexo_agre ORDER BY 1",
    "area": "SELECT DISTINCT area, COUNT(*) FROM raw_violencia_genero GROUP BY area ORDER BY 1",
    "tipo_seguridad_social": "SELECT DISTINCT tipo_seguridad_social, COUNT(*) FROM raw_violencia_genero GROUP BY tipo_seguridad_social ORDER BY 1",
    "edad_agre (nulos/raros)": "SELECT edad_agre, COUNT(*) FROM raw_violencia_genero GROUP BY edad_agre ORDER BY 2 DESC LIMIT 10",
    "fec_hecho + hora_hecho (muestra)": "SELECT fec_hecho, hora_hecho FROM raw_violencia_genero LIMIT 5",
}

for titulo, sql in consultas.items():
    print(f"\n{'=' * 60}\n{titulo}\n{'=' * 60}")
    print(pd.read_sql(sql, engine).to_string(index=False))