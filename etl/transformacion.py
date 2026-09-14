import re
import pandas as pd
from sqlalchemy import create_engine, text

# ============================================================
# 0. CONFIGURACIÓN
# ============================================================
DB_USER = "postgres"
DB_PASSWORD = "Tu_clave_postgresql"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "violencia_genero_db"

engine = create_engine(
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ============================================================
# 1. DICCIONARIOS DE DECODIFICACIÓN 
# ============================================================
CON_FIN_MAP = {
    "1": "Vivo",
    "2": "Muerto",
    "0": "Sin información",   # no está en el diccionario oficial; el patrón
                               # de los datos confirma que es "sin dato", no
                               # un cuarto estado real.
}

ESCENARIO_MAP = {
    "1": "Vía pública",
    "2": "Vivienda",
    "3": "Establecimiento educativo",
    "4": "Lugar de trabajo",
    "7": "Otro",
    "8": "Comercio y áreas de servicios",
    "9": "Otros espacios abiertos",
    "10": "Lugares de esparcimiento con expendido de alcohol",
    "11": "Institución de salud",
    "12": "Área deportiva y recreativa",
}

BOOL_MAP = {"1": True, "2": False}

PARENTEZCO_TYPOS = {
    "Padrasto": "Padrastro",
}


# ============================================================
# 2. FUNCIONES DE LIMPIEZA
# ============================================================
def normalizar_texto(valor):
    """Quita espacios y convierte vacíos/None a None real."""
    if valor is None:
        return None
    v = str(valor).strip()
    return v if v not in ("", "None", "nan") else None


def limpiar_parentezco(valor):
    """Corrige tipeos y normaliza espacios/guiones en parentezco_vict.
    No corrige la contradicción 'Madre' con agresor masculino: eso se
    documenta como limitación de calidad de datos, no se adivina."""
    v = normalizar_texto(valor)
    if v is None:
        return None
    v = re.sub(r"\s*-\s*", "-", v)          # "Ex- algo" -> "Ex-algo"
    v = v.replace("comañero", "compañero")   # typo real visto en el CSV
    v = re.sub(r"\s+\(", " (", v)            # un solo espacio antes de "("
    v = re.sub(r"\(\s*a\s*\)", "(a)", v, flags=re.IGNORECASE)
    return PARENTEZCO_TYPOS.get(v, v)


def limpiar_hora(valor):
    """Limpia 'a. m.'/'p. m.' (incluye espacio no separador \\xa0) y
    convierte a datetime.time. Si no hay dato o no se puede parsear,
    devuelve None (no se inventa una hora)."""
    if valor is None:
        return None
    v = str(valor).replace("\xa0", " ").strip()
    if v == "":
        return None
    v = re.sub(r"\s+", " ", v)
    v_norm = (
        v.upper()
        .replace("A. M.", "AM")
        .replace("P. M.", "PM")
        .replace("A.M.", "AM")
        .replace("P.M.", "PM")
        .replace("A M", "AM")
        .replace("P M", "PM")
    )
    for fmt in ("%I:%M:%S %p", "%H:%M:%S"):
        try:
            return pd.to_datetime(v_norm, format=fmt).time()
        except ValueError:
            continue
    return None


# ============================================================
# 3. LEER LA RAW YA FILTRADA (los dos filtros del plan)
# ============================================================
raw = pd.read_sql(
    "SELECT * FROM raw_violencia_genero "
    "WHERE sexo = 'Femenino' AND sexo_agre = 'Masculino'",
    engine,
)
total = pd.read_sql("SELECT COUNT(*) AS n FROM raw_violencia_genero", engine).iloc[0, 0]
solo_mujer = pd.read_sql(
    "SELECT COUNT(*) AS n FROM raw_violencia_genero WHERE sexo = 'Femenino'", engine
).iloc[0, 0]
print(f"Total raw: {total}")
print(f"Víctima mujer: {solo_mujer}")
print(f"Víctima mujer + agresor hombre (base de este script): {len(raw)}")

# Columnas de trabajo, ya limpias en pandas antes de tocar SQL
raw["departamento"] = raw["departamento"].apply(normalizar_texto)
raw["municipio"] = raw["municipio"].apply(normalizar_texto)
raw["area"] = raw["area"].apply(normalizar_texto)
raw["grupo_edad"] = raw["grupo_edad"].apply(normalizar_texto)
raw["ciclo_vida"] = raw["ciclo_vida"].apply(normalizar_texto)
raw["tipo_seg_social_norm"] = raw["tipo_seguridad_social"].apply(
    lambda v: normalizar_texto(v).title() if normalizar_texto(v) else None
)
raw["actividad_norm"] = raw["nom_actividad"].apply(normalizar_texto)
raw["parentezco_norm"] = raw["parentezco_vict"].apply(limpiar_parentezco)
raw["def_naturaleza_norm"] = raw["def_naturaleza"].apply(normalizar_texto)
raw["naturaleza_num"] = pd.to_numeric(raw["naturaleza"], errors="coerce")
raw["year_num"] = pd.to_numeric(raw["year"], errors="coerce")
raw["mes_num"] = pd.to_numeric(raw["mes"], errors="coerce")
raw["semana_num"] = pd.to_numeric(raw["semana"], errors="coerce")

# Diagnóstico: cuántos nulos hay en cada columna de tiempo antes de rellenar
print(f"Nulos en year: {raw['year_num'].isna().sum()} / mes: {raw['mes_num'].isna().sum()} "
      f"/ semana: {raw['semana_num'].isna().sum()} (de {len(raw)} filas)")

# Fallback: si year/mes/semana vienen vacíos en la raw (pasa en el dataset
# histórico de Bucaramanga), se derivan de fec_hecho en vez de perder filas.
fecha_aux = pd.to_datetime(raw["fec_hecho"], errors="coerce")
raw["year_num"] = raw["year_num"].fillna(fecha_aux.dt.year)
raw["mes_num"] = raw["mes_num"].fillna(fecha_aux.dt.month)
raw["semana_num"] = raw["semana_num"].fillna(fecha_aux.dt.isocalendar().week.astype(float))

# edad_agre: el diccionario dice que el rango válido es 10-99. Cualquier
# valor FUERA de ese rango (no solo el 0) se convierte a NULL, porque un
# valor como 5 no es un "código de sin dato" documentado, es un dato
# inválido/atípico igual de no confiable que el 0.
raw["edad_agre_num"] = pd.to_numeric(raw["edad_agre"], errors="coerce")
raw.loc[(raw["edad_agre_num"] < 10) | (raw["edad_agre_num"] > 99), "edad_agre_num"] = pd.NA

# ============================================================
# PARTE 1: Jerarquía geográfica
# ============================================================
with engine.begin() as conn:
    conn.execute(text(
        "INSERT INTO dim_pais (nombre_pais) VALUES ('Colombia') "
        "ON CONFLICT (nombre_pais) DO NOTHING"
    ))
    id_pais = conn.execute(text(
        "SELECT id_pais FROM dim_pais WHERE nombre_pais = 'Colombia'"
    )).scalar()

    for depto in raw["departamento"].dropna().unique():
        conn.execute(text(
            "INSERT INTO dim_departamento (id_pais, nombre_departamento) "
            "VALUES (:id_pais, :depto) "
            "ON CONFLICT (id_pais, nombre_departamento) DO NOTHING"
        ), {"id_pais": id_pais, "depto": depto})

    municipios = raw[["departamento", "municipio", "area"]].dropna().drop_duplicates()
    for _, fila in municipios.iterrows():
        id_departamento = conn.execute(text(
            "SELECT id_departamento FROM dim_departamento "
            "WHERE id_pais = :id_pais AND nombre_departamento = :depto"
        ), {"id_pais": id_pais, "depto": fila["departamento"]}).scalar()
        conn.execute(text(
            "INSERT INTO dim_municipio (id_departamento, nombre_municipio, area) "
            "VALUES (:id_departamento, :municipio, :area) "
            "ON CONFLICT (id_departamento, nombre_municipio, area) DO NOTHING"
        ), {"id_departamento": id_departamento, "municipio": fila["municipio"], "area": fila["area"]})

print("PARTE 1 lista: jerarquía geográfica cargada.")

# Lookup completo de municipio para usarlo en la PARTE 6
dim_municipio_lookup = pd.read_sql(
    """
    SELECT m.id_municipio, d.nombre_departamento AS departamento,
           m.nombre_municipio AS municipio, m.area
    FROM dim_municipio m
    JOIN dim_departamento d ON m.id_departamento = d.id_departamento
    """,
    engine,
)

# ============================================================
# PARTE 2: dim_tiempo
# ============================================================
tiempos = raw[["year_num", "mes_num", "semana_num"]].dropna().drop_duplicates()
with engine.begin() as conn:
    for _, fila in tiempos.iterrows():
        conn.execute(text(
            "INSERT INTO dim_tiempo (year, mes, semana) "
            "VALUES (:year, :mes, :semana) "
            "ON CONFLICT (year, mes, semana) DO NOTHING"
        ), {"year": int(fila["year_num"]), "mes": int(fila["mes_num"]), "semana": int(fila["semana_num"])})
print(f"PARTE 2 lista: dim_tiempo ({len(tiempos)} combinaciones únicas).")

dim_tiempo_lookup = pd.read_sql("SELECT * FROM dim_tiempo", engine)

# ============================================================
# PARTE 3: dim_victima
# ============================================================
victimas = raw[["grupo_edad", "ciclo_vida", "tipo_seg_social_norm", "actividad_norm"]].drop_duplicates()
with engine.begin() as conn:
    for _, fila in victimas.iterrows():
        conn.execute(text(
            "INSERT INTO dim_victima (grupo_edad, ciclo_vida, tipo_seguridad_social, actividad) "
            "VALUES (:grupo_edad, :ciclo_vida, :tipo_seg, :actividad) "
            "ON CONFLICT (grupo_edad, ciclo_vida, tipo_seguridad_social, actividad) DO NOTHING"
        ), {
            "grupo_edad": fila["grupo_edad"], "ciclo_vida": fila["ciclo_vida"],
            "tipo_seg": fila["tipo_seg_social_norm"], "actividad": fila["actividad_norm"],
        })
print(f"PARTE 3 lista: dim_victima ({len(victimas)} combinaciones únicas).")

dim_victima_lookup = pd.read_sql("SELECT * FROM dim_victima", engine)

# ============================================================
# PARTE 4: dim_agresor
# ============================================================
agresores = raw[["edad_agre_num", "parentezco_norm"]].drop_duplicates()
with engine.begin() as conn:
    for _, fila in agresores.iterrows():
        edad = None if pd.isna(fila["edad_agre_num"]) else int(fila["edad_agre_num"])
        conn.execute(text(
            "INSERT INTO dim_agresor (edad_agre, sexo_agre, parentezco_vict) "
            "VALUES (:edad, 'Masculino', :parentezco) "
            "ON CONFLICT (edad_agre, sexo_agre, parentezco_vict) DO NOTHING"
        ), {"edad": edad, "parentezco": fila["parentezco_norm"]})
print(f"PARTE 4 lista: dim_agresor ({len(agresores)} combinaciones únicas).")

dim_agresor_lookup = pd.read_sql("SELECT * FROM dim_agresor", engine)

# ============================================================
# PARTE 5: dim_tipo_violencia
# ============================================================
tipos = raw[["naturaleza_num", "def_naturaleza_norm"]].dropna().drop_duplicates()
with engine.begin() as conn:
    for _, fila in tipos.iterrows():
        conn.execute(text(
            "INSERT INTO dim_tipo_violencia (naturaleza, def_naturaleza) "
            "VALUES (:naturaleza, :def_naturaleza) "
            "ON CONFLICT (naturaleza) DO NOTHING"
        ), {"naturaleza": int(fila["naturaleza_num"]), "def_naturaleza": fila["def_naturaleza_norm"]})
print(f"PARTE 5 lista: dim_tipo_violencia ({len(tipos)} tipos únicos).")

dim_tipo_lookup = pd.read_sql("SELECT * FROM dim_tipo_violencia", engine)

# ============================================================
# PARTE 6: hechos_violencia
# ============================================================
df = raw.copy()

# --- resolver id_municipio ---
df = df.merge(
    dim_municipio_lookup,
    on=["departamento", "municipio", "area"],
    how="left",
)

# --- resolver id_tiempo ---
df = df.merge(
    dim_tiempo_lookup.rename(columns={"year": "year_num", "mes": "mes_num", "semana": "semana_num"}),
    on=["year_num", "mes_num", "semana_num"],
    how="left",
)

# --- resolver id_victima ---
df = df.merge(
    dim_victima_lookup.rename(columns={
        "grupo_edad": "grupo_edad", "ciclo_vida": "ciclo_vida",
        "tipo_seguridad_social": "tipo_seg_social_norm", "actividad": "actividad_norm",
    }),
    on=["grupo_edad", "ciclo_vida", "tipo_seg_social_norm", "actividad_norm"],
    how="left",
)

# --- resolver id_agresor (usando -1 como comodín para que NaN "matchee" con NaN) ---
SENTINELA = -1
df["edad_merge_key"] = df["edad_agre_num"].fillna(SENTINELA)
dim_agresor_lookup["edad_merge_key"] = dim_agresor_lookup["edad_agre"].fillna(SENTINELA)
df = df.merge(
    dim_agresor_lookup.rename(columns={"parentezco_vict": "parentezco_norm"})[
        ["id_agresor", "edad_merge_key", "parentezco_norm"]
    ],
    on=["edad_merge_key", "parentezco_norm"],
    how="left",
)

# --- resolver id_tipo_violencia ---
df = df.merge(
    dim_tipo_lookup.rename(columns={"naturaleza": "naturaleza_num", "def_naturaleza": "def_naturaleza_norm"}),
    on=["naturaleza_num", "def_naturaleza_norm"],
    how="left",
)

# --- decodificar columnas propias de hechos_violencia ---
df["con_fin_dec"] = df["con_fin"].map(CON_FIN_MAP)
df["escenario_dec"] = df["escenario"].map(ESCENARIO_MAP)
df["pac_hos_bool"] = df["pac_hos"].map(BOOL_MAP)
df["sust_vict_bool"] = df["sust_vict"].map(BOOL_MAP)
df["fecha_hecho_dt"] = pd.to_datetime(df["fec_hecho"], errors="coerce").dt.date
df["hora_hecho_time"] = df["hora_hecho"].apply(limpiar_hora)
df["zona_conf_raw"] = df["zona_conf"].apply(normalizar_texto)  # se guarda sin decodificar (ver plan)

# --- filas que no lograron mapear alguna FK (dato faltante en columnas clave) ---
claves_fk = ["id_municipio", "id_tiempo", "id_victima", "id_agresor", "id_tipo_violencia"]
sin_fk = df[df[claves_fk].isna().any(axis=1)]
if len(sin_fk) > 0:
    print(f"AVISO: {len(sin_fk)} filas no encontraron alguna llave y se descartan de hechos_violencia "
          f"(revisa columnas clave con valores vacíos en la raw).")
df = df.dropna(subset=claves_fk)

df_hechos = pd.DataFrame({
    "id_municipio": df["id_municipio"].astype(int),
    "id_tiempo": df["id_tiempo"].astype(int),
    "id_victima": df["id_victima"].astype(int),
    "id_agresor": df["id_agresor"].astype(int),
    "id_tipo_violencia": df["id_tipo_violencia"].astype(int),
    "escenario": df["escenario_dec"],
    "zona_conf": df["zona_conf_raw"],
    "con_fin": df["con_fin_dec"],
    "pac_hos": df["pac_hos_bool"],
    "sust_vict": df["sust_vict_bool"],
    "fecha_hecho": df["fecha_hecho_dt"],
    "hora_hecho": df["hora_hecho_time"],
    "fuente": df["fuente"],
})

# hechos_violencia no tiene una llave natural única, así que para que el
# script se pueda volver a correr sin duplicar, se borra primero lo que ya
# exista de esta misma fuente antes de insertar de nuevo.
fuente_actual = df_hechos["fuente"].iloc[0] if len(df_hechos) else None
with engine.begin() as conn:
    if fuente_actual:
        conn.execute(text("DELETE FROM hechos_violencia WHERE fuente = :f"), {"f": fuente_actual})
    df_hechos.to_sql("hechos_violencia", conn, if_exists="append", index=False)

print(f"PARTE 6 lista: {len(df_hechos)} filas insertadas en hechos_violencia (fuente='{fuente_actual}').")
print("\nTransformación completa.")