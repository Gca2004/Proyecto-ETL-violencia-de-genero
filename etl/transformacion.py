"""
etl/transformacion.py
Transformación y carga del modelo dimensional (estrella) de violencia de género.

Lee   : raw_violencia_genero            (tabla cruda cargada por la ingesta)
Escribe: dim_pais, dim_departamento, dim_municipio, dim_tiempo, dim_victima,
         dim_agresor, dim_tipo_violencia, hechos_violencia   (sql/esquema.sql)

Reglas clave
------------
* El esquema se crea UNA vez con sql/esquema.sql. Este módulo NO borra ni
  recrea tablas: vacía (TRUNCATE) y carga (append) dentro de UNA transacción.
  Si algo falla (CHECK, FK, etc.) se hace ROLLBACK y la BD queda como estaba.
* Textos: MAYÚSCULAS y sin tildes; desconocido = 'NO REPORTADO'.
  El turno NO pasa por normalizar_cadena (conserva 'MAÑANA').
* Nunca se "rellenan" claves foráneas: si un merge falla, se lanza error.

CAMBIO (entrega 2): hechos_violencia guarda además el sexo de la víctima
(sexo_victima) y la comuna (id_comuna -> dim_comuna). La comuna se homologa
con etl/homologacion.py para poder comparar SIVIGILA con los boletines OSEC.
Requiere haber ejecutado sql/esquema_entrega2.sql.

Uso:
    python etl/transformacion.py          (desde la raíz del proyecto)
    python -m etl.transformacion          (o desde Airflow con la raíz en PYTHONPATH)
"""

import logging
import os
import unicodedata
import warnings
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

try:  # python -m etl.transformacion  /  Airflow con la raíz del proyecto en PYTHONPATH
    from etl.turno import NO_REPORTADO, TURNOS_VALIDOS, calcular_turno
except ImportError:  # python etl/transformacion.py
    from turno import NO_REPORTADO, TURNOS_VALIDOS, calcular_turno

try:
    from etl.homologacion import comuna_a_id, sexo as sexo_homologado
except ImportError:
    from homologacion import comuna_a_id, sexo as sexo_homologado

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
)
logger = logging.getLogger("ETL_Transformacion")

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
RUTA_ENV = RAIZ_PROYECTO / ".env"
load_dotenv(RUTA_ENV)  # busca el .env en la raíz del proyecto, sin depender de dónde ejecutes

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "violencia_genero_db")

TABLA_RAW = "raw_violencia_genero"
FUENTE_POR_DEFECTO = "SIVIGILA_Colombia"

# Orden de carga = orden de dependencias de las llaves foráneas
ORDEN_CARGA = [
    "dim_pais",
    "dim_departamento",
    "dim_municipio",
    "dim_tiempo",
    "dim_victima",
    "dim_agresor",
    "dim_tipo_violencia",
    "hechos_violencia",
]
# Tablas cuyo id se inserta explícito -> hay que reajustar su secuencia SERIAL
SECUENCIAS = {
    "dim_pais": "id_pais",
    "dim_departamento": "id_departamento",
    "dim_municipio": "id_municipio",
    "dim_tiempo": "id_tiempo",
    "dim_victima": "id_victima",
    "dim_agresor": "id_agresor",
    "dim_tipo_violencia": "id_tipo_violencia",
}


# ----------------------------------------------------------------------------
# Conexión
# ----------------------------------------------------------------------------
def obtener_motor_db():
    if not DB_PASSWORD:
        raise RuntimeError(
            f"Falta DB_PASSWORD. Se buscó en: {RUTA_ENV} "
            f"(existe: {RUTA_ENV.exists()}). Crea esa línea: DB_PASSWORD=tu_clave"
        )
    url = URL.create(  # escapa bien contraseñas con @ : / etc.
        "postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=int(DB_PORT),
        database=DB_NAME,
    )
    return create_engine(url)


# ----------------------------------------------------------------------------
# Limpieza de valores
# ----------------------------------------------------------------------------
def normalizar_cadena(texto) -> str:
    """MAYÚSCULAS, sin tildes, sin espacios sobrantes. Vacío/nulo -> 'NO REPORTADO'."""
    if texto is None or pd.isna(texto):
        return NO_REPORTADO
    val = unicodedata.normalize("NFKD", str(texto).strip().upper())
    val = val.encode("ASCII", "ignore").decode("utf-8")
    val = " ".join(val.split())
    return val if val else NO_REPORTADO


def _mapear_unicos(serie: pd.Series, fn) -> pd.Series:
    """
    Aplica fn solo a los valores distintos (rápido). Los nulos (None, NaN, NA,
    NaT) se tratan aparte con fn(None): mezclar None y NaN en un dict hace
    fallar a Series.map con InvalidIndexError.
    """
    nulos = serie.isna()
    mapa = {v: fn(v) for v in serie[~nulos].unique()}
    resultado = serie.map(mapa)
    return resultado.where(~nulos, fn(None))


def _texto(serie: pd.Series) -> pd.Series:
    return _mapear_unicos(serie, normalizar_cadena).astype(object)


_VERDADERO = {"1", "SI", "S", "TRUE", "VERDADERO", "YES", "Y"}
_FALSO = {"2", "NO", "N", "FALSE", "FALSO"}


def _a_booleano(valor):
    """1/SI/True -> True ; 2/NO/False -> False ; cualquier otra cosa -> None."""
    if valor is None or pd.isna(valor):
        return None
    s = normalizar_cadena(valor)
    if s.endswith(".0"):
        s = s[:-2]
    if s in _VERDADERO:
        return True
    if s in _FALSO:
        return False
    return None


def _booleano(serie: pd.Series) -> pd.Series:
    r = _mapear_unicos(serie, _a_booleano).astype(object)
    return r.where(r.notna(), None)


def _edad(serie: pd.Series) -> pd.Series:
    """Edad numérica; fuera de 10..99 (lo que acepta el CHECK) -> -1 (= desconocida)."""
    n = pd.to_numeric(serie, errors="coerce")
    n = n.where(n.between(10, 99))
    return n.round().fillna(-1).astype(int)


def _parsear_fecha(serie: pd.Series) -> pd.Series:
    """
    Acepta aaaa-mm-dd, aaaa/mm/dd (año primero) y dd/mm/aaaa, dd-mm-aaaa
    (año al final, formato colombiano). Ignora la hora. Lo no interpretable -> NaT.
    """
    if pd.api.types.is_datetime64_any_dtype(serie):
        return serie.dt.tz_localize(None) if serie.dt.tz is not None else serie
    s = serie.astype("string").str.strip()
    p = s.str.extract(r"^(\d{1,4})[-/.](\d{1,2})[-/.](\d{1,4})")
    a, b, c = (pd.to_numeric(p[i], errors="coerce").astype("float64") for i in range(3))
    anio_primero = p[0].str.len() == 4
    anio = a.where(anio_primero, c)
    dia = c.where(anio_primero, a)
    anio = anio.where(anio.between(1900, 2100))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        return pd.to_datetime(
            pd.DataFrame({"year": anio, "month": b, "day": dia}), errors="coerce"
        )


# ----------------------------------------------------------------------------
# Construcción de dimensiones
# ----------------------------------------------------------------------------
def _dimension(w: pd.DataFrame, claves: list, id_col: str) -> pd.DataFrame:
    d = w[claves].drop_duplicates().reset_index(drop=True)
    d.insert(0, id_col, range(1, len(d) + 1))
    return d


def _unir(w: pd.DataFrame, dim: pd.DataFrame, claves: list, id_col: str) -> pd.DataFrame:
    # validate evita que una dimensión con claves repetidas multiplique las filas
    return w.merge(dim[claves + [id_col]], on=claves, how="left", validate="many_to_one")


# ----------------------------------------------------------------------------
# Transformación
# ----------------------------------------------------------------------------
def ejecutar_transformacion_y_enrutado() -> dict:
    logger.info("Iniciando transformación y enrutado dimensional...")
    engine = obtener_motor_db()
    try:
        raw = pd.read_sql(f"SELECT * FROM {TABLA_RAW}", engine)
    finally:
        engine.dispose()

    if raw.empty:
        logger.error("La tabla '%s' está vacía.", TABLA_RAW)
        return {}
    n_raw = len(raw)

    faltantes = []

    def col(*nombres):
        """Primera columna existente de raw; si no hay ninguna, serie de nulos."""
        for nom in nombres:
            if nom in raw.columns:
                return raw[nom]
        faltantes.append("/".join(nombres))
        return pd.Series(None, index=raw.index, dtype="object")

    w = pd.DataFrame(index=raw.index)

    # --- geografía -----------------------------------------------------------
    w["id_pais"] = 1
    w["departamento"] = _texto(col("departamento", "ndep_resi"))
    w["municipio"] = _texto(col("municipio", "nmun_resi"))
    w["area"] = _texto(col("area"))

    # --- tiempo --------------------------------------------------------------
    fecha = _parsear_fecha(col("fecha_hecho", "fec_hecho"))
    anio_raw = pd.to_numeric(col("year", "anio"), errors="coerce")
    mes_raw = pd.to_numeric(col("mes"), errors="coerce")
    sem_raw = pd.to_numeric(col("semana"), errors="coerce")
    # Si hay fecha se derivan year/mes/semana ISO de ella (consistente entre fuentes);
    # si no, se usan las columnas crudas y, en último caso, 0 (= desconocido).
    w["year"] = fecha.dt.year.astype("float64").fillna(anio_raw).fillna(0).astype(int)
    w["mes"] = fecha.dt.month.astype("float64").fillna(mes_raw).fillna(0).astype(int)
    w["semana"] = (
        fecha.dt.isocalendar().week.astype("float64").fillna(sem_raw).fillna(0).astype(int)
    )
    w["fecha_hecho"] = fecha.dt.date.where(fecha.notna(), None)

    # --- turno (NO pasa por normalizar_cadena) --------------------------------
    w["turno"] = _mapear_unicos(col("hora_hecho", "hora"), calcular_turno)
    invalidos = ~w["turno"].isin(TURNOS_VALIDOS)
    if invalidos.any():
        raise ValueError(f"Turnos fuera de {TURNOS_VALIDOS}: {w.loc[invalidos, 'turno'].unique()[:5]}")

    # --- víctima -------------------------------------------------------------
    w["grupo_edad"] = _texto(col("grupo_edad"))
    w["ciclo_vida"] = _texto(col("ciclo_vida"))
    w["tipo_seguridad_social"] = _texto(col("tipo_seguridad_social"))
    w["actividad"] = _texto(col("actividad"))
    w["sexo_victima"] = _mapear_unicos(col("sexo"), sexo_homologado).astype(object)

    # --- comuna (entrega 2): solo tiene sentido para Bucaramanga; otro municipio -> 0 (SIN COMUNA)
    id_comuna = _mapear_unicos(col("comuna"), comuna_a_id).astype(int)
    w["id_comuna"] = id_comuna.where(w["municipio"] == "BUCARAMANGA", 0).astype(int)

    # --- agresor -------------------------------------------------------------
    w["edad_agre_k"] = _edad(col("edad_agre"))
    w["sexo_agre"] = _texto(col("sexo_agre"))
    w["parentezco_vict"] = _texto(col("parentezco_vict"))

    # --- tipo de violencia -----------------------------------------------------
    nat_raw = col("naturaleza")
    nat = pd.to_numeric(nat_raw, errors="coerce")
    no_numerica = nat_raw.notna() & nat.isna()
    if no_numerica.any():
        logger.warning(
            "naturaleza no numérica en %d filas (ej. %s): se asignó código 0. "
            "Revisa la columna en la tabla cruda.",
            int(no_numerica.sum()), list(nat_raw[no_numerica].astype(str).unique()[:3]),
        )
    w["naturaleza_cod"] = nat.round().fillna(0).astype(int)
    w["def_naturaleza"] = _texto(col("def_naturaleza"))

    # --- atributos del hecho ---------------------------------------------------
    w["escenario"] = _texto(col("escenario"))
    w["zona_conf"] = _texto(col("zona_conf"))
    w["con_fin"] = _texto(col("con_fin"))
    w["pac_hos"] = _booleano(col("pac_hos"))
    w["sust_vict"] = _booleano(col("sust_vict"))
    fuente = col("fuente")
    w["fuente"] = fuente.where(fuente.notna(), FUENTE_POR_DEFECTO).astype(str).str.strip()

    if faltantes:
        logger.warning(
            "Columnas no encontradas en '%s' (se usó NO REPORTADO / 0 / NULL): %s",
            TABLA_RAW, ", ".join(faltantes),
        )

    # ========================= DIMENSIONES =====================================
    dim_pais = pd.DataFrame({"id_pais": [1], "nombre_pais": ["COLOMBIA"]})

    dep = _dimension(w, ["id_pais", "departamento"], "id_departamento")
    w = _unir(w, dep, ["id_pais", "departamento"], "id_departamento")
    dim_departamento = dep.rename(columns={"departamento": "nombre_departamento"})[
        ["id_departamento", "id_pais", "nombre_departamento"]
    ]

    mun_claves = ["id_departamento", "municipio", "area"]
    mun = _dimension(w, mun_claves, "id_municipio")
    w = _unir(w, mun, mun_claves, "id_municipio")
    dim_municipio = mun.rename(columns={"municipio": "nombre_municipio"})[
        ["id_municipio", "id_departamento", "nombre_municipio", "area"]
    ]

    t_claves = ["year", "mes", "semana"]
    dim_tiempo = _dimension(w, t_claves, "id_tiempo")
    w = _unir(w, dim_tiempo, t_claves, "id_tiempo")

    v_claves = ["grupo_edad", "ciclo_vida", "tipo_seguridad_social", "actividad"]
    dim_victima = _dimension(w, v_claves, "id_victima")
    w = _unir(w, dim_victima, v_claves, "id_victima")

    a_claves = ["edad_agre_k", "sexo_agre", "parentezco_vict"]
    ag = _dimension(w, a_claves, "id_agresor")
    w = _unir(w, ag, a_claves, "id_agresor")
    dim_agresor = ag.copy()
    dim_agresor["edad_agre"] = dim_agresor["edad_agre_k"].where(dim_agresor["edad_agre_k"] != -1).astype("Int64")
    dim_agresor = dim_agresor[["id_agresor", "edad_agre", "sexo_agre", "parentezco_vict"]]

    # UNIQUE(naturaleza): un solo registro por código, prefiriendo la def. informada
    tv = w[["naturaleza_cod", "def_naturaleza"]].copy()
    tv["_sin_def"] = tv["def_naturaleza"] == NO_REPORTADO
    tv = (
        tv.sort_values("_sin_def", kind="stable")
        .drop_duplicates("naturaleza_cod")
        .sort_values("naturaleza_cod")
        .drop(columns="_sin_def")
        .reset_index(drop=True)
    )
    tv.insert(0, "id_tipo_violencia", range(1, len(tv) + 1))
    w = _unir(w, tv, ["naturaleza_cod"], "id_tipo_violencia")
    dim_tipo_violencia = tv.rename(columns={"naturaleza_cod": "naturaleza"})[
        ["id_tipo_violencia", "naturaleza", "def_naturaleza"]
    ]

    # ========================= HECHOS ==========================================
    ids = ["id_municipio", "id_tiempo", "id_victima", "id_agresor", "id_tipo_violencia"]
    if len(w) != n_raw:
        raise ValueError(f"Los merges cambiaron el número de filas: {n_raw} -> {len(w)}")
    if w[ids].isna().any().any():
        raise ValueError(f"Hay claves foráneas sin resolver:\n{w[ids].isna().sum()}")

    hechos = w[
        ids + ["escenario", "zona_conf", "con_fin", "pac_hos", "sust_vict",
               "fecha_hecho", "turno", "fuente", "sexo_victima", "id_comuna"]
    ].reset_index(drop=True)
    for c in ids + ["id_comuna"]:
        hechos[c] = hechos[c].astype(int)

    logger.info(
        "Transformación completada: %d filas crudas -> %d en hechos_violencia "
        "(municipios=%d, tiempos=%d, víctimas=%d, agresores=%d, tipos=%d).",
        n_raw, len(hechos), len(dim_municipio), len(dim_tiempo),
        len(dim_victima), len(dim_agresor), len(dim_tipo_violencia),
    )

    return {
        "dim_pais": dim_pais,
        "dim_departamento": dim_departamento,
        "dim_municipio": dim_municipio,
        "dim_tiempo": dim_tiempo,
        "dim_victima": dim_victima,
        "dim_agresor": dim_agresor,
        "dim_tipo_violencia": dim_tipo_violencia,
        "hechos_violencia": hechos,
    }


# ----------------------------------------------------------------------------
# Carga
# ----------------------------------------------------------------------------
def persistir_esquema_dimensional(tablas: dict):
    """Vacía y carga todas las tablas del modelo en UNA transacción (todo o nada)."""
    if not tablas:
        logger.warning("No hay tablas para persistir.")
        return

    engine = obtener_motor_db()
    try:
        with engine.begin() as conn:
            existentes = {
                r[0] for r in conn.execute(text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = current_schema()"))
            }
            sin_crear = [t for t in ORDEN_CARGA if t not in existentes]
            if sin_crear:
                raise RuntimeError(
                    f"Faltan tablas en la BD: {sin_crear}. Ejecuta primero sql/esquema.sql"
                )

            conn.execute(text(
                "TRUNCATE TABLE " + ", ".join(reversed(ORDEN_CARGA)) + " RESTART IDENTITY CASCADE"
            ))

            for nombre in ORDEN_CARGA:
                df = tablas[nombre]
                df.to_sql(nombre, conn, if_exists="append", index=False,
                          method="multi", chunksize=1000)
                logger.info("Tabla '%s' cargada (%d filas).", nombre, len(df))

            for tabla, pk in SECUENCIAS.items():  # ids insertados explícitos -> ajustar SERIAL
                conn.execute(text(
                    f"SELECT setval(pg_get_serial_sequence('{tabla}', '{pk}'), "
                    f"(SELECT COALESCE(MAX({pk}), 0) + 1 FROM {tabla}), false)"
                ))

            en_bd = conn.execute(text("SELECT COUNT(*) FROM hechos_violencia")).scalar()
            if en_bd != len(tablas["hechos_violencia"]):
                raise RuntimeError(
                    f"hechos_violencia: se esperaban {len(tablas['hechos_violencia'])} filas y hay {en_bd}"
                )
    finally:
        engine.dispose()


if __name__ == "__main__":
    tablas_dw = ejecutar_transformacion_y_enrutado()
    persistir_esquema_dimensional(tablas_dw)
    print("\n¡Transformación y carga dimensional completadas!")
