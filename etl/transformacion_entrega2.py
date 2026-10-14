"""
etl/transformacion_entrega2.py
Transformación de las fuentes nuevas (API Medicina Legal, boletines OSEC y
ECV 2024) e integración de las 4 fuentes en el conjunto unificado.

El proceso tiene DOS pasos, y cada uno es una tarea del DAG:

1. transformar_fuentes_nuevas()
   Lee las tablas crudas (raw_*) y el modelo de SIVIGILA, limpia, homologa y
   escribe el resultado en las tablas de STAGING (stg_*). Todavía no toca el
   modelo final.

2. cargar_fuentes_nuevas()
   Ejecuta sql/cargar_entrega2.sql: resuelve los ids de las dimensiones y pasa
   stg_* a las tablas finales, todo en UNA transacción.

Entre los dos pasos va la validación con Great Expectations (Paso 7): si los
datos de staging no cumplen las reglas, la carga no se ejecuta.

Requisitos: sql/raw_fuentes_entrega2.sql y sql/esquema_entrega2.sql ejecutados,
y que etl/transformacion.py ya haya cargado el modelo de SIVIGILA.

Uso manual (desde la raíz del proyecto):
    python etl/transformacion_entrega2.py
"""

import logging
import os
import re
import time
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

try:  # Airflow / python -m
    from etl import homologacion as hom
    from etl.turno import calcular_turno
except ImportError:  # python etl/transformacion_entrega2.py
    import homologacion as hom
    from turno import calcular_turno

logging.basicConfig(level=logging.INFO, format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s")
logger = logging.getLogger("ETL_Transformacion_E2")

RAIZ_PROYECTO = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ_PROYECTO / ".env")
RUTA_SQL_CARGA = RAIZ_PROYECTO / "sql" / "cargar_entrega2.sql"

DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "violencia_genero_db")

URL_DIVIPOLA = "https://www.datos.gov.co/resource/gdxc-w37w.json"
# Respaldo si DIVIPOLA no responde: al menos Bucaramanga queda con su código
BUCARAMANGA_RESPALDO = {"nombre_departamento": "SANTANDER", "nombre_municipio": "BUCARAMANGA",
                        "codigo_dane": "68001", "latitud": 7.119349, "longitud": -73.122742}

# El conjunto unificado empieza cuando arranca el registro sistemático de SIVIGILA y la API.
# Los hechos anteriores (ocurridos antes de 2015 pero notificados después) siguen en
# hechos_violencia, pero no entran a la serie mensual: serían meses incompletos.
ANIO_INICIO_DATASET = 2015

TABLAS_STAGING = ["stg_hechos_api", "stg_hechos_osec", "stg_hechos_analisis",
                  "stg_codigo_dane", "stg_dataset_unificado"]


# ---------------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------------
def obtener_motor_db():
    if not DB_PASSWORD:
        raise RuntimeError(f"Falta DB_PASSWORD en {RAIZ_PROYECTO / '.env'}")
    url = URL.create("postgresql+psycopg2", username=DB_USER, password=DB_PASSWORD,
                     host=DB_HOST, port=int(DB_PORT), database=DB_NAME)
    return create_engine(url)


def _mapear(serie: pd.Series, funcion) -> pd.Series:
    """Aplica la función una vez por valor distinto (rápido) y la reparte a todas las filas."""
    mapa = {v: funcion(v) for v in serie.dropna().unique()}
    nulo = funcion(None)
    return serie.map(mapa).where(serie.notna(), nulo)


def _entero(serie: pd.Series) -> pd.Series:
    """Texto con formato colombiano ("1.128") -> entero; lo no numérico -> NaN."""
    return pd.to_numeric(serie.astype(str).str.replace(".", "", regex=False).str.strip(), errors="coerce")


# ---------------------------------------------------------------------------
# Fuente 2: API Medicina Legal  ->  stg_hechos_api
# ---------------------------------------------------------------------------
def transformar_api(raw: pd.DataFrame) -> pd.DataFrame:
    """raw_api_violencia (alcance BUCARAMANGA) -> un caso por fila, solo víctimas mujeres."""
    sexo = _mapear(raw["sexo_de_la_victima"], hom.sexo)
    mujeres = raw[sexo == "FEMENINO"].copy()
    logger.info("API: %d casos de Bucaramanga, %d con víctima mujer (%d excluidos por sexo).",
                len(raw), len(mujeres), len(raw) - len(mujeres))

    contexto = _mapear(mujeres["contexto_del_hecho"], hom.normalizar).str.replace(r"^\d+\s+", "", regex=True)
    df = pd.DataFrame({
        "id_caso_api": mujeres["id"].astype(str),
        "anio": pd.to_numeric(mujeres["a_o_del_hecho"], errors="coerce").fillna(0).astype(int),
        "mes": _mapear(mujeres["mes_del_hecho"], hom.mes).astype(int),
        "area": _mapear(mujeres["zona_del_hecho"], hom.area),
        "ciclo_vida": _mapear(mujeres["ciclo_vital"], hom.ciclo_vida),
        "grupo_agresor": _mapear(mujeres["presunto_agresor_detallado"], hom.grupo_agresor),
        "presunto_agresor": _mapear(mujeres["presunto_agresor_detallado"], hom.normalizar),
        "sexo_agresor": _mapear(mujeres["sexo_del_agresor"], hom.sexo),
        "tipo_violencia": _mapear(mujeres["circunstancia_del_hecho_detallada"], hom.normalizar),
        "contexto": contexto,
        "escenario": _mapear(mujeres["escenario_del_hecho"], hom.normalizar),
        "turno": _mapear(mujeres["rango_de_hora_del_hecho_x_3_horas"], calcular_turno),
        "dias_incapacidad": _mapear(mujeres["dias_de_incapacidad_medicolegal"], hom.dias_incapacidad),
    }).reset_index(drop=True)
    return df


# ---------------------------------------------------------------------------
# Fuente 4: boletines OSEC  ->  stg_hechos_osec
# ---------------------------------------------------------------------------
def transformar_osec(raw: pd.DataFrame) -> pd.DataFrame:
    """
    raw_scraping_comunas -> una fila por comuna, mes y delito.
    Cada fila del bloque MES da dos datos: el mes en el año actual y el mismo
    mes en el año anterior. Si un mes aparece en varios boletines, se usa la
    cifra del boletín MÁS RECIENTE (la Policía corrige sus registros).
    El bloque ACUMULADO no se usa: es la suma de los meses (no se suman bloques).
    """
    mes_blq = raw[raw["bloque"] == "MES"].copy()
    mes_num = mes_blq["periodo"].str[4:6].astype(int)
    base = {"delito": mes_blq["delito"], "comuna": mes_blq["comuna"], "mes": mes_num, "publicado_en": mes_blq["periodo"]}
    largo = pd.concat([
        pd.DataFrame({**base, "anio": _entero(mes_blq["anio_actual"]), "casos": _entero(mes_blq["casos_anio_actual"])}),
        pd.DataFrame({**base, "anio": _entero(mes_blq["anio_anterior"]), "casos": _entero(mes_blq["casos_anio_anterior"])}),
    ], ignore_index=True)

    sin_numero = largo["casos"].isna().sum()
    if sin_numero:
        raise ValueError(f"OSEC: {sin_numero} cifras de casos no se pudieron leer como número")

    largo = (largo.sort_values("publicado_en")
             .drop_duplicates(["delito", "comuna", "anio", "mes"], keep="last"))
    largo["id_comuna"] = _mapear(largo["comuna"], hom.comuna_a_id).astype(int)

    df = (largo.groupby(["id_comuna", "anio", "mes", "delito"], as_index=False)
          .agg(casos=("casos", "sum"), publicado_en=("publicado_en", "max")))
    df["anio"] = df["anio"].astype(int)
    df["casos"] = df["casos"].astype(int)
    logger.info("OSEC: %d filas comuna-mes-delito, de %d-%02d a %d-%02d.", len(df),
                df["anio"].min(), df.loc[df["anio"] == df["anio"].min(), "mes"].min(),
                df["anio"].max(), df.loc[df["anio"] == df["anio"].max(), "mes"].max())
    return df[["id_comuna", "anio", "mes", "delito", "casos", "publicado_en"]]


# ---------------------------------------------------------------------------
# Fuente 3: ECV 2024 vivienda  ->  stg_hechos_analisis
# ---------------------------------------------------------------------------
def transformar_vivienda(raw: pd.DataFrame) -> pd.DataFrame:
    """raw_vivienda (nacional) -> viviendas de Bucaramanga con estrato 1 a 6."""
    dep = raw["p1_departamento"].astype(str).str.strip().str.zfill(2)
    mun = raw["p1_municipio"].astype(str).str.strip().str.zfill(3)
    bga = raw[(dep == "68") & (mun == "001")].copy()

    estrato = pd.to_numeric(bga["p8520s1a1"], errors="coerce")
    validas = bga[estrato.between(1, 6)].copy()
    logger.info("Vivienda: %d viviendas en el archivo, %d de Bucaramanga, %d con estrato 1 a 6 (%d excluidas).",
                len(raw), len(bga), len(validas), len(bga) - len(validas))

    zona = pd.to_numeric(validas["clase"], errors="coerce").astype(int)
    return pd.DataFrame({
        "area": zona.astype(str).map(hom.area),
        "zona_vivienda": zona,
        "tipo_vivienda": pd.to_numeric(validas["p1070"], errors="coerce").astype(int),
        "tiene_energia_electrica": pd.to_numeric(validas["p8520s1"], errors="coerce").astype(int),
        "id_estrato": pd.to_numeric(validas["p8520s1a1"], errors="coerce").astype(int),
    }).reset_index(drop=True)


# ---------------------------------------------------------------------------
# Código DANE para dim_municipio  ->  stg_codigo_dane
# ---------------------------------------------------------------------------
def _limpiar_nombre(t: str) -> str:
    t = hom.normalizar(t)
    t = re.sub(r",?\s*D\.\s*[CE]\.?", "", t)          # "BOGOTA, D.C." -> "BOGOTA"
    return " ".join(t.split())


def descargar_divipola() -> pd.DataFrame:
    """Tabla oficial de municipios (DANE) desde datos.gov.co. Si falla, devuelve vacío."""
    for intento in range(1, 4):
        try:
            r = requests.get(URL_DIVIPOLA, params={"$limit": 5000}, timeout=60,
                             headers={"User-Agent": "Mozilla/5.0 (proyecto academico ETL)"})
            r.raise_for_status()
            df = pd.DataFrame(r.json())
            logger.info("DIVIPOLA: %d municipios descargados.", len(df))
            return df
        except Exception as e:
            logger.warning("Intento %d/3 de descargar DIVIPOLA falló: %s", intento, e)
            time.sleep(5)
    logger.warning("No se pudo descargar DIVIPOLA: solo Bucaramanga recibirá su código DANE (respaldo).")
    return pd.DataFrame()


def codigos_dane(municipios: pd.DataFrame, divipola: pd.DataFrame) -> pd.DataFrame:
    """
    municipios: nombre_departamento, nombre_municipio (de dim_municipio).
    Cruza primero por (departamento, municipio); si no, por municipio cuando el
    nombre es único en el país; y si no, cuando el nombre de DIVIPOLA termina en
    el nombre del modelo dentro del mismo departamento ("SAN JOSE DE CUCUTA").
    """
    filas = []
    if not divipola.empty:
        dv = divipola.copy()
        dv["dep"] = dv["dpto"].map(_limpiar_nombre)
        dv["mun"] = dv["nom_mpio"].map(_limpiar_nombre)
        dv["lat"] = pd.to_numeric(dv["latitud"].astype(str).str.replace(",", "."), errors="coerce")
        dv["lon"] = pd.to_numeric(dv["longitud"].astype(str).str.replace(",", "."), errors="coerce")
        por_par = dv.drop_duplicates(["dep", "mun"]).set_index(["dep", "mun"])
        unicos = dv[~dv["mun"].duplicated(keep=False)].set_index("mun")

        for _, m in municipios.drop_duplicates().iterrows():
            clave = (_limpiar_nombre(m["nombre_departamento"]), _limpiar_nombre(m["nombre_municipio"]))
            fila = None
            if clave in por_par.index:
                fila = por_par.loc[clave]
            elif clave[1] in unicos.index:
                fila = unicos.loc[clave[1]]
            else:   # "CUCUTA" dentro de "SAN JOSE DE CUCUTA", solo si hay un único candidato
                patron = r"\b" + re.escape(clave[1]) + r"$"
                cand = dv[dv["mun"].str.contains(patron, regex=True) & (dv["dep"].str.contains(clave[0].split(" ")[0], regex=False))]
                if len(cand) == 1:
                    fila = cand.iloc[0]
            if fila is not None:
                filas.append({"nombre_departamento": m["nombre_departamento"], "nombre_municipio": m["nombre_municipio"],
                              "codigo_dane": str(fila["cod_mpio"]).zfill(5), "latitud": fila["lat"], "longitud": fila["lon"]})

    df = pd.DataFrame(filas, columns=list(BUCARAMANGA_RESPALDO))
    if not ((df["nombre_departamento"] == "SANTANDER") & (df["nombre_municipio"] == "BUCARAMANGA")).any():
        df = pd.concat([df, pd.DataFrame([BUCARAMANGA_RESPALDO])], ignore_index=True)
    logger.info("Código DANE asignado a %d de %d municipios del modelo.", len(df), len(municipios.drop_duplicates()))
    return df


# ---------------------------------------------------------------------------
# Conjunto unificado  ->  stg_dataset_unificado
# ---------------------------------------------------------------------------
def _mensual(df: pd.DataFrame, columnas: dict) -> pd.DataFrame:
    """Cuenta por (anio, mes) las filas que cumplen cada condición (desde ANIO_INICIO_DATASET)."""
    df = df[(df["mes"].between(1, 12)) & (df["anio"] >= ANIO_INICIO_DATASET)]
    salida = {nombre: df.loc[cond(df)].groupby(["anio", "mes"])["n"].sum() for nombre, cond in columnas.items()}
    return pd.DataFrame(salida)


def construir_dataset_unificado(siv: pd.DataFrame, api: pd.DataFrame, osec: pd.DataFrame,
                                vivienda: pd.DataFrame) -> pd.DataFrame:
    """
    Una fila por año-mes de Bucaramanga con las 4 fuentes lado a lado.
    siv: conteos de SIVIGILA por anio, mes, sexo_victima, grupo_agresor, turno, con_fin (columna n).
    Dentro del periodo que cubre cada fuente, un mes sin casos vale 0; por fuera de ese periodo es NULL.
    """
    fem = lambda d: d["sexo_victima"] == "FEMENINO"
    s = _mensual(siv, {
        "siv_casos_total": lambda d: d["n"] >= 0,
        "siv_casos_mujeres": fem,
        "siv_mujeres_agresor_pareja": lambda d: fem(d) & (d["grupo_agresor"] == "PAREJA O EXPAREJA"),
        "siv_mujeres_agresor_familiar": lambda d: fem(d) & d["grupo_agresor"].isin(["PADRE, MADRE O PADRASTROS", "OTRO FAMILIAR"]),
        "siv_mujeres_turno_noche": lambda d: fem(d) & (d["turno"] == "NOCHE"),
        "siv_mujeres_fallecidas": lambda d: fem(d) & (d["con_fin"] == "2"),
    })
    a = _mensual(api.assign(n=1), {
        "api_casos_mujeres": lambda d: d["n"] >= 0,
        "api_mujeres_incapacidad_grave": lambda d: d["dias_incapacidad"].isin(["31 A 90", "MAS DE 90"]),
    })
    o = _mensual(osec.rename(columns={"casos": "n"}), {
        "osec_denuncias_vif": lambda d: d["delito"] == "VIOLENCIA INTRAFAMILIAR",
        "osec_denuncias_sexuales": lambda d: d["delito"] == "DELITOS SEXUALES",
    })

    partes = [p for p in (s, a, o) if not p.empty]
    inicio = min(p.index.min() for p in partes)
    fin = max(p.index.max() for p in partes)
    meses = pd.period_range(pd.Period(year=inicio[0], month=inicio[1], freq="M"),
                            pd.Period(year=fin[0], month=fin[1], freq="M"), freq="M")
    df = pd.DataFrame({"anio": meses.year, "mes": meses.month})
    clave = pd.MultiIndex.from_arrays([df["anio"], df["mes"]])

    for parte in (s, a, o):
        if parte.empty:
            for c in parte.columns:
                df[c] = pd.NA
            continue
        cubierto = (clave >= parte.index.min()) & (clave <= parte.index.max())
        for c in parte.columns:
            valores = parte[c].reindex(clave).to_numpy()
            df[c] = pd.array(pd.Series(valores).fillna(0).where(cubierto, pd.NA), dtype="Int64")

    est = vivienda["id_estrato"]
    df["ecv_viviendas_bucaramanga"] = len(vivienda)
    df["ecv_pct_estrato_1_2"] = round(est.isin([1, 2]).mean() * 100, 2) if len(est) else pd.NA
    df["ecv_pct_estrato_5_6"] = round(est.isin([5, 6]).mean() * 100, 2) if len(est) else pd.NA
    df["fecha_mes"] = pd.to_datetime(dict(year=df["anio"], month=df["mes"], day=1)).dt.date
    df["fuentes_con_dato"] = df[["siv_casos_total", "api_casos_mujeres", "osec_denuncias_vif"]].notna().sum(axis=1)

    columnas = ["anio", "mes", "fecha_mes", "siv_casos_total", "siv_casos_mujeres", "siv_mujeres_agresor_pareja",
                "siv_mujeres_agresor_familiar", "siv_mujeres_turno_noche", "siv_mujeres_fallecidas",
                "api_casos_mujeres", "api_mujeres_incapacidad_grave", "osec_denuncias_vif", "osec_denuncias_sexuales",
                "ecv_viviendas_bucaramanga", "ecv_pct_estrato_1_2", "ecv_pct_estrato_5_6", "fuentes_con_dato"]
    logger.info("Conjunto unificado: %d meses (%d-%02d a %d-%02d).", len(df), inicio[0], inicio[1], fin[0], fin[1])
    return df[columnas]


SQL_SIVIGILA_MENSUAL = """
SELECT t.year AS anio, t.mes, h.sexo_victima, a.parentezco_vict, h.turno, h.con_fin, COUNT(*) AS n
FROM hechos_violencia h
JOIN dim_tiempo t       ON t.id_tiempo = h.id_tiempo
JOIN dim_agresor a      ON a.id_agresor = h.id_agresor
JOIN dim_municipio m    ON m.id_municipio = h.id_municipio
JOIN dim_departamento d ON d.id_departamento = m.id_departamento
WHERE m.nombre_municipio = 'BUCARAMANGA' AND d.nombre_departamento = 'SANTANDER'
GROUP BY t.year, t.mes, h.sexo_victima, a.parentezco_vict, h.turno, h.con_fin
"""


def preparar_sivigila(siv: pd.DataFrame) -> pd.DataFrame:
    siv = siv.copy()
    siv["anio"] = pd.to_numeric(siv["anio"], errors="coerce").fillna(0).astype(int)
    siv["mes"] = pd.to_numeric(siv["mes"], errors="coerce").fillna(0).astype(int)
    siv["n"] = pd.to_numeric(siv["n"]).astype(int)
    siv["grupo_agresor"] = _mapear(siv["parentezco_vict"], hom.grupo_agresor)
    siv["con_fin"] = siv["con_fin"].astype(str)
    return siv


# ---------------------------------------------------------------------------
# PASO 1: transformar  (raw -> staging)
# ---------------------------------------------------------------------------
def transformar_fuentes_nuevas() -> dict:
    engine = obtener_motor_db()
    try:
        with engine.connect() as conn:
            raw_api = pd.read_sql(text("SELECT * FROM raw_api_violencia WHERE alcance = 'BUCARAMANGA'"), conn)
            raw_osec = pd.read_sql(text("SELECT * FROM raw_scraping_comunas"), conn)
            raw_viv = pd.read_sql(text("SELECT p1_departamento, p1_municipio, clase, p1070, p8520s1, p8520s1a1 FROM raw_vivienda"), conn)
            siv = pd.read_sql(text(SQL_SIVIGILA_MENSUAL), conn)
            municipios = pd.read_sql(text(
                "SELECT DISTINCT d.nombre_departamento, m.nombre_municipio FROM dim_municipio m "
                "JOIN dim_departamento d ON d.id_departamento = m.id_departamento"), conn)

        for nombre, df in [("raw_api_violencia", raw_api), ("raw_scraping_comunas", raw_osec), ("raw_vivienda", raw_viv)]:
            if df.empty:
                raise ValueError(f"La tabla {nombre} está vacía: ejecuta primero la ingesta.")
        if siv.empty:
            raise ValueError("hechos_violencia no tiene casos de Bucaramanga: ejecuta primero etl/transformacion.py")

        tablas = {
            "stg_hechos_api": transformar_api(raw_api),
            "stg_hechos_osec": transformar_osec(raw_osec),
            "stg_hechos_analisis": transformar_vivienda(raw_viv),
            "stg_codigo_dane": codigos_dane(municipios, descargar_divipola()),
        }
        tablas["stg_dataset_unificado"] = construir_dataset_unificado(
            preparar_sivigila(siv), tablas["stg_hechos_api"], tablas["stg_hechos_osec"], tablas["stg_hechos_analisis"])

        with engine.begin() as conn:   # staging se reemplaza completo, en una transacción
            conn.execute(text("TRUNCATE " + ", ".join(TABLAS_STAGING)))
            for nombre in TABLAS_STAGING:
                tablas[nombre].to_sql(nombre, conn, if_exists="append", index=False, method="multi", chunksize=500)
                logger.info("Staging '%s': %d filas.", nombre, len(tablas[nombre]))
        return {nombre: len(df) for nombre, df in tablas.items()}
    finally:
        engine.dispose()


# ---------------------------------------------------------------------------
# PASO 2: cargar  (staging -> modelo final)
# ---------------------------------------------------------------------------
def cargar_fuentes_nuevas() -> dict:
    script = RUTA_SQL_CARGA.read_text(encoding="utf-8")
    engine = obtener_motor_db()
    try:
        with engine.begin() as conn:    # todo o nada
            conn.exec_driver_sql(script)
            conteos = {t: conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()
                       for t in ["hechos_api_violencia", "hechos_osec_comuna", "hechos_analisis", "dataset_unificado"]}
            conteos["municipios_con_codigo_dane"] = conn.execute(
                text("SELECT COUNT(*) FROM dim_municipio WHERE codigo_dane IS NOT NULL")).scalar()
        for tabla, n in conteos.items():
            logger.info("Modelo final '%s': %d filas.", tabla, n)
        return conteos
    finally:
        engine.dispose()


if __name__ == "__main__":
    print("Staging:", transformar_fuentes_nuevas())
    print("Modelo final:", cargar_fuentes_nuevas())