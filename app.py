"""
app.py
Dashboard en tiempo real de las predicciones del modelo (Streamlit).

Fuente : tabla predicciones_agresor, escrita SOLO por consumer.py a partir de los
         mensajes de Kafka. El dashboard NO consulta el modelo dimensional
         (hechos/dimensiones): solo muestra lo que el consumer va procesando.
Vistas : 🆕 Casos nuevos  -> casos que llegaron por Kafka en la ventana elegida
         📚 Todos los casos -> todo lo procesado
Mapa   : Santander -> Bucaramanga, con los casos divididos por área (cabecera, centro
         poblado, rural disperso). Los valores de área o municipio que no se reconocen
         NO rompen el mapa: van a "OTRA ÁREA" / "SIN UBICACIÓN" y se listan para revisarlos.
Revisión: lista de casos con alerta, ordenados por probabilidad, para revisión manual
          (con descarga en CSV).
Refresco: cada pocos segundos (configurable en la barra lateral).

Uso (con consumer.py corriendo en otra terminal):
    streamlit run app.py
"""

import json
import math
import os
import unicodedata
from datetime import timedelta
from pathlib import Path

import altair as alt
import pandas as pd
import pydeck as pdk
import streamlit as st
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

RAIZ = Path(__file__).resolve().parent
ZONA_HORARIA = "America/Bogota"  # la base guarda la hora en UTC; se muestra en hora de Colombia
load_dotenv(RAIZ / ".env")

st.set_page_config(page_title="Alertas de protección · Violencia intrafamiliar", page_icon="🛡️", layout="wide")

VENTANAS = {"Última hora": timedelta(hours=1), "Últimas 24 horas": timedelta(hours=24),
            "Últimos 7 días": timedelta(days=7)}
NR = "NO REPORTADO"

# ---- Mapa -----------------------------------------------------------------
# Las 3 áreas oficiales del DANE + dos "cajones" de seguridad para que un valor nuevo no rompa nada
AREA_CABECERA, AREA_CENTRO, AREA_RURAL = "CABECERA MUNICIPAL", "CENTRO POBLADO", "RURAL DISPERSO"
AREA_OTRA = "OTRA ÁREA (no reconocida)"
AREAS_ORDEN = [AREA_CABECERA, AREA_CENTRO, AREA_RURAL, NR, AREA_OTRA]
SIN_DATO = {"", "NO REPORTADO", "SIN INFORMACION", "SIN DATO", "NO APLICA", "NO DISPONIBLE",
            "DESCONOCIDO", "NAN", "NONE", "NULL", "N/A", "NA"}
# Respaldo si la base no entrega coordenadas (DIVIPOLA, cabecera de Bucaramanga, aprox.)
COORDENADAS_RESPALDO = {"BUCARAMANGA": (7.1254, -73.1198)}
# Un color fijo por área (paleta categórica validada para modo oscuro; gris = sin dato)
COLOR_AREA_HEX = {AREA_CABECERA: "#3987e5", AREA_CENTRO: "#d95926", AREA_RURAL: "#199e70",
                  NR: "#8a8984", AREA_OTRA: "#c98500"}
VISTAS_MAPA = {
    "📍 Bucaramanga": {"zoom": 11},
    "🗺️ Santander": {"latitude": 6.75, "longitude": -73.35, "zoom": 7},
}


# ---------------------------------------------------------------------------
# Conexión y consultas
# ---------------------------------------------------------------------------
@st.cache_resource
def motor_db():
    host = os.getenv("DB_HOST", "localhost")
    if host in ("host.docker.internal", "postgres_violencia_db"):
        host = "localhost"
    url = URL.create(
        "postgresql+psycopg2",
        username=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"),
        host=host,
        port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "violencia_genero_db"),
    )
    return create_engine(url, pool_pre_ping=True)


@st.cache_data
def metadata_modelo() -> dict:
    with open(RAIZ / "models" / "metadata_modelo.json", encoding="utf-8") as f:
        return json.load(f)


# Última predicción de cada caso (un caso = id_caso_api; si falta, id_hecho_api)
SQL_ULTIMA_POR_CASO = """
SELECT DISTINCT ON (clave) *
FROM (SELECT *, COALESCE(id_caso_api, id_hecho_api::TEXT) AS clave FROM predicciones_agresor) p
ORDER BY clave, procesado_en DESC
"""
SQL_HISTORIAL = "SELECT * FROM predicciones_agresor ORDER BY procesado_en DESC LIMIT :n"
SQL_EVENTOS = "SELECT COUNT(*) AS eventos FROM predicciones_agresor"


def sin_tildes(valor) -> str:
    texto = unicodedata.normalize("NFKD", str(valor)).encode("ascii", "ignore").decode("ascii")
    return " ".join(texto.upper().split())


def es_vacio(valor) -> bool:
    return valor is None or (isinstance(valor, float) and math.isnan(valor)) or valor is pd.NA


def normalizar_area(valor) -> str:
    """Lleva cualquier texto de área a una de las categorías conocidas. Nunca lanza error."""
    if es_vacio(valor):
        return NR
    t = sin_tildes(valor)
    if t in SIN_DATO:
        return NR
    if "CABECERA" in t or "URBAN" in t or t == "1":
        return AREA_CABECERA
    if any(p in t for p in ("CENTRO POBLADO", "CORREGIMIENTO", "INSPECCION", "CASERIO")) or t == "2":
        return AREA_CENTRO
    if any(p in t for p in ("RURAL", "VEREDA", "DISPERSO", "CAMPO")) or t == "3":
        return AREA_RURAL
    return AREA_OTRA


def hex_a_rgb(color: str) -> list[int]:
    color = color.lstrip("#")
    return [int(color[i:i + 2], 16) for i in (0, 2, 4)]


def normalizar_municipio(valor) -> str:
    return "SIN MUNICIPIO" if es_vacio(valor) or sin_tildes(valor) in SIN_DATO else sin_tildes(valor)


@st.cache_data(ttl=3600)
def coordenadas_municipios() -> dict:
    """Tabla de referencia (dimensión geográfica, DIVIPOLA). Se lee UNA vez por hora, no en cada
    refresco: no es una consulta analítica repetida. Si falla, se usa el respaldo."""
    coords = dict(COORDENADAS_RESPALDO)
    try:
        df = leer("""SELECT DISTINCT ON (UPPER(nombre_municipio)) nombre_municipio, latitud, longitud
                     FROM dim_municipio WHERE latitud IS NOT NULL AND longitud IS NOT NULL""")
        for fila in df.itertuples():
            coords[sin_tildes(fila.nombre_municipio)] = (float(fila.latitud), float(fila.longitud))
    except Exception:
        pass
    return coords


# ---- Zonas reales del DANE (MGN, capa Clase) -------------------------------------------
RUTA_ZONAS = RAIZ / "data" / "geo" / "bucaramanga_zonas_dane.geojson"
RUTA_SANTANDER = RAIZ / "data" / "geo" / "santander_limite_dane.geojson"


def _poligonos(geom: dict) -> list:
    """Lista de polígonos (cada uno = [anillo exterior, huecos...]) de un Polygon/MultiPolygon."""
    if not geom:
        return []
    if geom.get("type") == "Polygon":
        return [geom["coordinates"]]
    if geom.get("type") == "MultiPolygon":
        return list(geom["coordinates"])
    return []


def _area(anillo) -> float:
    return 0.5 * sum(x1 * y2 - x2 * y1 for (x1, y1, *_), (x2, y2, *_) in zip(anillo, anillo[1:] + anillo[:1]))


def _dentro_anillo(x, y, anillo) -> bool:
    dentro = False
    for (x1, y1, *_), (x2, y2, *_) in zip(anillo, anillo[1:] + anillo[:1]):
        if (y1 > y) != (y2 > y) and x < (x2 - x1) * (y - y1) / ((y2 - y1) or 1e-12) + x1:
            dentro = not dentro
    return dentro


def _dentro(x, y, poligono) -> bool:
    return _dentro_anillo(x, y, poligono[0]) and not any(_dentro_anillo(x, y, h) for h in poligono[1:])


def _punto_etiqueta(poligonos: list) -> tuple[float, float]:
    """Punto DENTRO de la zona para poner su nombre (el centroide puede caer en un hueco)."""
    mayor = max(poligonos, key=lambda p: abs(_area(p[0])))
    exterior = mayor[0]
    a = _area(exterior) or 1e-12
    cx = sum((x1 + x2) * (x1 * y2 - x2 * y1) for (x1, y1, *_), (x2, y2, *_)
             in zip(exterior, exterior[1:] + exterior[:1])) / (6 * a)
    cy = sum((y1 + y2) * (x1 * y2 - x2 * y1) for (x1, y1, *_), (x2, y2, *_)
             in zip(exterior, exterior[1:] + exterior[:1])) / (6 * a)
    if _dentro(cx, cy, mayor) and len(mayor) == 1:  # polígono sin huecos: el centroide sirve
        return cx, cy
    xs, ys = [v[0] for v in exterior], [v[1] for v in exterior]
    # bordes (exterior + huecos): la etiqueta se ubica lo más lejos posible de todos ellos
    segmentos = []
    for anillo in mayor:
        paso = max(1, len(anillo) // 150)
        pts = anillo[::paso] + [anillo[0]]
        segmentos += list(zip(pts, pts[1:]))

    def distancia_borde(x, y):
        mejor_d = float("inf")
        for (x1, y1, *_), (x2, y2, *_) in segmentos:
            dx, dy = x2 - x1, y2 - y1
            t = max(0.0, min(1.0, ((x - x1) * dx + (y - y1) * dy) / ((dx * dx + dy * dy) or 1e-18)))
            mejor_d = min(mejor_d, (x - x1 - t * dx) ** 2 + (y - y1 - t * dy) ** 2)
        return mejor_d
    mejor, distancia = (xs[0], ys[0]), -1.0
    for i in range(1, 25):
        for j in range(1, 25):
            x = min(xs) + (max(xs) - min(xs)) * i / 25
            y = min(ys) + (max(ys) - min(ys)) * j / 25
            if _dentro(x, y, mayor):
                d = distancia_borde(x, y)
                if d > distancia:
                    mejor, distancia = (x, y), d
    return mejor


# Textura = una forma distinta por zona, para no depender solo del color (daltonismo, impresión)
SIMBOLO_ZONA = {AREA_CABECERA: "", AREA_CENTRO: "▲", AREA_RURAL: "•"}


def _texturas(por_zona: dict) -> list[dict]:
    """Rural disperso: trama de puntos dentro del polígono. Centros poblados: un ▲ en cada uno.
    Cabecera: relleno sólido (sin símbolo)."""
    puntos = []
    for zona, poligonos in por_zona.items():
        if zona == AREA_CENTRO:
            puntos += [{"zona": zona, "lon": x, "lat": y} for x, y in (_punto_etiqueta([p]) for p in poligonos)]
        elif zona == AREA_RURAL:
            for p in poligonos:
                xs, ys = [v[0] for v in p[0]], [v[1] for v in p[0]]
                paso = max(max(xs) - min(xs), max(ys) - min(ys)) / 28 or 1
                y = min(ys) + paso / 2
                fila = 0
                while y < max(ys):
                    x = min(xs) + paso / 2 + (paso / 2 if fila % 2 else 0)  # trama en zigzag
                    while x < max(xs):
                        if _dentro(x, y, p):
                            puntos.append({"zona": zona, "lon": x, "lat": y})
                        x += paso
                    y += paso
                    fila += 1
    return puntos


@st.cache_data
def cargar_zonas():
    """GeoJSON de zonas (descargado con etl/descargar_zonas_dane.py) + punto de etiqueta por zona."""
    try:
        geo = json.loads(RUTA_ZONAS.read_text(encoding="utf-8"))
        por_zona = {}
        for f in geo.get("features", []):
            zona = normalizar_area(f.get("properties", {}).get("zona"))
            if zona in (AREA_CABECERA, AREA_CENTRO, AREA_RURAL):
                f["properties"]["zona_norm"] = zona
                por_zona.setdefault(zona, []).extend(_poligonos(f.get("geometry")))
        if not por_zona:
            return None
        etiquetas = {z: _punto_etiqueta(polis) for z, polis in por_zona.items() if polis}
        return {"geojson": geo, "etiquetas": etiquetas, "texturas": _texturas(por_zona),
                "fuente": geo.get("metadata", {}).get("fuente", "DANE MGN")}
    except Exception:
        return None


@st.cache_data
def cargar_santander():
    try:
        return json.loads(RUTA_SANTANDER.read_text(encoding="utf-8"))
    except Exception:
        return None


MENORES = {"PRIMERA INFANCIA", "INFANCIA", "ADOLESCENCIA"}
UMBRAL_MUY_ALTO = 0.60  # probabilidad desde la cual la prioridad sube a "muy alta"
MUESTRA_PEQUENA = 50     # con menos casos, un % por zona es poco estable


def accion_sugerida(alerta: bool, prob: float, ciclo_vida) -> tuple[str, str]:
    """Convierte la predicción en una decisión operativa: (prioridad, acción). No acusa a nadie."""
    menor = sin_tildes(ciclo_vida) in MENORES if not es_vacio(ciclo_vida) else False
    if alerta and menor:
        nivel = "1 · Muy alta" if prob >= UMBRAL_MUY_ALTO else "2 · Alta"
        return nivel, "Activar ruta de protección de niños, niñas y adolescentes (ICBF / Comisaría de Familia)"
    if alerta:
        nivel = "2 · Alta" if prob >= UMBRAL_MUY_ALTO else "3 · Media"
        return nivel, "Revisar el vínculo con el agresor y valorar medidas de protección (Comisaría de Familia)"
    return "4 · Seguimiento", "Seguimiento ordinario del caso"


def leer(sql: str, **params) -> pd.DataFrame:
    with motor_db().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params)


@st.cache_data(ttl=60, show_spinner=False)
def _leer_con_cache(sql: str, parametros: tuple) -> pd.DataFrame:
    return leer(sql, **dict(parametros))


def consultar(sql: str, **params) -> pd.DataFrame:
    """Lectura de las predicciones: sin caché en la demostración (cada evento se ve al instante);
    con caché de 60 s en operación diaria (evita consultar la base en cada refresco)."""
    if TTL_CACHE:
        return _leer_con_cache(sql, tuple(sorted(params.items())))
    return leer(sql, **params)


def hora_local(valor):
    """Convierte la hora UTC de la base a hora de Colombia (sirve para un valor o una columna)."""
    convertido = pd.to_datetime(valor, utc=True)
    if isinstance(convertido, pd.Series):
        return convertido.dt.tz_convert(ZONA_HORARIA)
    return convertido.tz_convert(ZONA_HORARIA)


# ---------------------------------------------------------------------------
# Barra lateral
# ---------------------------------------------------------------------------
meta = metadata_modelo()
umbral = float(meta["umbral_alerta"])

with st.sidebar:
    st.header("🔎 ¿Qué casos ver?")
    modo = st.radio("Vista", ["📚 Todos los casos", "🆕 Casos nuevos"], label_visibility="collapsed")
    ventana = st.selectbox("Casos nuevos = llegaron en…", list(VENTANAS), index=0,
                           disabled=modo != "🆕 Casos nuevos")
    st.divider()
    st.header("⚙️ Configuración")
    modo_operacion = st.radio(
        "Modo de operación", ["🎬 Demostración en vivo", "🏢 Operación diaria"],
        help="Demostración: refresco de 1 a 10 s y datos siempre frescos, para ver llegar cada caso. "
             "Operación diaria: el DAG trae casos una vez al día, así que basta refrescar cada 60 s y se usa "
             "caché de 60 s para no sobrecargar la base de datos.")
    if modo_operacion == "🎬 Demostración en vivo":
        segundos = st.slider("Actualizar cada (segundos)", 1, 10, 2)
        TTL_CACHE = 0
    else:
        segundos = st.select_slider("Actualizar cada (segundos)", [30, 60, 120, 300], value=60)
        TTL_CACHE = 60
    st.caption(f"Refresco: cada {segundos} s · Caché de consultas: "
               f"{'sin caché (datos frescos)' if TTL_CACHE == 0 else f'{TTL_CACHE} s'}")
    n_historial = st.slider("Eventos en el historial", 10, 100, 25, step=5)
    st.divider()
    st.subheader("🤖 Modelo")
    st.write(f"**Algoritmo:** {meta['modelo'].replace('_', ' ')}")
    st.write(f"**Umbral de alerta:** {umbral:.1%}")
    mp = meta["metricas_prueba"]
    st.write(f"**En prueba ({meta['division']['prueba_desde']}+):** "
             f"ROC-AUC {mp['roc_auc']:.2f} · recall {mp['recall_padres']:.0%} · precisión {mp['precision_padres']:.0%}")
    st.caption("Fuente: tabla predicciones_agresor, escrita por consumer.py desde Kafka.")

st.title("🛡️ Alertas de protección en tiempo real")
st.caption(
    "Cada caso de lesiones por violencia intrafamiliar que llega por Kafka recibe la probabilidad de que el "
    f"agresor sea **padre, madre o padrastro**. Si supera {umbral:.1%}, se marca como **alerta** para priorizar "
    "la ruta de protección. Es una herramienta de priorización: no identifica ni acusa a ninguna persona."
)


# ---------------------------------------------------------------------------
# Secciones
# ---------------------------------------------------------------------------
def seccion_metricas(casos: pd.DataFrame, eventos: int):
    n_casos, n_alertas = len(casos), int(casos["alerta"].sum())
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Casos", f"{n_casos:,}", help="Casos distintos (última predicción de cada uno)")
    c2.metric("🔴 Alertas", f"{n_alertas:,}", f"{n_alertas / n_casos:.0%} de los casos" if n_casos else None,
              delta_color="off")
    c3.metric("Prob. promedio padre/madre", f"{casos['prob_padre_madre'].mean():.1%}" if n_casos else "—")
    c4.metric("Eventos de Kafka recibidos (total)", f"{eventos:,}")


def seccion_ultimo_caso():
    ultimo = consultar(SQL_HISTORIAL, n=1).iloc[0]
    p = float(ultimo["prob_padre_madre"])
    st.subheader("Último caso recibido")
    izq, der = st.columns([1, 2])
    with izq:
        st.markdown(f"**Caso #{int(ultimo['id_hecho_api'])}**")
        prioridad, accion = accion_sugerida(bool(ultimo["alerta"]), p, ultimo["ciclo_vida"])
        if ultimo["alerta"]:
            st.error(f"🔴 ALERTA · Prioridad {prioridad}")
        else:
            st.success("🟢 Sin alerta")
        st.markdown(f"**Acción sugerida:** {accion}")
        st.caption(f"Recibido: {hora_local(ultimo['procesado_en']).strftime('%Y-%m-%d %H:%M:%S')}")
    with der:
        st.write(f"**Padre, madre o padrastro:** {p:.1%}")
        st.progress(min(max(p, 0.0), 1.0))
        st.write(f"**Otro familiar:** {1 - p:.1%}")
        st.progress(min(max(1 - p, 0.0), 1.0))
        st.caption(
            f"Víctima: {ultimo['ciclo_vida'] or NR} · Escenario: {ultimo['escenario'] or NR} · "
            f"Turno: {ultimo['turno'] or NR} · Incapacidad: {ultimo['dias_incapacidad'] or NR}"
        )


COLOR_CON_ALERTA, COLOR_SIN_ALERTA = "#e0524a", "#8a8984"  # rojo = alerta (siempre con su etiqueta), gris = sin alerta
ORDEN_CICLO = ["PRIMERA INFANCIA", "INFANCIA", "ADOLESCENCIA", "JUVENTUD", "ADULTEZ", "PERSONA MAYOR"]
ORDEN_TURNO = ["MAÑANA", "TARDE", "NOCHE"]
TOP_ESCENARIOS = 7


def conteo_apilado(casos: pd.DataFrame, columna: str, orden: list | None = None,
                   top: int | None = None) -> pd.DataFrame:
    """Casos con y sin alerta por categoría. NUNCA descarta casos: vacíos -> NO REPORTADO,
    categorías fuera del orden conocido se conservan, y si hay 'top', el resto va a OTROS."""
    valores = casos[columna].map(lambda v: NR if es_vacio(v) or sin_tildes(v) in SIN_DATO else str(v))
    if top is not None:
        principales = valores.value_counts().head(top).index
        valores = valores.where(valores.isin(principales), "OTROS")
    tabla = (pd.crosstab(valores, casos["alerta"].map({True: "Con alerta", False: "Sin alerta"}))
             .reindex(columns=["Con alerta", "Sin alerta"], fill_value=0))
    if orden is not None:
        conocidos = [c for c in orden if c in tabla.index]
        resto = [c for c in tabla.index if c not in orden and c not in (NR, "OTROS")]
        cola = [c for c in ("OTROS", NR) if c in tabla.index]
        tabla = tabla.reindex(conocidos + resto + cola)
    else:
        tabla = tabla.loc[tabla.sum(axis=1).sort_values(ascending=False).index]
        tabla = tabla.reindex([c for c in tabla.index if c not in ("OTROS", NR)] +
                              [c for c in ("OTROS", NR) if c in tabla.index])
    tabla.index = [str(c).title() for c in tabla.index]
    return tabla


def grafica_apilada(tabla: pd.DataFrame, horizontal: bool = False):
    """Barras apiladas (con / sin alerta) respetando el orden de las categorías, con tooltip."""
    largo = (tabla.rename_axis("Categoría").reset_index()
             .melt(id_vars="Categoría", var_name="Estado", value_name="Casos"))
    orden = list(tabla.index)
    eje_cat = alt.Y if horizontal else alt.X
    eje_val = alt.X if horizontal else alt.Y
    grafica = (alt.Chart(largo).mark_bar(cornerRadius=3).encode(
        eje_cat("Categoría:N", sort=orden, title=None, axis=alt.Axis(labelLimit=220, labelAngle=0)),
        eje_val("sum(Casos):Q", title="Casos"),
        color=alt.Color("Estado:N", scale=alt.Scale(domain=["Con alerta", "Sin alerta"],
                                                     range=[COLOR_CON_ALERTA, COLOR_SIN_ALERTA]),
                        legend=alt.Legend(orient="bottom", title=None)),
        order=alt.Order("Estado:N", sort="ascending"),
        tooltip=["Categoría:N", "Estado:N", alt.Tooltip("Casos:Q", format=",")],
    ))
    alto = max(200, 44 * len(orden) + 70) if horizontal else 320
    st.altair_chart(grafica.properties(height=alto - 70), width="stretch", height=alto)
    st.caption(f"Total en la gráfica: **{int(tabla.values.sum()):,} casos** "
               f"({int(tabla['Con alerta'].sum()):,} con alerta · {int(tabla['Sin alerta'].sum()):,} sin alerta)")


def seccion_graficas(casos: pd.DataFrame):
    g1, g2 = st.columns(2)
    with g1:
        st.subheader("Casos por ciclo de vida de la víctima")
        tabla_ciclo = conteo_apilado(casos, "ciclo_vida", orden=ORDEN_CICLO)
        grafica_apilada(tabla_ciclo, horizontal=True)
        st.caption("En menores de edad casi todos los casos generan alerta: el modelo se apoya sobre todo en la edad.")
    with g2:
        st.subheader("Distribución de probabilidades")
        bins = pd.cut(casos["prob_padre_madre"].clip(0, 1), bins=[0, .1, .2, .3, .4, .5, .6, .7, .8, .9, 1.0],
                      include_lowest=True, labels=[f"{i*10}-{i*10+10}%" for i in range(10)])
        conteo_bins = bins.value_counts().sort_index()
        st.bar_chart(conteo_bins, y_label="Casos", x_label="P(padre/madre)", color=COLOR_SIN_ALERTA)
        st.caption(f"Total en la gráfica: **{int(conteo_bins.sum()):,} casos**. "
                   f"Desde {umbral:.0%} en adelante el caso genera alerta.")

    g3, g4 = st.columns(2)
    with g3:
        st.subheader("Casos por escenario del hecho")
        tabla_esc = conteo_apilado(casos, "escenario", top=TOP_ESCENARIOS)
        grafica_apilada(tabla_esc, horizontal=True)
        st.caption(f"Se muestran los {TOP_ESCENARIOS} escenarios más frecuentes; el resto se suma en 'Otros'.")
    with g4:
        st.subheader("Casos por turno")
        grafica_apilada(conteo_apilado(casos, "turno", orden=ORDEN_TURNO))

    # Cuadre: cada agrupación debe sumar exactamente los casos de la vista (ningún caso se pierde)
    n = len(casos)
    sumas = {"ciclo de vida": int(tabla_ciclo.values.sum()), "probabilidad": int(conteo_bins.sum()),
             "escenario": int(tabla_esc.values.sum()),
             "turno": int(conteo_apilado(casos, "turno", orden=ORDEN_TURNO).values.sum()),
             "área": int(casos["area"].map(normalizar_area).value_counts().sum())}
    descuadre = {k: v for k, v in sumas.items() if v != n}
    if descuadre:
        st.error(f"⚠️ Descuadre: la vista tiene {n:,} casos pero {descuadre}. Revisar los datos.")
    else:
        st.success(f"✅ Cuadre de datos: todas las gráficas y el mapa suman exactamente **{n:,} casos**.")


def mapa_por_zonas(datos: pd.DataFrame, zonas_dane: dict, enfoque: str, coords: dict):
    """Polígonos oficiales del DANE, un color por zona, con casos y alertas de cada una."""
    resumen = datos.groupby("area_norm").agg(c=("alerta", "size"), a=("alerta", "sum"))
    municipio = "Bucaramanga"

    geo = json.loads(json.dumps(zonas_dane["geojson"]))  # copia: se le agregan colores y cifras
    for f in geo.get("features", []):
        zona = f["properties"].get("zona_norm")
        if zona is None:
            continue
        c = int(resumen.loc[zona, "c"]) if zona in resumen.index else 0
        a_ = int(resumen.loc[zona, "a"]) if zona in resumen.index else 0
        r, g, b = hex_a_rgb(COLOR_AREA_HEX[zona])
        f["properties"].update({
            "titulo": f"{municipio} - {zona.title()}", "casos": f"{c:,}", "alertas": f"{a_:,}",
            "tasa": (f"{a_ / c:.0%}" if c else "—"),
            "aviso": (f"⚠️ Solo {c} casos: el % es poco estable, compararlo con cautela"
                      if 0 < c < MUESTRA_PEQUENA else ""),
            "relleno": [r, g, b, 120 if c else 40], "borde": [r, g, b, 255],
        })
    # Orden de dibujo: rural (fondo) -> centros poblados -> cabecera (encima), para que cada borde se vea completo
    orden = {AREA_RURAL: 0, AREA_CENTRO: 1, AREA_CABECERA: 2}
    geo["features"] = sorted((f for f in geo.get("features", []) if f["properties"].get("zona_norm")),
                             key=lambda f: orden[f["properties"]["zona_norm"]])

    capas = []
    santander = cargar_santander()
    if santander:
        capas.append(pdk.Layer("GeoJsonLayer", data=santander, stroked=True, filled=False,
                               get_line_color=[200, 200, 200, 160], line_width_min_pixels=1.5))
    capas += [
        pdk.Layer("GeoJsonLayer", data=geo, stroked=True, filled=True, pickable=True, auto_highlight=True,
                  get_fill_color="properties.relleno", get_line_color="properties.borde",
                  line_width_min_pixels=2, highlight_color=[255, 255, 255, 60]),
    ]
    textura = pd.DataFrame(zonas_dane.get("texturas", []))
    if not textura.empty:
        textura["simbolo"] = textura["zona"].map(SIMBOLO_ZONA)
        textura["color"] = textura["zona"].map(lambda z: hex_a_rgb(COLOR_AREA_HEX[z]) + [255])
        capas.append(pdk.Layer("TextLayer", data=textura, get_position="[lon, lat]", get_text="simbolo",
                               get_size=11, get_color="color", character_set="'auto'",
                               get_text_anchor="'middle'", get_alignment_baseline="'center'"))
    vista = dict(VISTAS_MAPA[enfoque])
    if "latitude" not in vista:
        lat, lon = coords.get("BUCARAMANGA", COORDENADAS_RESPALDO["BUCARAMANGA"])
        vista.update(latitude=lat, longitude=lon, zoom=10.3)
    st.pydeck_chart(pdk.Deck(
        layers=capas, initial_view_state=pdk.ViewState(**vista, pitch=0),
        views=[pdk.View(type="MapView", controller={"scrollZoom": False, "dragPan": True,
                                                    "doubleClickZoom": True, "touchZoom": True})],
        # El nombre "Ciudad - Zona" aparece SOLO al pasar el cursor por encima de la zona
        tooltip={"html": "<div style='font-size:14px;font-weight:600;margin-bottom:4px'>{titulo}</div>"
                         "{casos} casos · {alertas} alertas ({tasa})"
                         "<div style='margin-top:4px;font-style:italic;opacity:.85'>{aviso}</div>",
                 "style": {"backgroundColor": "#1a1a19", "color": "#f0efec", "border": "1px solid #555",
                           "borderRadius": "6px", "padding": "8px 10px"}}),
        height=460)

    sin_zona = int(resumen.loc[[z for z in (NR, AREA_OTRA) if z in resumen.index], "c"].sum())
    st.caption(f"Límites oficiales: {zonas_dane['fuente']}. Cada zona tiene su color; pasa el cursor por "
               "encima para ver su nombre y sus cifras. Además del color, cada zona tiene su forma: "
               "cabecera = relleno sólido, centro poblado = ▲, rural disperso = trama de puntos. "
               "Medicina Legal reporta la zona del hecho, no la dirección: las cifras son por zona completa."
               + (f" {sin_zona:,} casos sin zona reportada no se dibujan (ver tabla)." if sin_zona else ""))


def seccion_mapa(casos: pd.DataFrame):
    st.subheader("🗺️ Mapa: casos y alertas por municipio y área")
    try:
        datos = casos.copy()
        if "nombre_municipio" not in datos.columns:  # predicciones antiguas sin la columna
            datos["nombre_municipio"] = None
        datos["municipio_norm"] = datos["nombre_municipio"].map(normalizar_municipio)
        # Casos guardados antes de agregar el municipio: todo el flujo actual es de Bucaramanga
        datos.loc[datos["municipio_norm"] == "SIN MUNICIPIO", "municipio_norm"] = "BUCARAMANGA"
        datos["area_norm"] = datos["area"].map(normalizar_area)

        coords = coordenadas_municipios()
        datos["tiene_coord"] = datos["municipio_norm"].isin(coords)
        ubicados, sin_ubicacion = datos[datos["tiene_coord"]], datos[~datos["tiene_coord"]]

        enfoque = st.radio("Enfoque del mapa", list(VISTAS_MAPA), horizontal=True, key="enfoque_mapa")
        col_mapa, col_area = st.columns([3, 2])

        zonas_dane = cargar_zonas()
        with col_mapa:
          if zonas_dane is not None:  # límites oficiales DANE
            mapa_por_zonas(datos, zonas_dane, enfoque, coords)
          else:  # respaldo: punto por municipio
            st.info("Para ver las zonas reales (cabecera, centros poblados y rural disperso) ejecuta una vez "
                    "`python etl/descargar_zonas_dane.py`. Mientras tanto se muestra el mapa por municipio.")
            puntos, etiquetas = [], []
            for municipio, grupo in ubicados.groupby("municipio_norm"):
                lat, lon = coords[municipio]
                n, n_al = len(grupo), int(grupo["alerta"].sum())
                por_area = grupo.groupby("area_norm").agg(c=("alerta", "size"), a=("alerta", "sum"))
                detalle = "<br/>".join(
                    f"<span style='color:{COLOR_AREA_HEX[a]}'>●</span> {a.title()}: "
                    f"{int(por_area.loc[a, 'c']):,} casos · {int(por_area.loc[a, 'a']):,} alertas"
                    for a in AREAS_ORDEN if a in por_area.index)
                tasa = n_al / n if n else 0
                puntos.append({
                    "municipio": municipio.title(), "lat": lat, "lon": lon, "casos": f"{n:,}",
                    "alertas": f"{n_al:,}", "tasa": f"{tasa:.0%}", "detalle": detalle,
                    # tamaño en PÍXELES (no en metros): nunca tapa la ciudad, crece poco con más casos
                    "radio": 7 + 3 * math.log10(1 + n),
                })
                # Una etiqueta por zona: "Ciudad - Zona", apiladas sobre el punto del municipio
                zonas = [z for z in AREAS_ORDEN if z in por_area.index]
                for i, zona in enumerate(reversed(zonas)):
                    c_z, a_z = int(por_area.loc[zona, "c"]), int(por_area.loc[zona, "a"])
                    etiquetas.append({
                        "lat": lat, "lon": lon,
                        "texto": f"{municipio.title()} - {zona.title()}   {c_z:,} casos · {a_z / c_z:.0%} alertas",
                        "borde": hex_a_rgb(COLOR_AREA_HEX[zona]),
                        "offset": [0, -34 - 26 * i],
                    })
            if not puntos:
                st.info("Ningún caso de esta vista tiene un municipio con coordenadas conocidas.")
            else:
                base = pd.DataFrame(puntos)
                vista = dict(VISTAS_MAPA[enfoque])
                if "latitude" not in vista:  # enfoque en el municipio con más casos (Bucaramanga)
                    principal = base.sort_values("radio", ascending=False).iloc[0]
                    vista.update(latitude=principal["lat"], longitude=principal["lon"])
                capas = [
                    # halo suave + punto: marca la ubicación sin pintar un área falsa
                    pdk.Layer("ScatterplotLayer", data=base, get_position="[lon, lat]",
                              get_radius="radio * 2.2", radius_units="'pixels'",
                              get_fill_color=[214, 69, 65, 45], pickable=False),
                    pdk.Layer("ScatterplotLayer", data=base, get_position="[lon, lat]", get_radius="radio",
                              radius_units="'pixels'", get_fill_color=[214, 69, 65, 235], stroked=True,
                              get_line_color=[255, 255, 255], line_width_min_pixels=2, pickable=True),
                    pdk.Layer("TextLayer", data=pd.DataFrame(etiquetas), get_position="[lon, lat]",
                              get_text="texto", get_size=13, get_color=[240, 239, 236],
                              get_pixel_offset="offset", character_set="'auto'",
                              font_family="'Source Sans Pro, Helvetica, Arial, sans-serif'",
                              background=True, get_background_color=[26, 26, 25, 235],
                              get_border_color="borde", get_border_width=2,
                              background_padding=[8, 4, 8, 4],
                              get_text_anchor="'middle'", get_alignment_baseline="'bottom'"),
                ]
                st.pydeck_chart(pdk.Deck(
                    layers=capas, initial_view_state=pdk.ViewState(**vista, pitch=0),
                    # sin zoom con la rueda: así la página se puede desplazar sin mover el mapa
                    views=[pdk.View(type="MapView", controller={"scrollZoom": False, "dragPan": True,
                                                                "doubleClickZoom": True, "touchZoom": True})],
                    tooltip={"html": "<b>{municipio}</b><br/>{casos} casos · {alertas} alertas ({tasa})"
                                     "<br/><br/>{detalle}",
                             "style": {"fontSize": "12px", "backgroundColor": "#1a1a19", "color": "#f0efec"}}),
                    height=380)
                st.caption("Cada punto es un municipio en su ubicación real (DIVIPOLA); encima, una etiqueta por "
                           "zona con el color de su área. Pasa el cursor para ver el detalle. Zoom: doble clic o el selector de enfoque. Medicina Legal reporta "
                           "municipio y área, no dirección: por eso no se dibujan zonas dentro de la ciudad.")

        with col_area:
            resumen = (datos.groupby("area_norm")
                       .agg(Casos=("alerta", "size"), Alertas=("alerta", "sum"))
                       .reindex(AREAS_ORDEN, fill_value=0))
            resumen["% alerta"] = (resumen["Alertas"] / resumen["Casos"].where(resumen["Casos"] > 0)).fillna(0) * 100
            presentes = [a for a in AREAS_ORDEN if resumen.loc[a, "Casos"] > 0]

            st.markdown("**¿En qué área ocurrieron los casos?**")
            total = max(int(resumen["Casos"].sum()), 1)
            segmentos = "".join(
                f"<div title='{a.title()}: {int(resumen.loc[a, 'Casos']):,} casos' style='width:"
                f"{100 * resumen.loc[a, 'Casos'] / total:.2f}%;background:{COLOR_AREA_HEX[a]};"
                f"min-width:3px;margin-right:2px'></div>" for a in presentes)
            leyenda = "".join(
                f"<div style='display:flex;align-items:center;gap:6px;margin:3px 0'>"
                f"<span style='width:10px;height:10px;border-radius:2px;background:{COLOR_AREA_HEX[a]}'></span>"
                f"<span style='width:12px;text-align:center;color:{COLOR_AREA_HEX[a]}'>"
                f"{SIMBOLO_ZONA.get(a) or ('■' if a == AREA_CABECERA else '')}</span>"
                f"<span>{a.title()}</span><span style='margin-left:auto;opacity:.75'>"
                f"{int(resumen.loc[a, 'Casos']):,} · {100 * resumen.loc[a, 'Casos'] / total:.1f}%</span></div>"
                for a in presentes)
            st.html(f"<div style='font-size:13px'><div style='display:flex;height:16px;border-radius:4px;"
                    f"overflow:hidden;margin:4px 0 10px'>{segmentos}</div>{leyenda}</div>")

            st.markdown("**Alertas por área**")
            tabla = pd.DataFrame({
                "Área": [f"● {a.title()}" for a in presentes],
                "Casos": [int(resumen.loc[a, "Casos"]) for a in presentes],
                "Alertas": [int(resumen.loc[a, "Alertas"]) for a in presentes],
                "% alerta": [float(resumen.loc[a, "% alerta"]) for a in presentes],
            })
            st.dataframe(
                tabla.style.apply(lambda col: [f"color: {COLOR_AREA_HEX[a]}" for a in presentes], subset=["Área"]),
                hide_index=True, width="stretch",
                column_config={"% alerta": st.column_config.ProgressColumn(
                    "% alerta", format="%.0f%%", min_value=0.0, max_value=100.0)})

        otros = datos.loc[datos["area_norm"] == AREA_OTRA, "area"].dropna().unique()
        if len(otros) or len(sin_ubicacion):
            with st.expander("⚠️ Valores que el mapa no reconoce (revisar y agregarlos al catálogo)"):
                if len(otros):
                    st.write("**Áreas nuevas:**", ", ".join(map(str, otros)))
                if len(sin_ubicacion):
                    st.write(f"**{len(sin_ubicacion)} casos sin coordenadas** (municipios: "
                             f"{', '.join(sorted(sin_ubicacion['municipio_norm'].unique()))})")
    except Exception as e:  # el mapa nunca tumba el dashboard
        st.warning(f"No se pudo dibujar el mapa en este refresco ({type(e).__name__}). El resto del panel sigue funcionando.")


def seccion_revision(casos: pd.DataFrame):
    st.subheader("🔴 Casos con alerta para revisión manual")
    alertas = casos[casos["alerta"]].sort_values("prob_padre_madre", ascending=False)
    if alertas.empty:
        st.info("No hay casos con alerta en esta vista.")
        return
    col1, col2 = st.columns([2, 1])
    with col1:
        ciclos = st.multiselect("Filtrar por ciclo de vida", sorted(alertas["ciclo_vida"].fillna(NR).unique()))
    with col2:
        p_min = st.slider("Probabilidad mínima (%)", int(umbral * 100), 100, int(umbral * 100))
    if ciclos:
        alertas = alertas[alertas["ciclo_vida"].fillna(NR).isin(ciclos)]
    alertas = alertas[alertas["prob_padre_madre"] * 100 >= p_min]

    acciones = [accion_sugerida(True, float(p_), c_) for p_, c_ in
                zip(alertas["prob_padre_madre"], alertas["ciclo_vida"])]
    tabla = pd.DataFrame({
        "Prioridad": [a_[0] for a_ in acciones],
        "Acción sugerida": [a_[1] for a_ in acciones],
        "Recibido": hora_local(alertas["procesado_en"]).dt.strftime("%Y-%m-%d %H:%M"),
        "Caso": alertas["id_hecho_api"],
        "Id API": alertas["id_caso_api"],
        "P(padre/madre)": alertas["prob_padre_madre"] * 100,
        "Ciclo de vida": alertas["ciclo_vida"],
        "Escenario": alertas["escenario"],
        "Turno": alertas["turno"],
        "Incapacidad": alertas["dias_incapacidad"],
        "Área": alertas["area"],
        "Mes": alertas["mes"],
    })
    tabla = tabla.sort_values(["Prioridad", "P(padre/madre)"], ascending=[True, False])
    st.caption(f"{len(tabla)} casos, ordenados por prioridad y probabilidad. La acción sugerida orienta la "
               "ruta de atención: el modelo no identifica ni acusa a ninguna persona.")
    st.dataframe(tabla, hide_index=True, width="stretch", height=320,
                 column_config={
                     "P(padre/madre)": st.column_config.ProgressColumn(
                         "P(padre/madre)", format="%.1f%%", min_value=0.0, max_value=100.0),
                     "Acción sugerida": st.column_config.TextColumn("Acción sugerida", width="large"),
                     "Prioridad": st.column_config.TextColumn("Prioridad", width="small")})
    st.download_button("⬇️ Descargar lista para revisión (CSV)", tabla.to_csv(index=False).encode("utf-8-sig"),
                       file_name="casos_alerta_revision.csv", mime="text/csv")


SQL_RITMO = """
SELECT date_trunc('minute', procesado_en) AS minuto,
       COUNT(*) FILTER (WHERE alerta)     AS con_alerta,
       COUNT(*) FILTER (WHERE NOT alerta) AS sin_alerta
FROM predicciones_agresor
WHERE procesado_en >= NOW() - (:minutos || ' minutes')::INTERVAL
GROUP BY 1 ORDER BY 1
"""


def hace_cuanto(momento, ahora) -> str:
    seg = max(0, int((ahora - momento).total_seconds()))
    if seg < 60:
        return f"hace {seg} s"
    if seg < 3600:
        return f"hace {seg // 60} min"
    if seg < 86400:
        return f"hace {seg // 3600} h"
    return f"hace {seg // 86400} d"


def seccion_historial():
    st.subheader("📡 Flujo de eventos en tiempo real")
    ahora = pd.Timestamp.now(tz="UTC")
    h = consultar(SQL_HISTORIAL, n=max(n_historial * 4, 200))
    h["procesado_en"] = pd.to_datetime(h["procesado_en"], utc=True)
    h["prob_padre_madre"] = h["prob_padre_madre"].astype(float)

    # --- Pulso del flujo: ¿está llegando información? ---
    ultimo = h["procesado_en"].max()
    en_5min = int((h["procesado_en"] >= ahora - pd.Timedelta(minutes=5)).sum())
    k1, k2, k3 = st.columns(3)
    activo = (ahora - ultimo) <= pd.Timedelta(minutes=2)
    k1.metric("Estado del flujo", "🟢 Recibiendo" if activo else "⏸️ En espera",
              help="Recibiendo = llegó al menos un evento en los últimos 2 minutos")
    k2.metric("Último evento", hace_cuanto(ultimo, ahora))
    k3.metric("Eventos últimos 5 min", f"{en_5min:,}" if en_5min < len(h) else f"{en_5min:,}+")

    # --- Ritmo de llegada por minuto (última hora) ---
    ritmo = consultar(SQL_RITMO, minutos=60)
    if not ritmo.empty:
        ritmo["minuto"] = hora_local(ritmo["minuto"]).dt.tz_localize(None)
        largo = ritmo.melt(id_vars="minuto", value_vars=["con_alerta", "sin_alerta"],
                           var_name="Estado", value_name="Eventos")
        largo["Estado"] = largo["Estado"].map({"con_alerta": "Con alerta", "sin_alerta": "Sin alerta"})
        grafica = alt.Chart(largo).mark_bar(cornerRadius=2).encode(
            x=alt.X("minuto:T", title="Hora de llegada (última hora)", axis=alt.Axis(format="%H:%M")),
            y=alt.Y("sum(Eventos):Q", title="Eventos por minuto"),
            color=alt.Color("Estado:N", scale=alt.Scale(domain=["Con alerta", "Sin alerta"],
                                                         range=[COLOR_CON_ALERTA, COLOR_SIN_ALERTA]),
                            legend=alt.Legend(orient="bottom", title=None)),
            order=alt.Order("Estado:N", sort="ascending"),
            tooltip=[alt.Tooltip("minuto:T", title="Minuto", format="%H:%M"), "Estado:N",
                     alt.Tooltip("Eventos:Q", format=",")],
        ).properties(height=220)
        st.altair_chart(grafica, width="stretch", height=320)
        st.caption("Cada barra es un minuto: muestra el ritmo con el que llegan los casos por Kafka.")
    else:
        st.caption("No llegaron eventos en la última hora.")

    # --- Feed de los últimos eventos ---
    filtro = st.radio("Mostrar", ["Todos los eventos", "Solo alertas"], horizontal=True, key="filtro_hist")
    if filtro == "Solo alertas":
        h = h[h["alerta"]]
    h = h.head(n_historial)
    acciones = [accion_sugerida(bool(a_), p_, c_) for a_, p_, c_ in
                zip(h["alerta"], h["prob_padre_madre"], h["ciclo_vida"])]
    nuevo = h["procesado_en"] >= ahora - pd.Timedelta(seconds=max(segundos * 2, 5))
    tabla = pd.DataFrame({
        " ": ["🆕" if n_ else "" for n_ in nuevo],
        "Hora": hora_local(h["procesado_en"]).dt.strftime("%H:%M:%S"),
        "Hace": [hace_cuanto(t, ahora) for t in h["procesado_en"]],
        "Caso": h["id_hecho_api"],
        "Estado": h["alerta"].map({True: "🔴 Alerta", False: "🟢 Sin alerta"}),
        "Prioridad": [a_[0] for a_ in acciones],
        "P(padre/madre)": h["prob_padre_madre"] * 100,
        "Víctima": h["ciclo_vida"].fillna(NR).str.title(),
        "Escenario": h["escenario"].fillna(NR).str.title(),
        "Turno": h["turno"].fillna(NR).str.title(),
        "Zona": h["area"].map(normalizar_area).str.title(),
    })
    st.caption(f"Últimos {len(tabla)} eventos, del más reciente al más antiguo. 🆕 = llegó en el último refresco.")
    st.dataframe(
        tabla, hide_index=True, width="stretch", height=min(38 * len(tabla) + 40, 520),
        column_config={
            " ": st.column_config.TextColumn(" ", width=40),
            "P(padre/madre)": st.column_config.ProgressColumn(
                "P(padre/madre)", format="%.1f%%", min_value=0.0, max_value=100.0),
            "Caso": st.column_config.NumberColumn("Caso", format="%d"),
        })


# ---------------------------------------------------------------------------
# Panel que se actualiza solo
# ---------------------------------------------------------------------------
@st.fragment(run_every=f"{segundos}s")
def panel():
    try:
        todos = consultar(SQL_ULTIMA_POR_CASO)
        eventos = int(consultar(SQL_EVENTOS).iloc[0]["eventos"])
    except Exception as e:  # tabla aún no creada o base apagada
        st.warning(f"Esperando datos del consumer… ({type(e).__name__})")
        return

    if todos.empty:
        st.info("Aún no hay predicciones. Inicia `python consumer.py` y publica mensajes con el producer.")
        return

    todos["prob_padre_madre"] = todos["prob_padre_madre"].astype(float)
    todos["procesado_en"] = pd.to_datetime(todos["procesado_en"], utc=True)
    st.caption(f"🔄 Panel actualizado a las {hora_local(pd.Timestamp.now(tz='UTC')).strftime('%H:%M:%S')} · "
               f"{modo_operacion} · refresco cada {segundos} s")

    if modo == "🆕 Casos nuevos":
        desde = pd.Timestamp.now(tz="UTC") - VENTANAS[ventana]
        casos = todos[todos["procesado_en"] >= desde]
        st.markdown(f"### 🆕 Casos nuevos · {ventana.lower()} "
                    f"(desde {hora_local(desde).strftime('%Y-%m-%d %H:%M')})")
        st.info(f"Esta vista muestra **{len(casos):,} de {len(todos):,} casos**: solo los que llegaron "
                f"{ventana.lower()}. Para ver todos, elige '📚 Todos los casos' en la barra lateral.")
    else:
        casos = todos
        st.markdown("### 📚 Todos los casos procesados")

    seccion_metricas(casos, eventos)
    seccion_ultimo_caso()
    if casos.empty:
        st.info("No llegaron casos nuevos en esta ventana. Cambia la ventana o mira 'Todos los casos'.")
    else:
        seccion_graficas(casos)
        seccion_mapa(casos)
        seccion_revision(casos)
    seccion_historial()


panel()