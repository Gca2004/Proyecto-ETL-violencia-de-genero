"""
reporte_estatico.py
Reporte analítico ESTÁTICO de una sola página (vía analítica de la arquitectura).

Lee   : el modelo dimensional VALIDADO en PostgreSQL (hechos_violencia, hechos_api_violencia,
        hechos_osec_comuna, hechos_analisis, dataset_unificado + dimensiones) y log_validaciones.
        Además, los artefactos del modelo (models/metricas_experimentos.csv, models/metadata_modelo.json).
        NO lee Kafka ni la tabla de predicciones: es una foto en un punto del tiempo.
Genera: Docs/reporte_estatico.html  (una página; se abre en el navegador y se imprime a PDF)

Pregunta del proyecto:
    ¿Cómo se manifiesta la violencia contra las mujeres en Bucaramanga según el vínculo con el agresor,
    el ciclo de vida de la víctima, el tipo de violencia, la comuna y el contexto socioeconómico?

Cada gráfica trae su hallazgo escrito, calculado a partir de los datos (no es texto fijo).

Regenerar (desde la raíz del proyecto, con PostgreSQL encendido y el DAG ya ejecutado):
    python reporte_estatico.py
"""

import base64
import io
import json
import os
from datetime import datetime
from html import escape
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd
from dotenv import load_dotenv

RAIZ = Path(__file__).resolve().parent
load_dotenv(RAIZ / ".env")
SALIDA = RAIZ / "Docs" / "reporte_estatico.html"
# Nombres COMPLETOS del equipo (observación del profesor en la entrega anterior)
EQUIPO = "Leidy Johanna Alomia Zapata · Gustavo Coque · Carlos Cortes"

# Paleta categórica validada (modo claro) + neutros
AZUL, NARANJA, GRIS, TINTA, TINTA_2 = "#2a78d6", "#eb6834", "#b9b8b1", "#1f1f1e", "#5f5e58"
NR = "NO REPORTADO"
ORDEN_CICLO = ["PRIMERA INFANCIA", "INFANCIA", "ADOLESCENCIA", "JUVENTUD", "ADULTEZ", "PERSONA MAYOR"]
MENORES = ORDEN_CICLO[:3]

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 8.5, "axes.edgecolor": "#d6d5cf",
    "axes.labelcolor": TINTA_2, "xtick.color": TINTA_2, "ytick.color": TINTA_2,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
    "grid.color": "#ecebe6", "grid.linewidth": 0.8, "axes.axisbelow": True,
    "axes.titlesize": 9.5, "axes.titleweight": "bold", "axes.titlecolor": TINTA,
})


# ---------------------------------------------------------------------------
# 1. Datos (solo el modelo dimensional validado)
# ---------------------------------------------------------------------------
SQL = {
    "sivigila": """
        SELECT t.year AS anio,
               CASE WHEN v.ciclo_vida LIKE 'PRIMERA INFANCIA%' THEN 'PRIMERA INFANCIA'
                    WHEN v.ciclo_vida LIKE 'INFANCIA%'         THEN 'INFANCIA'
                    WHEN v.ciclo_vida LIKE 'ADOLESCENCIA%'     THEN 'ADOLESCENCIA'
                    WHEN v.ciclo_vida LIKE 'JOVEN%' OR v.ciclo_vida LIKE 'JUVENTUD%' THEN 'JUVENTUD'
                    WHEN v.ciclo_vida LIKE 'ADULTEZ%'          THEN 'ADULTEZ'
                    WHEN v.ciclo_vida LIKE '%MAYOR%'           THEN 'PERSONA MAYOR'
                    ELSE 'NO REPORTADO' END AS ciclo_vida,
               CASE WHEN a.parentezco_vict ~ '(PAREJA|ESPOSO|COMPANERO|COMANERO|NOVIO|AMANTE)' THEN 'PAREJA O EXPAREJA'
                    WHEN a.parentezco_vict ~ '^(PADRE|MADRE|PADRAST|MADRAST)' THEN 'PADRE, MADRE O PADRASTROS'
                    WHEN a.parentezco_vict ~ '^(HERMANO|HIJO|TIO|ABUELO|PRIMO|CUNADO|SUEGRO|SOBRINO|NIETO|YERNO|NUERA)'
                         OR a.parentezco_vict LIKE '%FAMILIAR%' THEN 'OTRO FAMILIAR'
                    WHEN a.parentezco_vict ~ '(ENCARGADO|PROFESOR|CUSTODIA)' OR a.parentezco_vict = 'OTROS'
                         THEN 'CUIDADOR U OTRO CONOCIDO'
                    WHEN a.parentezco_vict = 'NINGUNO' THEN 'SIN VINCULO'
                    ELSE 'NO REPORTADO' END AS grupo_agresor,
               CASE WHEN tv.naturaleza = 1 THEN 'FISICA'
                    WHEN tv.naturaleza = 2 THEN 'PSICOLOGICA'
                    WHEN tv.naturaleza IN (4, 5, 6, 7, 10, 12, 14) THEN 'SEXUAL'
                    WHEN tv.naturaleza = 3 THEN 'NEGLIGENCIA Y ABANDONO'
                    ELSE 'SIN INFORMACION' END AS tipo_violencia,
               c.numero_comuna, c.nombre_comuna, c.tipo AS tipo_comuna
        FROM hechos_violencia h
        JOIN dim_tiempo t          ON t.id_tiempo = h.id_tiempo
        JOIN dim_victima v         ON v.id_victima = h.id_victima
        JOIN dim_agresor a         ON a.id_agresor = h.id_agresor
        JOIN dim_tipo_violencia tv ON tv.id_tipo_violencia = h.id_tipo_violencia
        JOIN dim_municipio m       ON m.id_municipio = h.id_municipio
        LEFT JOIN dim_comuna c     ON c.id_comuna = h.id_comuna
        WHERE m.codigo_dane = '68001' AND h.sexo_victima = 'FEMENINO'""",
    "medicina_legal": """
        SELECT t.year AS anio, a.ciclo_vida, a.grupo_agresor
        FROM hechos_api_violencia a JOIN dim_tiempo t ON t.id_tiempo = a.id_tiempo""",
    "anual": """
        SELECT anio,
               SUM(siv_casos_mujeres) AS sivigila, SUM(api_casos_mujeres) AS medicina_legal,
               COUNT(*) FILTER (WHERE siv_casos_mujeres IS NOT NULL) AS meses_sivigila,
               COUNT(*) FILTER (WHERE api_casos_mujeres IS NOT NULL) AS meses_api
        FROM dataset_unificado GROUP BY anio ORDER BY anio""",
    "osec_comuna": """
        SELECT c.numero_comuna, c.nombre_comuna,
               SUM(o.casos) FILTER (WHERE o.delito = 'VIOLENCIA INTRAFAMILIAR') AS osec_vif,
               MIN(t.year) AS desde, MAX(t.year) AS hasta
        FROM hechos_osec_comuna o
        JOIN dim_comuna c ON c.id_comuna = o.id_comuna
        JOIN dim_tiempo t ON t.id_tiempo = o.id_tiempo
        WHERE c.tipo = 'COMUNA'
        GROUP BY c.numero_comuna, c.nombre_comuna""",
    "estrato": """
        SELECT e.id_estrato, COUNT(*) AS viviendas
        FROM hechos_analisis h JOIN dim_estrato e ON e.id_estrato = h.id_estrato
        GROUP BY e.id_estrato ORDER BY e.id_estrato""",
    "calidad": """
        WITH ultima AS (SELECT suite, MAX(fecha_ejecucion) AS fecha FROM log_validaciones GROUP BY suite)
        SELECT l.suite, COUNT(*) AS expectativas, COUNT(*) FILTER (WHERE l.exito) AS cumplidas,
               MAX(l.fecha_ejecucion) AS fecha
        FROM log_validaciones l
        JOIN ultima u ON u.suite = l.suite AND u.fecha = l.fecha_ejecucion   -- solo la última ejecución de cada suite
        GROUP BY l.suite ORDER BY l.suite""",
}


def motor_db():
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    host = os.getenv("DB_HOST", "localhost")
    if host in ("host.docker.internal", "postgres_violencia_db"):
        host = "localhost"  # el reporte corre en el PC, no dentro de Docker
    return create_engine(URL.create(
        "postgresql+psycopg2", username=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD"), host=host, port=int(os.getenv("DB_PORT", "5432")),
        database=os.getenv("DB_NAME", "violencia_genero_db")))


def cargar_datos() -> dict:
    from sqlalchemy import text

    engine = motor_db()
    try:
        with engine.connect() as conn:
            datos = {k: pd.read_sql(text(q), conn) for k, q in SQL.items()}
    finally:
        engine.dispose()
    ruta_csv = RAIZ / "models" / "metricas_experimentos.csv"
    ruta_meta = RAIZ / "models" / "metadata_modelo.json"
    datos["modelo"] = pd.read_csv(ruta_csv) if ruta_csv.exists() else pd.DataFrame()
    datos["metadata"] = json.loads(ruta_meta.read_text(encoding="utf-8")) if ruta_meta.exists() else {}
    return datos


# ---------------------------------------------------------------------------
# 2. Utilidades de gráficas
# ---------------------------------------------------------------------------
def a_png(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=170, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def hora_colombia(valor):
    """log_validaciones guarda la hora en UTC (sin zona): se muestra en hora de Colombia."""
    t = pd.Timestamp(valor)
    t = t.tz_localize("UTC") if t.tzinfo is None else t
    return t.tz_convert("America/Bogota")


def pct(parte, total) -> float:
    return 100.0 * parte / total if total else 0.0


def barras_h(ax, etiquetas, valores, color=AZUL, formato="{:.0f}%", destacar=None):
    """Barras horizontales delgadas, extremos redondeados visualmente con etiqueta directa."""
    y = range(len(valores))
    colores = [color if destacar is None or i == destacar else GRIS for i in y]
    ax.barh(list(y), valores, color=colores, height=0.62)
    ax.set_yticks(list(y), etiquetas)
    ax.invert_yaxis()
    ax.grid(axis="y", visible=False)
    tope = max(valores) if len(valores) else 1
    for i, v in enumerate(valores):
        ax.text(v + tope * 0.02, i, formato.format(v), va="center", fontsize=8, color=TINTA)
    ax.set_xlim(0, tope * 1.18)


def titulo_caso(texto: str) -> str:
    return texto.title().replace(" O ", " o ").replace(" U ", " u ").replace(" Y ", " y ")


# ---------------------------------------------------------------------------
# 3. Paneles: cada uno devuelve (título, imagen, hallazgo)
# ---------------------------------------------------------------------------
def panel_tendencia(d):
    anual = d["anual"].copy()
    completos = anual[(anual["meses_sivigila"] == 12) | (anual["meses_api"] == 12)]
    fig, ax = plt.subplots(figsize=(4.4, 2.4))
    meses = {"sivigila": "meses_sivigila", "medicina_legal": "meses_api"}
    for col, color, nombre in [("sivigila", AZUL, "SIVIGILA (salud)"), ("medicina_legal", NARANJA, "Medicina Legal")]:
        serie = anual[(anual[meses[col]] == 12)].dropna(subset=[col])  # solo años completos: no simula caídas
        ax.plot(serie["anio"], serie[col], color=color, lw=2, marker="o", ms=3.5, label=nombre)
        if len(serie):
            ax.annotate(f"{int(serie[col].iloc[-1]):,}", (serie["anio"].iloc[-1], serie[col].iloc[-1]),
                        xytext=(4, 0), textcoords="offset points", fontsize=7.5, color=color, va="center")
    ax.set_ylabel("Casos de mujeres por año")
    ax.legend(frameon=False, fontsize=7.5, loc="upper left")
    ax.xaxis.set_major_locator(matplotlib.ticker.MaxNLocator(integer=True))
    siv = anual[anual["meses_sivigila"] == 12].dropna(subset=["sivigila"])
    pico = siv.loc[siv["sivigila"].idxmax()] if len(siv) else None
    incompletos = sorted(set(anual["anio"]) - set(completos["anio"]))
    hallazgo = (f"SIVIGILA alcanzó su máximo en {int(pico['anio'])} con {int(pico['sivigila']):,} casos de mujeres. "
                if pico is not None else "")
    hallazgo += ("Las dos fuentes miden cosas distintas (atención en salud vs. valoración forense), "
                 "por eso se comparan sus tendencias y no se suman.")
    if incompletos:
        hallazgo += f" No se grafican años incompletos (menos de 12 meses): {', '.join(map(str, incompletos))}."
    return "¿Cómo ha evolucionado la violencia contra las mujeres?", a_png(fig), hallazgo


def panel_vinculo(d):
    s = d["sivigila"]
    conteo = s["grupo_agresor"].value_counts()
    conocidos = conteo.drop(labels=[NR], errors="ignore")
    total = conocidos.sum()
    valores = [pct(v, total) for v in conocidos.values]
    fig, ax = plt.subplots(figsize=(4.4, 2.4))
    barras_h(ax, [titulo_caso(i) for i in conocidos.index], valores, destacar=0)
    ax.set_xlabel("% de casos con vínculo reportado")
    p_pareja = pct(conocidos.get("PAREJA O EXPAREJA", 0), total)
    p_familia = pct(conocidos.get("PADRE, MADRE O PADRASTROS", 0) + conocidos.get("OTRO FAMILIAR", 0), total)
    hallazgo = (f"El agresor más frecuente es {titulo_caso(conocidos.index[0]).lower()} ({valores[0]:.0f}%). "
                f"Pareja o expareja: {p_pareja:.0f}%; familiares: {p_familia:.0f}%. "
                f"{pct(conteo.get(NR, 0), conteo.sum()):.0f}% de los casos no reporta el vínculo.")
    return "¿Quién es el agresor? (SIVIGILA)", a_png(fig), hallazgo


def panel_tipo(d):
    conteo = d["sivigila"]["tipo_violencia"].value_counts()
    total = conteo.sum()
    valores = [pct(v, total) for v in conteo.values]
    fig, ax = plt.subplots(figsize=(4.4, 2.4))
    barras_h(ax, [titulo_caso(i) for i in conteo.index], valores, destacar=0)
    ax.set_xlabel("% de casos de mujeres")
    p_sexual = pct(conteo.get("SEXUAL", 0), total)
    hallazgo = (f"La violencia {conteo.index[0].lower()} es la más registrada ({valores[0]:.0f}%). "
                f"La violencia sexual representa el {p_sexual:.0f}% de los casos de mujeres.")
    return "¿Qué tipo de violencia sufren?", a_png(fig), hallazgo


def panel_ciclo(d):
    ml = d["medicina_legal"]
    ml = ml[ml["grupo_agresor"].isin(["PADRE, MADRE O PADRASTROS", "OTRO FAMILIAR"])]
    tasa = (ml.assign(padres=ml["grupo_agresor"] == "PADRE, MADRE O PADRASTROS")
            .groupby("ciclo_vida")["padres"].agg(["mean", "size"]))
    tasa = tasa.reindex([c for c in ORDEN_CICLO if c in tasa.index])
    valores = (tasa["mean"] * 100).tolist()
    fig, ax = plt.subplots(figsize=(4.4, 2.4))
    colores = [AZUL if c in MENORES else GRIS for c in tasa.index]
    ax.barh(range(len(valores)), valores, color=colores, height=0.62)
    ax.set_yticks(range(len(valores)), [f"{titulo_caso(c)} (n={n:,})" for c, n in zip(tasa.index, tasa["size"])])
    ax.invert_yaxis()
    ax.grid(axis="y", visible=False)
    for i, v in enumerate(valores):
        ax.text(v + 2, i, f"{v:.0f}%", va="center", fontsize=8, color=TINTA)
    ax.set_xlim(0, 115)
    ax.set_xlabel("% de casos donde el agresor es padre, madre o padrastro")
    menores = ml[ml["ciclo_vida"].isin(MENORES)]
    adultos = ml[~ml["ciclo_vida"].isin(MENORES)]
    p_men = pct((menores["grupo_agresor"] == "PADRE, MADRE O PADRASTROS").sum(), len(menores))
    p_adu = pct((adultos["grupo_agresor"] == "PADRE, MADRE O PADRASTROS").sum(), len(adultos))
    hallazgo = (f"En víctimas menores de edad el agresor es padre, madre o padrastro en el {p_men:.0f}% de los casos, "
                f"frente al {p_adu:.0f}% en adultas. ")
    if p_men - p_adu >= 15:
        hallazgo += ("Esta diferencia es la señal principal que usa el modelo de ML para priorizar rutas de "
                     "protección de niñas y adolescentes.")
    else:
        hallazgo += "La edad por sí sola no separa bien los grupos de agresor."
    return "¿Cambia el agresor según la edad? (Medicina Legal)", a_png(fig), hallazgo


def sivigila_periodo_osec(d) -> tuple[pd.DataFrame, str]:
    """Casos SIVIGILA por comuna en el MISMO periodo que cubre el OSEC (comparación justa)."""
    s = d["sivigila"]
    s = s[s["tipo_comuna"] == "COMUNA"]
    if len(d["osec_comuna"]):
        desde, hasta = int(d["osec_comuna"]["desde"].min()), int(d["osec_comuna"]["hasta"].max())
        recientes = s[s["anio"] >= desde]
        if len(recientes) >= 100:  # solo si hay suficientes casos en ese periodo
            return recientes, f"{desde}-{hasta}"
    return s, "todo el histórico"


def panel_territorio(d):
    s, periodo_txt = sivigila_periodo_osec(d)
    siv = s.groupby(["numero_comuna", "nombre_comuna"]).size().rename("sivigila")
    osec = d["osec_comuna"].set_index(["numero_comuna", "nombre_comuna"])["osec_vif"]
    tabla = pd.concat([siv, osec], axis=1).fillna(0)
    tabla["pct_siv"] = tabla["sivigila"] / tabla["sivigila"].sum() * 100
    tabla["pct_osec"] = tabla["osec_vif"] / (tabla["osec_vif"].sum() or 1) * 100
    top = tabla.sort_values("pct_siv", ascending=False).head(8)
    fig, ax = plt.subplots(figsize=(4.4, 2.6))
    y = range(len(top))
    ax.barh([i - 0.19 for i in y], top["pct_siv"], height=0.36, color=AZUL, label="SIVIGILA (casos de mujeres)")
    ax.barh([i + 0.19 for i in y], top["pct_osec"], height=0.36, color=NARANJA, label="OSEC (denuncias VIF)")
    ax.set_yticks(list(y), [f"{int(n):02d}. {titulo_caso(c)}" for n, c in top.index])
    ax.invert_yaxis()
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("% del total de la ciudad en cada fuente")
    ax.legend(frameon=False, fontsize=7, loc="lower center", bbox_to_anchor=(0.5, 1.0), ncol=2)
    top3 = tabla.sort_values("pct_siv", ascending=False).head(3)
    top3_osec = set(tabla.sort_values("pct_osec", ascending=False).head(3).index)
    coinciden = len(set(top3.index) & top3_osec)
    nombres = ", ".join(titulo_caso(c) for _, c in top3.index)
    hallazgo = (f"Mismo periodo en ambas fuentes ({periodo_txt}). Las 3 comunas con más casos en SIVIGILA ({nombres}) "
                f"concentran el {top3['pct_siv'].sum():.0f}% de la ciudad; {coinciden} de ellas también están entre las 3 "
                "con más denuncias en el OSEC. Se comparan porcentajes porque las fuentes cuentan cosas distintas.")
    return f"¿Dónde se concentra? (comunas, {periodo_txt})", a_png(fig), hallazgo


def panel_estrato(d):
    e = d["estrato"]
    total = e["viviendas"].sum()
    valores = [pct(v, total) for v in e["viviendas"]]
    fig, ax = plt.subplots(figsize=(4.4, 2.3))
    colores = [AZUL if i <= 2 else GRIS for i in e["id_estrato"]]
    ax.bar([str(i) for i in e["id_estrato"]], valores, color=colores, width=0.62)
    ax.set_xlabel("Estrato (azul = estratos 1 y 2)")
    for i, v in enumerate(valores):
        ax.text(i, v + 0.8, f"{v:.0f}%", ha="center", fontsize=8, color=TINTA)
    ax.set_ylabel("% de viviendas (ECV 2024)")
    ax.grid(axis="x", visible=False)
    p_bajo = pct(e.loc[e["id_estrato"] <= 2, "viviendas"].sum(), total)
    hallazgo = (f"El {p_bajo:.0f}% de las viviendas encuestadas de Bucaramanga son de estrato 1 y 2. "
                "La ECV describe la ciudad, no a cada víctima: sirve como contexto socioeconómico, "
                "no para afirmar que la pobreza causa la violencia.")
    return "¿Cuál es el contexto socioeconómico?", a_png(fig), hallazgo


def panel_modelo(d):
    m, meta = d["modelo"], d["metadata"]
    if m.empty or not meta:
        return "Desempeño del modelo de ML", None, "Ejecuta python ml/entrenar_modelo.py para incluir el modelo."
    filas = m[m["features"] == "sin_contexto"].set_index("modelo")
    nombres = {"linea_base": "Línea base", "regresion_logistica": "Regresión logística", "random_forest": "Random forest"}
    orden = [k for k in nombres if k in filas.index]
    valores = [filas.loc[k, "test_roc_auc"] for k in orden]
    fig, ax = plt.subplots(figsize=(4.4, 2.2))
    elegido = meta.get("modelo")
    barras_h(ax, [nombres[k] for k in orden], valores, formato="{:.2f}",
             destacar=orden.index(elegido) if elegido in orden else None)
    ax.set_xlabel("ROC-AUC en años no vistos (prueba)")
    ax.set_xlim(0, 1.12)
    mp = meta.get("metricas_prueba", {})
    div = meta.get("division", {})
    hallazgo = (f"El modelo elegido ({nombres.get(elegido, elegido).lower()}) alcanza ROC-AUC {mp.get('roc_auc', 0):.2f} "
                f"en {div.get('prueba_desde', '')}+ frente a 0.50 de la línea base. Con el umbral de alerta "
                f"({meta.get('umbral_alerta', 0):.0%}) detecta el {mp.get('recall_padres', 0):.0%} de los casos de padre/madre "
                f"con precisión de {mp.get('precision_padres', 0):.0%}.")
    return "¿Funciona el modelo con datos nuevos?", a_png(fig), hallazgo


def panel_calidad(d):
    c = d["calidad"]
    if c.empty:
        return "Calidad de datos (Great Expectations)", None, "Sin registros en log_validaciones."
    c = c.assign(pct=c["cumplidas"] / c["expectativas"] * 100)
    fig, ax = plt.subplots(figsize=(4.4, 2.3))
    etiquetas = [f"{s} ({int(n)})" for s, n in zip(c["suite"], c["expectativas"])]
    barras_h(ax, etiquetas, c["pct"].tolist(), color=AZUL)
    ax.set_xlabel("% de expectativas cumplidas (última ejecución)")
    ax.set_xlim(0, 118)
    total, ok = int(c["expectativas"].sum()), int(c["cumplidas"].sum())
    fecha = hora_colombia(c["fecha"].max()).strftime("%Y-%m-%d %H:%M")
    hallazgo = (f"{ok} de {total} expectativas cumplidas en {len(c)} suites ({fecha}, hora Colombia). Las validaciones corren "
                "antes y después de transformar; si una crítica falla, la carga al modelo dimensional se bloquea.")
    return "¿Los datos son confiables?", a_png(fig), hallazgo


# ---------------------------------------------------------------------------
# 4. Conclusiones y recomendaciones (insight -> decisión), calculadas con los datos
# ---------------------------------------------------------------------------
def conclusiones(d) -> list[str]:
    out = []
    # Edad -> ruta de protección
    ml = d["medicina_legal"]
    ml = ml[ml["grupo_agresor"].isin(["PADRE, MADRE O PADRASTROS", "OTRO FAMILIAR"])]
    men, adu = ml[ml["ciclo_vida"].isin(MENORES)], ml[~ml["ciclo_vida"].isin(MENORES)]
    p_men = pct((men["grupo_agresor"] == "PADRE, MADRE O PADRASTROS").sum(), len(men))
    p_adu = pct((adu["grupo_agresor"] == "PADRE, MADRE O PADRASTROS").sum(), len(adu))
    rec = d["metadata"].get("metricas_prueba", {}).get("recall_padres")
    out.append(f"<b>Proteger primero a niñas y adolescentes.</b> Cuando la víctima es menor, el agresor es su padre, madre "
               f"o padrastro en el {p_men:.0f}% de los casos (vs. {p_adu:.0f}% en adultas). <i>Decisión:</i> activar la ruta "
               "del ICBF / Comisaría de Familia desde la primera atención; el modelo en tiempo real ya prioriza estos casos"
               + (f" y detecta el {rec:.0%} de ellos." if rec else "."))
    # Vínculo -> el hogar como escenario
    s = d["sivigila"]
    agr = s["grupo_agresor"][s["grupo_agresor"] != NR]
    p_par = pct((agr == "PAREJA O EXPAREJA").sum(), len(agr))
    p_fam = pct(agr.isin(["PADRE, MADRE O PADRASTROS", "OTRO FAMILIAR"]).sum(), len(agr))
    out.append(f"<b>La violencia ocurre sobre todo en el círculo cercano.</b> {p_par + p_fam:.0f}% de los agresores son pareja, "
               f"expareja o familiares ({p_par:.0f}% pareja, {p_fam:.0f}% familia). <i>Decisión:</i> enfocar la prevención en "
               "relaciones de pareja y en el hogar, no solo en el espacio público.")
    # Territorio -> focalización + brecha entre fuentes
    s_periodo, periodo_txt = sivigila_periodo_osec(d)
    sc = s_periodo.groupby("nombre_comuna").size()
    osec = d["osec_comuna"].groupby("nombre_comuna")["osec_vif"].sum()
    t = pd.concat([sc.rename("siv"), osec.rename("osec")], axis=1).fillna(0)
    t["p_siv"], t["p_osec"] = t["siv"] / t["siv"].sum() * 100, t["osec"] / (t["osec"].sum() or 1) * 100
    top = t.sort_values("p_siv", ascending=False).head(3)
    brecha = (t["p_osec"] - t["p_siv"]).sort_values(ascending=False)
    texto = (f"<b>Focalizar el territorio ({periodo_txt}).</b> {', '.join(titulo_caso(c) for c in top.index)} concentran el "
             f"{top['p_siv'].sum():.0f}% de los casos atendidos en salud. ")
    if len(brecha) and brecha.iloc[0] >= 5:
        c = brecha.index[0]
        texto += (f"En {titulo_caso(c)} aparece una brecha entre fuentes: tiene el {t.loc[c, 'p_osec']:.0f}% de las denuncias "
                  f"del OSEC pero solo el {t.loc[c, 'p_siv']:.0f}% de los casos atendidos en salud, en el mismo periodo. "
                  "<i>Decisión:</i> investigar esa brecha (¿las víctimas denuncian pero no llegan a salud?, ¿cómo se "
                  "registra la comuna en cada fuente?) antes de asignar recursos.")
    else:
        texto += "<i>Decisión:</i> priorizar allí campañas de prevención y puntos de atención."
    out.append(texto)
    # Merge -> cómo usar las fuentes
    ruta_merge = RAIZ / "Docs" / "evaluacion_merge" / "resultados_merge.json"
    if ruta_merge.exists():
        rm = json.loads(ruta_merge.read_text(encoding="utf-8"))
        t5, t6 = rm["T5"]["mensual_siv_vs_api"], rm["T6"]
        out.append(f"<b>Las fuentes se comparan, no se suman (evaluación del merge).</b> El cruce no altera ningún caso "
                   f"(χ² p = 1). En el tiempo, SIVIGILA y Medicina Legal no se mueven juntas (Spearman ρ = {t5['rho']}), "
                   f"pero en el territorio sí concuerdan (ρ = {t6['rho']}). <i>Decisión:</i> usar cada fuente como un lente "
                   "distinto del mismo problema y la comparación por comuna para priorizar.")
    return out


# ---------------------------------------------------------------------------
# 5. Página HTML
# ---------------------------------------------------------------------------
def kpis(d) -> list[tuple[str, str]]:
    s, ml = d["sivigila"], d["medicina_legal"]
    agr = s["grupo_agresor"][s["grupo_agresor"] != NR]
    c = d["calidad"]
    return [
        (f"{len(s):,}", "casos de mujeres en SIVIGILA"),
        (f"{len(ml):,}", "casos de mujeres en Medicina Legal"),
        (f"{pct((agr == 'PAREJA O EXPAREJA').sum(), len(agr)):.0f}%", "agresor pareja o expareja"),
        (f"{pct((s['tipo_violencia'] == 'SEXUAL').sum(), len(s)):.0f}%", "violencia sexual"),
        (f"{pct(c['cumplidas'].sum(), c['expectativas'].sum()):.0f}%" if len(c) else "—", "validaciones GE cumplidas"),
    ]


def construir_html(d: dict) -> str:
    paneles = [panel_tendencia(d), panel_vinculo(d), panel_tipo(d), panel_ciclo(d),
               panel_territorio(d), panel_estrato(d), panel_modelo(d), panel_calidad(d)]
    tarjetas = "".join(
        f"<section class='panel'><h3>{escape(t)}</h3>"
        + (f"<img src='data:image/png;base64,{img}' alt='{escape(t)}'>" if img else "")
        + f"<p class='hallazgo'><b>Hallazgo:</b> {escape(h)}</p></section>"
        for t, img, h in paneles)
    banda = "".join(f"<div class='kpi'><div class='v'>{v}</div><div class='l'>{escape(l)}</div></div>"
                    for v, l in kpis(d))
    generado = datetime.now().strftime("%Y-%m-%d %H:%M")
    lista = "".join(f"<li>{c}</li>" for c in conclusiones(d))
    return f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8">
<title>Reporte analítico · Violencia contra las mujeres en Bucaramanga</title>
<style>
  @page {{ size: A3 landscape; margin: 8mm; }}
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; padding: 14px 18px; font-family: "Segoe UI", system-ui, sans-serif; color: {TINTA};
         background: #fff; font-size: 11.5px; }}
  header {{ display: flex; justify-content: space-between; align-items: flex-end; border-bottom: 2px solid {AZUL};
           padding-bottom: 6px; margin-bottom: 8px; }}
  h1 {{ font-size: 19px; margin: 0; }}
  .sub {{ color: {TINTA_2}; margin-top: 2px; }}
  .meta {{ text-align: right; color: {TINTA_2}; font-size: 10.5px; }}
  .kpis {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 8px; margin-bottom: 8px; }}
  .kpi {{ border: 1px solid #e4e3dd; border-radius: 6px; padding: 6px 10px; }}
  .kpi .v {{ font-size: 20px; font-weight: 700; }}
  .kpi .l {{ color: {TINTA_2}; }}
  .grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 8px; }}
  .panel {{ border: 1px solid #e4e3dd; border-radius: 6px; padding: 6px 8px; break-inside: avoid; }}
  .panel h3 {{ font-size: 12.5px; margin: 0 0 4px; }}
  .panel img {{ width: 100%; height: auto; display: block; }}
  .hallazgo {{ margin: 4px 0 0; line-height: 1.35; }}
  .conclusiones {{ margin-top: 8px; border: 1px solid {AZUL}; border-left: 5px solid {AZUL}; border-radius: 6px;
                   padding: 6px 12px; background: #f6f9fe; }}
  .conclusiones h3 {{ font-size: 13px; margin: 0 0 4px; }}
  .conclusiones ol {{ margin: 0; padding-left: 18px; display: grid; grid-template-columns: 1fr 1fr; gap: 4px 24px; }}
  .conclusiones li {{ line-height: 1.35; }}
  footer {{ margin-top: 8px; color: {TINTA_2}; font-size: 10px; display: flex; justify-content: space-between; gap: 12px; }}
</style></head><body>
<header>
  <div><h1>Violencia contra las mujeres en Bucaramanga · Reporte analítico</h1>
  <div class="sub">¿Cómo se manifiesta según el vínculo con el agresor, la edad de la víctima, el tipo de violencia,
  la comuna y el contexto socioeconómico? · ODS 5</div></div>
  <div class="meta">Generado: {generado}<br>Fuente: modelo dimensional validado (PostgreSQL)</div>
</header>
<div class="kpis">{banda}</div>
<div class="grid">{tarjetas}</div>
<section class="conclusiones"><h3>Conclusiones y recomendaciones</h3><ol>{lista}</ol></section>
<footer>
  <span>Fuentes: SIVIGILA (casos atendidos en salud) · Medicina Legal, API datos.gov.co (valoración forense) ·
  OSEC Alcaldía de Bucaramanga (denuncias) · ECV 2024 DANE (vivienda). Las fuentes se comparan, no se suman.</span>
  <span>Regenerar: <code>python reporte_estatico.py</code> · Equipo: {escape(EQUIPO)}</span>
</footer>
</body></html>"""


def main():
    datos = cargar_datos()
    SALIDA.parent.mkdir(parents=True, exist_ok=True)
    SALIDA.write_text(construir_html(datos), encoding="utf-8")
    print(f"✅ Reporte generado: {SALIDA}")


if __name__ == "__main__":
    main()