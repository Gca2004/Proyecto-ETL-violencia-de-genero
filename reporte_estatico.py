"""
reporte_estatico.py
Reporte analítico ESTÁTICO de una sola página (vía analítica de la arquitectura).

Lee   : el modelo dimensional VALIDADO en PostgreSQL (hechos_violencia, hechos_api_violencia,
        hechos_osec_comuna, hechos_analisis + dimensiones) y log_validaciones.
        Además, los artefactos del modelo (models/metricas_experimentos.csv, models/metadata_modelo.json).
        NO lee Kafka ni la tabla de predicciones: es una foto en un punto del tiempo.
Genera: Docs/reporte_estatico.html  (una página; se abre en el navegador y se imprime a PDF)

Pregunta del proyecto:
    ¿Quién agrede a las mujeres en Bucaramanga, cómo cambia según la edad de la víctima,
    dónde se concentra y sirve el modelo para priorizar casos nuevos?

Solo 4 gráficas (las preguntas que más influyen en las decisiones); el resto va como cifra clave.
Cada gráfica trae su hallazgo escrito, calculado a partir de los datos (no es texto fijo).

Accesibilidad de color:
    - Solo dos colores de serie: azul y naranja (par validado para daltonismo: protán, deután y tritán).
    - El color nunca es la única pista: cada barra lleva su valor escrito, hay leyenda, y la serie
      naranja tiene además una textura de rayas (se distingue incluso impresa en blanco y negro).

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
from matplotlib.patches import Patch

RAIZ = Path(__file__).resolve().parent
load_dotenv(RAIZ / ".env")
SALIDA = RAIZ / "Docs" / "reporte_estatico.html"
# Nombres COMPLETOS del equipo (observación del profesor en la entrega anterior)
EQUIPO = "Leidy Johanna Alomia Zapata · Gustavo Coque · Carlos Cortes"

# Paleta apta para daltonismo (validada: protán/deután/tritán ΔE ≥ 24, contraste ≥ 3:1)
AZUL, NARANJA = "#2a78d6", "#eb6834"
GRIS = "#a3a29b"                 # contexto (no destacado); más oscuro para que se vea sobre blanco
TINTA, TINTA_2 = "#1f1f1e", "#52514e"
TEXTURA = "////"                 # segunda pista visual para la serie naranja
NR = "NO REPORTADO"
ORDEN_CICLO = ["PRIMERA INFANCIA", "INFANCIA", "ADOLESCENCIA", "JUVENTUD", "ADULTEZ", "PERSONA MAYOR"]
MENORES = ORDEN_CICLO[:3]
TAM_FIG = (4.6, 2.7)             # todas las gráficas del mismo tamaño: tarjetas alineadas

plt.rcParams.update({
    "font.family": "DejaVu Sans", "font.size": 9, "axes.edgecolor": "#d6d5cf",
    "axes.labelcolor": TINTA_2, "xtick.color": TINTA_2, "ytick.color": TINTA,
    "axes.spines.top": False, "axes.spines.right": False, "axes.grid": True,
    "grid.color": "#ecebe6", "grid.linewidth": 0.8, "axes.axisbelow": True,
    "hatch.color": "white", "hatch.linewidth": 1.0,
})


# ---------------------------------------------------------------------------
# 1. Datos (solo el modelo dimensional validado)
# ---------------------------------------------------------------------------
SQL = {
    "sivigila": """
        SELECT t.year AS anio,
               CASE WHEN a.parentezco_vict ~ '(PAREJA|ESPOSO|COMPANERO|COMANERO|NOVIO|AMANTE)' THEN 'PAREJA O EXPAREJA'
                    WHEN a.parentezco_vict ~ '^(PADRE|MADRE|PADRAST|MADRAST)' THEN 'PADRE, MADRE O PADRASTROS'
                    WHEN a.parentezco_vict ~ '^(HERMANO|HIJO|TIO|ABUELO|PRIMO|CUNADO|SUEGRO|SOBRINO|NIETO|YERNO|NUERA)'
                         OR a.parentezco_vict LIKE '%FAMILIAR%' THEN 'OTRO FAMILIAR'
                    WHEN a.parentezco_vict ~ '(ENCARGADO|PROFESOR|CUSTODIA)' OR a.parentezco_vict = 'OTROS'
                         THEN 'CUIDADOR U OTRO CONOCIDO'
                    WHEN a.parentezco_vict = 'NINGUNO' THEN 'SIN VINCULO'
                    ELSE 'NO REPORTADO' END AS grupo_agresor,
               CASE WHEN tv.naturaleza IN (4, 5, 6, 7, 10, 12, 14) THEN 'SEXUAL' ELSE 'OTRA' END AS tipo_violencia,
               c.numero_comuna, c.nombre_comuna, c.tipo AS tipo_comuna
        FROM hechos_violencia h
        JOIN dim_tiempo t          ON t.id_tiempo = h.id_tiempo
        JOIN dim_agresor a         ON a.id_agresor = h.id_agresor
        JOIN dim_tipo_violencia tv ON tv.id_tipo_violencia = h.id_tipo_violencia
        JOIN dim_municipio m       ON m.id_municipio = h.id_municipio
        LEFT JOIN dim_comuna c     ON c.id_comuna = h.id_comuna
        WHERE m.codigo_dane = '68001' AND h.sexo_victima = 'FEMENINO'""",
    "medicina_legal": """
        SELECT t.year AS anio, a.ciclo_vida, a.grupo_agresor
        FROM hechos_api_violencia a JOIN dim_tiempo t ON t.id_tiempo = a.id_tiempo""",
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
# 2. Utilidades
# ---------------------------------------------------------------------------
def a_png(fig) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=200, bbox_inches="tight")
    plt.close(fig)
    return base64.b64encode(buf.getvalue()).decode("ascii")


def hora_colombia(valor):
    """log_validaciones guarda la hora en UTC (sin zona): se muestra en hora de Colombia."""
    t = pd.Timestamp(valor)
    t = t.tz_localize("UTC") if t.tzinfo is None else t
    return t.tz_convert("America/Bogota")


def pct(parte, total) -> float:
    return 100.0 * parte / total if total else 0.0


def titulo_caso(texto: str) -> str:
    return texto.title().replace(" O ", " o ").replace(" U ", " u ").replace(" Y ", " y ")


def barras_h(ax, etiquetas, valores, colores, formato="{:.0f}%", tope_x=None):
    """Barras horizontales con el valor escrito al final (el color nunca es la única pista)."""
    y = list(range(len(valores)))
    ax.barh(y, valores, color=colores, height=0.62, edgecolor="white", linewidth=1.5)
    ax.set_yticks(y, etiquetas)
    ax.invert_yaxis()
    ax.grid(axis="y", visible=False)
    tope = tope_x or (max(valores) * 1.2 if len(valores) else 1)
    for i, v in enumerate(valores):
        ax.text(v + tope * 0.015, i, formato.format(v), va="center", fontsize=8.5, color=TINTA, fontweight="bold")
    ax.set_xlim(0, tope)


def leyenda(ax, items, **kw):
    """items = [(etiqueta, color, textura)]"""
    ax.legend(handles=[Patch(facecolor=c, hatch=h, edgecolor="white", label=t) for t, c, h in items],
              frameon=False, fontsize=8, loc="lower center", bbox_to_anchor=(0.5, 1.0),
              ncol=len(items), handlelength=1.6, **kw)


def filtro_familiares(ml: pd.DataFrame) -> pd.DataFrame:
    return ml[ml["grupo_agresor"].isin(["PADRE, MADRE O PADRASTROS", "OTRO FAMILIAR"])]


def tasa_padres(df: pd.DataFrame) -> float:
    return pct((df["grupo_agresor"] == "PADRE, MADRE O PADRASTROS").sum(), len(df))


# ---------------------------------------------------------------------------
# 3. Las 4 preguntas que más influyen: cada panel devuelve (título, imagen, hallazgo)
# ---------------------------------------------------------------------------
def panel_vinculo(d):
    """P1. ¿Quién es el agresor? -> define a quién dirigir la prevención."""
    s = d["sivigila"]
    conteo = s["grupo_agresor"].value_counts()
    conocidos = conteo.drop(labels=[NR], errors="ignore")
    total = conocidos.sum()
    valores = [pct(v, total) for v in conocidos.values]
    cercanos = {"PAREJA O EXPAREJA", "PADRE, MADRE O PADRASTROS", "OTRO FAMILIAR"}
    colores = [AZUL if g in cercanos else GRIS for g in conocidos.index]
    fig, ax = plt.subplots(figsize=TAM_FIG)
    barras_h(ax, [titulo_caso(i) for i in conocidos.index], valores, colores)
    leyenda(ax, [("Círculo cercano (pareja o familia)", AZUL, ""), ("Otros", GRIS, "")])
    ax.set_xlabel("% de casos con vínculo reportado")
    p_pareja = pct(conocidos.get("PAREJA O EXPAREJA", 0), total)
    p_familia = pct(conocidos.get("PADRE, MADRE O PADRASTROS", 0) + conocidos.get("OTRO FAMILIAR", 0), total)
    hallazgo = (f"El {p_pareja + p_familia:.0f}% de los agresores pertenece al círculo cercano de la víctima: "
                f"pareja o expareja ({p_pareja:.0f}%) o familiares ({p_familia:.0f}%). "
                "La violencia ocurre sobre todo en casa, no en la calle.")
    return "1. ¿Quién es el agresor?", a_png(fig), hallazgo


def panel_ciclo(d):
    """P2. ¿Cambia el agresor según la edad? -> es la señal que usa el modelo de ML."""
    ml = filtro_familiares(d["medicina_legal"])
    tasa = (ml.assign(padres=ml["grupo_agresor"] == "PADRE, MADRE O PADRASTROS")
            .groupby("ciclo_vida")["padres"].agg(["mean", "size"]))
    tasa = tasa.reindex([c for c in ORDEN_CICLO if c in tasa.index])
    valores = (tasa["mean"] * 100).tolist()
    colores = [AZUL if c in MENORES else GRIS for c in tasa.index]
    fig, ax = plt.subplots(figsize=TAM_FIG)
    barras_h(ax, [f"{titulo_caso(c)} (n={n:,})" for c, n in zip(tasa.index, tasa["size"])],
             valores, colores, tope_x=115)
    leyenda(ax, [("Menores de edad", AZUL, ""), ("Mayores de edad", GRIS, "")])
    ax.set_xlabel("% de casos donde el agresor es padre, madre o padrastro")
    p_men = tasa_padres(ml[ml["ciclo_vida"].isin(MENORES)])
    p_adu = tasa_padres(ml[~ml["ciclo_vida"].isin(MENORES)])
    hallazgo = (f"Cuando la víctima es menor de edad, el agresor es su padre, madre o padrastro en el {p_men:.0f}% "
                f"de los casos, frente al {p_adu:.0f}% en adultas. ")
    hallazgo += ("Esta diferencia es la señal principal que usa el modelo para priorizar la protección de "
                 "niñas y adolescentes." if p_men - p_adu >= 15 else
                 "La edad por sí sola no separa bien los grupos de agresor.")
    return "2. ¿Cambia el agresor según la edad?", a_png(fig), hallazgo


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


def tabla_territorio(d) -> tuple[pd.DataFrame, str]:
    s, periodo_txt = sivigila_periodo_osec(d)
    siv = s.groupby(["numero_comuna", "nombre_comuna"]).size().rename("sivigila")
    osec = d["osec_comuna"].set_index(["numero_comuna", "nombre_comuna"])["osec_vif"]
    tabla = pd.concat([siv, osec], axis=1).fillna(0)
    tabla["pct_siv"] = tabla["sivigila"] / (tabla["sivigila"].sum() or 1) * 100
    tabla["pct_osec"] = tabla["osec_vif"] / (tabla["osec_vif"].sum() or 1) * 100
    return tabla, periodo_txt


def panel_territorio(d):
    """P3. ¿Dónde se concentra? -> define dónde poner recursos."""
    tabla, periodo_txt = tabla_territorio(d)
    top = tabla.sort_values("pct_siv", ascending=False).head(6)
    fig, ax = plt.subplots(figsize=TAM_FIG)
    y = list(range(len(top)))
    ax.barh([i - 0.2 for i in y], top["pct_siv"], height=0.38, color=AZUL, edgecolor="white", linewidth=1)
    ax.barh([i + 0.2 for i in y], top["pct_osec"], height=0.38, color=NARANJA, hatch=TEXTURA,
            edgecolor="white", linewidth=1)
    tope = max(top["pct_siv"].max(), top["pct_osec"].max()) * 1.22
    for i, (a, b) in enumerate(zip(top["pct_siv"], top["pct_osec"])):
        ax.text(a + tope * 0.015, i - 0.2, f"{a:.0f}%", va="center", fontsize=7.5, color=TINTA)
        ax.text(b + tope * 0.015, i + 0.2, f"{b:.0f}%", va="center", fontsize=7.5, color=TINTA)
    ax.set_xlim(0, tope)
    ax.set_yticks(y, [f"{int(n):02d}. {titulo_caso(c)}" for n, c in top.index])
    ax.invert_yaxis()
    ax.grid(axis="y", visible=False)
    ax.set_xlabel("% del total de la ciudad en cada fuente")
    leyenda(ax, [("SIVIGILA (atendidos en salud)", AZUL, ""), ("OSEC (denuncias)", NARANJA, TEXTURA)])
    top3 = tabla.sort_values("pct_siv", ascending=False).head(3)
    coinciden = len(set(top3.index) & set(tabla.sort_values("pct_osec", ascending=False).head(3).index))
    nombres = ", ".join(titulo_caso(c) for _, c in top3.index)
    verbo = "está" if coinciden == 1 else "están"
    hallazgo = (f"Mismo periodo en ambas fuentes ({periodo_txt}). {nombres} concentran el "
                f"{top3['pct_siv'].sum():.0f}% de los casos atendidos en salud; {coinciden} de ellas también {verbo} "
                "entre las 3 con más denuncias. Se comparan porcentajes porque cada fuente cuenta algo distinto.")
    return f"3. ¿Dónde se concentra? (comunas, {periodo_txt})", a_png(fig), hallazgo


def panel_modelo(d):
    """P4. ¿El modelo sirve con casos nuevos? -> justifica usarlo en tiempo real."""
    m, meta = d["modelo"], d["metadata"]
    if m.empty or not meta:
        return "4. ¿Funciona el modelo con datos nuevos?", None, "Ejecuta python ml/entrenar_modelo.py para incluir el modelo."
    filas = m[m["features"] == "sin_contexto"].set_index("modelo")
    nombres = {"linea_base": "Línea base (azar)", "regresion_logistica": "Regresión logística",
               "random_forest": "Random forest"}
    orden = [k for k in nombres if k in filas.index]
    valores = [filas.loc[k, "test_roc_auc"] for k in orden]
    elegido = meta.get("modelo")
    colores = [AZUL if k == elegido else GRIS for k in orden]
    fig, ax = plt.subplots(figsize=TAM_FIG)
    barras_h(ax, [nombres[k] for k in orden], valores, colores, formato="{:.2f}", tope_x=1.12)
    leyenda(ax, [("Modelo elegido", AZUL, ""), ("Alternativas", GRIS, "")])
    ax.set_xlabel("ROC-AUC en años que el modelo no vio (prueba)")
    mp, div = meta.get("metricas_prueba", {}), meta.get("division", {})
    desde = f" ({div.get('prueba_desde')} en adelante)" if div.get("prueba_desde") else ""
    hallazgo = (f"Con casos que nunca vio{desde}, la {nombres.get(elegido, elegido).lower()} distingue el grupo de "
                f"agresor con ROC-AUC {mp.get('roc_auc', 0):.2f} (el azar da 0.50). Con la alerta en "
                f"{meta.get('umbral_alerta', 0):.0%} de probabilidad detecta el {mp.get('recall_padres', 0):.0%} "
                f"de los casos de padre o madre; {mp.get('precision_padres', 0):.0%} de las alertas son correctas.")
    return "4. ¿Funciona el modelo con datos nuevos?", a_png(fig), hallazgo


# ---------------------------------------------------------------------------
# 4. Conclusiones y recomendaciones (insight -> decisión), calculadas con los datos
# ---------------------------------------------------------------------------
def conclusiones(d) -> list[str]:
    out = []
    ml = filtro_familiares(d["medicina_legal"])
    p_men = tasa_padres(ml[ml["ciclo_vida"].isin(MENORES)])
    p_adu = tasa_padres(ml[~ml["ciclo_vida"].isin(MENORES)])
    rec = d["metadata"].get("metricas_prueba", {}).get("recall_padres")
    out.append(f"<b>Proteger primero a niñas y adolescentes.</b> En menores, el agresor es padre, madre o padrastro en el "
               f"{p_men:.0f}% de los casos (vs. {p_adu:.0f}% en adultas). <i>Decisión:</i> activar la ruta del ICBF / "
               "Comisaría de Familia desde la primera atención; el modelo en tiempo real ya prioriza estos casos"
               + (f" y detecta el {rec:.0%} de ellos." if rec else "."))

    agr = d["sivigila"]["grupo_agresor"]
    agr = agr[agr != NR]
    p_par = pct((agr == "PAREJA O EXPAREJA").sum(), len(agr))
    p_fam = pct(agr.isin(["PADRE, MADRE O PADRASTROS", "OTRO FAMILIAR"]).sum(), len(agr))
    out.append(f"<b>La violencia ocurre en el círculo cercano.</b> {p_par + p_fam:.0f}% de los agresores son pareja, "
               f"expareja o familiares. <i>Decisión:</i> enfocar la prevención en las relaciones de pareja y en el hogar.")

    t, periodo_txt = tabla_territorio(d)
    t = t.groupby(level="nombre_comuna")[["pct_siv", "pct_osec"]].sum()
    top = t.sort_values("pct_siv", ascending=False).head(3)
    brecha = (t["pct_osec"] - t["pct_siv"]).sort_values(ascending=False)
    texto = (f"<b>Focalizar el territorio ({periodo_txt}).</b> {', '.join(titulo_caso(c) for c in top.index)} concentran "
             f"el {top['pct_siv'].sum():.0f}% de los casos atendidos en salud. ")
    if len(brecha) and brecha.iloc[0] >= 5:
        c = brecha.index[0]
        texto += (f"{titulo_caso(c)} tiene el {t.loc[c, 'pct_osec']:.0f}% de las denuncias pero solo el "
                  f"{t.loc[c, 'pct_siv']:.0f}% de los casos en salud. <i>Decisión:</i> investigar esa brecha antes "
                  "de asignar recursos.")
    else:
        texto += "<i>Decisión:</i> priorizar allí campañas de prevención y puntos de atención."
    out.append(texto)

    ruta_merge = RAIZ / "Docs" / "evaluacion_merge" / "resultados_merge.json"
    if ruta_merge.exists():
        rm = json.loads(ruta_merge.read_text(encoding="utf-8"))
        t5, t6 = rm["T5"]["mensual_siv_vs_api"], rm["T6"]
        out.append(f"<b>Las fuentes se comparan, no se suman.</b> El cruce no altera ningún caso (χ² p = 1). En el tiempo, "
                   f"SIVIGILA y Medicina Legal no se mueven juntas (ρ = {t5['rho']}), pero por comuna sí concuerdan "
                   f"(ρ = {t6['rho']}). <i>Decisión:</i> usar la comparación por comuna para priorizar.")
    return out


# ---------------------------------------------------------------------------
# 5. Página HTML
# ---------------------------------------------------------------------------
def kpis(d) -> list[tuple[str, str]]:
    s, ml, c, e = d["sivigila"], d["medicina_legal"], d["calidad"], d["estrato"]
    p_bajo = pct(e.loc[e["id_estrato"] <= 2, "viviendas"].sum(), e["viviendas"].sum()) if len(e) else None
    total, ok = (int(c["expectativas"].sum()), int(c["cumplidas"].sum())) if len(c) else (0, 0)
    return [
        (f"{len(s):,}", "casos de mujeres atendidos en salud (SIVIGILA)"),
        (f"{len(ml):,}", "casos de mujeres con valoración forense (Medicina Legal)"),
        (f"{pct((s['tipo_violencia'] == 'SEXUAL').sum(), len(s)):.0f}%", "de los casos son violencia sexual"),
        (f"{p_bajo:.0f}%" if p_bajo is not None else "—", "de las viviendas son estrato 1 y 2 (contexto ECV 2024)"),
        (f"{pct(ok, total):.0f}%" if total else "—", f"de las validaciones de calidad cumplidas ({ok}/{total})"),
    ]


def construir_html(d: dict) -> str:
    paneles = [panel_vinculo(d), panel_ciclo(d), panel_territorio(d), panel_modelo(d)]
    tarjetas = "".join(
        f"<section class='panel'><h3>{escape(t)}</h3>"
        + (f"<img src='data:image/png;base64,{img}' alt='{escape(t)}'>" if img else "")
        + f"<p class='hallazgo'><b>Hallazgo:</b> {escape(h)}</p></section>"
        for t, img, h in paneles)
    banda = "".join(f"<div class='kpi'><div class='v'>{v}</div><div class='l'>{escape(l)}</div></div>"
                    for v, l in kpis(d))
    generado = datetime.now().strftime("%Y-%m-%d %H:%M")
    c = d["calidad"]
    validado = (f" · Última validación: {hora_colombia(c['fecha'].max()):%Y-%m-%d %H:%M} (hora Colombia)"
                if len(c) else "")
    lista = "".join(f"<li>{x}</li>" for x in conclusiones(d))
    return f"""<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8">
<title>Reporte analítico · Violencia contra las mujeres en Bucaramanga</title>
<style>
  @page {{ size: A3 landscape; margin: 8mm; }}
  * {{ box-sizing: border-box; }}
  body {{ margin: 0; padding: 14px 18px; font-family: "Segoe UI", system-ui, sans-serif; color: {TINTA};
         background: #fff; font-size: 12.5px; }}
  header {{ display: flex; justify-content: space-between; align-items: flex-end; border-bottom: 2px solid {AZUL};
           padding-bottom: 6px; margin-bottom: 10px; }}
  h1 {{ font-size: 21px; margin: 0; }}
  .sub {{ color: {TINTA_2}; margin-top: 3px; font-size: 13px; }}
  .meta {{ text-align: right; color: {TINTA_2}; font-size: 11px; }}
  .kpis {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 10px; margin-bottom: 10px; }}
  .kpi {{ border: 1px solid #e4e3dd; border-radius: 6px; padding: 8px 12px; }}
  .kpi .v {{ font-size: 24px; font-weight: 700; }}
  .kpi .l {{ color: {TINTA_2}; }}
  .grid {{ display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; }}
  .panel {{ border: 1px solid #e4e3dd; border-radius: 6px; padding: 8px 10px; break-inside: avoid; }}
  .panel h3 {{ font-size: 14px; margin: 0 0 6px; }}
  .panel img {{ width: 100%; height: auto; display: block; }}
  .hallazgo {{ margin: 6px 0 0; line-height: 1.4; }}
  .conclusiones {{ margin-top: 10px; border: 1px solid {AZUL}; border-left: 5px solid {AZUL}; border-radius: 6px;
                   padding: 8px 14px; background: #f6f9fe; }}
  .conclusiones h3 {{ font-size: 14px; margin: 0 0 6px; }}
  .conclusiones ol {{ margin: 0; padding-left: 18px; display: grid; grid-template-columns: 1fr 1fr; gap: 6px 28px; }}
  .conclusiones li {{ line-height: 1.4; }}
  footer {{ margin-top: 8px; color: {TINTA_2}; font-size: 10.5px; display: flex; justify-content: space-between; gap: 12px; }}
</style></head><body>
<header>
  <div><h1>Violencia contra las mujeres en Bucaramanga · Reporte analítico</h1>
  <div class="sub">¿Quién agrede, cómo cambia según la edad de la víctima, dónde se concentra y sirve el modelo
  para priorizar casos nuevos? · ODS 5, meta 5.2</div></div>
  <div class="meta">Generado: {generado}{validado}<br>Fuente: modelo dimensional validado (PostgreSQL)</div>
</header>
<div class="kpis">{banda}</div>
<div class="grid">{tarjetas}</div>
<section class="conclusiones"><h3>Conclusiones y recomendaciones</h3><ol>{lista}</ol></section>
<footer>
  <span>Fuentes: SIVIGILA (atención en salud) · Medicina Legal, API datos.gov.co (valoración forense) ·
  OSEC Alcaldía de Bucaramanga (denuncias) · ECV 2024 DANE (vivienda). Las fuentes se comparan, no se suman.
  Colores aptos para daltonismo: cada barra lleva su valor y la serie naranja tiene textura.</span>
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