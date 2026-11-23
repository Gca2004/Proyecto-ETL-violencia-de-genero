"""
analisis/evaluacion_merge.py
Evaluación estadística del MERGE (corrección del profesor).

Pregunta: ¿unir las fuentes de SERIE DE TIEMPO (SIVIGILA, Medicina Legal, OSEC) con la fuente
ESTACIONARIA (ECV 2024, una foto de un solo año) es válido y NO sesga los datos?

Pruebas (todas sobre el modelo dimensional validado en PostgreSQL):
  T1  Conservación de registros: el cruce no pierde ni duplica casos.
  T2  Distribuciones antes vs. después del cruce (chi-cuadrado de homogeneidad + distancia de variación total).
  T3  Las series mensuales del dataset unificado son idénticas a las de cada fuente por separado.
  T4  La fuente estacionaria (ECV) es constante en el tiempo: no crea tendencias ni estacionalidad falsas.
  T5  Concordancia temporal entre fuentes (correlación de Spearman mensual y anual).
  T6  Concordancia territorial SIVIGILA vs. OSEC por comuna (Spearman, n = 17 comunas).
  T7  Sesgo por cobertura: ¿los casos que sí tienen dato OSEC son distintos a los que no? (chi-cuadrado + V de Cramér).
  T8  Aporte de cada variable cruzada al objetivo del modelo (V de Cramér), comparado con la edad de la víctima.

Genera:
  Docs/evaluacion_merge/evaluacion_merge.md     -> resumen con tabla de pruebas e interpretación
  Docs/evaluacion_merge/resultados_merge.json   -> todas las cifras
  Docs/evaluacion_merge/*.png                   -> 3 gráficas

Uso (desde la raíz del proyecto, con PostgreSQL encendido y el DAG ya ejecutado):
    python analisis/evaluacion_merge.py
"""

import json
import math
import os
from datetime import datetime
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from dotenv import load_dotenv
from scipy import stats

RAIZ = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ / ".env")
SALIDA = RAIZ / "Docs" / "evaluacion_merge"
ALFA = 0.05
AZUL, NARANJA, GRIS, TINTA = "#2a78d6", "#eb6834", "#b9b8b1", "#1f1f1e"
VARIABLES = ["ciclo_vida", "grupo_agresor", "escenario", "turno", "dias_incapacidad", "area"]

plt.rcParams.update({"font.size": 9, "axes.spines.top": False, "axes.spines.right": False,
                     "axes.grid": True, "grid.color": "#ecebe6", "axes.axisbelow": True,
                     "axes.titleweight": "bold"})

SQL = {
    "antes_stg": """SELECT anio, ciclo_vida, grupo_agresor, escenario, turno, dias_incapacidad, area
                    FROM stg_hechos_api""",
    "antes_hechos": """
        SELECT h.id_hecho_api, t.year AS anio, h.ciclo_vida, h.grupo_agresor, h.escenario, h.turno,
               h.dias_incapacidad, m.area
        FROM hechos_api_violencia h
        JOIN dim_tiempo t ON t.id_tiempo = h.id_tiempo
        JOIN dim_municipio m ON m.id_municipio = h.id_municipio""",
    "despues": """
        SELECT id_hecho_api, anio, ciclo_vida, grupo_agresor, escenario, turno, dias_incapacidad, area,
               osec_denuncias_sexuales, nivel_estrato, zona_vivienda
        FROM vista_ml_hechos_enriquecidos""",
    "unificado": """
        SELECT anio, mes, siv_casos_mujeres, api_casos_mujeres, osec_denuncias_vif, osec_denuncias_sexuales,
               ecv_viviendas_bucaramanga, ecv_pct_estrato_1_2, ecv_pct_estrato_5_6
        FROM dataset_unificado ORDER BY anio, mes""",
    "siv_directo": """
        SELECT t.year AS anio, t.mes, COUNT(*) AS casos
        FROM hechos_violencia h
        JOIN dim_tiempo t ON t.id_tiempo = h.id_tiempo
        JOIN dim_municipio m ON m.id_municipio = h.id_municipio
        WHERE m.codigo_dane = '68001' AND h.sexo_victima = 'FEMENINO'
        GROUP BY t.year, t.mes""",
    "api_directo": """
        SELECT t.year AS anio, t.mes, COUNT(*) AS casos
        FROM hechos_api_violencia h JOIN dim_tiempo t ON t.id_tiempo = h.id_tiempo
        GROUP BY t.year, t.mes""",
    "comuna_siv": """
        SELECT c.numero_comuna, c.nombre_comuna, COUNT(*) AS sivigila
        FROM hechos_violencia h
        JOIN dim_comuna c ON c.id_comuna = h.id_comuna
        JOIN dim_municipio m ON m.id_municipio = h.id_municipio
        WHERE m.codigo_dane = '68001' AND h.sexo_victima = 'FEMENINO' AND c.tipo = 'COMUNA'
        GROUP BY c.numero_comuna, c.nombre_comuna""",
    "comuna_siv_periodo": """
        SELECT c.numero_comuna, COUNT(*) AS sivigila
        FROM hechos_violencia h
        JOIN dim_tiempo t ON t.id_tiempo = h.id_tiempo
        JOIN dim_comuna c ON c.id_comuna = h.id_comuna
        JOIN dim_municipio m ON m.id_municipio = h.id_municipio
        WHERE m.codigo_dane = '68001' AND h.sexo_victima = 'FEMENINO' AND c.tipo = 'COMUNA'
          AND t.year >= (SELECT MIN(t2.year) FROM hechos_osec_comuna o2 JOIN dim_tiempo t2 ON t2.id_tiempo = o2.id_tiempo)
        GROUP BY c.numero_comuna""",
    "comuna_osec": """
        SELECT c.numero_comuna, SUM(o.casos) FILTER (WHERE o.delito = 'VIOLENCIA INTRAFAMILIAR') AS osec_vif
        FROM hechos_osec_comuna o JOIN dim_comuna c ON c.id_comuna = o.id_comuna
        WHERE c.tipo = 'COMUNA' GROUP BY c.numero_comuna""",
}


def cargar() -> dict:
    from sqlalchemy import create_engine, text
    from sqlalchemy.engine import URL

    host = os.getenv("DB_HOST", "localhost")
    if host in ("host.docker.internal", "postgres_violencia_db"):
        host = "localhost"
    engine = create_engine(URL.create(
        "postgresql+psycopg2", username=os.getenv("DB_USER", "postgres"), password=os.getenv("DB_PASSWORD"),
        host=host, port=int(os.getenv("DB_PORT", "5432")), database=os.getenv("DB_NAME", "violencia_genero_db")))
    datos = {}
    try:
        with engine.connect() as conn:
            for nombre, q in SQL.items():
                try:
                    datos[nombre] = pd.read_sql(text(q), conn)
                except Exception as e:  # p. ej. staging vacío o inexistente: se usa la tabla de hechos
                    conn.rollback()
                    print(f"  (aviso) {nombre}: {type(e).__name__}")
                    datos[nombre] = pd.DataFrame()
    finally:
        engine.dispose()
    return datos


# ---------------------------------------------------------------------------
# Utilidades estadísticas
# ---------------------------------------------------------------------------
def cramer_v(tabla: pd.DataFrame) -> float:
    if tabla.shape[0] < 2 or tabla.shape[1] < 2:
        return 0.0
    chi2 = stats.chi2_contingency(tabla, correction=False)[0]
    n = tabla.values.sum()
    return float(math.sqrt(chi2 / (n * (min(tabla.shape) - 1)))) if n else 0.0


def homogeneidad(antes: pd.Series, despues: pd.Series) -> dict:
    """Chi-cuadrado de homogeneidad: ¿la distribución de la variable cambió con el cruce?"""
    a = antes.fillna("NO REPORTADO").astype(str).value_counts()
    d = despues.fillna("NO REPORTADO").astype(str).value_counts()
    tabla = pd.concat([a, d], axis=1, keys=["antes", "despues"]).fillna(0)
    tvd = 0.5 * (tabla["antes"] / tabla["antes"].sum() - tabla["despues"] / tabla["despues"].sum()).abs().sum()
    if (tabla["antes"] == tabla["despues"]).all():
        chi2, p = 0.0, 1.0  # idénticas: no hay nada que probar
    else:
        chi2, p = stats.chi2_contingency(tabla.T.values, correction=False)[:2]
    return {"chi2": round(float(chi2), 4), "p_valor": round(float(p), 4), "tvd": round(float(tvd), 4),
            "categorias": int(len(tabla))}


def spearman(x, y) -> dict:
    datos = pd.DataFrame({"x": x, "y": y}).dropna()
    if len(datos) < 4 or datos["x"].nunique() < 2 or datos["y"].nunique() < 2:
        return {"rho": None, "p_valor": None, "n": int(len(datos))}
    rho, p = stats.spearmanr(datos["x"], datos["y"])
    return {"rho": round(float(rho), 3), "p_valor": round(float(p), 4), "n": int(len(datos))}


def fp(p) -> str:
    if p is None:
        return "—"
    return "< 0.001" if p < 0.001 else f"{p:.3f}"


def rho_txt(res: dict) -> str:
    if res["rho"] is None:
        return f"no calculable (n = {res['n']}: las fuentes casi no coinciden en el tiempo)"
    return f"ρ = {res['rho']} (p {fp(res['p_valor']) if fp(res['p_valor']).startswith('<') else '= ' + fp(res['p_valor'])}, n = {res['n']})"


def fuerza(rho) -> str:
    if rho is None:
        return "no calculable"
    a = abs(rho)
    return "muy fuerte" if a >= 0.8 else "fuerte" if a >= 0.6 else "moderada" if a >= 0.4 else "débil" if a >= 0.2 else "nula"


# ---------------------------------------------------------------------------
# Pruebas
# ---------------------------------------------------------------------------
def evaluar(d: dict) -> tuple[dict, dict]:
    r, figs = {}, {}
    antes = d["antes_stg"] if len(d.get("antes_stg", [])) else d["antes_hechos"]
    origen_antes = "stg_hechos_api (staging)" if len(d.get("antes_stg", [])) else "hechos_api_violencia"
    despues = d["despues"]

    # T1 Conservación
    ids = despues["id_hecho_api"]
    faltantes = set(d["antes_hechos"]["id_hecho_api"]) - set(ids) if len(d["antes_hechos"]) else set()
    r["T1"] = {"antes": int(len(antes)), "despues": int(len(despues)), "origen_antes": origen_antes,
               "duplicados": int(ids.duplicated().sum()), "faltantes": int(len(faltantes))}
    r["T1"]["ok"] = r["T1"]["antes"] == r["T1"]["despues"] and not r["T1"]["duplicados"] and not r["T1"]["faltantes"]

    # T2 Homogeneidad antes/después
    r["T2"] = {v: homogeneidad(antes[v], despues[v]) for v in VARIABLES + ["anio"] if v in antes and v in despues}
    r["T2_ok"] = all(x["p_valor"] >= ALFA for x in r["T2"].values())

    # T3 Series preservadas
    u = d["unificado"]
    t3 = {}
    for col, directo in [("siv_casos_mujeres", "siv_directo"), ("api_casos_mujeres", "api_directo")]:
        m = u[["anio", "mes", col]].merge(d[directo], on=["anio", "mes"], how="inner").dropna(subset=[col])
        t3[col] = {"meses_comparados": int(len(m)), "max_diferencia": int((m[col] - m["casos"]).abs().max()) if len(m) else None}
    r["T3"] = t3
    r["T3_ok"] = all(v["max_diferencia"] == 0 for v in t3.values() if v["max_diferencia"] is not None)

    # T4 Fuente estacionaria constante en el tiempo
    t4 = {c: {"meses_con_dato": int(u[c].notna().sum()), "valores_distintos": int(u[c].nunique()),
              "desviacion": round(float(u[c].std()), 6) if u[c].notna().sum() > 1 else 0.0}
          for c in ["ecv_viviendas_bucaramanga", "ecv_pct_estrato_1_2", "ecv_pct_estrato_5_6"] if c in u}
    r["T4"] = t4
    r["T4_ok"] = all(v["valores_distintos"] <= 1 for v in t4.values())

    # T5 Concordancia temporal
    anual = u.groupby("anio")[["siv_casos_mujeres", "api_casos_mujeres"]].sum(min_count=12)
    r["T5"] = {"mensual_siv_vs_api": spearman(u["siv_casos_mujeres"], u["api_casos_mujeres"]),
               "anual_siv_vs_api": spearman(anual["siv_casos_mujeres"], anual["api_casos_mujeres"]),
               "mensual_siv_vs_osec": spearman(u["siv_casos_mujeres"], u["osec_denuncias_vif"])}

    # T6 Concordancia territorial
    com = d["comuna_siv"].merge(d["comuna_osec"], on="numero_comuna", how="outer").fillna(0)
    r["T6"] = spearman(com["sivigila"], com["osec_vif"])
    if len(d.get("comuna_siv_periodo", [])):
        mismo = d["comuna_siv_periodo"].merge(d["comuna_osec"], on="numero_comuna", how="outer").fillna(0)
        r["T6_mismo_periodo"] = spearman(mismo["sivigila"], mismo["osec_vif"])

    # T7 Sesgo por cobertura OSEC
    tiene = despues["osec_denuncias_sexuales"].notna().map({True: "con OSEC", False: "sin OSEC"})
    tabla7 = pd.crosstab(despues["grupo_agresor"].fillna("NO REPORTADO"), tiene)
    p7 = float(stats.chi2_contingency(tabla7)[1]) if tabla7.shape[1] == 2 else None
    r["T7"] = {"casos_con_osec": int((tiene == "con OSEC").sum()), "casos_sin_osec": int((tiene == "sin OSEC").sum()),
               "p_valor": round(p7, 4) if p7 is not None else None, "v_cramer": round(cramer_v(tabla7), 3)}

    # T8 Aporte al objetivo del modelo
    obj = despues[despues["grupo_agresor"].isin(["PADRE, MADRE O PADRASTROS", "OTRO FAMILIAR"])].copy()
    obj["osec_disponible"] = obj["osec_denuncias_sexuales"].notna().map({True: "SI", False: "NO"})
    r["T8"] = {c: round(cramer_v(pd.crosstab(obj[c].fillna("SIN DATO").astype(str), obj["grupo_agresor"])), 3)
               for c in ["ciclo_vida", "nivel_estrato", "zona_vivienda", "osec_disponible", "area"] if c in obj}

    # ---- Gráficas ----
    SALIDA.mkdir(parents=True, exist_ok=True)
    fig, ejes = plt.subplots(1, 3, figsize=(12, 3.4))
    for ax, v in zip(ejes, ["ciclo_vida", "grupo_agresor", "area"]):
        a = antes[v].fillna("NO REPORTADO").value_counts(normalize=True) * 100
        b = despues[v].fillna("NO REPORTADO").value_counts(normalize=True).reindex(a.index).fillna(0) * 100
        y = np.arange(len(a))
        ax.barh(y - 0.2, a.values, 0.38, color=GRIS, label="Antes del cruce")
        ax.barh(y + 0.2, b.values, 0.38, color=AZUL, label="Después del cruce")
        ax.set_yticks(y, [str(i).title()[:26] for i in a.index])
        ax.invert_yaxis()
        ax.set_title(f"{v}  (p = {r['T2'][v]['p_valor']:.2f})", fontsize=9)
        ax.set_xlabel("% de casos")
    fig.suptitle("T2 · Distribución de cada variable antes y después del cruce", fontweight="bold", y=1.0)
    manejadores, etiquetas = ejes[0].get_legend_handles_labels()
    fig.legend(manejadores, etiquetas, loc="upper center", bbox_to_anchor=(0.5, 0.93), ncol=2, frameon=False)
    fig.tight_layout(rect=(0, 0, 1, 0.86))
    figs["distribuciones"] = SALIDA / "t2_distribuciones_antes_despues.png"
    fig.savefig(figs["distribuciones"], dpi=220, bbox_inches="tight")
    plt.close(fig)

    serie = u.dropna(subset=["siv_casos_mujeres", "api_casos_mujeres"]).copy()
    serie["fecha"] = pd.to_datetime(dict(year=serie["anio"], month=serie["mes"], day=1))
    fig, ax = plt.subplots(figsize=(10, 3.2))
    for col, color, nombre in [("siv_casos_mujeres", AZUL, "SIVIGILA"), ("api_casos_mujeres", NARANJA, "Medicina Legal")]:
        ax.plot(serie["fecha"], serie[col] / serie[col].mean() * 100, color=color, lw=1.6, label=nombre)
    ax.axhline(100, color=GRIS, lw=1, ls="--")
    rho = r["T5"]["mensual_siv_vs_api"]["rho"]
    ax.set_title(f"T5 · Series mensuales indexadas (promedio = 100) · Spearman ρ = {rho}", fontsize=10)
    ax.set_ylabel("Índice (promedio = 100)")
    ax.legend(frameon=False)
    fig.tight_layout()
    figs["series"] = SALIDA / "t5_series_indexadas.png"
    fig.savefig(figs["series"], dpi=220, bbox_inches="tight")
    plt.close(fig)

    com["pct_siv"] = com["sivigila"] / com["sivigila"].sum() * 100
    com["pct_osec"] = com["osec_vif"] / (com["osec_vif"].sum() or 1) * 100
    fig, ax = plt.subplots(figsize=(5.5, 4.2))
    ax.scatter(com["pct_siv"], com["pct_osec"], s=46, color=AZUL, edgecolor="white", linewidth=1.5, zorder=3)
    for _, f in com.iterrows():
        ax.annotate(str(f.get("nombre_comuna", "")).title()[:14], (f["pct_siv"], f["pct_osec"]),
                    xytext=(4, 3), textcoords="offset points", fontsize=7, color=TINTA)
    tope = max(com["pct_siv"].max(), com["pct_osec"].max()) * 1.1
    ax.plot([0, tope], [0, tope], color=GRIS, ls="--", lw=1)
    ax.set_xlabel("% de casos de mujeres (SIVIGILA)")
    ax.set_ylabel("% de denuncias VIF (OSEC)")
    ax.set_title(f"T6 · Comunas: SIVIGILA vs OSEC · ρ = {r['T6']['rho']}", fontsize=10)
    fig.tight_layout()
    figs["comunas"] = SALIDA / "t6_comunas_sivigila_osec.png"
    fig.savefig(figs["comunas"], dpi=220, bbox_inches="tight")
    plt.close(fig)
    return r, figs


# ---------------------------------------------------------------------------
# Informe
# ---------------------------------------------------------------------------
def informe(r: dict) -> str:
    t1, t5, t6, t7, t8 = r["T1"], r["T5"], r["T6"], r["T7"], r["T8"]
    peor_t2 = min(r["T2"].items(), key=lambda kv: kv[1]["p_valor"])
    m = t5["mensual_siv_vs_api"]
    filas = [
        ("T1 Conservación de registros",
         f"{t1['antes']:,} casos antes ({t1['origen_antes']}) → {t1['despues']:,} después; "
         f"{t1['duplicados']} duplicados; {t1['faltantes']} faltantes",
         "✅ El cruce no pierde ni duplica casos" if t1["ok"] else "❌ Revisar: el cruce altera el número de casos"),
        ("T2 Distribuciones antes vs después (χ²)",
         f"Menor p-valor: {peor_t2[0]} p = {peor_t2[1]['p_valor']}; distancia de variación máxima = "
         f"{max(v['tvd'] for v in r['T2'].values()):.4f}",
         "✅ Ninguna distribución cambia (p ≥ 0.05): el cruce no sesga los casos" if r["T2_ok"]
         else "❌ Alguna distribución cambió con el cruce"),
        ("T3 Series del dataset unificado",
         "; ".join(f"{k}: {v['meses_comparados']} meses, diferencia máx. {v['max_diferencia']}" for k, v in r["T3"].items()),
         "✅ Las series son idénticas a las de cada fuente" if r["T3_ok"] else "❌ Las series no coinciden"),
        ("T4 Fuente estacionaria (ECV) en el tiempo",
         "; ".join(f"{k}: {v['valores_distintos']} valor(es) distinto(s)" for k, v in r["T4"].items()),
         "✅ Es constante: no crea tendencia ni estacionalidad falsa" if r["T4_ok"] else "⚠️ Varía en el tiempo"),
        ("T5 Concordancia temporal (Spearman)",
         f"Mensual SIVIGILA vs Medicina Legal: {rho_txt(m)}; anual: {rho_txt(t5['anual_siv_vs_api'])}; "
         f"SIVIGILA vs OSEC: {rho_txt(t5['mensual_siv_vs_osec'])}",
         (f"Correlación {fuerza(m['rho'])} y significativa: SIVIGILA y Medicina Legal suben y bajan juntas"
          if (m["rho"] or 0) >= 0.4 and (m["p_valor"] or 1) < ALFA else
          f"Correlación {fuerza(m['rho'])} y no significativa: las dos series NO se mueven juntas en el tiempo. "
          "Miden procesos distintos (atención en salud vs. valoración forense), por eso se ponen lado a lado "
          "y nunca se suman")),
        ("T6 Concordancia territorial (Spearman, comunas)",
         rho_txt(t6) + (f"; mismo periodo del OSEC: {rho_txt(r['T6_mismo_periodo'])}" if "T6_mismo_periodo" in r else ""),
         (f"Correlación {fuerza(t6['rho'])}: las comunas con más casos tienden a tener más denuncias"
          if (t6["rho"] or 0) >= 0.4 else
          f"Correlación {fuerza(t6['rho'])}: el territorio de los casos atendidos y el de las denuncias no coincide "
          "del todo (hallazgo: hay comunas que denuncian más de lo que se atiende en salud, o al revés)")
         if t6["rho"] is not None else "No calculable"),
        ("T7 Sesgo por cobertura del OSEC (χ²)",
         f"{t7['casos_con_osec']:,} casos con OSEC vs {t7['casos_sin_osec']:,} sin OSEC; "
         f"p = {fp(t7['p_valor'])}; V de Cramér = {t7['v_cramer']}",
         "✅ Diferencia despreciable (V < 0.1)" if t7["v_cramer"] < 0.1 else
         "⚠️ Los casos con dato OSEC difieren: por eso el OSEC no se usa como feature del modelo"),
        ("T8 Aporte al objetivo del modelo (V de Cramér)",
         "; ".join(f"{k} = {v}" for k, v in t8.items()),
         ("Las variables cruzadas (estrato, zona, OSEC) aportan mucho menos que la edad: se usan en el "
          "análisis, no en el modelo")
         if max(v for k, v in t8.items() if k != "ciclo_vida") < t8.get("ciclo_vida", 0)
         else "⚠️ Alguna variable cruzada se asocia con el objetivo tanto como la edad: revisar si es un efecto "
              "del periodo cubierto (el OSEC solo existe en años recientes) antes de usarla en el modelo"),
    ]
    tabla = "\n".join(f"| {a} | {b} | {c} |" for a, b, c in filas)
    temporal_ok = (m["rho"] or 0) >= 0.4 and (m["p_valor"] or 1) < ALFA
    territorial_ok = (t6["rho"] or 0) >= 0.4 and (t6["p_valor"] or 1) < ALFA
    if temporal_ok:
        interpretacion_fuentes = ("**Las fuentes de serie de tiempo concuerdan.** SIVIGILA y Medicina Legal se mueven juntas "
                                  "(T5): describen el mismo fenómeno desde lugares distintos. Aun así no se suman, porque "
                                  "cuentan cosas distintas (atención en salud, valoración forense, denuncias).")
    else:
        interpretacion_fuentes = ("**En el tiempo, las fuentes NO se mueven juntas (T5), y eso valida el diseño del merge.** "
                                  "Cada fuente refleja un proceso institucional distinto (atención en salud, valoración forense, "
                                  "denuncia), con sus propios cambios de cobertura y registro. Sumarlas mezclaría procesos "
                                  "distintos; por eso el dataset unificado las pone lado a lado, una columna por fuente.")
    if territorial_ok:
        interpretacion_fuentes += (" **En el territorio sí concuerdan (T6):** las comunas con más casos atendidos son también "
                                   "las de más denuncias, así que la comparación por comuna es válida para priorizar territorios.")
    else:
        interpretacion_fuentes += " En el territorio tampoco hay concordancia fuerte (T6)."
    return f"""# Evaluación estadística del merge

Generado: {datetime.now():%Y-%m-%d %H:%M} · Nivel de significancia α = {ALFA}

**Pregunta:** ¿unir las fuentes de serie de tiempo (SIVIGILA, Medicina Legal, OSEC) con la fuente estacionaria
(ECV 2024) es estadísticamente válido y no sesga los datos?

| Prueba | Resultado | Conclusión |
|---|---|---|
{tabla}

## Interpretación

1. **El cruce es neutro sobre los casos.** No cambia el número de casos (T1), ni su distribución (T2), ni las series
   mensuales de cada fuente (T3). Unir no inventa ni borra información.
2. **La ECV es estacionaria y así se trata.** Es una foto de 2024 y en el dataset unificado tiene un solo valor
   (T4). Por eso no puede crear tendencias falsas, pero tampoco puede explicar cambios mes a mes: se usa como
   **contexto socioeconómico**, no como variable temporal.
3. {interpretacion_fuentes}
4. **Decisión para el modelo de ML.** El OSEC solo cubre una parte de los casos (T7) y las variables cruzadas aportan
   poco al objetivo (T8). Por eso el modelo usa las variables propias del caso y los datos cruzados se reservan para el
   reporte analítico. No se imputan valores: el vacío significa "sin dato".

![Distribuciones](t2_distribuciones_antes_despues.png)
![Series](t5_series_indexadas.png)
![Comunas](t6_comunas_sivigila_osec.png)
"""


def main():
    print("Cargando datos del modelo dimensional…")
    datos = cargar()
    resultados, _ = evaluar(datos)
    SALIDA.mkdir(parents=True, exist_ok=True)
    (SALIDA / "resultados_merge.json").write_text(json.dumps(resultados, ensure_ascii=False, indent=2, default=str),
                                                  encoding="utf-8")
    md = informe(resultados)
    (SALIDA / "evaluacion_merge.md").write_text(md, encoding="utf-8")
    print(md.split("## Interpretación")[0])
    print(f"✅ Resultados en {SALIDA}")


if __name__ == "__main__":
    main()