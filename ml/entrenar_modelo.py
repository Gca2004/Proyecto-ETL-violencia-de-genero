"""
ml/entrenar_modelo.py
Entrenamiento del modelo de clasificación del vínculo del agresor.

Pregunta que responde:
    Dado un caso de lesiones no fatales por violencia intrafamiliar en Bucaramanga,
    ¿qué tan probable es que el agresor sea PADRE, MADRE O PADRASTROS (1)
    frente a OTRO FAMILIAR (0)?

Uso: priorizar casos para activar rutas de protección (p. ej. ICBF cuando la
víctima es menor de edad). El modelo NO identifica personas: entrega una
probabilidad de riesgo para orientar la atención.

Lee   : vista_ml_hechos_enriquecidos (PostgreSQL, datos ya validados por GE)
Guarda: models/model_pipeline.pkl        -> pipeline completo (preprocesamiento + modelo)
        models/metadata_modelo.json      -> features, clases, corte temporal, métricas
        models/metricas_experimentos.csv -> comparación de todos los modelos
        Docs/Graficas/ml_matriz_confusion.png

Reglas anti-sobreajuste (para que funcione con datos nuevos):
    1. División TEMPORAL: se entrena con los años antiguos y se evalúa con los
       más recientes (simula recibir casos del futuro).
    2. No se usan 'anio' ni ids como features (un año nuevo nunca se ha visto).
    3. Hiperparámetros elegidos con validación cruzada SOLO dentro del
       entrenamiento; el conjunto de prueba se usa una sola vez al final.
    4. Modelos regularizados (C pequeño, árboles poco profundos, hojas mínimas).
    5. Categorías raras (< 10 casos) se agrupan y las categorías nuevas que
       nunca se vieron no rompen el modelo (handle_unknown).
    6. Se reporta la brecha entrenamiento vs. prueba para detectar memorización.
    7. Sin class_weight: las probabilidades quedan reales (calibradas) para el dashboard.
       La clase minoritaria se protege con un UMBRAL DE ALERTA elegido con
       predicciones fuera de muestra del entrenamiento (recall >= RECALL_OBJETIVO).

Uso (desde la raíz del proyecto):
    python ml/entrenar_modelo.py
"""

import json
import os
from datetime import datetime, timezone
from pathlib import Path

import joblib
import matplotlib

matplotlib.use("Agg")  # genera imágenes sin abrir ventanas
import matplotlib.pyplot as plt
import pandas as pd
import sklearn
from dotenv import load_dotenv
from sklearn.base import clone
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.impute import SimpleImputer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    ConfusionMatrixDisplay,
    balanced_accuracy_score,
    brier_score_loss,
    precision_recall_curve,
    precision_score,
    classification_report,
    f1_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import GridSearchCV, StratifiedKFold, cross_val_predict
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder

# ---------------------------------------------------------------------------
# Configuración
# ---------------------------------------------------------------------------
RAIZ = Path(__file__).resolve().parent.parent
load_dotenv(RAIZ / ".env")

DIR_MODELOS = RAIZ / "models"
DIR_GRAFICAS = RAIZ / "Docs" / "Graficas"
SEMILLA = 42
PROPORCION_PRUEBA = 0.25  # ~25 % de los casos más recientes para la prueba
RECALL_OBJETIVO = 0.90    # la alerta debe detectar al menos el 90 % de los casos padre/madre

TARGET = "grupo_agresor"
CLASE_POSITIVA = "PADRE, MADRE O PADRASTROS"  # 1
CLASE_NEGATIVA = "OTRO FAMILIAR"              # 0

# Pistas permitidas: solo lo que se conoce SIN saber quién fue el agresor.
FEATURES_BASE = ["escenario", "ciclo_vida", "turno", "dias_incapacidad", "area", "mes"]
# Experimento: ¿cuánto cambia si agregamos 'contexto'? (se reporta, no se usa en producción)
FEATURES_CON_CONTEXTO = FEATURES_BASE + ["contexto"]

# Columnas que NUNCA pueden ser features (fuga de la respuesta)
PROHIBIDAS = {"grupo_agresor", "presunto_agresor", "sexo_agresor", "edad_agre",
              "parentezco_vict", "parentesco_vict", "anio", "id_hecho_api"}


# ---------------------------------------------------------------------------
# 1. Datos
# ---------------------------------------------------------------------------
def motor_db():
    from sqlalchemy import create_engine
    from sqlalchemy.engine import URL

    host = os.getenv("DB_HOST", "localhost")
    # Este script corre en el PC, no dentro de Docker: se usa localhost.
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
    return create_engine(url)


def cargar_datos() -> pd.DataFrame:
    from sqlalchemy import text

    columnas = ["id_hecho_api", "anio", TARGET] + FEATURES_CON_CONTEXTO
    query = f"SELECT {', '.join(columnas)} FROM vista_ml_hechos_enriquecidos"
    engine = motor_db()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
    finally:
        engine.dispose()
    return df


def preparar_datos(df: pd.DataFrame) -> pd.DataFrame:
    total = len(df)
    df = df[df[TARGET].isin([CLASE_POSITIVA, CLASE_NEGATIVA])].copy()
    print(f"Casos en la vista: {total} | usados (2 clases): {len(df)} | "
          f"excluidos (cuidador / no reportado): {total - len(df)}")
    df["y"] = (df[TARGET] == CLASE_POSITIVA).astype(int)
    return df


# ---------------------------------------------------------------------------
# 2. División temporal
# ---------------------------------------------------------------------------
def elegir_anio_corte(df: pd.DataFrame) -> int:
    """Último año de entrenamiento: deja ~PROPORCION_PRUEBA de casos recientes para prueba."""
    conteo = df.groupby("anio").size().sort_index()
    acumulado = conteo.cumsum() / conteo.sum()
    candidatos = acumulado[acumulado <= 1 - PROPORCION_PRUEBA]
    return int(candidatos.index.max()) if len(candidatos) else int(conteo.index[0])


def dividir(df: pd.DataFrame):
    anio_corte = elegir_anio_corte(df)
    train = df[df["anio"] <= anio_corte]
    test = df[df["anio"] > anio_corte]
    print(f"\nCorte temporal: entrenamiento {int(df['anio'].min())}-{anio_corte} "
          f"({len(train)} casos) | prueba {anio_corte + 1}-{int(df['anio'].max())} ({len(test)} casos)")
    print(f"% padre/madre/padrastro -> entrenamiento: {train['y'].mean():.1%} | prueba: {test['y'].mean():.1%}")
    return train, test, anio_corte


# ---------------------------------------------------------------------------
# 3. Pipeline (preprocesamiento + modelo, todo en un solo objeto)
# ---------------------------------------------------------------------------
def construir_preprocesador(features: list[str]) -> ColumnTransformer:
    categoricas = [f for f in features if f != "mes"]
    return ColumnTransformer(
        transformers=[
            ("categoricas", Pipeline([
                # vacío -> "NO REPORTADO" (no se inventa un valor real)
                ("imputar", SimpleImputer(strategy="constant", fill_value="NO REPORTADO")),
                # categorías con < 10 casos se agrupan; categorías nuevas van al mismo grupo
                ("codificar", OneHotEncoder(handle_unknown="infrequent_if_exist",
                                            min_frequency=10, sparse_output=False)),
            ]), categoricas),
            ("mes", Pipeline([
                ("imputar", SimpleImputer(strategy="most_frequent")),
                ("codificar", OneHotEncoder(handle_unknown="ignore", sparse_output=False)),
            ]), ["mes"]),
        ],
        remainder="drop",  # cualquier columna extra del mensaje se ignora
    )


MODELOS = {
    "regresion_logistica": (
        LogisticRegression(max_iter=2000),
        {"modelo__C": [0.01, 0.1, 1.0]},
    ),
    "random_forest": (
        RandomForestClassifier(n_estimators=300, random_state=SEMILLA, n_jobs=-1),
        {"modelo__max_depth": [4, 6, 8], "modelo__min_samples_leaf": [10, 25]},
    ),
}


def metricas(modelo, X, y, umbral: float = 0.5) -> dict:
    proba = modelo.predict_proba(X)[:, 1]
    pred = (proba >= umbral).astype(int)
    return {
        "roc_auc": roc_auc_score(y, proba),
        "brier": brier_score_loss(y, proba),
        "precision_padres": precision_score(y, pred, zero_division=0),
        "f1_macro": f1_score(y, pred, average="macro"),
        "balanced_accuracy": balanced_accuracy_score(y, pred),
        "recall_padres": recall_score(y, pred, zero_division=0),
    }


# ---------------------------------------------------------------------------
# 4. Experimentos
# ---------------------------------------------------------------------------
def entrenar_y_comparar(train: pd.DataFrame, test: pd.DataFrame):
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=SEMILLA)
    filas, ajustados = [], {}

    for nombre_set, features in [("sin_contexto", FEATURES_BASE),
                                 ("con_contexto", FEATURES_CON_CONTEXTO)]:
        assert not PROHIBIDAS.intersection(features), "Fuga: hay una columna prohibida en las features"
        X_tr, X_te = train[features], test[features]
        y_tr, y_te = train["y"], test["y"]

        # Línea base "tonta": siempre responde la clase más común
        base = Pipeline([("prep", construir_preprocesador(features)),
                         ("modelo", DummyClassifier(strategy="prior"))]).fit(X_tr, y_tr)
        filas.append({"features": nombre_set, "modelo": "linea_base", "cv_roc_auc": 0.5,
                      **{f"train_{k}": v for k, v in metricas(base, X_tr, y_tr).items()},
                      **{f"test_{k}": v for k, v in metricas(base, X_te, y_te).items()},
                      "hiperparametros": "{}"})

        for nombre_modelo, (estimador, grilla) in MODELOS.items():
            pipe = Pipeline([("prep", construir_preprocesador(features)), ("modelo", estimador)])
            busqueda = GridSearchCV(pipe, grilla, cv=cv, scoring="roc_auc", n_jobs=-1)
            busqueda.fit(X_tr, y_tr)  # la prueba NO participa en la elección
            mejor = busqueda.best_estimator_
            ajustados[(nombre_set, nombre_modelo)] = busqueda
            filas.append({"features": nombre_set, "modelo": nombre_modelo,
                          "cv_roc_auc": busqueda.best_score_,
                          **{f"train_{k}": v for k, v in metricas(mejor, X_tr, y_tr).items()},
                          **{f"test_{k}": v for k, v in metricas(mejor, X_te, y_te).items()},
                          "hiperparametros": json.dumps(busqueda.best_params_)})

    resultados = pd.DataFrame(filas)
    resultados["brecha_roc_auc"] = resultados["train_roc_auc"] - resultados["test_roc_auc"]
    return resultados, ajustados


# ---------------------------------------------------------------------------
# 5. Modelo final
# ---------------------------------------------------------------------------
def elegir_umbral(modelo, X_tr, y_tr) -> float:
    """Umbral más alto que mantiene recall >= RECALL_OBJETIVO, usando predicciones
    fuera de muestra del ENTRENAMIENTO (la prueba no se toca)."""
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=SEMILLA)
    proba_oof = cross_val_predict(clone(modelo), X_tr, y_tr, cv=cv, method="predict_proba")[:, 1]
    precision, recall, umbrales = precision_recall_curve(y_tr, proba_oof)
    validos = umbrales[recall[:-1] >= RECALL_OBJETIVO]
    return float(validos.max()) if len(validos) else 0.5


def guardar_matriz_confusion(y, pred, nombre_modelo: str, umbral: float):
    DIR_GRAFICAS.mkdir(parents=True, exist_ok=True)
    fig, ax = plt.subplots(figsize=(6, 5))
    ConfusionMatrixDisplay.from_predictions(
        y, pred, display_labels=["Otro familiar", "Padre/madre/padrastro"],
        cmap="Blues", colorbar=False, ax=ax)
    ax.set_title(f"Matriz de confusión en prueba (años recientes)\n"
                 f"{nombre_modelo} · alerta si probabilidad >= {umbral:.0%}")
    ax.set_xlabel("Predicción")
    ax.set_ylabel("Valor real")
    fig.tight_layout()
    ruta = DIR_GRAFICAS / "ml_matriz_confusion.png"
    fig.savefig(ruta, dpi=150)
    plt.close(fig)
    return ruta


def main():
    df = preparar_datos(cargar_datos())
    train, test, anio_corte = dividir(df)

    resultados, ajustados = entrenar_y_comparar(train, test)
    pd.set_option("display.width", 200)
    print("\n=== Comparación de modelos (prueba = años recientes; f1 y recall con umbral 0.5) ===")
    print(resultados[["features", "modelo", "cv_roc_auc", "train_roc_auc", "test_roc_auc",
                      "brecha_roc_auc", "test_f1_macro", "test_recall_padres"]].round(3).to_string(index=False))

    # Producción: solo features sin contexto; el mejor se elige por validación cruzada (NO por la prueba)
    sin_ctx = resultados[(resultados["features"] == "sin_contexto") & (resultados["modelo"] != "linea_base")]
    fila_final = sin_ctx.loc[sin_ctx["cv_roc_auc"].idxmax()]
    nombre_final = fila_final["modelo"]
    busqueda = ajustados[("sin_contexto", nombre_final)]
    print(f"\nModelo elegido (por validación cruzada): {nombre_final} {busqueda.best_params_}")

    X_tr, y_tr = train[FEATURES_BASE], train["y"]
    X_te, y_te = test[FEATURES_BASE], test["y"]
    mejor = busqueda.best_estimator_

    umbral = elegir_umbral(mejor, X_tr, y_tr)
    print(f"Umbral de alerta (recall >= {RECALL_OBJETIVO:.0%} en entrenamiento): {umbral:.3f}")

    proba_te = mejor.predict_proba(X_te)[:, 1]
    print(f"Calibración en prueba -> probabilidad promedio: {proba_te.mean():.1%} | "
          f"tasa real padre/madre: {y_te.mean():.1%}")

    metricas_umbral = {"prueba": metricas(mejor, X_te, y_te, umbral),
                       "entrenamiento": metricas(mejor, X_tr, y_tr, umbral)}
    pred_te = (proba_te >= umbral).astype(int)
    print("\nReporte en prueba con el umbral de alerta:")
    print(classification_report(y_te, pred_te,
                                target_names=["Otro familiar", "Padre/madre/padrastro"], zero_division=0))
    ruta_grafica = guardar_matriz_confusion(y_te, pred_te, nombre_final, umbral)

    # Ya evaluado: se reentrena con TODOS los datos y los mismos hiperparámetros
    # para que el modelo desplegado aprenda también de los años más recientes.
    modelo_final = clone(busqueda.best_estimator_).fit(df[FEATURES_BASE], df["y"])

    DIR_MODELOS.mkdir(parents=True, exist_ok=True)
    joblib.dump(modelo_final, DIR_MODELOS / "model_pipeline.pkl")
    resultados.to_csv(DIR_MODELOS / "metricas_experimentos.csv", index=False)

    metadata = {
        "fecha_entrenamiento": datetime.now(timezone.utc).isoformat(),
        "version_sklearn": sklearn.__version__,
        "pregunta": "Probabilidad de que el agresor sea PADRE, MADRE O PADRASTROS frente a OTRO FAMILIAR",
        "target": TARGET,
        "clases": {"0": CLASE_NEGATIVA, "1": CLASE_POSITIVA},
        "features": FEATURES_BASE,
        "modelo": nombre_final,
        "hiperparametros": busqueda.best_params_,
        "division": {"tipo": "temporal", "entrenamiento_hasta": anio_corte,
                     "prueba_desde": anio_corte + 1,
                     "casos_entrenamiento": len(train), "casos_prueba": len(test)},
        "metricas_prueba": {k: round(float(v), 4) for k, v in metricas_umbral["prueba"].items()},
        "metricas_entrenamiento": {k: round(float(v), 4) for k, v in metricas_umbral["entrenamiento"].items()},
        "modelo_desplegado": "reentrenado con todos los datos tras la evaluación",
        "umbral_alerta": round(umbral, 4),
        "recall_objetivo": RECALL_OBJETIVO,
    }
    with open(DIR_MODELOS / "metadata_modelo.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)

    print(f"\nGuardado: {DIR_MODELOS / 'model_pipeline.pkl'}")
    print(f"Guardado: {DIR_MODELOS / 'metadata_modelo.json'}")
    print(f"Guardado: {DIR_MODELOS / 'metricas_experimentos.csv'}")
    print(f"Guardado: {ruta_grafica}")


if __name__ == "__main__":
    main()