"""
etl/extraer_scraping.py
Fuente 4 (scraping): Observatorio de Seguridad, Convivencia y Acceso a la
Justicia (OSEC) - Alcaldía de Bucaramanga.

Nivel 1 (HTML): lee la tabla de la página del OSEC con la lista de boletines
                mensuales de delitos (nombre, periodo y enlace al PDF).
Nivel 2 (PDF) : descarga cada boletín y extrae las tablas
                "Violencia Intrafamiliar por Comuna" y "Delitos Sexuales por Comuna".

Reglas
------
* NO transforma: los conteos y la variación se guardan como TEXTO, tal cual
  vienen en el PDF (la limpieza se hace en la transformación).
* Los años NO se toman del encabezado del PDF, porque el boletín de agosto 2026
  trae un encabezado equivocado en la tabla mensual ("Casos 2024 / Casos 2025").
  Se derivan del periodo del boletín: anio_actual = periodo[:4], anio_anterior = anio_actual - 1.
* Es incremental: si se le pasa la lista de periodos ya cargados, solo baja
  los boletines nuevos (así Airflow no repite trabajo cada día).

Uso de prueba:
    python etl/extraer_scraping.py
"""

import io
import logging
import re
import time
from datetime import datetime

import pandas as pd
import pdfplumber
import requests
from bs4 import BeautifulSoup

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(name)s - %(message)s",
)
logger = logging.getLogger("ETL_Scraping_OSEC")

URL_OSEC = "https://www.bucaramanga.gov.co/datos/observatorios/osec/"
HEADERS = {"User-Agent": "Mozilla/5.0 (proyecto academico ETL - UAO)"}
FUENTE = "Scraping_OSEC_Bucaramanga"

# Título de la página del PDF -> nombre del delito que guardamos
TABLAS_OBJETIVO = {
    "violencia intrafamiliar por comuna": "VIOLENCIA INTRAFAMILIAR",
    "delitos sexuales por comuna": "DELITOS SEXUALES",
}

# 17 comunas + 3 corregimientos + "NO DISPONIBLE" = 21 filas por bloque
FILAS_ESPERADAS = 21

# Una fila de la tabla: COMUNA  casos_anterior  casos_actual  diferencia  variación %
# Ej.: "01. NORTE 172 196 24 13,95 %"  |  "CORREGIMIENTO 1 5 4 -1 -20,00 %"
RE_FILA = re.compile(
    r"(\d{2}\.\s+[A-ZÁÉÍÓÚÑ ]+?|CORREGIMIENTO\s+\d|NO DISPONIBLE)\s+"
    r"(-?\d[\d.]*)\s+(-?\d[\d.]*)\s+(-?\d[\d.]*)\s+(-?\d[\d.,]*)\s*%"
)
RE_PERIODO = re.compile(r"_(\d{6})")


# ----------------------------------------------------------------------------
# Utilidades
# ----------------------------------------------------------------------------
def _pedir(url: str, intentos: int = 3, espera: int = 5, timeout: int = 120) -> requests.Response:
    """GET con reintentos (la página de la Alcaldía a veces responde lento)."""
    ultimo_error = None
    for i in range(1, intentos + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r
        except requests.RequestException as e:
            ultimo_error = e
            logger.warning("Intento %d/%d falló para %s: %s", i, intentos, url, e)
            if i < intentos:
                time.sleep(espera)
    raise ultimo_error


# ----------------------------------------------------------------------------
# Nivel 1: lista de boletines (HTML)
# ----------------------------------------------------------------------------
def listar_boletines(url: str = URL_OSEC) -> pd.DataFrame:
    """Devuelve un DataFrame con: nombre_boletin, periodo, url_pdf."""
    logger.info("Leyendo la lista de boletines en %s", url)
    r = _pedir(url, timeout=60)
    r.encoding = "utf-8"  # evita textos como "BOLETÃN"
    soup = BeautifulSoup(r.text, "html.parser")

    filas = []
    for tabla in soup.find_all("table"):
        for tr in tabla.find_all("tr"):
            celdas = tr.find_all("td")
            enlace = tr.find("a", href=True)
            if not (celdas and enlace and enlace["href"].lower().endswith(".pdf")):
                continue
            periodo = RE_PERIODO.search(enlace["href"])
            filas.append({
                "nombre_boletin": " ".join(celdas[0].get_text().split()),  # quita espacios dobles
                "periodo": periodo.group(1) if periodo else None,
                "url_pdf": enlace["href"],
            })

    df = pd.DataFrame(filas, columns=["nombre_boletin", "periodo", "url_pdf"])
    sin_periodo = df["periodo"].isna().sum()
    if sin_periodo:
        logger.warning("%d boletines sin periodo en el enlace (se omiten).", sin_periodo)
        df = df[df["periodo"].notna()]
    df = df.drop_duplicates("periodo").sort_values("periodo").reset_index(drop=True)
    logger.info("Boletines encontrados: %d (de %s a %s)", len(df),
                df["periodo"].min() if len(df) else "-", df["periodo"].max() if len(df) else "-")
    return df


# ----------------------------------------------------------------------------
# Nivel 2: tablas por comuna dentro del PDF
# ----------------------------------------------------------------------------
def extraer_filas_de_texto(texto: str) -> list:
    """
    Lee el texto de UNA página y devuelve las filas de la tabla.
    Cada línea del PDF trae dos bloques: primero el ACUMULADO del año y
    después el MES de referencia.
    """
    filas = []
    for linea in texto.splitlines():
        coincidencias = list(RE_FILA.finditer(linea))
        if not coincidencias:
            continue
        if len(coincidencias) == 2:
            bloques = ["ACUMULADO", "MES"]
        else:
            bloques = ["DESCONOCIDO"] * len(coincidencias)
        for bloque, m in zip(bloques, coincidencias):
            comuna, anterior, actual, diferencia, variacion = m.groups()
            filas.append({
                "bloque": bloque,
                "comuna": " ".join(comuna.split()),
                "casos_anio_anterior": anterior,
                "casos_anio_actual": actual,
                "diferencia": diferencia,
                "variacion_pct": variacion,
            })
    return filas


def extraer_tablas_comuna(pdf_bytes: bytes) -> pd.DataFrame:
    """Busca en el PDF las páginas objetivo y devuelve sus filas."""
    resultado = []
    ya_extraido = set()
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for n_pagina, pagina in enumerate(pdf.pages, start=1):
            texto = pagina.extract_text() or ""
            texto_min = texto.lower()
            for titulo, delito in TABLAS_OBJETIVO.items():
                if delito in ya_extraido or titulo not in texto_min:
                    continue
                filas = extraer_filas_de_texto(texto)
                if not filas:  # p. ej. la página del mapa tiene el mismo título pero no filas
                    continue
                for f in filas:
                    f["delito"] = delito
                    f["pagina_pdf"] = n_pagina
                resultado.extend(filas)
                ya_extraido.add(delito)

    faltan = set(TABLAS_OBJETIVO.values()) - ya_extraido
    if faltan:
        logger.warning("No se encontraron en el PDF las tablas de: %s", ", ".join(faltan))
    return pd.DataFrame(resultado)


# ----------------------------------------------------------------------------
# Función principal de la fuente (la que llamará Airflow)
# ----------------------------------------------------------------------------
def extraer_scraping_osec(periodos_ya_cargados=None) -> tuple:
    """
    Devuelve (df_boletines, df_comunas).
    periodos_ya_cargados: lista/conjunto de periodos 'AAAAMM' que ya están en la BD;
    esos boletines no se vuelven a descargar.
    """
    periodos_ya_cargados = set(periodos_ya_cargados or [])
    fecha_extraccion = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    boletines = listar_boletines()
    nuevos = boletines[~boletines["periodo"].isin(periodos_ya_cargados)].copy()
    logger.info("Boletines nuevos por descargar: %d", len(nuevos))

    partes = []
    estados = []
    for _, b in nuevos.iterrows():
        try:
            pdf_bytes = _pedir(b["url_pdf"]).content
            df = extraer_tablas_comuna(pdf_bytes)
        except Exception as e:  # un boletín dañado no debe tumbar toda la extracción
            logger.error("Error con el boletín %s: %s", b["periodo"], e)
            estados.append("ERROR")
            continue

        if df.empty:
            estados.append("SIN_TABLAS")
            continue

        anio_actual = int(b["periodo"][:4])
        df["periodo"] = b["periodo"]
        df["anio_actual"] = anio_actual
        df["anio_anterior"] = anio_actual - 1
        df["url_pdf"] = b["url_pdf"]
        partes.append(df)

        # Control: 21 filas por bloque y delito
        conteo = df.groupby(["delito", "bloque"]).size()
        raros = conteo[conteo != FILAS_ESPERADAS]
        if len(raros):
            logger.warning("Boletín %s con filas inesperadas:\n%s", b["periodo"], raros.to_string())
        estados.append("OK")
        logger.info("Boletín %s: %d filas extraídas", b["periodo"], len(df))

    nuevos["estado_extraccion"] = estados
    nuevos["fecha_extraccion"] = fecha_extraccion
    nuevos["fuente"] = FUENTE

    columnas = ["periodo", "delito", "bloque", "comuna", "casos_anio_anterior",
                "casos_anio_actual", "diferencia", "variacion_pct", "anio_anterior",
                "anio_actual", "pagina_pdf", "url_pdf"]
    comunas = pd.concat(partes, ignore_index=True)[columnas] if partes else pd.DataFrame(columns=columnas)
    comunas["fecha_extraccion"] = fecha_extraccion
    comunas["fuente"] = FUENTE

    logger.info("Scraping OSEC terminado: %d boletines procesados, %d filas por comuna.",
                len(nuevos), len(comunas))
    return nuevos.reset_index(drop=True), comunas


if __name__ == "__main__":
    df_boletines, df_comunas = extraer_scraping_osec()
    print("\n=== BOLETINES ===")
    print(df_boletines[["periodo", "nombre_boletin", "estado_extraccion"]].to_string(index=False))
    print("\n=== FILAS POR COMUNA (primeras 10) ===")
    print(df_comunas.head(10).to_string(index=False))
    print("\n=== CONTROL: filas por boletín, delito y bloque (deberían ser 21) ===")
    print(df_comunas.groupby(["periodo", "delito", "bloque"]).size().unstack(fill_value=0).to_string())