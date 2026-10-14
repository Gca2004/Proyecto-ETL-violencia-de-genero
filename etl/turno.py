"""
etl/turno.py
Convierte la hora del hecho (en el formato que traiga cada fuente) a un TURNO.

Ventanas (hora local, 24 h):
    MAÑANA       : 07:00 <= hora < 12:00
    TARDE        : 12:00 <= hora < 22:00
    NOCHE        : 22:00 <= hora < 24:00  y  00:00 <= hora < 07:00
    NO REPORTADO : vacío, "Sin información" o valor que no se pudo interpretar

Formatos soportados:
    - 24 h              "13:00", "13:00:00"
    - 12 h con marcador "1:00 PM", "01:00:00 p. m.", "1:00 a.m."  (incluye NBSP)
    - rangos            "(18:00 a 20:59)"  -> turno que cubre la mayoría de las horas
    - etiquetas         "mañana", "tarde", "noche", "madrugada"
    - datetime.time / Timestamp

IMPORTANTE: no pases la columna `turno` por normalizar_cadena(): esa función
quita tildes y "MAÑANA" quedaría como "MANANA", rompiendo el CHECK de la BD
y las expectativas de Great Expectations. Usa siempre las constantes de aquí.
"""

import math
import re
import unicodedata
from collections import Counter

import pandas as pd

# ----------------------------------------------------------------------------
# Constantes (única fuente de verdad: úsalas también en el DDL y en GE)
# ----------------------------------------------------------------------------
MANANA = "MAÑANA"
TARDE = "TARDE"
NOCHE = "NOCHE"
NO_REPORTADO = "NO REPORTADO"
TURNOS_VALIDOS = [MANANA, TARDE, NOCHE, NO_REPORTADO]

# En el CSV de Bucaramanga "12:00:00 a. m." (= 00:00:00) es el valor más
# frecuente por mucho (850 filas, ~3x cualquier otra hora), señal típica de un
# marcador de "hora desconocida". Con True, una hora exacta 00:00:00 se trata
# como NO REPORTADO. Confírmalo en el EDA; si decides que es hora real, ponlo en False.
MEDIANOCHE_ES_PLACEHOLDER = True

# Un rango que cruza una frontera de turno se asigna al turno que cubre al
# menos esta fracción de sus horas; si ninguno llega, NO REPORTADO.
FRACCION_MINIMA_RANGO = 2 / 3

# ----------------------------------------------------------------------------
# Expresiones regulares
# ----------------------------------------------------------------------------
# "a. m." / "a.m." / "am" / "p. m." ... -> "am" / "pm"
_RE_AMPM = re.compile(r"(?<![a-z])([ap])\s*\.?\s*m\b\.?")
_RE_RANGO = re.compile(
    r"^\(?\s*(\d{1,2})[:.](\d{2})\s*(?:a|-|–)\s*(\d{1,2})[:.](\d{2})\s*\)?$"
)
_RE_HORA = re.compile(r"^(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(am|pm)?$")


# ----------------------------------------------------------------------------
# Utilidades internas
# ----------------------------------------------------------------------------
def _normalizar(valor) -> str:
    """Minúsculas, espacios colapsados (incluye NBSP) y a. m./p. m. -> am/pm."""
    s = unicodedata.normalize("NFKC", str(valor)).lower()
    s = re.sub(r"\s+", " ", s).strip()
    return _RE_AMPM.sub(lambda m: m.group(1) + "m", s)


def _sin_tildes(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()


def _es_vacio(valor) -> bool:
    if valor is None or valor is pd.NA or valor is pd.NaT:
        return True
    return isinstance(valor, float) and math.isnan(valor)


def _turno_de_hora(h: int) -> str:
    """h en 0..23."""
    if 7 <= h < 12:
        return MANANA
    if 12 <= h < 22:
        return TARDE
    return NOCHE


def _a_24h(h: int, ampm):
    """Convierte a 0..23. Sin marcador se asume 24 h. Devuelve None si es inválida."""
    if ampm is None:
        return h if 0 <= h <= 23 else None
    if not 1 <= h <= 12:
        return None
    if ampm == "am":
        return 0 if h == 12 else h        # 12 a. m. = 00:xx
    return 12 if h == 12 else h + 12      # 12 p. m. = 12:xx ; 1 p. m. = 13:xx


def _turno_de_hms(h: int, mi: int, se: int) -> str:
    if MEDIANOCHE_ES_PLACEHOLDER and h == 0 and mi == 0 and se == 0:
        return NO_REPORTADO
    return _turno_de_hora(h)


def _turno_de_rango(h1: int, h2: int) -> str:
    if not (0 <= h1 <= 23 and 0 <= h2 <= 23):
        return NO_REPORTADO
    horas = list(range(h1, h2 + 1)) if h1 <= h2 else list(range(h1, 24)) + list(range(0, h2 + 1))
    turno, n = Counter(_turno_de_hora(h) for h in horas).most_common(1)[0]
    return turno if n >= FRACCION_MINIMA_RANGO * len(horas) else NO_REPORTADO


# ----------------------------------------------------------------------------
# API pública
# ----------------------------------------------------------------------------
def calcular_turno(valor) -> str:
    """Devuelve MAÑANA | TARDE | NOCHE | NO REPORTADO."""
    if _es_vacio(valor):
        return NO_REPORTADO

    # datetime.time, datetime.datetime, pd.Timestamp
    if hasattr(valor, "hour") and hasattr(valor, "minute"):
        return _turno_de_hms(valor.hour, valor.minute, getattr(valor, "second", 0))

    s = _normalizar(valor)
    if not s:
        return NO_REPORTADO

    m = _RE_RANGO.match(s)
    if m:
        return _turno_de_rango(int(m.group(1)), int(m.group(3)))

    m = _RE_HORA.match(s)
    if m:
        h, mi, se = int(m.group(1)), int(m.group(2)), int(m.group(3) or 0)
        if mi > 59 or se > 59:
            return NO_REPORTADO
        h24 = _a_24h(h, m.group(4))
        return NO_REPORTADO if h24 is None else _turno_de_hms(h24, mi, se)

    # Etiquetas de texto (sin dígitos)
    if not re.search(r"\d", s):
        t = _sin_tildes(s)
        if "manana" in t:
            return MANANA
        if "tarde" in t:
            return TARDE
        if "noche" in t or "madrugada" in t:
            return NOCHE

    return NO_REPORTADO


def diagnosticar_formatos(serie: pd.Series) -> pd.DataFrame:
    """
    Para el EDA: clasifica cada valor distinto por formato y cuenta filas.
    Sirve para DECIDIR (no asumir) cómo se interpreta cada fuente:
      - '24h' con max_hora > 12  -> confirma que esa fuente usa 24 h.
      - '24h' con max_hora <= 12 -> AMBIGUO (podría ser 12 h sin marcador).
    """
    def tipo_y_hora(v):
        if _es_vacio(v):
            return "vacio", None
        if hasattr(v, "hour"):
            return "objeto hora", v.hour
        s = _normalizar(v)
        if not s:
            return "vacio", None
        if _RE_RANGO.match(s):
            return "rango", None
        m = _RE_HORA.match(s)
        if m:
            return ("12h (am/pm)", None) if m.group(4) else ("24h", int(m.group(1)))
        if not re.search(r"\d", s):
            return "texto", None
        return "no interpretable", None

    conteos = serie.value_counts(dropna=False)
    filas = []
    for valor, n in conteos.items():
        t, h = tipo_y_hora(valor)
        filas.append({"formato": t, "n": int(n), "hora": h, "ejemplo": valor})
    df = pd.DataFrame(filas)
    resumen = df.groupby("formato").agg(
        filas=("n", "sum"),
        valores_distintos=("n", "size"),
        max_hora=("hora", "max"),
        ejemplos=("ejemplo", lambda x: list(x.astype(str))[:3]),
    ).sort_values("filas", ascending=False)
    return resumen


def aplicar_turno(serie: pd.Series) -> pd.Series:
    """Aplica calcular_turno a una columna (cómputo por valor distinto: rápido)."""
    mapa = {v: calcular_turno(v) for v in serie.unique()}
    return serie.map(mapa)


# ----------------------------------------------------------------------------
# Autoprueba: python etl/turno.py
# ----------------------------------------------------------------------------
if __name__ == "__main__":
    import datetime as dt

    casos = {
        # rangos de la API
        "(00:00 a 02:59)": NOCHE, "(03:00 a 05:59)": NOCHE, "(06:00 a 08:59)": MANANA,
        "(09:00 a 11:59)": MANANA, "(12:00 a 14:59)": TARDE, "(15:00 a 17:59)": TARDE,
        "(18:00 a 20:59)": TARDE, "(21:00 a 23:59)": NOCHE, "(00:00 a 23:59)": NO_REPORTADO,
        # CSV en 12 h con marcador (formato real de Bucaramanga)
        "12:00:00 a. m.": NO_REPORTADO, "01:00:00 a. m.": NOCHE, "06:59:59 a. m.": NOCHE,
        "07:00:00 a. m.": MANANA, "10:00:00 a. m.": MANANA, "11:59:00 a. m.": MANANA,
        "12:00:00 p. m.": TARDE, "01:00:00 p. m.": TARDE, "05:00:00 p. m.": TARDE,
        "06:00:00 p. m.": TARDE, "09:59:00 p. m.": TARDE, "10:00:00 p. m.": NOCHE,
        "11:00:00 p. m.": NOCHE,
        # variantes de escritura
        "1:00 PM": TARDE, "1:00 AM": NOCHE, "1:00 p.m.": TARDE, "1:00\xa0p.\xa0m.": TARDE,
        "1:00pm": TARDE,
        # 24 h
        "13:00": TARDE, "13:00:00": TARDE, "21:59": TARDE, "22:00": NOCHE, "07:00": MANANA,
        "00:00:00": NO_REPORTADO,
        # etiquetas
        "mañana": MANANA, "Tarde": TARDE, "NOCHE": NOCHE, "madrugada": NOCHE,
        # vacíos y basura
        "Sin información": NO_REPORTADO, "": NO_REPORTADO, None: NO_REPORTADO,
        float("nan"): NO_REPORTADO, "25:00": NO_REPORTADO, "13:00 pm": NO_REPORTADO,
        "10:75": NO_REPORTADO, "abc": NO_REPORTADO,
        # objetos hora
        dt.time(23, 0): NOCHE, dt.time(8, 30): MANANA, dt.time(15, 0): TARDE,
    }
    fallos = [(k, esperado, calcular_turno(k)) for k, esperado in casos.items()
              if calcular_turno(k) != esperado]
    for k, e, o in fallos:
        print(f"FALLA  {k!r}: esperado {e}, obtenido {o}")
    assert not fallos, f"{len(fallos)} casos fallaron"

    # Con el placeholder desactivado, la medianoche es NOCHE
    MEDIANOCHE_ES_PLACEHOLDER = False
    assert calcular_turno("12:00:00 a. m.") == NOCHE
    MEDIANOCHE_ES_PLACEHOLDER = True

    print(f"OK: {len(casos)} casos de calcular_turno")
    s = pd.Series(["(18:00 a 20:59)", "12:00:00 a. m.", "10:00:00 a. m.", "13:00", "Sin información", None])
    print("\nDiagnóstico de formatos:\n", diagnosticar_formatos(s).to_string())