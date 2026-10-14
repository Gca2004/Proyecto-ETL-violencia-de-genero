"""
etl/homologacion.py
Reglas de homologación: llevan las categorías de cada fuente a un mismo
"idioma" para poder compararlas (SIVIGILA, API Medicina Legal y OSEC).

Las reglas salen del EDA (Docs/notebook/eda_perfilamiento.ipynb, Parte 6) y
de los valores reales de las tablas crudas. Todas las funciones reciben un
valor crudo y devuelven la categoría común; lo que no se reconoce queda como
NO REPORTADO (o 0 en el caso de la comuna), nunca se inventa una categoría.
"""

import re
import unicodedata

import pandas as pd

NO_REPORTADO = "NO REPORTADO"


# ---------------------------------------------------------------------------
# Normalización de texto
# ---------------------------------------------------------------------------
def normalizar(valor) -> str:
    """
    MAYÚSCULAS, sin tildes, espacios simples y puntuación uniforme:
    "Hermano(a)" y "Hermano (a)"  -> "HERMANO (A)"
    "Calle (Autopista,Avenida)"   -> "CALLE (AUTOPISTA, AVENIDA)"
    Vacío o nulo -> NO REPORTADO.
    """
    if valor is None or (isinstance(valor, float) and pd.isna(valor)) or valor is pd.NA:
        return NO_REPORTADO
    t = unicodedata.normalize("NFKD", str(valor).upper()).encode("ascii", "ignore").decode()
    t = re.sub(r"\s*\(\s*", " (", t)
    t = re.sub(r"\s*\)", ")", t)
    t = re.sub(r"\s*,\s*", ", ", t)
    t = " ".join(t.split()).strip()
    return t if t else NO_REPORTADO


_SIN_DATO = {"SIN INFORMACION", "NO SABE / NO INFORMA", "NO SABE/NO INFORMA", "NO APLICA",
             "SIN DATO", "NO REPORTADO", "NO DISPONIBLE", "POR DETERMINAR"}


def es_sin_dato(texto_normalizado: str) -> bool:
    return texto_normalizado in _SIN_DATO


# ---------------------------------------------------------------------------
# Sexo de la víctima  (API: Mujer/Hombre ; SIVIGILA: Femenino/Masculino)
# ---------------------------------------------------------------------------
def sexo(valor) -> str:
    t = normalizar(valor)
    if t in ("MUJER", "FEMENINO", "F"):
        return "FEMENINO"
    if t in ("HOMBRE", "MASCULINO", "M"):
        return "MASCULINO"
    return NO_REPORTADO


# ---------------------------------------------------------------------------
# Ciclo de vida  (API: "(18 a 28) Juventud" ; SIVIGILA: "Jovenes")
# ---------------------------------------------------------------------------
CICLOS_VIDA = ["PRIMERA INFANCIA", "INFANCIA", "ADOLESCENCIA", "JUVENTUD", "ADULTEZ", "PERSONA MAYOR"]


def ciclo_vida(valor) -> str:
    t = normalizar(valor)
    if "PRIMERA INFANCIA" in t:
        return "PRIMERA INFANCIA"
    if "INFANCIA" in t:
        return "INFANCIA"
    if "ADOLESCENCIA" in t:
        return "ADOLESCENCIA"
    if "JUVENTUD" in t or "JOVEN" in t:
        return "JUVENTUD"
    if "ADULTO MAYOR" in t or "PERSONA MAYOR" in t:
        return "PERSONA MAYOR"
    if "ADULTEZ" in t:
        return "ADULTEZ"
    return NO_REPORTADO


# ---------------------------------------------------------------------------
# Relación con el agresor (grupos comunes entre SIVIGILA y la API)
# ---------------------------------------------------------------------------
GRUPOS_AGRESOR = ["PAREJA O EXPAREJA", "PADRE, MADRE O PADRASTROS", "OTRO FAMILIAR",
                  "CUIDADOR U OTRO CONOCIDO", "SIN VINCULO", NO_REPORTADO]

_PAREJA = ("PAREJA", "ESPOSO", "COMPANERO", "COMANERO", "NOVIO", "AMANTE")
_PADRES = ("PADRE", "MADRE", "PADRAST", "MADRAST")
_FAMILIAR = ("HERMANO", "HIJO", "TIO", "ABUELO", "PRIMO", "CUNADO", "SUEGRO", "SOBRINO",
             "NIETO", "YERNO", "NUERA", "FAMILIAR")
_CUIDADOR = ("ENCARGADO", "PROFESOR", "CUSTODIA")


def grupo_agresor(valor) -> str:
    t = normalizar(valor)
    if es_sin_dato(t):
        return NO_REPORTADO
    palabra = t.split(" ")[0]
    if any(p in t for p in _PAREJA):                     # incluye EX- PAREJA, EX-NOVIO, EX-AMANTE...
        return "PAREJA O EXPAREJA"
    if any(palabra.startswith(p) for p in _PADRES):
        return "PADRE, MADRE O PADRASTROS"
    if any(palabra.startswith(p) for p in _FAMILIAR) or "FAMILIAR" in t:
        return "OTRO FAMILIAR"
    if any(p in t for p in _CUIDADOR) or t == "OTROS":
        return "CUIDADOR U OTRO CONOCIDO"
    if t == "NINGUNO":
        return "SIN VINCULO"
    return NO_REPORTADO


# ---------------------------------------------------------------------------
# Zona / área  (mismos valores que dim_municipio.area)
# ---------------------------------------------------------------------------
AREAS = ["CABECERA MUNICIPAL", "CENTRO POBLADO", "RURAL DISPERSO"]


def area(valor) -> str:
    t = normalizar(valor)
    if "CABECERA" in t or t == "1":
        return "CABECERA MUNICIPAL"
    if "CENTRO POBLADO" in t or t == "2":
        return "CENTRO POBLADO"
    if "RURAL" in t or "VEREDA" in t or t == "3":
        return "RURAL DISPERSO"
    return NO_REPORTADO


# ---------------------------------------------------------------------------
# Mes del hecho  ("febrero", "Febrero" -> 2)
# ---------------------------------------------------------------------------
MESES = {"ENERO": 1, "FEBRERO": 2, "MARZO": 3, "ABRIL": 4, "MAYO": 5, "JUNIO": 6, "JULIO": 7,
         "AGOSTO": 8, "SEPTIEMBRE": 9, "SETIEMBRE": 9, "OCTUBRE": 10, "NOVIEMBRE": 11, "DICIEMBRE": 12}


def mes(valor) -> int:
    """Nombre o número del mes -> 1..12 ; desconocido -> 0 (igual que dim_tiempo)."""
    t = normalizar(valor)
    if t in MESES:
        return MESES[t]
    if t.isdigit() and 1 <= int(t) <= 12:
        return int(t)
    return 0


# ---------------------------------------------------------------------------
# Días de incapacidad médico legal (la API los publica en rangos)
# ---------------------------------------------------------------------------
RANGOS_INCAPACIDAD = ["CERO DIAS", "1 A 30", "31 A 90", "MAS DE 90", NO_REPORTADO]


def dias_incapacidad(valor) -> str:
    t = normalizar(valor)
    if "MAS DE 90" in t:
        return "MAS DE 90"
    if "31 A 90" in t:
        return "31 A 90"
    if "1 A 30" in t:
        return "1 A 30"
    if "CERO" in t or "SIN DIAS" in t:
        return "CERO DIAS"
    if t.isdigit():                                   # por si algún año viene como número
        n = int(t)
        return "CERO DIAS" if n == 0 else "1 A 30" if n <= 30 else "31 A 90" if n <= 90 else "MAS DE 90"
    return NO_REPORTADO


# ---------------------------------------------------------------------------
# Comunas de Bucaramanga  (llave común SIVIGILA <-> OSEC)
#   id 1..17  -> comunas (mismo número oficial)
#   id 101..103 -> corregimientos 1, 2 y 3
#   id 0      -> SIN COMUNA (no disponible, sin información u otro municipio)
# ---------------------------------------------------------------------------
COMUNAS = {
    1: "NORTE", 2: "NORORIENTAL", 3: "SAN FRANCISCO", 4: "OCCIDENTAL", 5: "GARCIA ROVIRA",
    6: "LA CONCORDIA", 7: "CIUDADELA REAL DE MINAS", 8: "SUROCCIDENTE", 9: "LA PEDREGOSA",
    10: "PROVENZA", 11: "SUR", 12: "CABECERA DEL LLANO", 13: "ORIENTAL", 14: "MORRORICO",
    15: "CENTRO", 16: "TEJAR", 17: "MUTIS",
}
# Nombres con los que la misma comuna aparece en otras fuentes
NOMBRES_ALTERNOS = {7: ["LA CIUDADELA"], 8: ["SUR OCCIDENTE"], 16: ["LAGOS DEL CACIQUE"]}
ID_SIN_COMUNA = 0


def _por_nombre(t: str):
    candidatos = [(n, num) for num, n in COMUNAS.items()]
    candidatos += [(n, num) for num, alts in NOMBRES_ALTERNOS.items() for n in alts]
    for nombre, num in sorted(candidatos, key=lambda x: -len(x[0])):   # primero los nombres largos
        if re.search(r"\b" + re.escape(nombre) + r"\b", t):
            return num
    return None


def comuna_a_id(valor) -> int:
    """
    SIVIGILA ("08. SUR OCCIDENTE", "CORREG. 3", "SIN INFORMACION") y
    OSEC ("01. NORTE", "CORREGIMIENTO 1", "NO DISPONIBLE") -> id de dim_comuna.
    El número al inicio tiene prioridad sobre el nombre.
    """
    t = normalizar(valor)
    if es_sin_dato(t):
        return ID_SIN_COMUNA
    m = re.search(r"CORREG\w*\.?\s*(\d)", t)
    if m and 1 <= int(m.group(1)) <= 3:
        return 100 + int(m.group(1))
    m = re.match(r"^(\d{1,2})\s*\.", t)
    if m:
        n = int(m.group(1))
        return n if 1 <= n <= 17 else ID_SIN_COMUNA   # 21 a 30 son comunas de Floridablanca
    return _por_nombre(t) or ID_SIN_COMUNA


if __name__ == "__main__":   # autoprueba: python etl/homologacion.py
    casos = {
        comuna_a_id: {"08. SUR OCCIDENTE": 8, "08. SUROCCIDENTE": 8, "07. LA CIUDADELA": 7, "16. LAGOS DEL CACIQUE": 16,
                      "CORREG. 3": 103, "CORREGIMIENTO 1": 101, "24. CALDAS - REPOSO": 0, "FLORIDABLANCA": 0,
                      "SIN INFORMACION": 0, "NO DISPONIBLE": 0, None: 0, "01. NORTE": 1, "30. CORREG": 0, "11. SUR": 11},
        grupo_agresor: {"Hermano (a)": "OTRO FAMILIAR", "Hermano(a)": "OTRO FAMILIAR", "Padre": "PADRE, MADRE O PADRASTROS",
                        "Padrasto": "PADRE, MADRE O PADRASTROS", "Madrastra": "PADRE, MADRE O PADRASTROS",
                        "Ex- pareja": "PAREJA O EXPAREJA", "Ex- comañero (a) permanente": "PAREJA O EXPAREJA",
                        "Compañero (a) permanente": "PAREJA O EXPAREJA", "Novio (a)": "PAREJA O EXPAREJA",
                        "Ex-amante": "PAREJA O EXPAREJA", "Otros familiares civiles o consanguineos": "OTRO FAMILIAR",
                        "Familiar": "OTRO FAMILIAR", "Yerno": "OTRO FAMILIAR", "Ninguno": "SIN VINCULO",
                        "Otros": "CUIDADOR U OTRO CONOCIDO", "Encargado (a) del NNA y adulto mayor": "CUIDADOR U OTRO CONOCIDO",
                        "Profesor (a)": "CUIDADOR U OTRO CONOCIDO", "Sin información": NO_REPORTADO, None: NO_REPORTADO,
                        "Hijo": "OTRO FAMILIAR", "Esposo (a)": "PAREJA O EXPAREJA"},
        ciclo_vida: {"(18 a 28) Juventud": "JUVENTUD", "Jovenes": "JUVENTUD", "(Más de 60) Adulto Mayor": "PERSONA MAYOR",
                     "Persona Mayor": "PERSONA MAYOR", "(00 a 05) Primera Infancia": "PRIMERA INFANCIA",
                     "Infancia": "INFANCIA", "Sin información": NO_REPORTADO},
        mes: {"febrero": 2, "Febrero": 2, "Septiembre": 9, "x": 0, None: 0},
        area: {"Cabecera municipal": "CABECERA MUNICIPAL", "Parte rural (vereda y campo)": "RURAL DISPERSO",
               "Centro poblado(corregimiento, inspección de policía y caserío)": "CENTRO POBLADO", "Sin información": NO_REPORTADO},
        dias_incapacidad: {"1 a 30": "1 A 30", "Cero días y Sin información": "CERO DIAS", "Sin días de incapacidad": "CERO DIAS",
                           "Más de 90": "MAS DE 90", "Sin información": NO_REPORTADO},
        normalizar: {"Calle (Autopista,Avenida,Dentro de La Ciudad)": "CALLE (AUTOPISTA, AVENIDA, DENTRO DE LA CIUDAD)",
                     "Calle (autopista, avenida, dentro de la ciudad)": "CALLE (AUTOPISTA, AVENIDA, DENTRO DE LA CIUDAD)"},
    }
    fallos = [(f.__name__, k, e, f(k)) for f, cs in casos.items() for k, e in cs.items() if f(k) != e]
    for f in fallos:
        print("FALLA", f)
    assert not fallos
    print(f"OK: {sum(len(c) for c in casos.values())} casos de homologación")
