"""
etl/descargar_zonas_dane.py
Descarga los límites REALES de las zonas de Bucaramanga desde el Marco Geoestadístico
Nacional (MGN) del DANE, para dibujarlas en el mapa del dashboard.

Fuente oficial: geoportal del DANE, servicio ArcGIS REST "Serv_CapasMGN_<año>",
capa "Clase", donde CLAS_CCDGO = 1 Cabecera municipal, 2 Centro poblado, 3 Rural disperso.
Son las MISMAS categorías de área que reporta Medicina Legal, así que cada caso cae en
su zona sin inventar ubicaciones.

Genera:
    data/geo/bucaramanga_zonas_dane.geojson   (cabecera, centros poblados, rural disperso)
    data/geo/santander_limite_dane.geojson     (contorno del departamento, para la vista Santander)

Se ejecuta UNA vez (necesita internet); el dashboard lee los archivos guardados.
Uso (desde la raíz del proyecto):
    python etl/descargar_zonas_dane.py
"""

import json
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

RAIZ = Path(__file__).resolve().parent.parent
DIR_GEO = RAIZ / "data" / "geo"
BASE = "https://geoportal.dane.gov.co/mparcgis/rest/services"
ANIOS_MGN = [2025, 2024, 2023, 2022, 2021, 2018]  # se usa el más reciente que responda
DPTO, MPIO = "68", "001"                          # Santander, Bucaramanga
NOMBRE_CLASE = {"1": "CABECERA MUNICIPAL", "2": "CENTRO POBLADO", "3": "RURAL DISPERSO"}


def pedir_json(url: str, params: dict | None = None) -> dict:
    if params:
        url = f"{url}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"User-Agent": "proyecto-etl-violencia-genero/1.0"})
    with urllib.request.urlopen(req, timeout=60) as resp:
        datos = json.loads(resp.read().decode("utf-8"))
    if isinstance(datos, dict) and "error" in datos:
        raise RuntimeError(datos["error"])
    return datos


def buscar_capa(servicio: str, nombre: str) -> dict:
    """Devuelve la metadata (id y campos) de la capa con ese nombre."""
    for capa in pedir_json(f"{servicio}/layers", {"f": "json"}).get("layers", []):
        if capa.get("name", "").strip().lower() == nombre:
            return capa
    raise LookupError(f"No existe la capa '{nombre}' en {servicio}")


def campo(capa: dict, *candidatos: str) -> str | None:
    nombres = {f["name"].lower(): f["name"] for f in capa.get("fields", [])}
    return next((nombres[c] for c in candidatos if c in nombres), None)


def filtro_municipio(capa: dict) -> str:
    """El nombre del campo del municipio cambia entre versiones del MGN: se detecta."""
    concatenado = campo(capa, "mpio_cdpmp", "mpio_ccnct")
    if concatenado:
        return f"{concatenado}='{DPTO}{MPIO}'"
    dpto, mpio = campo(capa, "dpto_ccdgo"), campo(capa, "mpio_ccdgo")
    if dpto and mpio:
        return f"{dpto}='{DPTO}' AND ({mpio}='{MPIO}' OR {mpio}='{DPTO}{MPIO}')"
    raise LookupError("No se encontró el campo de código de municipio en la capa")


def consultar_geojson(servicio: str, capa: dict, where: str, simplificar: float) -> dict:
    params = {"where": where, "outFields": "*", "returnGeometry": "true", "outSR": 4326,
              "f": "geojson", "geometryPrecision": 6, "maxAllowableOffset": simplificar}
    return pedir_json(f"{servicio}/{capa['id']}/query", params)


def descargar():
    DIR_GEO.mkdir(parents=True, exist_ok=True)
    errores = []
    for anio in ANIOS_MGN:
        servicio = f"{BASE}/MGN{anio}/Serv_CapasMGN_{anio}/FeatureServer"
        try:
            capa_clase = buscar_capa(servicio, "clase")
            campo_clase = campo(capa_clase, "clas_ccdgo")
            zonas = consultar_geojson(servicio, capa_clase, filtro_municipio(capa_clase), 0.0001)
            if not zonas.get("features"):
                raise LookupError("la consulta no devolvió zonas")

            for f in zonas["features"]:
                codigo = str(f["properties"].get(campo_clase, "")).strip()
                f["properties"] = {"clase": codigo, "zona": NOMBRE_CLASE.get(codigo, "OTRA"),
                                   "municipio": "BUCARAMANGA"}
            zonas["metadata"] = {"fuente": f"DANE - MGN {anio} - capa Clase",
                                 "servicio": servicio,
                                 "descargado": datetime.now(timezone.utc).isoformat()}
            (DIR_GEO / "bucaramanga_zonas_dane.geojson").write_text(json.dumps(zonas), encoding="utf-8")
            conteo = {}
            for f in zonas["features"]:
                conteo[f["properties"]["zona"]] = conteo.get(f["properties"]["zona"], 0) + 1
            print(f"✅ Zonas de Bucaramanga (MGN {anio}): {conteo}")

            try:  # contorno de Santander (opcional)
                capa_dpto = buscar_capa(servicio, "departamento")
                c_dpto = campo(capa_dpto, "dpto_ccdgo")
                santander = consultar_geojson(servicio, capa_dpto, f"{c_dpto}='{DPTO}'", 0.002)
                (DIR_GEO / "santander_limite_dane.geojson").write_text(json.dumps(santander), encoding="utf-8")
                print("✅ Contorno de Santander descargado")
            except Exception as e:
                print(f"⚠️ No se pudo descargar el contorno de Santander ({e}); el mapa funciona sin él")
            print(f"Archivos en: {DIR_GEO}")
            return
        except Exception as e:
            errores.append(f"MGN {anio}: {e}")
            print(f"… MGN {anio} no disponible ({e}). Probando la versión anterior.")
    raise SystemExit("❌ No se pudo descargar desde ninguna versión del MGN:\n  " + "\n  ".join(errores))


if __name__ == "__main__":
    descargar()