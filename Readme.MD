# Proyecto ETL – Violencia de Género en Colombia (ODS 5)

Proyecto académico del curso **ETL (G51)** — Ingeniería de Datos e Inteligencia Artificial.

## Descripción

Este proyecto construye un pipeline ETL completo para analizar la violencia de género contra las mujeres en Colombia, usando el dataset oficial **SIVIGILA** (Ministerio de Salud) complementado con un dataset histórico de **Bucaramanga**. El objetivo es entender los patrones sociodemográficos y familiares de víctimas y agresores para aportar evidencia a estrategias de prevención, en línea con el **ODS 5 (Igualdad de Género)**.

## Pregunta problema

- **Global:** ¿Por qué ocurre la violencia de género contra las mujeres en Colombia, cómo se manifiesta según los factores sociodemográficos y familiares de víctimas y agresores, y qué estrategias de prevención pueden formularse a partir de un análisis de los datos?
- **De esta entrega:** ¿Qué diferencias existen entre los casos de violencia de género con desenlace fatal (feminicidio) y los casos no fatales en Bucaramanga, en cuanto al tipo de violencia, la relación con el agresor, la zona de ocurrencia y la edad de la víctima?

## Dataset

- **SIVIGILA – Violencia de Género (Colombia):** fuente oficial y pública, +10,000 filas, 32 variables.
- **Histórico Bucaramanga:** 7 años de antigüedad, aporta dimensión temporal y local (Santander).

## Arquitectura

```
Dataset.csv → [Ingesta] → Raw Dataset (PostgreSQL)
                              ↓
                     [Transformación / EDA / Lasso]
                              ↓
                    Transformed Dataset (esquema estrella)
                              ↓
                          Dashboard
```

Todo el flujo está versionado con Git y GitHub.

## Modelo de datos (esquema estrella)

- **Tabla de hechos:** `hechos_violencia`
- **Dimensiones:** `dim_tiempo`, `dim_victima`, `dim_agresor`, `dim_tipo_violencia`
- **Jerarquía geográfica normalizada:** `dim_pais` → `dim_departamento` → `dim_municipio`

## Stack tecnológico

| Capa | Herramienta |
|---|---|
| Lenguaje | Python (pandas, sqlalchemy) |
| Base de datos | PostgreSQL |
| Transformación / EDA | pandas, seaborn |
| Visualización | matplotlib / seaborn / plotly |
| Notebook | Jupyter |
| Control de versiones | Git / GitHub |

## Estructura del repositorio

```
data/       # CSV originales (no se versionan si son pesados, ver .gitignore)
sql/        # Scripts de creación de tablas (raw y esquema final)
etl/        # Scripts de ingesta, transformación y carga
notebooks/  # EDA y visualizaciones
docs/       # Informe técnico, diccionario de datos, diagramas
```

## Cómo ejecutar el proyecto

1. Clonar el repositorio: `git clone <url>`
2. Crear un entorno virtual e instalar dependencias: `pip install -r requirements.txt`
3. Crear la base de datos en PostgreSQL y ejecutar en orden:
   - `sql/raw_violencia_genero.sql`
   - `sql/esquema_violencia_genero.sql`
4. Ejecutar `etl/ingesta.py` (CSV → tabla raw)
5. Ejecutar `etl/transformacion.py` y `etl/carga.py` (raw → tablas finales)
6. Abrir `notebooks/eda_violencia_genero.ipynb` para el EDA y las visualizaciones

## Equipo

| Integrante | Rol |
|---|---|
| Gustavo | Arquitectura, esquema PostgreSQL, transformación y carga, informe final, organización del repositorio |
| Leidy | Ingesta, EDA, visualizaciones / dashboards |
| Carlos | Selección de la pregunta de Bucaramanga, diccionario de datos |