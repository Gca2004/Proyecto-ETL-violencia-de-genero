# Imagen de Airflow del proyecto: la oficial + las librerías que usa nuestro código de etl/
FROM apache/airflow:3.3.2

COPY requirements-airflow.txt /requirements-airflow.txt

# Se fija apache-airflow a la misma versión de la imagen para que pip no la cambie
RUN pip install --no-cache-dir "apache-airflow==3.3.2" -r /requirements-airflow.txt