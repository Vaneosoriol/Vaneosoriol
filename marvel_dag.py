from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from airflow.hooks.base_hook import BaseHook
import requests
import hashlib
import time
import json
import pandas as pd
import psycopg2

# Obtener la conexión segura para la API de Marvel
marvel_conn = BaseHook.get_connection("marvel_api_connection")

# Parámetros de autenticación
public_key = marvel_conn.password  # Clave pública de la API
private_key = 'b053f57769d354aa4c80c5049a89c5e6b578cda4'  # Clave privada de la API

# Definir la función que ejecutará tu pipeline
def execute_marvel_pipeline():
    timestamp = str(int(time.time()))
    hash_value = hashlib.md5((timestamp + private_key + public_key).encode('utf-8')).hexdigest()

    # URL de la API de Marvel
    url = 'https://gateway.marvel.com/v1/public/characters'
    params = {
        'apikey': public_key,
        'ts': timestamp,
        'hash': hash_value
    }

    # Resto del código ...

# Definir los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 21),
    'depends_on_past': False,
    'retries': 1,
}

# Crear el DAG
dag = DAG(
    'marvel_data_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1),  # Ejecutar diariamente
    catchup=False,
)

# Definir la tarea que ejecutará la función
execute_task = PythonOperator(
    task_id='execute_task',
    python_callable=execute_marvel_pipeline,
    dag=dag,
)
