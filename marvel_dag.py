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
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator

# Obtener la conexión segura para la API de Marvel
marvel_conn = BaseHook.get_connection("marvel_api_connection")


# Define tu función execute_marvel_pipeline aquí
def execute_marvel_pipeline():
    # Parámetros de autenticación
    public_key = 'd267a7c924c86a59f19352c3223721fb'
    private_key = 'b053f57769d354aa4c80c5049a89c5e6b578cda4'
    timestamp = str(int(time.time()))
    hash_value = hashlib.md5((timestamp + private_key + public_key).encode('utf-8')).hexdigest()

    # URL de la API de Marvel
    url = 'https://gateway.marvel.com/v1/public/characters'
    params = {
        'apikey': public_key,
        'ts': timestamp,
        'hash': hash_value
    }

    # Realizar la solicitud GET a la API de Marvel
    response = requests.get(url, params=params)
    data = response.json()

    # Acceder a los resultados del JSON
    results = data['data']['results']

    # Crear el DataFrame
    df = pd.json_normalize(results)

    df.drop_duplicates(inplace=True)

    # Obtener el valor de "Series_Available"
    series_available = df['Series_Available'].iloc[0]



    # Conexión a Redshift
    redshift_conn = BaseHook.get_connection("redshift_connection")
    conn = psycopg2.connect(
        host=redshift_conn.host,
        port=redshift_conn.port,
        database=redshift_conn.schema,
        user=redshift_conn.login,
        password=redshift_conn.password
    )

    cur = conn.cursor()

    # Nombre de la tabla en Redshift
    table_name = 'vane_carop19_coderhouse.info_marvel'

    # Consulta SQL para el ingreso de los datos
    insert_query = f"INSERT INTO {table_name} (id, name, description, modified, resourceuri, urls, thumbnail_path, " \
            f"thumbnail_extension, comics_available, comics_collectionuri, comics_items, comics_returned, " \
            f"series_available, series_collectionuri, series_items, series_returned, stories_available, " \
            f"stories_collectionuri, stories_items, stories_returned, events_available, events_collectionuri, " \
            f"events_items, events_returned) VALUES ({','.join(['%s']*len(df.columns))})"

    # Insertar datos en la tabla
    json_columns = ['urls', 'comics_collectionuri', 'comics_items', 'series_collectionuri', 'series_items',
                    'stories_collectionuri', 'stories_items', 'events_collectionuri', 'events_items']
    for _, row in df.iterrows():
        # Reemplazar valores en blanco en la columna 'modified' con la fecha y hora actual
        if pd.isnull(row['modified']):
            row['modified'] = datetime.now()

        # Convertir la columna 'modified' a formato de fecha y hora compatible con Redshift
        row['modified'] = row['modified'].strftime('%Y-%m-%d %H:%M:%S')

        # Crear una nueva lista para almacenar los valores después de convertir a JSON las columnas de dict
        row_data = [convert_to_json(item) if col in json_columns else item for col, item in row.items()]

        try:
            cur.execute(insert_query, row_data)
        except psycopg2.errors.UniqueViolation:
            # Si ocurre una violación de clave única, se omite esta fila y se continúa
            conn.rollback()

            return series_available  # Retorna el valor de "Series_Available"

    # Guardar los cambios y cerrar la conexión
    conn.commit()
    cur.close()
    conn.close()


# Definir los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
}

# Crear el DAG
dag = DAG(
    'marvel_data_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Definir la tarea que ejecutará la función
execute_task = PythonOperator(
    task_id='execute_task',
    python_callable=execute_marvel_pipeline,
    dag=dag,
)

# Función para enviar un correo de alerta si el valor de "Series_Available" es menor a 70
def send_alert_email():
    # Obtener el valor de "Series_Available"
    series_available = df['Series_Available'].iloc[0]  

    if series_available < 70:
        subject = f"¡Alerta! Menos de 70 series disponibles"
        body = f"Hay menos de 70 series disponibles ({series_available} series disponibles en total)."
        
        email_task = EmailOperator(
            task_id='send_alert_email',
            to=['Vane.carop19@gmail.com'],
            subject=subject,
            html_content=body,
            dag=dag,
        )

        email_task.execute()

# Definir los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'depends_on_past': False,
    'retries': 1,
}

# Crear el DAG
dag = DAG(
    'marvel_data_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

# Definir la tarea que ejecutará la función
execute_task = PythonOperator(
    task_id='execute_task',
    python_callable=execute_marvel_pipeline,
    dag=dag,
)

# Definir la tarea que enviará la alerta por correo si "Series_Available" es menor a 70
send_alert_email_task = PythonOperator(
    task_id='send_alert_email',
    python_callable=send_alert_email,
    dag=dag,
)

# Definir las dependencias de las tareas
execute_task >> send_alert_email_task