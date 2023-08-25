import requests
import hashlib
import time
import json
import pandas as pd

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

# Realizar la solicitud GET a la API
response = requests.get(url, params=params)
data = response.json()

# Imprimir el JSON completo para analizar su estructura
print(json.dumps(data, indent=4))

# Acceder a los resultados del JSON
# Asegúrate de ajustar la ruta correcta según la estructura del JSON devuelto
results = data['data']['results']

# Crear el DataFrame
df = pd.json_normalize(results)

df.drop_duplicates(inplace=True)


# Guardar el DataFrame en un archivo CSV
df.to_csv('marvel_data.csv', index=False)


import requests
import hashlib
import time
import json
import pandas as pd
import psycopg2
from datetime import datetime

# convertir diccionarios y listas a cadenas JSON
def convert_to_json(item):
    if isinstance(item, (dict, list)):
        return json.dumps(item)
    return item

# Pasos para establecer conexión con Redshift
conn = psycopg2.connect(
    host='data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com',
    port='5439',
    database='data-engineer-database',
    user='vane_carop19_coderhouse',
    password='xOV0NeY8L0'
)

cur = conn.cursor()

# Nombre de la tabla en Redshift
table_name = 'vane_carop19_coderhouse.Informacion_Marvel'

# Consulta SQL para el ingreso de los datos
insert_query = f"INSERT INTO {table_name} (id, name, description, modified, resourceuri, urls, thumbnail_path, " \
        f"thumbnail_extension, comics_available, comics_collectionuri, comics_items, comics_returned, " \
        f"series_available, series_collectionuri, series_items, series_returned, stories_available, " \
        f"stories_collectionuri, stories_items, stories_returned, events_available, events_collectionuri, " \
        f"events_items, events_returned) VALUES ({','.join(['%s']*len(df.columns))})"

# Insertar datos en la tabla
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
        # Si ocurre una violación de clave única, se omitime esta fila y continua
        conn.rollback()

# Guardar los cambios y cerrar la conexión
conn.commit()
cur.close()
conn.close()