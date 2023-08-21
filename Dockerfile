# Utilizar una imagen base de Python
FROM python:3.8

# Establecer el directorio de trabajo en el contenedor
WORKDIR /app

# Copiar los archivos necesarios al contenedor
COPY requirements.txt .
COPY marvel_data_pipeline.py .
COPY your_dag_script.py ./dags/

# Instalar las dependencias necesarias
RUN pip install -r requirements.txt

# Instalar Apache Airflow (si es necesario)
RUN pip install apache-airflow

# Comando a ejecutar al iniciar el contenedor
CMD ["python", "marvel_data_pipeline.py"]

