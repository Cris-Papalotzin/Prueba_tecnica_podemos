# -*- coding: utf-8 -*-
"""
Created on Sun Jan 19 10:19:00 2025

@author: silve
"""
import os
os.environ['HADOOP_HOME'] = 'C:/winutils'
os.environ['PATH'] += os.pathsep + os.environ['HADOOP_HOME'] + '/bin'
os.environ['JAVA_HOME'] = 'C:/Users/silve/anaconda3/Library'
os.environ['SPARK_HOME'] = 'C:/Spark/spark-3.5.4-bin-hadoop3/spark-3.5.4-bin-hadoop3'

import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import col, udf, unix_timestamp
from pyspark.sql.types import StringType
import uuid
import random
from datetime import datetime, timedelta
import json
from pyspark import SparkConf
from pyspark.sql.functions import year, col
import pandas as pd
#######################Parte 1: Simulacion y procesamiento con PySpark
    
#Ejecutar de manera local sin necesidad de Hadoop   
spark = SparkSession.builder \
    .appName("SimulacionDeDatos50GB") \
    .master("local[1]")  \
    .config("spark.security.manager", "false") \
    .config("spark.hadoop.fs.native.lib", "false") \
    .getOrCreate()



# Función para generar UUID
def genera_uuid():
    return str(uuid.uuid4()) #Se  utiliza uuid4 para



# Función para generar tipo de evento 
def genera_evento():
    eventos = ["lunar", "solar", "asteroid", "cometa", "eclipse"]
    return random.choice(eventos)


# Función para generar timestamp aleatorio
def genera_timestamp(start_date="2020-01-01", end_date="2025-01-01"):
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d")
    end_datetime = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end_datetime - start_datetime
    random_days = random.randint(0, delta.days)
    random_seconds = random.randint(0, 86400)  # 24 horas en segundos
    random_datetime = start_datetime + timedelta(days=random_days, seconds=random_seconds)
    return random_datetime.strftime("%Y-%m-%d %H:%M:%S")


# Función para generar coordenadas geograficas
def genera_location():
    lat = round(random.uniform(-90.0, 90.0), 6)  # Latitud, -90 representan el hemisferio sur, 90 el hemisferio norte
    lon = round(random.uniform(0.0, 360.0), 6)  # Longitud
    return f"{lat},{lon}"



# Función para generar detalles del evento
def genera_details(tipo_evento):
    # Diccionario que asocia cada tipo de evento con su descripción
    event_descriptions = {
        "lunar": "Luna llena",
        "solar": "Explosión solar y emisión de radiación.",
        "asteroid": "Un asteroide pasó cerca de la tierra.",
        "cometa": "Cometa visible.",
        "eclipse": "Ocurrencia de eclipse total de sol."
    }
    # Devuelve la descripción del evento basado en el tipo
    return event_descriptions.get(tipo_evento, "Descripción no disponible")  # Valor por defecto en caso de tipo desconocido

def genera_evento_espacial():
 
    evento_id = genera_uuid()
    tipo_evento = genera_evento()  # "lunar", "solar", etc.
    timestamp = genera_timestamp()
    localizacion = genera_location()
    descripcion = genera_details(tipo_evento)  
    

    # Crear un diccionario con la fila
    evento = {
        'evento_id': evento_id,
        'tipo_evento': tipo_evento,
        'timestamp': timestamp,
        'location': localizacion,
        'descripcion': descripcion,
        
    }
    return evento

# Generar una lista de eventos simulados
eventos = [genera_evento_espacial() for _ in range(1000000)]  

# Crear un DataFrame de Spark
df_eventos = spark.createDataFrame(eventos)
df_eventos = df_eventos.withColumn("year",year(col("timestamp")))
# Ver las primeras filas
df_eventos.show(5)

# Guarda el DataFrame como un archivo CSV
df_aux=df_eventos.toPandas()
df_aux.to_csv("C:/Users/silve/OneDrive/Documentos/Prueba_tecnica_podemos/events.csv", index=False)
#df_aux.to_parquet("C:/Users/silve/OneDrive/Documentos/Prueba_tecnica_podemos/events.parquet", index=False)
# df_eventos.write\
#     .option("header","true") \
#     .csv("C:/Users/silve/OneDrive/Documentos/Prueba_tecnica_podemos/events.csv")
    



#########Tareas de procesamiento

# 1. Filtrar los eventos lunares
df_lunares = df_eventos.filter(col("tipo_evento") == "lunar")

# 2. Crear una nueva columna "year" a partir de "fecha_evento"
df_lunares_con_year = df_lunares.withColumn("year",year(col("timestamp")))

# 3. Agrupar por "year" y contar los eventos lunares por año
df_eventos_lunares_por_year = df_lunares_con_year.groupBy("year").count()

# 4. Guardar los resultados en formato Parquet en la carpeta "datamart/"
#df_eventos_lunares_por_year.write.parquet("datamart/eventos_lunares_por_year.parquet")

# Ver el resultado para confirmar
df_eventos_lunares_por_year.show()




df_eventos.write.mode("overwrite").parquet("C:/Users/silve/OneDrive/Documentos/Prueba_tecnica_podemos/simulacion_datos_50GB.parquet")


