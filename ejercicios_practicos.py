# -*- coding: utf-8 -*-
"""
Created on Sun Jan 19 14:04:56 2025

@author: silve
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, current_date
from pyspark.sql.functions import count
from pyspark.sql.functions import broadcast
from pyspark.sql.functions import unix_timestamp
import random





# Inicializar Spark session
spark = SparkSession.builder.appName("Eventos").getOrCreate()

# Leer el archivo CSV
df = spark.read.csv("C:/Users/silve/OneDrive/Documentos/Prueba_tecnica_podemos/events.csv", header=True, inferSchema=True)

# --------------1 Filtrar eventos lunares y solares de los últimos dos años
df_filtrado = df.filter(
    (df.tipo_evento.isin("lunar", "solar")) & 
    (df.year >= (year(current_date()) - 2))
)

df_filtrado.show(5)

# -------------- 2 Agrupar por tipo de evento y año, luego calcular la media de eventos
df_agrupado = df.groupby("tipo_evento", "year").agg(count("tipo_evento").alias("event_count"))

df_agrupado.show()


# -------------- 3 Implementar un join utilizando broadcast para relacionar un dataset de eventos con uno de ubicaciones pequeñas
# df_ubicaciones = spark.read.csv("path_a_ubicaciones.csv", header=True, inferSchema=True)

# # Realizar un join utilizando broadcast
# df_join = df.join(broadcast(df_ubicaciones), df.location == df_ubicaciones.location)

# df_join.show()

# -------------- 4 Escribir los datos procesados en formato Parquet con particionamiento por año y ubicación
df.write.partitionBy("year", "location").parquet("path_a_guardar_datos_parquet")
# -------------- 5 Leer un CSV con datos corruptos y manejar los errores
df_con_errores = spark.read.option("mode", "DROPMALFORMED").csv("path_a_datos_corruptos.csv", header=True, inferSchema=True)

df_con_errores.show()

#--------------6 Calcular la duración promedio de eventos por tipo

df_with_duration = df.withColumn(
    "event_duration", 
    unix_timestamp("end_timestamp") - unix_timestamp("start_timestamp")
)

# Calcular la duración promedio por tipo de evento
df_avg_duration = df_with_duration.groupby("event_type").agg(
    (sum("event_duration") / count("event_type")).alias("avg_duration")
)

df_avg_duration.show()


#--------------Diseñar un pipeline que procese datos de manera incremental
# Suponiendo que tenemos una columna 'timestamp' que indica cuándo se creó cada evento
last_processed_timestamp = "2025-01-01"  # Este valor se debe almacenar y actualizar después de cada ejecución

# Leer solo los nuevos datos basados en el timestamp
df_incremental = df.filter(df.timestamp > last_processed_timestamp)

# Procesar e insertar los datos incrementales en la base de datos o almacenamiento
df_incremental.show()