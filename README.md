░░░░░░░░░░░░░░░░░░░░░Prueba técnica Podemos progresar: SIMULACIÓN DE EVENTOS ESPACIALES
░░░░░░░░░░░░░░░░░░░░░
░░████░░░░░░████░░░░░AUTOR: Cuevas Papalotzin H. Cristina
░░█░░███░████░░██░░░░
░░██░░░███░░░░░░█░░░░
░░░██████████████░░░░DESCRIPCIÓN:
░░░░░░░░█░░░░░░░░░░░░Este proyecto  utiliza pyspark para simular eventos espaciales (como eventos lunares, eclipses, asteroides, etc.)
░░░░░░░░█░░░░░░░░░░░░y los organiza en un DataMart para su posterior análisis.
░░░████░██░░░░░░░░░░░Con el objetivo es crear un esquema dimensional que permita realizar consultas eficientes sobre los eventos.
░░█░░█████░░░░░░░░░░░
░░████░░░█░░░░░░░░░░░
░░░░░░░░░█░░░░░░░░░░░
░░░░░░░███░░░░░░░░░░░
░░░░░░██░██░░░░░░░░░░
░░░░░██░░░██████░░░░░
░░░███░░░░░░░░░██░░░░
░░░█████████████░░░░░

-----------------------FLUJO DE DATOS DESDE LA SIMULACIÓN HASTA EL DATAMART-----------------

-----------Simulación de datos
Se genera un conjunto de eventos espaciales utilizando la librería PySpark. Los eventos simulados incluyen evento_id, tipo_evento, timestamp, localización y descripcio.

1.- Generación del evento _id: 
Para generar el evento_id, se utiliza la función genera_uuid(), está función únicamente utiliza uuid.uuid4 para generar un código alfanumérico para identificarlo de manera única. 
2.- Generación del tipo_de evento:
El tipo_evento se genera con la función genera_evento(), para está función se definió la lista eventos, y de forma “aleatoria” se selecciona uno de los eventos dentro de la lista.
3.- Generación de timestamp:
Para generar timestamp, se utiliza la función genera_timestamp(). 
Está función genera las fechas de forma “aleatoria” a partir de una fecha de inicio y de una fecha de fin.
Las fechas se pasan como  parámetros en la función start_date="2020-01-01", end_date="2025-01-01". 
Como los parámetros se pasan como texto es necesario convertirlos en fecha. 
A partir de estas fechas se elige de forma aleatoria el número de días que se le sumará a la fecha de inicio y así se obtiene el timestamp.
4.-Generación de location
La  localización también se genera de forma aleatoria, para generarla se utiliza la función genera_location(). 
Para la latitud se genera un número aleatorio entre -90 y 90 (considerando desde el hemisferio sur al hemisferio norte), en pasos de 6, esto debido a que está distancia angular se mide en grados sexagesimales.
Para la longitud se genera un número aleatorio entre 0 y 180 en pasos de 6, esto debido a que está distancia angular se mide en grados sexagesimales.
5.-Generación de descripcion
Para descriptions se utiliza la función genera_details() y necesita el parámetro evento. A para a partir de un diccionario regresar la descripción correspondiente al evento.
6. Generación de eventos
Para generar el evento se utiliza la función genera_evento_espacial().
Esta función simplemente llama las funciones descritas anteriormente y crea un diccionario. Está función se cicla para generar N eventos, hasta el tamaño de la información se aproxime a 50GB.
Con el metodo createDataFrame de spark se crea el dataframe de eventos.
Finalmente con el método write.mod se guarda el fataframe como parquet en una ruta especificada, en este caso DATAMART.

-----------------------JUSTIFICACIÓN DEL ESQUEMA DIMENSIONAL ------------------------------
Para el equema dimensional se propone un modelo tipo estrella. A continuación la justificación de la elección: 

-Se propone modelo estrella debido a que la estructura de los datos no es compleja por lo que no es necesario usar un modelo de copo de nieve. 
-Otra razón para usar estrella es que la tabla de hechos es simple y para este ejercicio no se requieren demasiadas tablas de dimensiones.
----------¿Cómo se soporta el análisis eficiente?
-La tabla de hechos contiene las claves foráneas que referencian las dimensiones para hacer uniones eficientes con ellas.
-Este esquema es escalable para grandes volúmenes de datos. Se puede ampliar fácilmente para agregar más dimensiones,por ejemplo, dim_tipo_evento.
-Las consultas serán rápidas y eficientes debido al uso de claves foráneas, lo que optimiza las uniones entre las tablas.
-Consultas: Las consultas típicas sobre este modelo incluirían:
-Cuántos eventos ocurrieron en un año determinado.
-La cantidad de eventos por tipo (p,e., cuántos eclipses solares versus eventos lunares).
-Cuántos eventos ocurrieron en una ubicación geográfica específica.


-----------------------HERRAMIENTAS PROPUESTAS PARA IMPLEMENTACIÓN EN PRODUCCIÓN
Airflow:
Airflow es una buena opción para la orquestación de flujos de trabajo en producción ya que automatiza la ejecución de scripts, su procesamiento en Spark y la carga en el DataMart.
Con Airflow, se puede programar y monitorear las tareas del flujo de trabajo, gestionando dependencias entre los pasos de ETL.


Almacenamiento en la Nube:
Amazon S3 es una excelente opción para almacenar datos formato Parquet ya que lo admite sin necesidades de instalación de depencias, ofrece caracteristicas de optmización de almacenamiento como compresión y deduplicaión que  ayudan a reducir el tamañode los parquet.
Es altamente escalables y permiten integración sencilla con Spark y otros sistemas de análisis de datos.
