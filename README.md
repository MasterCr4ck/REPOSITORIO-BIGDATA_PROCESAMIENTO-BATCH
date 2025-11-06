Este proyecto implementa una solución de procesamiento de datos utilizando Apache Spark sobre HDFS

El dataset utilizado se encuentra almacenado en HDFS, y se ejecutan tareas de:
Carga de datos desde HDFS
Limpieza y tratamiento de valores nulos
Transformaciones para enriquecer el dataset
Análisis Exploratorio de Datos (EDA)
Ejemplo de manipulación usando RDDs
Almacenamiento del resultado final nuevamente en HDFS


Instrucciones de ejecución:

Asegúrate de tener instalado y configurado:
  Hadoop en modo pseudo-distribuido
  Spark compatible con Java 8 o Java 11
  Python 3 + PySpark
  Servicio HDFS en ejecución

El archivo CSV debe estar ubicado en:
hdfs://localhost:9000/Tarea3/rows.csv

Dentro de la carpeta donde está el archivo tarea3.py:
spark-submit tarea3.py
