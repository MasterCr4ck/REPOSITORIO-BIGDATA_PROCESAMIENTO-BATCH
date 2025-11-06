#Importamos librerias necesarias
from pyspark.sql import SparkSession, functions as F
# Inicializa la sesión de Spark
spark = SparkSession.builder.appName('Tarea3').getOrCreate()

#************************ Cargar el conjunto de datos seleccionado desde la fuente original ************************


# Define la ruta del archivo .csv en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/rows.csv'
# Lee el archivo .csv
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)
#imprimimos el esquema
df.printSchema()
# Muestra las primeras filas del DataFrame
df.show()

#************************ Realizar operaciones de limpieza, transformación y análisis exploratorio de datos (EDA) utilizando RDDs o DataFrames ************************

# ==================== LIMPIEZA DE DATOS ====================

# 1️ Contar filas totales
print("Total de filas:", df.count())

# 2️ Revisar valores nulos por columna
df.select([F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in df.columns]).show()

# 3️ Eliminar filas totalmente vacías
df_clean = df.na.drop("all")

# 4️ Reemplazar valores nulos en columnas numéricas con media o en cadenas con "Desconocido"
numeric_cols = [c for c, t in df_clean.dtypes if t in ("int", "double", "float", "long")]
string_cols = [c for c, t in df_clean.dtypes if t == "string"]

for col in numeric_cols:
    mean_val = df_clean.select(F.mean(col)).first()[0]
    if mean_val is not None:
        df_clean = df_clean.na.fill({col: mean_val})

for col in string_cols:
    df_clean = df_clean.na.fill({col: "Desconocido"})

print("\n Limpieza aplicada correctamente")

# ==================== TRANSFORMACIONES ====================

#  Crear una columna de longitud de texto en la primera columna de tipo string
text_col = string_cols[0] if len(string_cols) > 0 else None
if text_col:
    df_transformed = df_clean.withColumn("Texto_Longitud", F.length(F.col(text_col)))
else:
    df_transformed = df_clean

df_transformed.show(10)

# ==================== ANÁLISIS EXPLORATORIO (EDA) ====================

print("\n Resumen estadístico:")
df_transformed.describe().show()

# Distribución de valores de una columna categórica
if text_col:
    print("\n Frecuencia de valores en columna categórica:")
    df_transformed.groupBy(text_col).count().orderBy(F.col("count").desc()).show(10)

# Conteo final
print("\n Filas después de limpieza:", df_transformed.count())

# ==================== EJEMPLO CON RDDs ====================

# Convertimos el DataFrame a RDD
rdd = df_transformed.rdd

# Ejemplo: contar registros con algún valor mayor a 0 en columnas numéricas
if len(numeric_cols) > 0:
    count_positive = rdd.filter(lambda row: any([(row[c] and row[c] > 0) for c in numeric_cols])).count()
    print(f"\nRDD → Registros con valores positivos en columnas numéricas: {count_positive}")

print("\n EDA con RDDs completado")


# ==================== GUARDAR RESULTADOS PROCESADOS ====================

output_path_hdfs = "hdfs://localhost:9000/Tarea3/resultado_final"

df_transformed.write.mode("overwrite").option("header", "true").csv(output_path_hdfs)

print("\n Resultado guardado en HDFS en:", output_path_hdfs)