from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum as _sum
import matplotlib.pyplot as plt

# ==========================================
# 1. Iniciar Spark
# ==========================================
spark = SparkSession.builder.appName('AnalisisMatricula').getOrCreate()

# ==========================================
# 2. Leer el archivo desde HDFS
# ==========================================
file_path = 'hdfs://localhost:9000/Tarea3Batch/rows.csv.1'
df = spark.read.format('csv').option('header','true').option('inferSchema', 'true').load(file_path)

# ==========================================
# 2.5 LIMPIEZA DE ESQUEMA (Renombrar columnas)
# ==========================================
df = df.withColumnRenamed("Código de la Institución", "Codigo_Institucion")
df = df.withColumnRenamed("Institución de Educación Superior (IES)", "Institucion_IES")
df = df.withColumnRenamed("Código del departamento(IES)", "Codigo_Depto_IES")
df = df.withColumnRenamed("Departamento de domicilio de la IES", "Depto_Domicilio_IES")
df = df.withColumnRenamed("Código del Municipio(IES)", "Codigo_Municipio_IES")
df = df.withColumnRenamed("Municipio dedomicilio de la IES", "Municipio_Domicilio_IES")
df = df.withColumnRenamed("Código SNIES delprograma", "Codigo_SNIES_Programa")
df = df.withColumnRenamed("Programa Académico", "Programa_Academico")
df = df.withColumnRenamed("Núcleo Básico del Conocimiento (NBC)", "Nucleo_Conocimiento")
df = df.withColumnRenamed("Código del Departamento(Programa)", "Codigo_Depto_Programa")
df = df.withColumnRenamed("Departamento de oferta del programa", "Depto_Oferta_Programa")
df = df.withColumnRenamed("Código del Municipio(Programa)", "Codigo_Municipio_Programa")
df = df.withColumnRenamed("Municipio de oferta del programa", "Municipio_Oferta_Programa")
df = df.withColumnRenamed("Id_Género", "Id_Genero")
df = df.withColumnRenamed("Año", "Anio")

# ==========================================
# 3. LIMPIEZA DE DATOS
# ==========================================
df_limpio = df.dropna(subset=['Total Matriculados'])
df_limpio = df_limpio.dropDuplicates()

# ==========================================
# 4. TRANSFORMACIÓN
# ==========================================
df_transformado = df_limpio \
    .withColumn("Genero_Texto", 
        when(col("Id_Genero") == 1, "Masculino")
        .when(col("Id_Genero") == 2, "Femenino")
        .otherwise("No definido")) \
    .withColumn("Sector_Texto",
        when(col("Id_Sector") == 1, "Pública")
        .when(col("Id_Sector") == 2, "Privada")
        .otherwise("No definido")) \
    .withColumn("Nivel_Texto",
        when(col("Id_Nivel") == 1, "Pregrado")
        .when(col("Id_Nivel") == 2, "Posgrado")
        .otherwise("Otro")) \
    .withColumn("Nivel_Formacion_Texto",
        when(col("Id_Nivel_Formacion") == 1, "Especialización")
        .when(col("Id_Nivel_Formacion") == 2, "Maestría")
        .when(col("Id_Nivel_Formacion") == 3, "Doctorado")
        .otherwise("No aplica / Pregrado"))

# ==========================================
# 5. EDA (Análisis Exploratorio)
# ==========================================
print("\n=== 1. TOP 10 PROGRAMAS DE PREGRADO CON MÁS ESTUDIANTES ===")
pregrado_df = df_transformado.filter(col("Id_Nivel") == 1)
pregrado_df.groupBy("Programa_Academico").agg(_sum("Total Matriculados").alias("Total")).orderBy(col("Total").desc()).show(10, truncate=False)

print("\n=== 2. DISTRIBUCIÓN DE GÉNERO EN PREGRADO ===")
pregrado_df.groupBy("Genero_Texto").agg(_sum("Total Matriculados").alias("Total")).show()

print("\n=== 3. TOP 5 UNIVERSIDADES PÚBLICAS CON MÁS ESTUDIANTES DE PREGRADO ===")
pregrado_df.filter(col("Sector_Texto") == "Pública") \
    .groupBy("Institucion_IES") \
    .agg(_sum("Total Matriculados").alias("Total")) \
    .orderBy(col("Total").desc()).show(5, truncate=False)

print("\n=== 4. MATRÍCULAS DE POSGRADO POR TIPO DE FORMACIÓN ===")
posgrado_df = df_transformado.filter(col("Id_Nivel") == 2)
posgrado_df.groupBy("Nivel_Formacion_Texto").agg(_sum("Total Matriculados").alias("Total")).show()

# ==========================================
# 6. VISUALIZACIÓN GRÁFICA (Matrícula por Año)
# ==========================================
print("\n=== GENERANDO GRÁFICO DE MATRÍCULA POR AÑO ===")

# 1. Filtramos los años de interés (2015 a 2021)
df_anios = df_transformado.filter((col("Anio") >= 2015) & (col("Anio") <= 2021))

# 2. Agrupamos por Año y sumamos el total de matriculados
matricula_por_anio = df_anios.groupBy("Anio").agg(_sum("Total Matriculados").alias("Total"))

# 3. Convertimos el resultado a Pandas (necesario para graficar)
datos_grafico = matricula_por_anio.toPandas()

# 4. Ordenamos los años para que la línea vaya en orden
datos_grafico = datos_grafico.sort_values(by="Anio")

# 5. Creamos el gráfico
plt.figure(figsize=(10, 6))
plt.plot(datos_grafico["Anio"], datos_grafico["Total"], marker='o', color='b', linestyle='-', linewidth=2)
plt.title("Evolución de la Matrícula en Educación Superior en Colombia (2015-2021)")
plt.xlabel("Año")
plt.ylabel("Total de Estudiantes Matriculados")
plt.grid(True)

# 6. Guardamos la imagen en la carpeta del usuario
plt.savefig("grafico_matriculas.png")
print("¡Gráfico guardado exitosamente como 'grafico_matriculas.png'!")

# 7. Mostramos los datos en consola (por si acaso)
matricula_por_anio.orderBy("Anio").show()