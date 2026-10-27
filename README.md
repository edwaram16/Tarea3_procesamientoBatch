# Tarea 3 - Procesamiento Batch con Apache Spark

**Autor:** EDUAR ALEXANDER VERA MENDOZA
**Curso:** Big Data - Código 202016911

## Descripción del Proyecto
Este repositorio contiene el código fuente desarrollado para el componente de **Procesamiento en Batch** de la Tarea 3. El objetivo principal es analizar el comportamiento histórico de la matrícula en educación superior en Colombia durante el periodo 2015-2021, utilizando el framework de Apache Spark para el procesamiento de grandes volúmenes de datos.

## Conjunto de Datos Seleccionado
Se trabajó con el conjunto de datos público **"MEN_MATRICULA_ESTADISTICA_ES"**, obtenido de la plataforma de Datos Abiertos Colombia (www.datos.gov.co). El dataset contiene más de 390,000 registros con información detallada sobre:
*   Instituciones de Educación Superior (IES).
*   Programas académicos y nivel de formación.
*   Datos demográficos (género, edad, departamento de origen).
*   Totales de matriculados por año y semestre.

## Tecnologías Utilizadas
*   **Apache Hadoop (HDFS):** Para el almacenamiento distribuido del archivo CSV.
*   **Apache Spark (PySpark):** Para la lectura, limpieza, transformación y análisis de los datos mediante DataFrames.
*   **Python 3:** Lenguaje de programación principal.
*   **Matplotlib:** Para la generación del gráfico de tendencia temporal.

## Instrucciones de Ejecución
Siga estos pasos para replicar el análisis:

1.  Asegúrese de tener el clúster de Hadoop encendido e iniciado los servicios (`start-all.sh`) y el archivo `rows.csv.1` cargado en el sistema de archivos HDFS dentro de la carpeta `/Tarea3Batch`o la que hayas creado
2.  Abra una terminal y ejecute el script de Python:
    ```bash
    python3 analisis_batch.py
```