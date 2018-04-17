#Importamos pyspark para realizar la experimentación
import pyspark

sc = pyspark.SparkContext.getOrCreate()

#Importamos la librería sql para tratar el csv como una tabla sql y facilitar el tratamiento de datos
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.conf import SparkConf

spark = SQLContext(sc)

#Leemos el archivo indicando que el delimitador es “;” y que el archivo desde el que lee es el que tiene los datos que queremos tratar
datos = spark.read.format("com.databricks.spark.csv").option("header", "true")\
.option("delimiter", ";").option("inferSchema","true").csv("datosOriginales.csv")

#Reemplazamos en los valores de radiación las comas por puntos para realizar las operaciones correspondientes y que no tengamos problemas de casting
datos = datos.withColumn('RADIACION', regexp_replace('RADIACION', ',', '.'))

#Seleccionamos las columnas que necesitamos para los resultados
datos = datos.select(col("IDESTACION").alias('Id_Estacion'),col("SESTACION").alias('Población'),
                       col("RADIACION").cast('float'))

#Filtramos las estaciones que no queremos procesar
datos = datos.filter((col("Id_Estacion") !=1) & (col("Id_Estacion") !=101) & (col("Id_Estacion") !=102) & 
                    (col("Id_Estacion") !=103) & (col("Id_Estacion") !=104))

#Agrupamos por estación, calculamos la media de radiación correspondiente y lo mostramos por pantalla
datos = datos.groupby(['Id_Estacion', 'Población']).agg(avg("RADIACION").alias("Radiación Media")).show()