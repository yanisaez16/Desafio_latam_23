#Author: Yanira Saez
#Date: 13/03/2023
import findspark
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
import pyspark.sql.functions
from pyspark.sql.functions import expr

findspark.init()

sparkConf = SparkConf().setAppName("My SparkQL Application")
sc = SparkContext(conf=sparkConf)
spark = SparkSession(sc)

**************** PYTHON ****************

# a) En primer instancia se deben cargar los archivos vuelos y pilotos a un dataframe
df_vuelos = spark.read.option("sep",",").option("header",True).option("inferSchema", True).csv("/Users/yanirasaez/Documents/ejercicios PYTHON 23/vuelos.csv")
df_pilotos = spark.read.option("sep",",").option("header",True).option("inferSchema", True).csv("/Users/yanirasaez/Documents/ejercicios PYTHON 23/pilotos.csv")


#Cambio los nombres para más fácil manejo de los campos
df_vuelos2 = df_vuelos.withColumnRenamed('Aerolínea','Aerolinea') \
            .withColumnRenamed( 'Codigo Piloto','Codigo_Piloto') \
            .withColumnRenamed('Minutos de retraso','Minutos_de_retraso')
                                   
df_vuelos2.printSchema()
df_vuelos2.show()

df_pilotos2 = df_pilotos.withColumnRenamed('Codigo Piloto','Codigo_Piloto')
df_pilotos2.printSchema()
df_pilotos2.show()

df_vuelos2.createOrReplaceTempView("vuelos_view")
df_pilotos2.createOrReplaceTempView("pilotos_view")


#b)	Agregar en la hoja Vuelos un campo para el nombre del piloto
#c)	Insertar el nombre del piloto

df_vuelos_pilotos = df_vuelos2.join(df_pilotos2,["Codigo_Piloto"]) 
df_vuelos_pilotos.show(truncate=False)
df_vuelos_pilotos.write.option("header",True).mode('append').csv("/Users/yanirasaez/Documents/ejercicios PYTHON 23/resultados/resultados_letraB-C.csv")


#d)	Descartar/marcar los registros donde Origen y Destino sean iguales.
df_destinos_distintos = df_vuelos_pilotos.filter(df_vuelos_pilotos.Origen != df_vuelos_pilotos.Destino)
df_destinos_distintos.show()
df_destinos_distintos.write.option("header",True).mode('append').csv("/Users/yanirasaez/Documents/ejercicios PYTHON 23/resultados/resultados_letraD.csv")


#e)	Agregar comentario en ONTIME, si el tiempo en valor absoluto es menor o igual a 30 A, si es esta entre 30 y 50 B, si es mayor que 50 C.
df_ontime = df_destinos_distintos.withColumn("OnTime", expr("CASE WHEN Minutos_de_retraso <= 30 THEN 'A' " + 
               "WHEN (30 < Minutos_de_retraso) and (Minutos_de_retraso<=50) THEN 'B' "+
               "WHEN Minutos_de_retraso >50 THEN 'C' END"))
df_ontime.show()
df_ontime.write.option("header",True).mode('append').csv("/Users/yanirasaez/Documents/ejercicios PYTHON 23/resultados/resultados_letraE.csv")

#f)	¿Quién es el piloto que tiene más vuelos A?

df_ontime.createOrReplaceTempView("df_ontime2")
df_piloto_max_A = spark.sql("SELECT Piloto, OnTime_Count, OnTime FROM (SELECT Piloto, COUNT(OnTime) as OnTime_CounT, OnTime FROM df_ontime2 where OnTime =='A' GROUP BY Piloto,OnTime) order by OnTime_Count desc limit 1")
df_piloto_max_A.show()
df_piloto_max_A.write.option("header",True).mode('append').csv("/Users/yanirasaez/Documents/ejercicios PYTHON 23/resultados/resultados_letraF.csv")

#g)	¿Qué aerolínea tiene más vuelos C?

df_aerolinea_max_C = spark.sql("SELECT Aerolinea, OnTime_Count, OnTime FROM (SELECT Aerolinea, COUNT(OnTime) as OnTime_Count, OnTime FROM df_ontime2 where OnTime =='C' GROUP BY Aerolinea,OnTime) order by OnTime_Count desc limit 1")
df_aerolinea_max_C.show()
df_aerolinea_max_C.write.option("header",True).mode('append').csv("/Users/yanirasaez/Documents/ejercicios PYTHON 23/resultados/resultados_letraG.csv")

#h)	¿Para qué aerolínea vuela Hung Cho?
df_aerolinea_hung_cho = spark.sql("SELECT Piloto,Aerolinea FROM df_ontime2 where Piloto= 'Hung Cho'")
df_aerolinea_hung_cho.show()
df_aerolinea_hung_cho.write.option("header",True).mode('append').csv("/Users/yanirasaez/Documents/ejercicios PYTHON 23/resultados/resultados_letraH.csv")

#i)	¿Cuántos vuelos A, B, C tiene Chao Ma?
df_aerolinea_chao_ma = spark.sql("SELECT Piloto,OnTime,COUNT(OnTime) as Cantidad_Vuelos FROM df_ontime2 where Piloto= 'Chao Ma' group by Piloto,OnTime")
df_aerolinea_chao_ma.show()
df_aerolinea_chao_ma.write.option("header",True).mode('append').csv("/Users/yanirasaez/Documents/ejercicios PYTHON 23/resultados/resultados_letraI.csv")



**************** SQL ****************


#¿Qué aerolínea tiene más vuelos?
# La aerolinea 7 con 457 vuelos
df_aerolinea_mas_vuelos = spark.sql("SELECT Aerolinea,Cantidad_Vuelos FROM (SELECT Aerolinea, COUNT(*) as Cantidad_Vuelos FROM vuelos_view GROUP BY Aerolinea) order by Cantidad_Vuelos desc limit 1")
df_aerolinea_mas_vuelos.show()

#¿Qué Origen se repite más?
#SAP es el origen que se repite más 
df_origen_repetido = spark.sql("SELECT Origen, Cantidad FROM (SELECT Origen, COUNT(*) as Cantidad FROM vuelos_view GROUP BY Origen) order by Cantidad desc limit 1")
df_origen_repetido.show()

#¿Desde donde vuela más la aerolínea 8?
# Es a SAP con 134 vuelos
df_origen_aero8= spark.sql("SELECT Aerolinea,Origen, Cantidad FROM (SELECT Aerolinea, Origen, COUNT(*) as Cantidad FROM vuelos_view where Aerolinea = '8' GROUP BY Aerolinea, Origen) order by Cantidad desc limit 1")
df_origen_aero8.show()

#¿Hacia dónde vuela más la aerolínea 4?
# Es a SAP con 141 vuelos
df_destino_aero4= spark.sql("SELECT Aerolinea,Destino, Cantidad FROM (SELECT Aerolinea, Destino, COUNT(*) as Cantidad FROM vuelos_view where Aerolinea = '4' GROUP BY Aerolinea, Destino) order by Cantidad desc limit 1")
df_destino_aero4.show()

#¿Qué piloto vuela más?
# John Pierson con 1028 vuelos
df_vuelos_pilotos.createOrReplaceTempView("vuelos_pilotos_view")
df_piloto_max= spark.sql("SELECT Piloto, Cantidad_Vuelos FROM (SELECT Piloto, COUNT(*) as Cantidad_Vuelos FROM vuelos_pilotos_view GROUP BY Piloto) order by Cantidad_Vuelos desc limit 1")
df_piloto_max.show()
