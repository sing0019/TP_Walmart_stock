#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 19:59:13 2021

@author: singaresekou
"""
# Importation des librairies

import pyspark
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import *

# Q1 : Instanciation

spark = SparkSession.builder\
                    .master("local[*]")\
                    .appName("df")\
                    .getOrCreate()
                    

# Q2 : Lecture du fichier walmart_stock

DF = spark.read.option("inferSchema", "true").option("header", "true").option("delimiter", ",").csv("walmart_stock.csv")
DF.show(truncate = True)

# Q3 : Noms des colonnes

DF.columns


# Q4 : Les différents schémas

DF.printSchema()


# Q5 : Création du nouveau dataframe avec la nouvelle colonne HV_ratio

DF2 = DF.withColumn("HV_ratio", F.col("High")/F.col("Volume"))

# Affichage du nouveau dataframe

DF2.show(3)


# Transformation du dataframe en table

DF2.createOrReplaceTempView("DFSQL") 

# Q6 : Le jour qui a connu le pic du prix le plus élevé 

# Avec Spark SQL

spark.sql("""select Date from DFSQL Order By High Desc limit 1""").show()

# Avec Spark DSL

DF2.select(F.col("Date"))\
   .orderBy(F.col("High").desc())\
   .show(1)
   
   
# Q7 : La moyenne de la colonne close
   
 # Avec SQL  
   
spark.sql("select round(avg(Close), 2) as moyenne from DFSQL").show() 
   
  # Avec DSL 

DF2.agg(F.round(F.mean("Close"), 2).alias("moyenne")).show()

# Q8 : Le maximum et le minimum de la colonne Volume

# Avec SQL

spark.sql("select max(cast(Volume as int)) as Maximum, min(cast(Volume as int)) as Minimum from DFSQL").show()

# Avec DSL

DF2.agg(F.max("Volume").alias("Maximum"), F.min("Volume").alias("Minimum"))\
   .show()

# Q9 : Le nombre de jours où la colonne close est inférieur à 6O.

# Avec SQL

spark.sql("""select count(Date) as Compte from DFSQL where Close < "60" """).show()

# Avec DSL

# agg toujours après le filter

DF2.select(F.col("Date"))\
   .filter(F.col("Close") < "60")\
   .agg(F.count("Date").alias("Compte"))\
   .show()

# Q10 : Le pourcentage du temps où la colonne "High" est supérieure à 80 dollars. 

# Avec SQL

spark.sql("""select round(count(*)/(select count(*) from DFSQL)*100, 2) as Pourcentage from DFSQL where High > 80""").show()

# Avec DSL

Temp  = DF2.filter(F.col("High") > 80)\
           .agg(F.count("*").alias("Comptage"))\
           .collect()[0][0]
DF2.agg(F.round((Temp/F.count("*")*100), 2).alias("Pourcentage"))\
   .show()

# Q11 : Le maximum de la colonne high par an.

# Avec SQL

spark.sql("select year(Date) as Annee, round(max(High), 2) as Maximum from DFSQL group by Annee order by Annee").show()

# Avec DSL

# Création de la colonne année dans un nouveau dataframe.

DF3 = DF2.withColumn("Annee", F.substring(F.col("Date"), 1, 4))

# Affichage du résultat

# groupBy avant agg
DF3.groupBy("Annee")\
   .agg(F.round(F.max("High"), 2).alias("Maximum"))\
   .orderBy("Annee")\
   .show()

# Q12 : La moyenne de la colonne close pour chaque mois calendaire sur l'ensemble des années. 

# Avec SQL

spark.sql("select month(Date) as Mois, round(avg(Close), 2) as Moyenne from DFSQL GROUP BY Mois ORDER BY Mois").show()

# Avec DSL

# Création de la colonne mois dans un nouveau dataframe.

DF4 = DF3.withColumn("Mois", F.substring(F.col("Date"), 6, 2))

# Affichage des moyennes.

# groupBy avant agg
DF4.groupBy("Mois")\
   .agg(F.round(F.mean("Close"), 2).alias("Moyenne"))\
   .orderBy("Mois")\
   .show()














