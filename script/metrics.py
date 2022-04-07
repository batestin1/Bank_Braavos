#!/usr/local/bin/python3
#coding: utf-8
# PERSONA

##################################################################################################################################################################
# Created on 21 de Julho de 2021
#
#     Projeto base: nasa mars
#     Repositorio: output/SQL
#     Author: Maycon Cypriano Batestin
#
##################################################################################################################################################################
##################################################################################################################################################################
# imports
import secrets

secret = open("../secrets/secrets.txt", 'r')
secret = list(secret)
import mysql.connector
from sqlalchemy import create_engine
from pyspark.sql import SparkSession
from pyspark import SparkContext
import findspark
findspark.add_packages('mysql:mysql-connector-java:8.0.11')
#connection
bank = mysql.connector.connect(
    host = "localhost",
    user= "root",
    password = ""
)

cursor = bank.cursor()
cursor.execute('CREATE DATABASE IF NOT EXISTS bankBraavos')
my_conn = create_engine('mysql+mysqldb://root:@localhost/bankBraavos?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC')


#################################CONFIGURE################################
spark = SparkSession.builder.master("local[1]").appName("local").getOrCreate()



print("Starting processing for metrics...")

###################extrac########################################

df = spark.read.parquet(f"{secret[0]}IronBankBraavos/datalake/gold/parquet/full/").createOrReplaceTempView("df")

df = spark.sql("SELECT * FROM df")

###############################################metrics###############################3

#by house
print("#"*100)
print("creating a metric")
print("#"*100)


spark.sql("""SELECT MAX(total_account_value) as total_account_value, house as house
FROM df
GROUP BY house
ORDER BY total_account_value DESC
LIMIT 1
""").createOrReplaceTempView('max')

spark.sql("""SELECT MIN(total_account_value) as total_account_value, house as house
FROM df
GROUP BY house
ORDER BY total_account_value DESC
LIMIT 1
""").createOrReplaceTempView('min')

spark.sql("""SELECT AVG(total_account_value) as total_account_value, house as house
FROM df
GROUP BY house
ORDER BY total_account_value DESC
LIMIT 1
""").createOrReplaceTempView('avg')

print("metrics by house")

house = spark.sql("""SELECT value,  name_of_house, total FROM
(SELECT 1 as index, "MAX" value, house as name_of_house,  total_account_value as total FROM max UNION
SELECT 2 as index, "MIN" value,  house as name_of_house, total_account_value as total FROM min UNION
SELECT 3 as index, "MEDIAN" value, house as name_of_house, round(total_account_value)  as total FROM avg
 ) ORDER BY total DESC
""")

##########################
print("metrics by gods")
gods = spark.sql("""SELECT god, total FROM( 
SELECT 1 as index, 'FATHER OF WATERSS' as god, count(god) as total FROM df where god =         "FATHER OF WATERS" UNION
SELECT 2 as index, 'BLACK GOAT' as god, count(god) as total FROM df where god =                "BLACK GOAT" UNION
SELECT 3 as index, 'MOONSINGERS'as god, count(god) as total FROM df where god =                "MOONSINGERS" UNION
SELECT 4 as index, 'MOTHER RHOYNE' as god, count(god) as total FROM df where god =             "MOTHER RHOYNE" UNION
SELECT 5 as index, 'STONE COW OF FAROSROS ' as god, count(god) as total FROM df where god =    "STONE COW OF FAROS" UNION
SELECT 6 as index, 'MOON MOTHER'  as god, count(god) as total FROM df where god =              "MOON MOTHER" UNION
SELECT 7 as index, 'FOUNTAIN OF THE DRUNKEN GOD' as god, count(god) as total FROM df where god = "FOUNTAIN OF THE DRUNKEN GOD" UNION
SELECT 8 as index, 'LION OF NIGHT' as god, count(god) as total FROM df where god =             "LION OF NIGHT"  UNION
SELECT 9 as index, 'BAKKALON' as god, count(god) as total FROM df where god =                  "BAKKALON" UNION
SELECT 10 as index,'MAIDEN-MADE-OF-LIGHT'  as god, count(god) as total FROM df where god =     "MAIDEN-MADE-OF-LIGHT" UNION
SELECT 11 as index,'CULT OF STARRY WISDOM ' as god, count(god) as total FROM df where god =    "CULT OF STARRY WISDOM" UNION
SELECT 12 as index,'SAAGAEL' as god, count(god) as total FROM df where god =                   "SAAGAEL" UNION
SELECT 13 as index,'SEMOSH AND SELLOSO ' as god, count(god) as total FROM df where god =       "SEMOSH AND SELLOSO" UNION
SELECT 14 as index,'UNINFORMED'  as god, count(god) as total FROM df where god =               "UNINFORMED" UNION
SELECT 15 as index,'YNDROS OF THE TWILIGHT 'as god, count(god) as total FROM df where god =    "YNDROS OF THE TWILIGHT" UNION
SELECT 16 as index,'HOODED WAYFARER'  as god, count(god) as total FROM df where god =          "HOODED WAYFARER" UNION
SELECT 17 as index,'SILENT GOD  ' as god, count(god) as total FROM df where god =              "SILENT GOD" UNION
SELECT 18 as index,'GREAT OTHER  ' as god, count(god) as total FROM df where god =             "GREAT OTHER" UNION
SELECT 19 as index,'GREAT SHEPHERD '  as god, count(god) as total FROM df where god =          "GREAT SHEPHERD" UNION
SELECT 20 as index,'LADY OF SPEARS '   as god, count(god) as total FROM df where god =         "LADY OF SPEARS" UNION
SELECT 21 as index,'SUN'  god, count(god) as total FROM df where god =                         "SUN" UNION
SELECT 22 as index,'MOON-PALE MAIDEN' as god, count(god) as total FROM df where god =          "MOON-PALE MAIDEN" UNION
SELECT 23 as index,'OTHER'  as god, count(god) as total FROM df where god =                    "OTHER" UNION
SELECT 24 as index,'WEEPING LADY OF LYS '  as god, count(god) as total FROM df where god =     "WEEPING LADY OF LYS" UNION
SELECT 25 as index,'HORSE GOD ' as god, count(god) as total FROM df where god =                "HORSE GOD" UNION
SELECT 26 as index,'PANTERA' as god, count(god) as total FROM df where god =                   "PANTERA" UNION
SELECT 27 as index,'MOON' as god, count(god) as total FROM df where god =                      "MOON" UNION
SELECT 28 as index,'AQUAN THE RED BULL '  as god, count(god) as total FROM df where god =      "AQUAN THE RED BULL" UNION
SELECT 29 as index,'PATTERN'  as god, count(god) as total FROM df where god =                  "PATTERN" UNION
SELECT 30 as index,'RHLLOR'  as god, count(god) as total FROM df where god =                   "R'HLLOR" UNION
SELECT 31 as index,'MERLING KING' as god, count(god) as total FROM df where god =              "MERLING KING"
order by total ASC)""")
print("metrics by faith ")
faith = spark.sql("""SELECT faith, total FROM (
    SELECT 1 as index, 'DOTHRAKI RELIGION' as faith, count(faith) as total FROM df WHERE faith = "DOTHRAKI RELIGION" UNION
    SELECT 2 as index, 'DROWNED GOD ' as faith, count(faith) as total FROM df WHERE faith = "DROWNED GOD" UNION
    SELECT 3 as index, 'FAITH OF THE SEVEN ' as faith, count(faith) as total FROM df WHERE faith = "FAITH OF THE SEVEN" UNION
    SELECT 4 as index, 'OLD GODS  ' as faith, count(faith) as total FROM df WHERE faith = "OLD GODS" UNION
    SELECT 5 as index, 'GARDENS OF GELENEI ' as faith, count(faith) as total FROM df WHERE faith = "GARDENS OF GELENEI" UNION
    SELECT 6 as index, 'UNINFORMED  ' as faith, count(faith) as total FROM df WHERE faith = "UNINFORMED" UNION
    SELECT 7 as index, 'MANY-FACED GOD ' as faith, count(faith) as total FROM df WHERE faith = "MANY-FACED GOD" UNION
    SELECT 2 as index, 'OTHER   ' as faith, count(faith) as total FROM df WHERE faith = "OTHER" 
)""")

print("#"*100)
print("loading tables in mysql")
print("#"*100)

df.write.format('jdbc').options(url='jdbc:mysql://localhost/bankBraavos?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='dbFull',user='root',password='').mode('append').save()
house.write.format('jdbc').options(url='jdbc:mysql://localhost/bankBraavos?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='houses',user='root',password='').mode('append').save()
gods.write.format('jdbc').options(url='jdbc:mysql://localhost/bankBraavos?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='gods',user='root',password='').mode('append').save()
faith.write.format('jdbc').options(url='jdbc:mysql://localhost/bankBraavos?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC',driver='com.mysql.cj.jdbc.Driver',dbtable='faith',user='root',password='').mode('append').save()
