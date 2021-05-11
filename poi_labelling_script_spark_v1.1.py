# import necessary packages

from pyspark import SparkContext, SparkConf, HiveContext
import pyspark.sql.functions as F
import pyspark.sql.types as T
from math import radians, cos, sin, asin, sqrt
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType


# Load data from S3

path = 's3a://ada-dev/ishti/mypb/my_pdb.csv'

df = spark.read.format("csv").option("header","true").load(path)

# Creating Ed_Class column with null values

df_a = df.withColumn('Ed_Class', lit(None).cast(StringType()))


# Creating array for string matching for kindergarten, primary and secondary schools

k_list = ["%TADIKA%", "%TABIK%","%TASKA%", "%LITTLE%", "%KIDS%", "%KID%", "%PRESCHOOL%", "%PRA%", "%KINDERGARTEN%", "%KINDERLAND%", "%KANAK%"]

p_list = ["SK%","%PRIMARY%","%SEKOLAH%", '%KEBANGSAAN%', '%SJKC%']

s_list = ["%SMK%", "%MENENGAH%"]


# Looping over each string keyword in array for each Ed_Class type

for i in k_list:
    df_a = df_a.withColumn('Ed_Class_1',F.when(F.col("NAME").like(i), "Kindergarten").otherwise(F.col('Ed_Class')))
    df_a = df_a.drop('Ed_Class').withColumnRenamed('Ed_Class_1', 'Ed_Class')

for i in p_list:
    df_a = df_a.withColumn('Ed_Class_1',F.when(F.col("NAME").like(i) & F.col('MAIN_CLASS').like('ELEMENTARY AND SECONDARY SCHOOLS'), "Primary").otherwise(F.col('Ed_Class')))
    df_a = df_a.drop('Ed_Class').withColumnRenamed('Ed_Class_1', 'Ed_Class')

for i in s_list:
    df_a = df_a.withColumn('Ed_Class_1',F.when(F.col("NAME").like(i), "Secondary").otherwise(F.col('Ed_Class')))
    df_a = df_a.drop('Ed_Class').withColumnRenamed('Ed_Class_1', 'Ed_Class')

# Check if all labels are there

df_a.select('NAME','Ed_Class').filter(F.col('Ed_Class')=='Kindergarten').take(10)
df_a.select('NAME','Ed_Class').filter(F.col('Ed_Class')=='Primary').take(10)
df_a.select('NAME','Ed_Class').filter(F.col('Ed_Class')=='Secondary').take(10)

# Write fileoutput

output = 's3a://ada-dev/ishti/erandi_bowser_poi_MY_X/'
df_a.write.format("parquet").option("compression", "snappy").save(output)
