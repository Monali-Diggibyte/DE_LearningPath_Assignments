# -*- coding: utf-8 -*-
"""
Created on Fri Jul 22 20:54:18 2022

@author: Monali
"""

import pandas as pd 

df_emp1 = pd.read_csv(r"D:\Diggibyte\SelfStudy_PySpark_Project\Databricks\Assignments\data files/employee_data.csv")

df_emp1.info()
df_emp1.head(10)

import pyspark
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('MyPractise').getOrCreate()
print(spark)

df_emp = spark.read.format("csv").options(inferSchema= True, header= True, sep= ",").load(r"D:\Diggibyte\SelfStudy_PySpark_Project\Databricks\Assignments\data files/employee_data.csv")

df_emp.show()
df_emp.printSchema()


from pyspark.sql.functions import *
df_emp_n = df_emp.select(col( "DOB"), to_date(col("DOB"), "dd-MM-yyyy").alias("Date of Birth"))
    
display(df_emp_n)
df_emp_n.show()
df_emp_n.printSchema()

df_emp_n.withColumn("Age in Months", months_between(current_date(),col("Date of Birth"))) \
        .withColumn("Age in Years", round(months_between(current_date(),col("Date of Birth"))/lit(12))) \
        .show()


df_emp_n1 = df_emp_n.select(col("Date of Birth"), current_date().alias("current_date"),
                            datediff(current_date(),col("Date of Birth")).alias("datediff"),
                            )

df_emp_n1.show()
df_emp_n1.printSchema()
