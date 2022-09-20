# Databricks notebook source
from pyspark.sql.functions import lit, col, when, upper

# COMMAND ----------

@udf
def fizzbuzzer(i):
    if i % 3 == 0 and i % 5 == 0:
        return('fizzbuzz')
    elif i % 3 == 0:
        return('fizz')
    elif i % 5 == 0:
        return('buzz')
    else:
        return(i)

# COMMAND ----------

#fizzbuzzer(32543543645)

# COMMAND ----------

#fizzbuzzer(5)

# COMMAND ----------

#fizzbuzzer(15)

# COMMAND ----------

df = spark.range(1, 1000000, 1)
df.display()

# COMMAND ----------

df.withColumn("FizzBuzz", fizzbuzzer(col("id")))
