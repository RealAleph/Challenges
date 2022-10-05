# Databricks notebook source
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

df = spark.range(1, 101)
df.withColumn("FizzBuzz", fizzbuzzer(df.id)).display()

# COMMAND ----------

#fizzbuzzer(32543543645)

# COMMAND ----------

#fizzbuzzer(5)

# COMMAND ----------

#fizzbuzzer(15)

# COMMAND ----------


# df.display()

# COMMAND ----------

# df.withColumn("FizzBuzz", fizzbuzzer(col("id")))
