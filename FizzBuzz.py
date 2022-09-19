# Databricks notebook source
from pyspark.sql.functions import lit, col, when

# COMMAND ----------

df = spark.range(1, 101, 1)
df.display()

# COMMAND ----------

df_threes = df.withColumn(
    "multipleOfThree",
    when((col("id") % lit(3)) == 0 , 1)
    .otherwise(lit(0))
)
df_threes.display()

# COMMAND ----------

df_fives = df_threes.withColumn(
  "multipleOfFive",
    when((col("id") % lit(5)) == 0 , 1)
    .otherwise(lit(0))
)
df_fives.display()

# COMMAND ----------

df_both = df_fives.withColumn(
  "multipleOfBoth", 
   col("multipleOfThree") * col("multipleOfFive"))
df_both.display()

# COMMAND ----------

df_fizzBuzz = df_both.withColumn(
  "FizzBuzz", 
    when(col("multipleOfBoth") == 1, "FizzBuzz")
   .when(col("multipleOfFive") == 1, "Buzz")
   .when(col("multipleOfThree") == 1, "Fizz")
   .otherwise(col("id")))
df_fizzBuzz.select("FizzBuzz").display()
