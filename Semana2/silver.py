# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

#criar parametros no databricks
dbutils.widgets.text("schema","")
dbutils.widgets.text("table","")

schema = dbutils.widgets.get("schema")
table = dbutils.widgets.get("table")

# COMMAND ----------

df = spark.sql(f"select * from {schema}.{table}")
display(df)

# COMMAND ----------

import pyspark.sql.functions as F

#df_silver = (
   # df.where("descricao is not null")
   #     .withColumn('row_number',F.expr("row_number() over(partition by codigo order by codigo"))
  #      .where("row_number = 1")
  #      .drop("row_number")
#)

df_silver = df.dropna(subset=["codigo"])
df_silver = df_silver.withColumn("date_partition",F.current_date())
display(df_silver)

# COMMAND ----------

df_silver.write.mode("overwrite")\
    .format("delta")\
    .partitionBy("date_partition")\
    .option("compression", "snappy")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{schema}_{table}_silver")

# COMMAND ----------

df.schema

# COMMAND ----------


