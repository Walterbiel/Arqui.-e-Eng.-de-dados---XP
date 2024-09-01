# Databricks notebook source
#instalação libs cmd
%pip install requests

# COMMAND ----------

#importando libs para o dataframe notebook
import requests
import pandas as pd
import io
import chardet

# COMMAND ----------

url = 'https://compras.dados.gov.br/servicos/v1/servicos.csv'
data = requests.get(url).content

# COMMAND ----------

#Formato da base
#result = chardet.detect(data)
#result

# COMMAND ----------

df = pd.read_csv(io.StringIO(data.decode('utf-8')))
df.head()

# COMMAND ----------

df = spark.createDataFrame(df)
display(df)

# COMMAND ----------

df.columns

# COMMAND ----------

df = (
    df
    .withColumnRenamed('Código','codigo')
    .withColumnRenamed('Descrição','descricao')
    .withColumnRenamed('Unidade medida','unidade_medida')
    .withColumnRenamed('CPC','cpc')
    .withColumnRenamed('Seção','secao')
    .withColumnRenamed('Divisão','divisao')
    .withColumnRenamed('Grupo','grupo')
    .withColumnRenamed('Classe','classe')
    .withColumnRenamed('Subclasse','subclasse')

)

# COMMAND ----------

df.write.mode('overwrite').saveAsTable('servicos')

# COMMAND ----------


