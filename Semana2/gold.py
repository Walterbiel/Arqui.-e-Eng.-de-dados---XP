# Databricks notebook source
# MAGIC %sql
# MAGIC create or replace table default.servicos_gold as(
# MAGIC select
# MAGIC secao,
# MAGIC count(codigo) as qtd
# MAGIC from default_servicos_silver
# MAGIC group by secao
# MAGIC order by secao desc
# MAGIC )
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from servicos_gold
