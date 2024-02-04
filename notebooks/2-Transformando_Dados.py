# Databricks notebook source
from pyspark.sql.functions import first,col,round

# COMMAND ----------

#Salvando os dados da bronze em um dataset
df_junto = spark.read.parquet("dbfs:/databricks-results/bronze/*/*/*")

# COMMAND ----------

#Só pra ficar igual ao curso
df_moedas = df_junto

# COMMAND ----------

from pyspark.sql.functions import to_date

df_moedas = df_moedas.withColumn("data", to_date("data"))

# COMMAND ----------

resultado_taxas_conversao = df_moedas.groupBy("data")\
                                              .pivot("moeda")\
                                              .agg(first("taxa"))\
                                              .orderBy("data", ascending=False)
                      

# COMMAND ----------

resultado_valores_reais = resultado_taxas_conversao.select("*")

# COMMAND ----------

moedas = resultado_valores_reais.schema.names
moedas.remove('data')

# COMMAND ----------

for moeda in moedas:
    resultado_valores_reais = resultado_valores_reais\
                                .withColumn(
                                    moeda, round(1/col(moeda), 4)
                                )

# COMMAND ----------

#Deixando em um único arquivo (Sem particionamento)
resultado_taxas_conversao = resultado_taxas_conversao.coalesce(1)
resultado_valores_reais = resultado_valores_reais.coalesce(1)

# COMMAND ----------

resultado_taxas_conversao.write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/databricks-results/prata/taxas_conversao")
    
resultado_valores_reais.write\
    .mode ("overwrite")\
    .format("csv")\
    .option("header", "true")\
    .save("dbfs:/databricks-results/prata/valores_reais")

