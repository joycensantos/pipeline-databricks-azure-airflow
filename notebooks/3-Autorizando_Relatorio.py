# Databricks notebook source
from slack_sdk import WebClient
from dotenv import dotenv_values
import pyspark.pandas as ps

# COMMAND ----------

#Acessando os segredos nesse notebook
secrets = dotenv_values(".env")
access_token = secrets['token_slack']

# COMMAND ----------

client = WebClient(token=access_token)

# COMMAND ----------

#Nome dinamico do arquivo do notebook 2-Transformando_Dados
nome_arquivo = dbutils.fs.ls("dbfs:/databricks-results/prata/valores_reais/")[-1].name

# COMMAND ----------

#Navegando até a pasta onde está o arquivo
path = "../../../../../../dbfs/databricks-results/prata/valores_reais/" + nome_arquivo

# COMMAND ----------

enviando_arquivo_csv = client.files_upload_v2(
    channel="C06H44G5GLS",  
    title="Arquivo no formato CSV do valor do real convertido",
    file=path,
    filename="valores_reais.csv",
    initial_comment="Segue anexo o arquivo CSV:",
)


# COMMAND ----------

df_valores_reais = ps.read_csv("dbfs:/databricks-results/prata/valores_reais/")

# COMMAND ----------

!mkdir imagens

# COMMAND ----------

df_valores_reais.columns[1:]

# COMMAND ----------

for moeda in df_valores_reais.columns[1:]:
    fig = df_valores_reais.plot.line(x="data",y=moeda)
    fig.write_image(f"./imagens/{moeda}.png")

# COMMAND ----------

def enviando_imagens(moeda_cotacao):
    enviando_imagens = client.files_upload_v2(
    channel="C06H44G5GLS",  
    title="Enviando imagens de cotacoes",
    file=f"./imagens/{moeda_cotacao}.png"
)


# COMMAND ----------

for moeda in df_valores_reais.columns[1:]:
    enviando_imagens(moeda)
