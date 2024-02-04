# Databricks notebook source
#Instalando pacote para proteção de tokens/keys
%pip install python-dotenv

# COMMAND ----------

#Importando pacote para proteção de tokens/keys
from dotenv import dotenv_values
import requests
from pyspark.sql.functions import lit

# COMMAND ----------

dbutils.widgets.text("data_execucao", "")
data_execucao = dbutils.widgets.get("data_execucao")

# COMMAND ----------

#Acessando os segredos nesse notebook
secrets = dotenv_values(".env")
access_token = secrets['token_api']

# COMMAND ----------

#Definindo a função para extração de dados da API 

def extraindo_dados(date,base="BRL"):

    url = f"https://api.apilayer.com/exchangerates_data/{date}&base={base}"

    headers= {
      "apikey": access_token
    }

    parametros = {"base": base,
                  "symbols": "USD,GBP,EUR"
                  }

    response = requests.request("GET", url, headers=headers,params=parametros)

    if response.status_code != 200: 
      raise Exception ("Não consegui extrair dados!")


    return response.json()

# COMMAND ----------

#Nova Função para só filtrar as colunas que precisamos
def dados_para_dataframe (dado_json): 
        dados_tupla = [(moeda, float (taxa)) for moeda, taxa in dado_json["rates"].items()]
        return dados_tupla

# COMMAND ----------

def salvar_arquivo_parquet(conversoes_extraidas):
    ano, mes, dia = conversoes_extraidas['date'].split('-')
    caminho_completo = f"dbfs:/databricks-results/bronze/{ano}/{mes}/{dia}"
    response = dados_para_dataframe(conversoes_extraidas)
    df_conversoes = spark.createDataFrame(response, schema=['moeda', 'taxa'])
    df_conversoes = df_conversoes.withColumn("data", lit(f"{ano}-{mes}-{dia}"))
    
    df_conversoes.write.format("parquet")\
                .mode("overwrite")\
                .save(caminho_completo)

    print(f"Os arquivos foram salvos em {caminho_completo}")

# COMMAND ----------

cotacoes = extraindo_dados(data_execucao)
salvar_arquivo_parquet(cotacoes)
