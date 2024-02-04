# Databricks notebook source
#Instalando pacote para proteção de tokens/keys
#pip install python-dotenv

# COMMAND ----------

#Importando pacote para proteção de tokens/keys
from dotenv import dotenv_values
import requests
from pyspark.sql.functions import lit

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

#Testando a função
extraindo_dados("2023-09-07")

# COMMAND ----------

ano, mes, dia = extraindo_dados("2023-09-07")['date'].split('-')
print(ano, mes, dia)

# COMMAND ----------

#Validando onde será salvo
display(dbutils.fs.ls("dbfs:/"))

# COMMAND ----------

#Definindo onde será o caminho
caminho_completo = f"dbfs:/databricks-results/bronze/{ano}/{mes}/{dia}"
print(caminho_completo)

# COMMAND ----------

#Nova Função para só filtrar as colunas que precisamos
def dados_para_dataframe (dado_json): 
        dados_tupla = [(moeda, float (taxa)) for moeda, taxa in dado_json["rates"].items()]
        return dados_tupla

# COMMAND ----------

response = dados_para_dataframe(extraindo_dados("2023-09-07"))
response

# COMMAND ----------

#Renomeando as colunas
f_conversoes = spark.createDataFrame(response,schema=['moeda','taxa'])
df_conversoes.show()

# COMMAND ----------

#Criando a coluna data
df_conversoes = df_conversoes.withColumn("data", lit(f"{ano}-{mes}-{dia}"))

# COMMAND ----------

df_conversoes.show()

# COMMAND ----------

#Salvando
df_conversoes.write.format("parquet")\
    .mode("overwrite")\
    .save(caminho_completo)

# COMMAND ----------

#Validando o salvamento
display(dbutils.fs.ls(f"{caminho_completo}"))
