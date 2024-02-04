# Databricks notebook source
#Instalando pacote para proteção de tokens/keys
#pip install python-dotenv

# COMMAND ----------

#Importando pacote para proteção de tokens/keys
from dotenv import dotenv_values

# COMMAND ----------

#Acessando os segredos nesse notebook
secrets = dotenv_values(".env")
access_token = secrets['token_api']

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

import requests

url = "https://api.apilayer.com/exchangerates_data/{date}?symbols={symbols}&base={base}"

payload = {}
headers= {
  "apikey": access_token
}

response = requests.request("GET", url, headers=headers, data = payload)

status_code = response.status_code
result = response.text
