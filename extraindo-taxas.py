from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    'Executando-Notebooks-Databricks',
    start_date=datetime(2024, 2, 3),
    schedule_interval="0 9 * * *" #Todos os dias as 9 da manhã
    ) as dag_executando_notebook_extracao:

    extraindo_dados = DatabricksRunNowOperator(
    task_id = 'Extraindo-conversoes',
    databricks_conn_id = 'databricks_default',
    job_id = 99781916360389,
    notebook_params={'data_execucao': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    transformando_dados = DatabricksRunNowOperator(
    task_id = 'Transformando-dados',
    databricks_conn_id = 'databricks_default',
    job_id = 180776324225346
    )

    enviando_relatorio = DatabricksRunNowOperator(
    task_id = 'Enviando-relatorio',
    databricks_conn_id = 'databricks_default',
    job_id = 177916302552193
    )

    extraindo_dados >> transformando_dados >> enviando_relatorio