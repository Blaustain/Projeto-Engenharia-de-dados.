
from __future__ import annotations

from airflow import DAG

from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

import pendulum
import airflow.providers.standard.operators.bash
import airflow.providers.standard.operators.python
from airflow.models.dag import DAG
airflow.providers.standard.operators.bash.BashOperator
airflow.providers.standard.operators.python.PythonOperator
import os
import pandas as pd
from sqlalchemy import create_engine

# Variáveis para as conexões
POSTGRES_CONN_ID = "postgres_dwh"
CSV_FILE_PATH = "/opt/airflow/dags/data/transacoes.csv" # Substitua pelo caminho correto
SQL_CONN_ID = "sql_source"

def extract_from_csv():
    # Lógica de extração e salvamento do CSV
    current_date = pendulum.now().format("YYYY-MM-DD")
    output_dir = f"/opt/airflow/data/{current_date}/csv_source/"
    os.makedirs(output_dir, exist_ok=True)
    
    # Simulação da extração (leitura e escrita idempotente)
    df = pd.read_csv(CSV_FILE_PATH)
    output_path = f"{output_dir}transacoes.csv"
    df.to_csv(output_path, index=False)
    print(f"Dados do CSV extraídos para: {output_path}")

def extract_from_sql():
    # Lógica de extração e salvamento do SQL
    current_date = pendulum.now().format("YYYY-MM-DD")
    output_dir = f"/opt/airflow/data/{current_date}/sql_source/"
    os.makedirs(output_dir, exist_ok=True)

    # Exemplo de extração de uma tabela
    engine = create_engine("postgresql://user:password@host:port/dbname") # Configure sua conexão
    df = pd.read_sql_table("nome_da_sua_tabela", con=engine)
    
    output_path = f"{output_dir}nome_da_sua_tabela.csv"
    df.to_csv(output_path, index=False)
    print(f"Dados do SQL extraídos para: {output_path}")

def load_to_dwh():
    # Lógica de carregamento no PostgreSQL
    # Esta função lerá os CSVs extraídos e os carregará no DW
    current_date = pendulum.now().format("YYYY-MM-DD")
    csv_path = f"/opt/airflow/data/{current_date}/csv_source/transacoes.csv"
    sql_path = f"/opt/airflow/data/{current_date}/sql_source/nome_da_sua_tabela.csv"

    # Ler os arquivos CSV
    df_csv = pd.read_csv(csv_path)
    df_sql = pd.read_csv(sql_path)
    
    # Conexão com o PostgreSQL
    engine = create_engine(f"postgresql://airflow:airflow@postgres:5432/airflow")

    # Exemplo de carregamento
    df_csv.to_sql("transacoes_dwh", engine, if_exists="replace", index=False)
    df_sql.to_sql("sql_source_dwh", engine, if_exists="replace", index=False)
    print("Dados carregados com sucesso no Data Warehouse!")


with DAG(
    dag_id="etl_financial_data",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="35 4 * * *",  # Executa todos os dias às 04:35 da manhã
    catchup=False,
    tags=["etl", "financeiro"],
) as dag:
    # Tarefas de extração (paralelas)
    extract_csv_task = PythonOperator(
        task_id="extract_from_csv",
        python_callable=extract_from_csv,
    )

    extract_sql_task = PythonOperator(
        task_id="extract_from_sql",
        python_callable=extract_from_sql,
    )

    # Tarefa de carregamento
    load_to_dwh_task = PythonOperator(
        task_id="load_to_dwh",
        python_callable=load_to_dwh,
    )

    # Definição das dependências
    [extract_csv_task, extract_sql_task] >> load_to_dwh_task