from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

with open("/opt/airflow/sql/ddl.sql", "r", encoding="utf-8") as f:
    ddl_sql = f.read()

with DAG(
    dag_id="init_schema",
    start_date=datetime(2024, 1, 1),  # ???
    schedule=None,
    catchup=False,
    tags=["final_project", "postgres", "ddl"],
) as dag:

    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="postgres_default",
        sql=ddl_sql,
    )
