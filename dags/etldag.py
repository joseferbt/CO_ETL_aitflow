from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

@dag(default_args=default_args, schedule_interval=None, catchup=False)
def data_warehouse_etl():

    # Extract TaskGroup
    with TaskGroup("extract_group") as extract_group:

        @task
        def extract_dimpersona():
            hook = PostgresHook(postgres_conn_id='source')
            df1 = hook.get_pandas_df("SELECT * FROM ips;")
            df2 = hook.get_pandas_df("SELECT * FROM table2;")
            return df1

        @task
        def extract_dimmedico():
            hook = PostgresHook(postgres_conn_id='source_postgres')
            df = hook.get_pandas_df("SELECT * FROM medico;")
            return df

        dimpersona_data = extract_dimpersona()
        dimmedico_data = extract_dimmedico()

    # Transform TaskGroup
    with TaskGroup("transform_group") as transform_group:

        @task
        def transform_dimpersona(data):
            df1 = data
            df_merged = df1
            return df_merged

        @task
        def transform_dimmedico(df):
            # Apply some transformations to dimmedico data
            df['new_column'] = df['licencia'] * 2
            return df

        transformed_dimpersona = transform_dimpersona(dimpersona_data)
        transformed_dimmedico = transform_dimmedico(dimmedico_data)

    # Load TaskGroup
    with TaskGroup("load_group") as load_group:

        @task
        def load_dimpersona(df):
            hook = PostgresHook(postgres_conn_id='dest')
            hook.run("create table if not exists dimpersona();")
            hook.insert_rows(table='dimpersona', rows=df.values.tolist(), target_fields=df.columns.tolist())

        @task
        def load_dimmedico(df):
            hook = PostgresHook(postgres_conn_id='dest_postgres')
            hook.insert_rows(table='dimmedico', rows=df.values.tolist(), target_fields=df.columns.tolist())

        load_dimpersona(transformed_dimpersona)
        load_dimmedico(transformed_dimmedico)

    # Define task group dependencies
    extract_group >> transform_group >> load_group

etl_dag = data_warehouse_etl()
