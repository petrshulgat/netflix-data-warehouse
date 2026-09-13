import pandas as pd
from datetime import datetime 

from airflow import DAG
#Defines a task that runs Python
from airflow.operators.python import PythonOperator 
#Defines a task that runs SQL directly
from airflow.providers.postgres.operators.postgres import PostgresOperator
#A connection helper, used inside Python code
from airflow.providers.postgres.hooks.postgres import PostgresHook

def load_raw_data():

    csv_path = "/opt/airflow/dags/netflix_dataset.csv"

    df = pd.read_csv(csv_path)

    # Get a connection to Postgres using the Airflow connection you created
    hook = PostgresHook(postgres_conn_id="netflix_postgres")
    engine = hook.get_sqlalchemy_engine()

    df.to_sql(
        "netflix_data_raw", engine, if_exists="replace", index=False
    )

with DAG(
    dag_id = "netflix_dag",
    description = "Load Netflix CSV into Postgres, then transform into a clean table",
    start_date = datetime(2026, 9, 13),
    schedule = None, # trigger manually
    catchup = False
) as dag:

    load_raw = PythonOperator(
        task_id = "load_raw_data",
        python_callable = load_raw_data
    )

    transform = PostgresOperator(
        task_id = "transform_raw_to_clean",
        postgres_conn_id = "netflix_postgres",
        sql = """
            DROP TABLE IF EXISTS netflix_data_clean;

            create table netflix_data_clean(
                show_id text primary key, 
                type varchar(50), 
                title text, 
                director varchar(100), 
                "cast" text, 
                country varchar(50), 
                date_added date, 
                release_year integer, 
                rating text, 
                duration text, 
                listed_in text, 
                description text
            );

            insert into netflix_data_clean (show_id, type, title, director, "cast", country, date_added, release_year, rating, duration, listed_in, description)
            select
                show_id,
                type,
                title,
                director,
                "cast",
                country,
                date_added::date,
                release_year::integer,
                rating,
                duration,
                listed_in,
                description
            from netflix_data_raw;
        """
    )