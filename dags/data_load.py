import os


from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
import pandas as pd

# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="data_load", start_date=datetime.today(), schedule="0 0 * * *") as dag:
    # Tasks are represented as operators
    hello = BashOperator(task_id="hmello", bash_command="echo hello")

    @task()
    def airflow():
        print("airflow")

    @task()
    def read_csv():

        file_path = os.path.join('/home', 'addisu', 'Desktop', '10 academy', 'Data-Engineering-Data-warehouse-tech-stack-with-MySQL-PostgreSQL-DBT-Airflow', 'dags', 'track_data.csv')
        # Read the CSV file
        df = pd.read_csv(file_path)

        # Perform some basic checks on the data
        print(f"Number of rows: {len(df)}")
        print(f"Column names: {df.columns}")
        print(f"First 5 rows:\n{df.head()}")

        return df

    # Set dependencies between tasks
    hello >> airflow() >> read_csv()
