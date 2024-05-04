# from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# import pandas as pd
# import psycopg2

# # Database connection details
# POSTGRES_HOST = 'localhost'
# POSTGRES_PORT = 5432
# POSTGRES_USER = 'airflow'
# POSTGRES_PASSWORD = 'airflow'
# POSTGRES_DB = 'airflow'

# # File paths
# PROD_FILE_PATH = '/home/addisu/Desktop/10 academy/data/raw_data.csv'
# DEV_FILE_PATH = '/home/addisu/Desktop/10 academy/data/raw_data_dev.csv'
# STAGING_FILE_PATH = '/home/addisu/Desktop/10 academy/data/raw_data_staging.csv'

# def load_data_to_postgres(env, file_path):
#     conn = psycopg2.connect(
#         host=POSTGRES_HOST,
#         port=POSTGRES_PORT,
#         user=POSTGRES_USER,
#         password=POSTGRES_PASSWORD,
#         database=POSTGRES_DB
#     )
#     cur = conn.cursor()

#     # Create tables
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS track_info (
#             track_id TEXT, type TEXT, traveled_d FLOAT, avg_speed FLOAT
#         )
#     """)
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS trajectory_info (
#             track_id TEXT, lat FLOAT, lon FLOAT, speed FLOAT, lon_acc FLOAT, lat_acc FLOAT, time FLOAT
#         )
#     """)

#     # Load data into tables
#     df_track = pd.read_csv(file_path, sep=';')
#     track_cols = ['track_id', 'type', 'traveled_d', 'avg_speed']
#     df_track[track_cols].to_sql('track_info', conn, if_exists='append', index=False)

#     df_trajectory = pd.DataFrame()
#     for i, row in df_track.iterrows():
#         track_id = row['track_id']
#         remaining_values = row.tolist()[4:]
#         trajectory_matrix = [[track_id] + remaining_values[j:j+6] for j in range(0, len(remaining_values), 6)]
#         df_trajectory = pd.concat([df_trajectory, pd.DataFrame(trajectory_matrix, columns=['track_id', 'lat', 'lon', 'speed', 'lon_acc', 'lat_acc', 'time'])])
#     df_trajectory.to_sql('trajectory_info', conn, if_exists='append', index=False)

#     conn.commit()
#     cur.close()
#     conn.close()
#     print(f"Data loaded successfully into {env} environment.")

# with DAG('load_data_dag', start_date=datetime(2023, 5, 3), schedule_interval=None) as dag:
#     load_prod_data = PythonOperator(
#         task_id='load_prod_data',
#         python_callable=load_data_to_postgres,
#         op_args=['Production', PROD_FILE_PATH]
#     )

#     load_dev_data = PythonOperator(
#         task_id='load_dev_data',
#         python_callable=load_data_to_postgres,
#         op_args=['Development', DEV_FILE_PATH]
#     )

#     load_staging_data = PythonOperator(
#         task_id='load_staging_data',
#         python_callable=load_data_to_postgres,
#         op_args=['Staging', STAGING_FILE_PATH]
#     )

#     load_prod_data >> load_dev_data >> load_staging_data










# from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# import pandas as pd
# import psycopg2

# # Database connection details
# POSTGRES_HOST = 'localhost'
# POSTGRES_PORT = 5432
# POSTGRES_USER = 'postgres'
# POSTGRES_PASSWORD = 'mypassword'
# POSTGRES_DB = 'airflow'

# # Host: localhost
# # Port: 5433
# # User: postgres
# # Password: mypassword

# # File paths
# PROD_FILE_PATH = '/home/addisu/Desktop/10 academy/data/raw_data.csv'
# DEV_FILE_PATH = '/home/addisu/Desktop/10 academy/data/raw_data_dev.csv'
# STAGING_FILE_PATH = '/home/addisu/Desktop/10 academy/data/raw_data_staging.csv'

# def load_data_to_postgres(env, file_path):
#     conn = psycopg2.connect(
#         host=POSTGRES_HOST,
#         port=POSTGRES_PORT,
#         user=POSTGRES_USER,
#         password=POSTGRES_PASSWORD,
#         database=POSTGRES_DB
#     )
#     cur = conn.cursor()

#     # Create tables
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS track_info (
#             track_id TEXT, type TEXT, traveled_d FLOAT, avg_speed FLOAT
#         )
#     """)
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS trajectory_info (
#             track_id TEXT, lat FLOAT, lon FLOAT, speed FLOAT, lon_acc FLOAT, lat_acc FLOAT, time FLOAT
#         )
#     """)

#     # Load data into tables
#     df_track = pd.read_csv(file_path, sep=';')
#     track_cols = ['track_id', 'type', 'traveled_d', 'avg_speed']
#     df_track[track_cols].to_sql('track_info', conn, if_exists='append', index=False)

#     df_trajectory = pd.DataFrame()
#     for i, row in df_track.iterrows():
#         track_id = row['track_id']
#         remaining_values = row.tolist()[4:]
#         trajectory_matrix = [[track_id] + remaining_values[j:j+6] for j in range(0, len(remaining_values), 6)]
#         df_trajectory = pd.concat([df_trajectory, pd.DataFrame(trajectory_matrix, columns=['track_id', 'lat', 'lon', 'speed', 'lon_acc', 'lat_acc', 'time'])])
#     df_trajectory.to_sql('trajectory_info', conn, if_exists='append', index=False)

#     conn.commit()
#     cur.close()
#     conn.close()
#     print(f"Data loaded successfully into {env} environment.")

# with DAG('load_data_dag', start_date=datetime(2023, 5, 3), schedule_interval=None) as dag:
#     save_prod_data = BashOperator(
#         task_id='save_prod_data',
#         bash_command=f'cp {PROD_FILE_PATH} /home/addisu/Desktop/10 academy/data/raw_data.csv'
#     )

#     save_dev_data = BashOperator(
#         task_id='save_dev_data',
#         bash_command=f'cp {DEV_FILE_PATH} /home/addisu/Desktop/10 academy/data/raw_data_dev.csv'
#     )

#     save_staging_data = BashOperator(
#         task_id='save_staging_data',
#         bash_command=f'cp {STAGING_FILE_PATH} /home/addisu/Desktop/10 academy/data/raw_data_staging.csv'
#     )

#     load_prod_data = PythonOperator(
#         task_id='load_prod_data',
#         python_callable=load_data_to_postgres,
#         op_args=['Production', PROD_FILE_PATH]
#     )

#     load_dev_data = PythonOperator(
#         task_id='load_dev_data',
#         python_callable=load_data_to_postgres,
#         op_args=['Development', DEV_FILE_PATH]
#     )

#     load_staging_data = PythonOperator(
#         task_id='load_staging_data',
#         python_callable=load_data_to_postgres,
#         op_args=['Staging', STAGING_FILE_PATH]
#     )

#     save_prod_data >> load_prod_data
#     save_dev_data >> load_dev_data
#     save_staging_data >> load_staging_data


# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from datetime import datetime, timedelta
# import pandas as pd
# import psycopg2

# # Database connection details
# POSTGRES_HOST = 'localhost'
# POSTGRES_PORT = 5432
# POSTGRES_USER = 'my-postgres'
# POSTGRES_PASSWORD = 'mypassword'
# POSTGRES_DB = 'airflow'

# # File paths
# DATA_DIR = '/home/addisu/Desktop/10 academy/data'
# TRACK_FILE_PATH = f'{DATA_DIR}/track_data.csv'

# def load_track_data_to_postgres():
#     # Read the CSV file
#     df_track = pd.read_csv(TRACK_FILE_PATH, sep=",")

#     conn = psycopg2.connect(
#         host=POSTGRES_HOST,
#         port=POSTGRES_PORT,
#         user=POSTGRES_USER,
#         password=POSTGRES_PASSWORD,
#         database=POSTGRES_DB
#     )
#     cur = conn.cursor()

#     # Create table
#     cur.execute("""
#         CREATE TABLE IF NOT EXISTS track_info (
#             track_id TEXT, type TEXT, traveled_d FLOAT, avg_speed FLOAT
#         )
#     """)

#     # Load data into table
#     df_track.to_sql('track_info', conn, if_exists='append', index=False)

#     conn.commit()
#     cur.close()
#     conn.close()
#     print("Track data loaded successfully into the database.")

# with DAG('load_data_dag', start_date=datetime(2023, 5, 3), schedule_interval=None) as dag:
#     load_track_data = PythonOperator(
#         task_id='load_track_data',
#         python_callable=load_track_data_to_postgres
#     )

