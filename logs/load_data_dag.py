import pandas as pd
import psycopg2

# Database connection details
POSTGRES_HOST = 'localhost'
POSTGRES_PORT = 5432
POSTGRES_USER = 'postgres'
POSTGRES_PASSWORD = 'mypassword'
POSTGRES_DB = 'airflow'

# File paths
DATA_DIR = '/home/addisu/Desktop/10 academy/data'
TRACK_FILE_PATH = f'{DATA_DIR}/track_data.csv'

def load_track_data_to_postgres():
    # Read the CSV file
    df_track = pd.read_csv(TRACK_FILE_PATH, sep=",")

    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        database=POSTGRES_DB
    )
    cur = conn.cursor()

    # Create table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS track_info (
            track_id TEXT, type TEXT, traveled_d FLOAT, avg_speed FLOAT
        )
    """)

    # Load data into table
    df_track.to_sql('track_info', conn, if_exists='append', index=False)

    conn.commit()
    cur.close()
    conn.close()
    print("Track data loaded successfully into the database.")

if __name__ == "__main__":
    load_track_data_to_postgres()
