try:
    import datetime
    import sqlalchemy
    import sqlite3
    import requests
    import json
    import pandas as pd
    # from datetime import datetime
    from sqlalchemy.orm import sessionmaker
    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
except Exception as e:
    print(f"Error {e}")


def check_if_valid_data(df):
    """Check if the DataFrame is empty."""
    print("DataFrame result:")
    print("\n", df)

    if df.empty:
        print("No songs downloaded. Finishing execution.")
        return False

    # Primary Key check
    if not pd.Series(df['played_at']).is_unique:
        raise Exception("Primary Key check is violated.")

    # Check for nulls
    if df.isnull().values.any():
        raise Exception("Null values found.")

    # Check that all timestamps are of yesterday's date
    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, '%Y-%m-%d') != yesterday:
            raise Exception("At least one of the returned songs does not have a yesterday's timestamp")

    return True

def extract_spotify_etl(**context):
    """Main Spotify ETL - Extract."""

    USER_ID = ''

    # Generate token via: https://developer.spotify.com/console/get-recently-played/
    # A Spotify account is required
    TOKEN = 'BQDb9zkeX_NiSRwYibWSjO8Q98E_aSZo2likWDxHWjqH6fJBgj5g-fsZYZjj2B9C1l-3cpw7pspnQ-Ai7Di04o3ze8mmnHPQkJ6FXH2_haEFzFAgseY1GMdYRKQ7iusC9VC_7NrHdn1Ie7Cx7sdS8gUIGuiyG_K2CId7pW4DdOrMGGTvdj7SLSlkd6V9niWfItt4WZSMbUwYWpA36kIYR-mlsSeFS6AH-E1-UDzADFb1_XtrIwUlVVffaZMNt6sqJIOaeKcWFwICnJyHFX6vKZSv90CfGlykoyRYv1cjEtHqC1iT'

    # Extract part of the ETL process
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }

    # Convert time to Unix timestamp in miliseconds
    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # Download all songs you've listened to "after yesterday",
    # which means in the last 24 hours
    r = requests.get(f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix_timestamp}",
        headers=headers)
    
    data = r.json()

    song_names = []
    artist_names = []
    played_at_list = []
    timestamps = []

    # Extracting only the relevant bits of data from the json object
    for song in data["items"]:
        song_names.append(song["track"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        played_at_list.append(song["played_at"])
        timestamps.append(song["played_at"][0:10])

    # Prepare a dictionary in order to turn it into a Pandas DataFrame
    song_dict = {
        "song_name": song_names,
        "artist_name": artist_names,
        "played_at": played_at_list,
        "timestamp": timestamps
    }
    
    song_df = pd.DataFrame(song_dict)

    # Validate the DataFrame
    if check_if_valid_data(song_df):
        print("Data validation sucess, proceeding to Load stage...")

    context['ti'].xcom_push(key='dict', value=song_dict)

def load_spotify_etl(**context):
    """Main Spotify ETL - Load."""

    DATABASE_LOCATION = "sqlite:///my_played_tracks.sqlite"

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    conn = sqlite3.connect('my_played_tracks.sqlite')
    cursor = conn.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        artist_name VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
    )
    """

    cursor.execute(sql_query)
    print("Open database successfully.")

    song_dict = context.get("ti").xcom_pull(key="dict")
    df = pd.DataFrame(song_dict)
    try:
        df.to_sql("my_played_tracks", engine, index=False, if_exists="append")
    except:
        print("Data already exists in the database.")
    
    conn.close()
    print("Close database successfully.")

with DAG(
    dag_id = "4-spotify-dag",        # The DAG id should be the same as the filename
    schedule_interval="@daily",
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=1),
        "start_date": datetime.datetime(2021, 7, 16)
    },
    catchup=False   # If the start date is declared before the current date
                    # and the catchup is True, all the DAG from start date
                    # to current date will be computed. Default is False.
) as f:

    extract_spotify_etl = PythonOperator(
        task_id="extract_spotify_etl",
        python_callable=extract_spotify_etl,
        provide_context=True,
    )

    load_spotify_etl = PythonOperator(
        task_id="load_spotify_etl",
        python_callable=load_spotify_etl,
        provide_context=True
    )

# Task dependancy
extract_spotify_etl >> load_spotify_etl