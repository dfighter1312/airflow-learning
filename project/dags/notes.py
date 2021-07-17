# def load_spotify_etl(**context):
#     df = context.get("ti").xcom_pull(key="df")
#     try:
#         # Connect to an existing database
#         conn = psycopg2.connext(
#             host="localhost",
#             database="airflow",
#             user="airflow",
#             password="airflow"
#         )

#         # Create a cursor to perform database operations
#         cursor = conn.cursor()

#         # Print PostgreSQL details
#         print("PostgreSQL server information")
#         print(conn.get_dsn_parameters(), "\n")

#         # Create my_played_tracks table
#         sql_query = """
#         CREATE TABLE IF NOT EXISTS my_played_tracks(
#             song_name VARCHAR(200),
#             artist_name VARCHAR(200),
#             played_at VARCHAR(200),
#             timestamp VARCHAR(200),
#             CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)
#         )
#         """
#         cursor.execute(sql_query)
#         print("Create table successfully or a table exists.")

#         # Push DataFrame to PostgreSQL
#         df.to_sql("my_played_tracks", engine, index=False, if_exists='append')

#     except (Exception, psycopg2.Error) as e:
#         print("Error while connecting to PostgreSQL", e)
#     finally:
#         if conn:
#             cursor.close()
#             conn.close()
#             print("PostgreSQL connection is closed.")