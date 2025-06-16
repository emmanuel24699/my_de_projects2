import pandas as pd
import logging
from io import StringIO
import csv
import os
import boto3
from datetime import datetime
from pandas.errors import EmptyDataError

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
processed_files_table = dynamodb.Table('ProcessedFiles')

def is_file_processed(file_key):
    try:
        resp = processed_files_table.get_item(Key={'file_key': file_key})
        return 'Item' in resp
    except Exception as e:
        logger.error(f"Error checking processed file {file_key}: {e}")
        raise

def mark_file_processed(file_key):
    try:
        processed_files_table.put_item(
            Item={
                'file_key': file_key,
                'bucket_name': 'amalitechde-music-streaming-lab',
                'processed_at': datetime.utcnow().isoformat()
            }
        )
        logger.info(f"Marked file as processed: s3://amalitechde-music-streaming-lab/{file_key}")
    except Exception as e:
        logger.error(f"Error marking file as processed: {e}")
        raise

def load_metadata_to_rds():
    try:
        logger.info("Loading RAW metadata to RDS")
        rds_hook = PostgresHook(postgres_conn_id='rds_postgres')
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket = 'amalitechde-music-streaming-lab'
        users_key = 'data/users/users.csv'
        songs_key = 'data/songs/songs.csv'
        conn = rds_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS staging_users (
            user_id INTEGER,
            user_name TEXT,
            user_age INTEGER,
            user_country TEXT,
            created_at DATE
        );
        CREATE TABLE IF NOT EXISTS staging_songs (
            id INTEGER,
            track_id TEXT,
            artists TEXT,
            album_name TEXT,
            track_name TEXT,
            popularity INTEGER,
            duration_ms INTEGER,
            explicit BOOLEAN,
            danceability FLOAT,
            key INTEGER,
            loudness FLOAT,
            mode INTEGER,
            speechiness FLOAT,
            acousticness FLOAT,
            instrumentalness FLOAT,
            liveness FLOAT,
            valence FLOAT,
            tempo FLOAT,
            time_signature INTEGER,
            track_genre TEXT
        );
        """)
        conn.commit()

        users_content = s3_hook.read_key(users_key, bucket)
        users_df = pd.read_csv(StringIO(users_content))
        users_df = users_df.drop_duplicates(subset=['user_id'], keep='last')
        output = StringIO()
        users_df.to_csv(output, index=False, header=False, na_rep='', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        output.seek(0)
        cursor.copy_expert(sql="COPY staging_users FROM STDIN WITH (FORMAT CSV)", file=output)
        conn.commit()

        songs_content = s3_hook.read_key(songs_key, bucket)
        songs_df = pd.read_csv(StringIO(songs_content))
        songs_df = songs_df.drop_duplicates(subset=['id'], keep='last')
        output = StringIO()
        songs_df.to_csv(output, index=False, header=False, na_rep='', quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        output.seek(0)
        cursor.copy_expert(sql="COPY staging_songs FROM STDIN WITH (FORMAT CSV)", file=output)
        conn.commit()

        cursor.execute("""
        INSERT INTO users (user_id, user_name, user_age, user_country, created_at)
        SELECT user_id, user_name, user_age, user_country, created_at FROM staging_users
        ON CONFLICT (user_id)
        DO UPDATE SET
            user_name = EXCLUDED.user_name,
            user_age = EXCLUDED.user_age,
            user_country = EXCLUDED.user_country,
            created_at = EXCLUDED.created_at;
        TRUNCATE TABLE staging_users;
        """)
        conn.commit()

        cursor.execute("""
        INSERT INTO songs (
            id, track_id, artists, album_name, track_name, popularity,
            duration_ms, explicit, danceability, energy, key, loudness,
            mode, speechiness, acousticness, instrumentalness, liveness,
            valence, tempo, time_signature, track_genre
        )
        SELECT id, track_id, artists, album_name, track_name, popularity,
               duration_ms, explicit, danceability, energy, key, loudness,
               mode, speechiness, acousticness, instrumentalness, liveness,
               valence, tempo, time_signature, track_genre
        FROM staging_songs
        ON CONFLICT (id)
        DO UPDATE SET
            track_id = EXCLUDED.track_id,
            artists = EXCLUDED.artists,
            album_name = EXCLUDED.album_name,
            track_name = EXCLUDED.track_name,
            popularity = EXCLUDED.popularity,
            duration_ms = EXCLUDED.duration_ms,
            explicit = EXCLUDED.explicit,
            danceability = EXCLUDED.danceability,
            energy = EXCLUDED.energy,
            key = EXCLUDED.key,
            loudness = EXCLUDED.loudness,
            mode = EXCLUDED.mode,
            speechiness = EXCLUDED.speechiness,
            acousticness = EXCLUDED.acousticness,
            instrumentalness = EXCLUDED.instrumentalness,
            liveness = EXCLUDED.liveness,
            valence = EXCLUDED.valence,
            tempo = EXCLUDED.tempo,
            time_signature = EXCLUDED.time_signature,
            track_genre = EXCLUDED.track_genre;
        TRUNCATE TABLE staging_songs;
        """)
        conn.commit()
        cursor.close()
        conn.close()
        logger.info("RAW metadata loaded to RDS.")
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error loading metadata to RDS: {e}")
        raise

def extract_metadata():
    try:
        logger.info("Extracting metadata from RDS...")
        rds_hook = PostgresHook(postgres_conn_id='rds_postgres')
        users_df = rds_hook.get_pandas_df("SELECT user_id, user_name, user_age, user_country, created_at FROM users")
        songs_df = rds_hook.get_pandas_df("SELECT * FROM songs")
        logger.info("Metadata extracted successfully.")
        return {
            'users': users_df.to_dict('records'),
            'songs': songs_df.to_dict('records')
        }
    except Exception as e:
        logger.error(f"Error extracting metadata: {e}")
        raise

def extract_streaming():
    try:
        logger.info("Extracting streaming data from S3...")
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket = 'amalitechde-music-streaming-lab'
        prefix = 'data/streams/'
        keys = s3_hook.list_keys(bucket_name=bucket, prefix=prefix)
        streaming_dfs = []
        new_files_count = 0
        for key in keys:
            if key.endswith('.csv') and not is_file_processed(key):
                file_content = s3_hook.read_key(key, bucket)
                df = pd.read_csv(StringIO(file_content))
                streaming_dfs.append(df)
                mark_file_processed(key)
                new_files_count += 1
        if not streaming_dfs:
            logger.warning("No new streaming data found in S3.")
            return []
        streaming_df = pd.concat(streaming_dfs, ignore_index=True)
        logger.info(f"Streaming data extracted successfully from {new_files_count} new files.")
        return streaming_df.to_dict('records')
    except Exception as e:
        logger.error(f"Error extracting streaming data: {e}")
        raise