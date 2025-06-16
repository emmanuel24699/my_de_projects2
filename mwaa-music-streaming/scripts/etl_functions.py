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

def validate_data(**kwargs):
    try:
        logger.info("Validating and cleaning data.")
        ti = kwargs['ti']
        metadata = ti.xcom_pull(task_ids='extract_metadata')
        streaming_data = ti.xcom_pull(task_ids='extract_streaming')
        users_df = pd.DataFrame(metadata['users'])
        songs_df = pd.DataFrame(metadata['songs'])
        streaming_df = pd.DataFrame(streaming_data) if streaming_data else pd.DataFrame()

        users_df.drop_duplicates(subset=['user_id'], keep='last', inplace=True)
        users_df['user_id'] = pd.to_numeric(users_df['user_id'], errors='coerce')
        users_df['user_age'] = pd.to_numeric(users_df['user_age'], errors='coerce')
        users_df['user_name'] = users_df['user_name'].astype(str).str.strip()
        users_df['user_country'] = users_df['user_country'].astype(str).str.strip()
        users_df['created_at'] = pd.to_datetime(users_df['created_at'], errors='coerce')
        users_df = users_df.dropna(subset=['user_id', 'user_name', 'user_age', 'user_country', 'created_at'])

        songs_df.drop_duplicates(subset=['id'], keep='last', inplace=True)
        songs_df['id'] = pd.to_numeric(songs_df['id'], errors='coerce')
        songs_df['popularity'] = pd.to_numeric(songs_df['popularity'], errors='coerce')
        songs_df['duration_ms'] = pd.to_numeric(songs_df['duration_ms'], errors='coerce')
        songs_df['explicit'] = songs_df['explicit'].astype(str).str.lower().replace({'yes': 'true', 'no': 'false', '1': 'true', '0': 'false'})
        songs_df['danceability'] = pd.to_numeric(songs_df['danceability'], errors='coerce')
        songs_df = songs_df.dropna(subset=['id', 'track_id', 'track_name', 'duration_ms'])

        if not streaming_df.empty:
            streaming_df.drop_duplicates(subset=['user_id', 'track_id', 'listen_time'], inplace=True)
            streaming_df['user_id'] = pd.to_numeric(streaming_df['user_id'], errors='coerce')
            streaming_df['track_id'] = streaming_df['track_id'].astype(str).str.strip()
            streaming_df['listen_time'] = pd.to_datetime(streaming_df['listen_time'], errors='coerce', format='mixed')
            streaming_df = streaming_df.dropna(subset=['user_id', 'track_id', 'listen_time'])

        users_df.to_csv('/tmp/validated_users.csv', index=False)
        songs_df.to_csv('/tmp/validated_songs.csv', index=False)
        if not streaming_df.empty:
            streaming_df['listen_time'] = streaming_df['listen_time'].dt.strftime('%Y-%m-%d %H:%M:%S')
        streaming_df.to_csv('/tmp/validated_streaming.csv', index=False)
        logger.info("Data validation and cleaning successful.")
        return {
            'users_path': '/tmp/validated_users.csv',
            'songs_path': '/tmp/validated_songs.csv',
            'streaming_path': '/tmp/validated_streaming.csv'
        }
    except Exception as e:
        logger.error(f"Error validating data: {e}")
        raise

def transform_kpis(**kwargs):
    try:
        logger.info("Computing KPIs...")
        ti = kwargs['ti']
        paths = ti.xcom_pull(task_ids='validate_data')
        users_path = paths['users_path']
        songs_path = paths['songs_path']
        streaming_path = paths['streaming_path']

        users_df = pd.read_csv(users_path)
        songs_df = pd.read_csv(songs_path)

        # Robust streaming data load: handle empty or headerless files
        if os.path.exists(streaming_path) and os.path.getsize(streaming_path) > 0:
            try:
                streaming_df = pd.read_csv(streaming_path)
            except EmptyDataError:
                logger.warning(f"{streaming_path} is empty. No streaming data to process.")
                streaming_df = pd.DataFrame()
        else:
            streaming_df = pd.DataFrame()

        if streaming_df.empty:
            logger.warning("No streaming data available for KPI computation.")
            return {
                'genre_kpis_path': None,
                'hourly_kpis_path': None
            }

        streaming_df['listen_time'] = pd.to_datetime(streaming_df['listen_time'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
        streaming_df = streaming_df.dropna(subset=['listen_time'])

        streaming_with_songs = streaming_df.merge(
            songs_df[['track_id', 'track_name', 'artists', 'track_genre', 'popularity', 'duration_ms']],
            on='track_id',
            how='inner'
        )
        streaming_with_songs = streaming_with_songs.dropna(subset=['listen_time'])

        # Genre-Level KPIs
        genre_kpis = streaming_with_songs.groupby('track_genre').agg({
            'track_id': 'count',
            'duration_ms': 'mean',
            'popularity': 'mean'
        }).reset_index()
        genre_kpis.columns = ['track_genre', 'listen_count', 'avg_track_duration_ms', 'avg_popularity']
        genre_kpis['avg_track_duration_ms'] = genre_kpis['avg_track_duration_ms'].round(2)
        genre_kpis['avg_popularity'] = genre_kpis['avg_popularity'].round(2)

        most_popular_tracks = streaming_with_songs.groupby(['track_genre', 'track_id', 'track_name']).agg({
            'track_id': 'count',
            'popularity': 'mean'
        }).rename(columns={'track_id': 'play_count'}).reset_index()
        most_popular_tracks['engagement_score'] = (most_popular_tracks['play_count'] * 0.7 + most_popular_tracks['popularity'] * 0.3).round(2)
        most_popular_tracks = most_popular_tracks.loc[
            most_popular_tracks.groupby('track_genre')['engagement_score'].idxmax()
        ][['track_genre', 'track_id', 'track_name', 'engagement_score']]

        genre_kpis = genre_kpis.merge(most_popular_tracks, on='track_genre', how='left')
        genre_kpis['popularity_index'] = (genre_kpis['listen_count'] * 0.5 + genre_kpis['avg_popularity'] * 0.5).round(2)
        genre_kpis = genre_kpis[['track_genre', 'listen_count', 'avg_track_duration_ms', 'popularity_index', 'track_id', 'track_name', 'engagement_score']]

        # Hourly KPIs
        streaming_with_songs['hour'] = streaming_with_songs['listen_time'].dt.hour
        hourly_kpis = streaming_with_songs.groupby('hour').agg({
            'user_id': 'nunique',
            'track_id': ['count', 'nunique'],
            'artists': lambda x: x.value_counts().index[0]
        }).reset_index()
        hourly_kpis.columns = ['hour', 'unique_listeners', 'total_plays', 'unique_tracks', 'top_artist']
        hourly_kpis['track_diversity_index'] = (hourly_kpis['unique_tracks'] / hourly_kpis['total_plays']).round(2)

        genre_kpis_path = '/tmp/genre_kpis.csv'
        hourly_kpis_path = '/tmp/hourly_kpis.csv'
        genre_kpis.to_csv(genre_kpis_path, index=False)
        hourly_kpis.to_csv(hourly_kpis_path, index=False)

        logger.info("KPI computation successful.")
        return {
            'genre_kpis_path': genre_kpis_path,
            'hourly_kpis_path': hourly_kpis_path
        }
    except Exception as e:
        logger.error(f"Error computing KPIs: {e}")
        raise

def load_redshift(**kwargs):
    try:
        logger.info("Loading data into Redshift using MERGE upsert.")
        ti = kwargs['ti']
        paths = ti.xcom_pull(task_ids='validate_data')
        kpi_paths = ti.xcom_pull(task_ids='transform_kpis')
        users_path = paths['users_path']
        songs_path = paths['songs_path']
        streaming_path = paths['streaming_path']
        genre_kpis_path = kpi_paths['genre_kpis_path']
        hourly_kpis_path = kpi_paths['hourly_kpis_path']

        redshift_hook = PostgresHook(postgres_conn_id='redshift_conn')
        s3_hook = S3Hook(aws_conn_id='aws_default')
        bucket = 'amalitechde-music-streaming-lab'
        conn = redshift_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            user_name TEXT,
            user_age INTEGER,
            user_country TEXT,
            created_at DATE
        )
        DISTKEY(user_id)               
        SORTKEY (created_at);

        CREATE TABLE IF NOT EXISTS songs (
            id INTEGER PRIMARY KEY,
            track_id TEXT,
            artists TEXT,
            album_name TEXT,
            track_name TEXT,
            popularity INTEGER,
            duration_ms INTEGER,
            explicit BOOLEAN,
            danceability FLOAT,
            energy FLOAT,
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
        )
        DISTKEY(id)
        SORTKEY(track_genre, popularity);

        CREATE TABLE IF NOT EXISTS streams (
            user_id INTEGER,
            track_id TEXT,
            listen_time TIMESTAMP,
            PRIMARY KEY (user_id, track_id, listen_time)
        )
        DISTKEY(user_id)
        SORTKEY(listen_time);

        CREATE TABLE IF NOT EXISTS genre_kpis (
            track_genre TEXT PRIMARY KEY,
            listen_count INTEGER,
            avg_track_duration_ms FLOAT,
            popularity_index FLOAT,
            track_id TEXT,
            track_name TEXT,
            engagement_score FLOAT
        );
        CREATE TABLE IF NOT EXISTS hourly_kpis (
            hour INTEGER PRIMARY KEY,
            unique_listeners INTEGER,
            total_plays INTEGER,
            unique_tracks INTEGER,
            top_artist TEXT,
            track_diversity_index FLOAT
        );
        """)
        conn.commit()

        # USERS
        users_key = 'data/staging/validated_users.csv'
        s3_hook.load_file(filename=users_path, key=users_key, bucket_name=bucket, replace=True)
        cursor.execute("CREATE TABLE IF NOT EXISTS stage_users (LIKE users);")
        cursor.execute(f"""
            COPY stage_users FROM 's3://{bucket}/{users_key}'
            IAM_ROLE 'arn:aws:iam::198170203035:role/AirflowMusicStreamingRole'
            CSV IGNOREHEADER 1;
        """)
        cursor.execute("""
            MERGE INTO users
            USING stage_users s
            ON users.user_id = s.user_id
            WHEN MATCHED THEN UPDATE SET
                user_name = s.user_name,
                user_age = s.user_age,
                user_country = s.user_country,
                created_at = s.created_at
            WHEN NOT MATCHED THEN
                INSERT (user_id, user_name, user_age, user_country, created_at)
                VALUES (s.user_id, s.user_name, s.user_age, s.user_country, s.created_at);
            DROP TABLE stage_users;
        """)

        # SONGS
        songs_key = 'data/staging/validated_songs.csv'
        s3_hook.load_file(filename=songs_path, key=songs_key, bucket_name=bucket, replace=True)
        cursor.execute("CREATE TABLE IF NOT EXISTS stage_songs (LIKE songs);")
        cursor.execute(f"""
            COPY stage_songs FROM 's3://{bucket}/{songs_key}'
            IAM_ROLE 'arn:aws:iam::198170203035:role/AirflowMusicStreamingRole'
            CSV IGNOREHEADER 1;
        """)
        cursor.execute("""
            MERGE INTO songs
            USING stage_songs s
            ON songs.id = s.id
            WHEN MATCHED THEN UPDATE SET
                track_id = s.track_id,
                artists = s.artists,
                album_name = s.album_name,
                track_name = s.track_name,
                popularity = s.popularity,
                duration_ms = s.duration_ms,
                explicit = s.explicit,
                danceability = s.danceability,
                energy = s.energy,
                key = s.key,
                loudness = s.loudness,
                mode = s.mode,
                speechiness = s.speechiness,
                acousticness = s.acousticness,
                instrumentalness = s.instrumentalness,
                liveness = s.liveness,
                valence = s.valence,
                tempo = s.tempo,
                time_signature = s.time_signature,
                track_genre = s.track_genre
            WHEN NOT MATCHED THEN
                INSERT (
                    id, track_id, artists, album_name, track_name, popularity,
                    duration_ms, explicit, danceability, energy, key, loudness,
                    mode, speechiness, acousticness, instrumentalness, liveness,
                    valence, tempo, time_signature, track_genre
                )
                VALUES (
                    s.id, s.track_id, s.artists, s.album_name, s.track_name, s.popularity,
                    s.duration_ms, s.explicit, s.danceability, s.energy, s.key, s.loudness,
                    s.mode, s.speechiness, s.acousticness, s.instrumentalness, s.liveness,
                    s.valence, s.tempo, s.time_signature, s.track_genre
                );
            DROP TABLE stage_songs;
        """)

        # STREAMS
        if os.path.exists(streaming_path) and os.path.getsize(streaming_path) > 0:
            streams_key = 'data/staging/validated_streaming.csv'
            s3_hook.load_file(filename=streaming_path, key=streams_key, bucket_name=bucket, replace=True)
            cursor.execute("CREATE TABLE IF NOT EXISTS stage_streams (LIKE streams);")
            cursor.execute(f"""
                COPY stage_streams FROM 's3://{bucket}/{streams_key}'
                IAM_ROLE 'arn:aws:iam::198170203035:role/AirflowMusicStreamingRole'
                CSV IGNOREHEADER 1;
            """)
            cursor.execute("""
                MERGE INTO streams
                USING stage_streams
                ON streams.user_id = stage_streams.user_id
                 AND streams.track_id = stage_streams.track_id
                 AND streams.listen_time = stage_streams.listen_time
                REMOVE DUPLICATES;
                DROP TABLE stage_streams;
            """)

        # GENRE KPIS
        if genre_kpis_path and os.path.exists(genre_kpis_path) and os.path.getsize(genre_kpis_path) > 0:
            genre_kpis_key = 'data/staging/genre_kpis.csv'
            s3_hook.load_file(filename=genre_kpis_path, key=genre_kpis_key, bucket_name=bucket, replace=True)
            cursor.execute("CREATE TABLE IF NOT EXISTS stage_genre_kpis (LIKE genre_kpis);")
            cursor.execute(f"""
                COPY stage_genre_kpis FROM 's3://{bucket}/{genre_kpis_key}'
                IAM_ROLE 'arn:aws:iam::198170203035:role/AirflowMusicStreamingRole'
                CSV IGNOREHEADER 1;
            """)
            cursor.execute("""
                MERGE INTO genre_kpis
                USING stage_genre_kpis s
                ON genre_kpis.track_genre = s.track_genre
                WHEN MATCHED THEN UPDATE SET
                    listen_count = s.listen_count,
                    avg_track_duration_ms = s.avg_track_duration_ms,
                    popularity_index = s.popularity_index,
                    track_id = s.track_id,
                    track_name = s.track_name,
                    engagement_score = s.engagement_score
                WHEN NOT MATCHED THEN
                INSERT (track_genre, listen_count, avg_track_duration_ms, popularity_index, track_id, track_name, engagement_score)
                VALUES (s.track_genre, s.listen_count, s.avg_track_duration_ms, s.popularity_index, s.track_id, s.track_name, s.engagement_score);
                DROP TABLE stage_genre_kpis;
            """)

        # HOURLY KPIS
        if hourly_kpis_path and os.path.exists(hourly_kpis_path) and os.path.getsize(hourly_kpis_path) > 0:
            hourly_kpis_key = 'data/staging/hourly_kpis.csv'
            s3_hook.load_file(filename=hourly_kpis_path, key=hourly_kpis_key, bucket_name=bucket, replace=True)
            cursor.execute("CREATE TABLE IF NOT EXISTS stage_hourly_kpis (LIKE hourly_kpis);")
            cursor.execute(f"""
                COPY stage_hourly_kpis FROM 's3://{bucket}/{hourly_kpis_key}'
                IAM_ROLE 'arn:aws:iam::198170203035:role/AirflowMusicStreamingRole'
                CSV IGNOREHEADER 1;
            """)
            cursor.execute("""
                MERGE INTO hourly_kpis
                USING stage_hourly_kpis s
                ON hourly_kpis.hour = s.hour
                WHEN MATCHED THEN UPDATE SET
                    unique_listeners = s.unique_listeners,
                    total_plays = s.total_plays,
                    unique_tracks = s.unique_tracks,
                    top_artist = s.top_artist,
                    track_diversity_index = s.track_diversity_index
                WHEN NOT MATCHED THEN
                    INSERT (hour, unique_listeners, total_plays, unique_tracks, top_artist, track_diversity_index)
                    VALUES (s.hour, s.unique_listeners, s.total_plays, s.unique_tracks, s.top_artist, s.track_diversity_index);
                DROP TABLE stage_hourly_kpis;
            """)

        conn.commit()
        cursor.close()
        conn.close()
        logger.info("All data and KPIs loaded into Redshift successfully using MERGE upsert.")
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Error loading to Redshift: {e}")
        raise