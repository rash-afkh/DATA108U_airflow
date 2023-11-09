from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from plugins.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from plugins.helpers import SqlQueries


default_args = {
    "owner": "rash-afkh",
    "start_date": datetime(2020, 1, 1),
    "depends_on_past": False,
    "email_on_retry": False,
    "retries": 4,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}

dag = DAG(
    "sparkify_etl",
    description="Load and transform data in Redshift with Airflow",
    default_args=default_args,
    schedule_interval="@hourly",
)

start_execution = EmptyOperator(task_id="start_execution", dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_events",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    extra_params="FORMAT AS JSON 's3://udacity-dend/log_json_path.json'",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    table="staging_songs",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    extra_params="JSON 'auto' COMPUPDATE OFF",
)

load_songs_dimension_table = LoadDimensionOperator(
    task_id="Load_songs_table",
    dag=dag,
    table="songs",
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_stmt=SqlQueries.song_table_insert,
)

load_songplays_fact_table = LoadFactOperator(
    task_id="Load_songplays_table",
    dag=dag,
    table="songplays",
    redshift_conn_id="redshift",
    load_sql_stmt=SqlQueries.songplay_table_insert,
)

load_users_dimension_table = LoadDimensionOperator(
    task_id="Load_users_table",
    dag=dag,
    table="users",
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_stmt=SqlQueries.user_table_insert,
)

load_time_dimension_table = LoadDimensionOperator(
    task_id="Load_time_table",
    dag=dag,
    table="time",
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_stmt=SqlQueries.time_table_insert,
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id="Load_artists_table",
    dag=dag,
    table="artists",
    redshift_conn_id="redshift",
    truncate_table=True,
    load_sql_stmt=SqlQueries.artist_table_insert,
)

run_quality_checks = DataQualityOperator(
    task_id="Run_data_quality_checks",
    dag=dag,
    dq_checks=[
        {
            "check_sql": "SELECT COUNT(*) FROM public.songs WHERE title IS NULL",
            "expected_result": 0,
        },
        {
            "check_sql": "SELECT COUNT(*) FROM public.songplays WHERE userid IS NULL",
            "expected_result": 0,
        },
        {
            "check_sql": 'SELECT COUNT(DISTINCT "level") FROM public.songplays',
            "expected_result": 2,
        },
        {
            "check_sql": 'SELECT COUNT(*) FROM public."time" WHERE weekday IS NULL',
            "expected_result": 0,
        },
        {
            "check_sql": "SELECT COUNT(*) FROM public.artists WHERE name IS NULL",
            "expected_result": 0,
        },
        {
            "check_sql": "SELECT COUNT(*) FROM public.users WHERE first_name IS NULL",
            "expected_result": 0,
        },
        {
            "check_sql": "SELECT COUNT(*) FROM public.songplays sp LEFT OUTER JOIN public.users us ON us.userid = sp.userid WHERE us.userid IS NULL",
            "expected_result": 0,
        },
    ],
    redshift_conn_id="redshift",
)

end_execution = EmptyOperator(task_id="end_execution", dag=dag)

start_execution >> stage_events_to_redshift
start_execution >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_fact_table
stage_songs_to_redshift >> load_songplays_fact_table

load_songplays_fact_table >> load_users_dimension_table
load_songplays_fact_table >> load_songs_dimension_table
load_songplays_fact_table >> load_artists_dimension_table
load_songplays_fact_table >> load_time_dimension_table

load_users_dimension_table >> run_quality_checks
load_songs_dimension_table >> run_quality_checks
load_artists_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_execution
