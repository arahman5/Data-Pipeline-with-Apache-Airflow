from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                DataQualityOperator, CreateTablesOperator, LoadDimensionOperator)
from helpers import SqlQueries

# Define all the default arguments as stated in the project
default_args = {
    'owner': 'ashrahman',
    'start_date': datetime(2018, 7, 1),
    'end_date': datetime(2018, 11, 30),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

start_date = datetime.utcnow() # Define the start_date

# Create the DAG
dag_name='AirflowProject'
dag = DAG(dag_name,
          default_args=default_args,
          description='Data Pipeline with Airflow',
          schedule_interval='0 * * * *',
          max_active_runs=3
        )

# Define a task to indicate the beginning of the data pipeline
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

# Task to create staging tables in redshift
create_redshift_tables = CreateTablesOperator(
    task_id='Create_tables',
    dag=dag,
    redshift_conn_id="redshift"
)

# Stage all event data to redshift from S3 bucket
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    provide_context=True,
    table="events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    region="us-west-2",
    file_format="JSON",
    execution_date=start_date
)

# Stage all song metadata to redshift from S3 bucket
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    provide_context=True,
    table="songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    region="us-west-2",
    data_format="JSON",
    execution_date=start_date
)

# Insert data into the songplays table in Redshift
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.songplay_table_insert
)

# Insert data into the users table in Redshift
load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.user_table_insert,
    table="users",
    start_date= datetime(2018, 7, 1),

)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.song_table_insert,
    table="songs",
    start_date= datetime(2018, 7, 1),
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.artist_table_insert,
    table="artists",
    start_date= datetime(2018, 7, 1),
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    sql_query=SqlQueries.time_table_insert,
    table="time",
    start_date= datetime(2018, 7, 1),
)

run_quality_checks = DataQualityOperator(
    task_id='data_quality_checks',
    dag=dag,
    provide_context=True,
    aws_credentials_id="aws_credentials",
    redshift_conn_id='redshift',
    tables=["songplays", "users", "song", "artist", "time"]
)

# Task to indicate end of the data pipeline
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Set up the tasks dependencies

start_operator >> create_redshift_tables >> [stage_songs_to_redshift, stage_events_to_redshift]

[stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table,load_time_dimension_table] >> run_quality_checks

run_quality_checks >> end_operator
