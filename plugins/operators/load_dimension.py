from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    # Set up the constructor based on the parameters provided by the user in the DAG script and inherit from Parent class
    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 sql_query="",
                 table="",
                 truncate="",
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.truncate = truncate # Parameter to switch between append-only and delete-load functionality

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:
            redshift.run(f"TRUNCATE TABLE {self.table}")
        formatted_sql = self.sql_query.format(self.table)
        redshift.run(formatted_sql)
        self.log.info(f"Success: {self.task_id}")
