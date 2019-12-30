from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    ## Set up the constructor based on the parameters provided by the user in the DAG script and inherit from Parent class
    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id,
        self.redshift_conn_id = redshift_conn_id,
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        # Loop through all the tables in Redshift and count how many rows exist in each table, if less than 1 row then log an error
        # message
        for table in self.tables:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1 or records[0][0] < 1:
                self.log.error(f"Data quality check failed. {table} returned no results")
                raise ValueError(f"Data quality check failed. {table} returned no results")
            self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")