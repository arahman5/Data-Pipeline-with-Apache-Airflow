from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    # Set up the constructor based on the parameters provided by the user in the DAG script and inherit from Parent class
    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id,
        self.redshift_conn_id = redshift_conn_id,
        self.sql_query = sql_query,

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_hook.run(str(self.sql_query))