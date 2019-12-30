from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTablesOperator(BaseOperator):
    ui_color = '#358140'
    sql_statement_file='create_tables.sql' # SQL Script to create tables provided by Udacity

    # Set up the constructor based on the parameters provided by the user in the DAG script and inherit from Parent class
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(CreateTablesOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Creating Redshift tables ")

        fd = open(CreateTablesOperator.sql_statement_file, 'r') # Open the SQL File and split out each query based on delimiter semi-colon
        sql_file = fd.read()
        fd.close()

        sql_commands = sql_file.split(';')

        # Execute each of the SQL statement separately through the below for loop and create tables in redshift
        for command in sql_commands:
            if command.rstrip() != '':
                redshift.run(command)