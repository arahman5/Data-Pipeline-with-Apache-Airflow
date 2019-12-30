from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",) #template variable
    # SQL statement to be run in redshift in the execute function. This statement copies data to a specified table in the redshift cluster hosted
    # at the specified region rom a S3 bucket.

    copy_sql = """
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            TIMEFORMAT as 'epochmillisecs'
            TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
            {} 'auto' 
            {}
        """

    ## Set up the constructor based on the parameters provided by the user in the DAG script and inherit from Parent class
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="", # Added in region as we want to connect to the redshift cluster in the region us-west-2
                 file_format="JSON", # This parameter will allow us to distinguish between different file formats
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region= region
        self.file_format = file_format
        self.aws_credentials_id = aws_credentials_id
        self.execution_date = kwargs.get('execution_date')

    # Code that actually defines the tasks of the operator
    def execute(self, context):

        aws_hook = AwsHook(self.aws_credentials_id) # Create a AWS Hook
        credentials = aws_hook.get_credentials() # Get the credentials for AWS
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id) # Create a hook for Postgres

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")

        s3_path = "s3://{}".format(self.s3_bucket)
        if self.execution_date:
            # Backfill a specific date and use template variable execution date to partition the data during staging
            year = self.execution_date.strftime("%Y")
            month = self.execution_date.strftime("%m")
            day = self.execution_date.strftime("%d")
            s3_path = '/'.join([s3_path, str(year), str(month), str(day)])
        s3_path = s3_path + '/' + self.s3_key

        additional=""
        if self.file_format == 'CSV':
            additional = " DELIMETER ',' IGNOREHEADER 1 "

        # Format the sql statement to reflect the parameters passed into the constructor from the DAG script
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.file_format,
            additional
        )
        redshift.run(formatted_sql) # Execute the SQL Query against the Postgres database in Redshift and copy the data from JSON files

        self.log.info(f"Success: Copying {self.table} from S3 to Redshift")

