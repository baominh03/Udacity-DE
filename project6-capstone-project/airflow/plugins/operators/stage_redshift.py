from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
import logging
import configparser

config_file_path = './home/workspace/dwh_p.cfg'

config = configparser.ConfigParser()
config.read_file(open(config_file_path))

DWH_ROLE_ARN = config.get('IAM_ROLE', 'DWH_ROLE_ARN')
 
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    copy_sql = """
            COPY {}
            FROM '{}'
            IAM_ROLE '{}'
            FORMAT AS PARQUET 
        """  
    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 aws_credentials_id,
                 table,
                 s3_bucket,
                 s3_key,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        logging.info("Redshift connection created.")
        logging.info("=== Preparing data from S3 to Redshift ===")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3a://{}/{}".format(self.s3_bucket, rendered_key)
        logging.info("=== Copying data to [{}] table from S3 path [{}] ".format(self.table, s3_path))
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            DWH_ROLE_ARN
        )
        redshift.run(formatted_sql)
        logging.info("=== Table [{}] copy operation DONE.")





