from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 sql_statement,
                 redshift_conn_id,
                 table,
                 append_data=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.append_data = append_data

    def execute(self, context):
        self.log.info("Access Redshift")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if not self.append_data:
            self.log.info("LoadDimensionOperator Selected Mode: delete then insert")
            self.log.info("Deleting data on: [{}] dim table".format(self.table))
            redshift.run("TRUNCATE {}".format(self.table))
        self.log.info("Inserting data on: [{}] dim table".format(self.table))
        redshift.run("INSERT INTO {} {}".format(self.table, self.sql_statement))
        self.log.info("=== INTERT COMPLETE {} dim table ===".format(self.table))
        
