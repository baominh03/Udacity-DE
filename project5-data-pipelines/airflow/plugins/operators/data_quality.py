from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id,
                 tables,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        dq_checks = [
            {'check_sql': "SELECT COUNT(*) FROM users WHERE userid is null",
             'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM songs WHERE songid is null",
             'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM artists WHERE artistid is null",
             'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM time WHERE start_time is null",
             'expected_result': 0},
            {'check_sql': "SELECT COUNT(*) FROM songplays WHERE userid is null",
             'expected_result': 0}
        ]
        error_sql = []
        # Refer lesson3. excercise4 Data Quality - https://knowledge.udacity.com/questions/399170
        redshift_hook = PostgresHook(self.redshift_conn_id)
        for dq_check in dq_checks:
            sql = dq_check.get('check_sql')
            exp_result = dq_check.get('expected_result')

            records = redshift_hook.get_records(sql)[0]

            error_count = 0

            if exp_result != records[0]:
                error_count += 1
                error_sql.append(sql)

            if error_count > 0:
                self.log.error('!!!=== SQL Tests failed ===!!!')
                self.log.error(error_sql)
                raise ValueError('Data quality check failed')

            if error_count == 0:
                self.log.info('=== SQL Tests Passed ===')

        error_tables_no_result = []
        error_tables_no_row = []
        flag_no_result = True
        flag_no_rows = True
        for table in self.tables:
            self.log.info(f"=== Checking Data Quality for {table} table ===")
            records = redshift_hook.get_records(
                f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(
                    f"Data quality check failed. {table} returned no results")
                flag_no_result = False
                error_tables_no_result.append(table)
            num_records = records[0][0]
            if num_records < 1:
                self.log.error(
                    f"Data quality check failed. {table} contained 0 rows ")
                flag_no_rows = False
                error_tables_no_row.append(table)
            else:
                self.log.info(
                    f"=>>> Data quality on table {table} check passed with {records[0][0]} records")
        if not flag_no_rows or not flag_no_result:
            raise ValueError("!!!!=== Data quality check failed.\nNo results for [{}] tables\n No rows for [{}] tables ===!!!"
                             .format(str(error_tables_no_result), str(error_tables_no_row)))
