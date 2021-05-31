from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id,
                 table_list,
                 quality_checks,
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table_list = table_list
        self.quality_checks = quality_checks

    def execute(self, context):
        # connect to Amazon Redshift
        redshift_hook = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        # Data quality check 1: The table has rows and records
        logging.info("DataQualityOperator: Starting row and record check")
        for table in self.table_list:
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed. {table} contained 0 rows")
            logging.info(f"Data quality on {table} table check passed with {records[0][0]} records")
            
        for check in self.quality_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            check_info = check.get('check_info')
            result = redshift_hook.get_records(sql)[0][0]
            if eval(exp_result):                    
                logging.info(f"{check_info} passed")
            else:
                raise ValueError(f"{check_info} failed. There is {result} NULL type record found")
        
        logging.info("DataQualityOperator: Data quality check is done")

            