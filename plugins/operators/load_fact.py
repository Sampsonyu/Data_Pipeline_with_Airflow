from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class LoadFactOperator(BaseOperator):
    '''
        This operator loads fact tables into Amazon Redshift
        Parameters
        ----------
        redshift_conn_id : str
            Amazon Redshift RDB credentials
            
        table : str
            Table name which data will be inserted into
            
        sql : str
            The sql query used to insert data into the fact table
    '''


    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql=sql

    def execute(self, context):
        # connect to Amazon Redshift
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info(f'Loading data into {self.table} fact table...')
        
        # Execute the insertion query on Redshift hook
        redshift_hook.run(f"INSERT INTO {self.table} {self.sql}")
        self.log.info(f"LoadFactOperator implemented: Data inserted into {self.table} in Redshift")
