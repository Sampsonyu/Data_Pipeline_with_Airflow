from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging


class LoadDimensionOperator(BaseOperator):
    '''
        This operator loads dimension tables into Amazon Redshift
        Parameters
        ----------
        redshift_conn_id : str
            Amazon Redshift RDB credentials
            
        table : str
            Table name which data will be inserted into
            
        sql : str
            The sql query that will be used to insert into dimension tables
            
        truncate : bool
            Flag indicats truncating inserted dimensions
    '''


    ui_color = '#80BD9E'
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 table="",
                 sql="",
                 truncate=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql=sql
        self.truncate=truncate
        
        
    def execute(self, context):
        # connect to Amazon Redshift
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info(f'Loading data into {self.table} dimension table...')
        
        # format query for loading the dimension table     
        if self.truncate:
            truncate_sql = f"TRUNCATE {self.table}"
            self.log.info("Deleting {} table's content".format(self.table))
            redshift_hook.run(truncate_sql)
               
        self.log.info(f"LoadDimensionOperator implemented: Data loaded into dimension table {self.table} in Redshift")
        formatted_sql = f"INSERT INTO {self.table} ({self.sql})"        
        redshift_hook.run(formatted_sql)        
