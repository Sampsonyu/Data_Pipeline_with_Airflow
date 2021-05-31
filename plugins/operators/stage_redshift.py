from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import logging
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    '''
        This operator loads data for S3 bucket in thier JSON 
        and transform them into columnar form and load it to Amazon Redshift RDB
        Parameters
        ----------
        redshift_conn_id : str
            Amazon Redshift RDB credentials
            
        aws_credentials_id : str
            IAM role credentials
            
        table : str
            table name which data will be inserted to
            
        sql : str
            the sql query that will be used to insert the fact table
            
            
       
        json_path_option : str
            JSONPaths file on S3 is loaded, which maps the source data to the table columns
            More info check copy command for Redshift                                        https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html#copy-from-json-examples-using-jsonpaths
        
        Example:
            copy category
            from 's3://mybucket/category_object_paths.json'
            iam_role 'arn:aws:iam::0123456789012:role/MyRedshiftRole' 
            json 's3://mybucket/category_jsonpath.json';  
   
    '''
    ui_color = '#358140'
    
    truncate_sql="TRUNCATE TABLE {}"
    
    # DELIMITER argument is not supported for JSON based COPY
    
    copy_sql = """
        COPY {}
        FROM '{}'
        FORMAT AS JSON '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    """
    #IGNOREHEADER {}
    #DELIMITER '{}'
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_path="",
                 table="",
                 s3_json_option="",  
                 *args, **kwargs):
#                  delimiter=",",
#                  ignore_headers=1,

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_path = s3_path
        self.table = table
        self.s3_json_option = s3_json_option
#         self.ignore_headers = ignore_headers
#         self.delimiter = delimiter

    def execute(self, context):
        self.log.info("StageToRedshiftOperator: starting")
        aws_hook = AwsHook(self.aws_credentials_id)
        self.log.info("StageToRedshiftOperator: getting aws credentials")
        credentials = aws_hook.get_credentials()

        self.log.info("StageToRedshiftOperator: getting redshift credentials")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('StageToRedshiftOperator: truncating {} table'.format(self.table))
        redshift_hook.run(StageToRedshiftOperator.truncate_sql.format(self.table))
                
        self.log.info(f'Preparing stage data from {self.s3_path} to {self.table} table...')
        print(self.table)
        print(self.s3_json_option)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
                    self.table,
                    self.s3_path,
                    self.s3_json_option,
                    credentials.access_key,
                    credentials.secret_key)
        redshift_hook.run(formatted_sql)
