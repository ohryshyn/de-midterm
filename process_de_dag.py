import boto3
import time

import airflow
from airflow import DAG
from datetime import timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.exceptions import AirflowException
from airflow.operators.dummy_operator import DummyOperator

CRAWLER_NAME = 'data_eng_bc_crawl_banking_data'

DEFAULT_ARGS = {
    'owner': 'oleh',
    'depends_on_past': False, # check for historical failures
    'start_date': airflow.utils.dates.days_ago(0), # right now is the starting day for all jobs
    'email': ['olegante10@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

SPARK_STEPS = [
    {
        'Name': 'wcd_date_eng_bootcamp2',
        'ActionOnFailure': "CONTINUE",
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                '/usr/bin/spark-submit',
              # '--class', 'Driver.MainApp',
                '--master', 'yarn',
                '--deploy-mode', 'cluster',
                '--num-executors', '2',
                '--driver-memory', '512m',
                '--executor-memory', '3g',
                '--executor-cores', '2',
              # 's3://demo-wcd/wcd_final_project_2.11-0.1.jar',
                '--py-files', 's3://data-eng-bc-midterm-entry-files/job.zip',
                's3://data-eng-bc-midterm-entry-files/workflow_entry.py',
                '-p','''{"input_path": "{{ task_instance.xcom_pull('parse_request', key='s3_location') }}",
                "name": "midterm_de",
                "file_type": "txt",
                "output_path": "s3://data-eng-bc-midterm-output/output/",
                "partition_column": "job"}'''
              # '-p', 'wcd-demo',
              # '-i', 'Csv',
              # '-s', "{{ task_instance.xcom_pull('parse_request', key='s3location') }}",
              # '-d', 's3://weclouddate-data-engineer-bootcamp2/banking',
              # '-c', 'job',
              # '-m', 'append',
            ]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'DE BC Midterm Cluster',
    'ReleaseLabel': 'emr-6.4.0',
    'Applications': [{'Name': 'Spark'}, {'Name': 'Hadoop'}, {'Name': 'Hive'}],
    'Instances': {
        'InstanceGroups': [
            {
                'Name': 'Primary node',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'MASTER',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 1,
            },
            {
                'Name': 'Core - 2',
                'Market': 'ON_DEMAND',
                'InstanceRole': 'CORE',
                'InstanceType': 'm5.xlarge',
                'InstanceCount': 2,
            }
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
#   'AvailabilityZone': 'us-east-1a',
#   "EmrManagedSlaveSecurityGroup": 'sg-0473afc4e1418ebfd',
#   "EmrManagedMasterSecurityGroup": 'sg-0628ef47b460340b0',
}

QUERY = '''CREATE EXTERNAL TABLE `output`(
  `age` string,
  `marital` string,
  `education` string,
  `default` string,
  `housing` string,
  `loan` string,
  `contact` string,
  `month` string,
  `day_of_week` string,
  `duration` string,
  `campaign` string,
  `pdays` string,
  `previous` string,
  `poutcome` string,
  `emp_var_rate` string,
  `cons_price_idx` string,
  `cons_conf_idx` string,
  `euribor3m` string,
  `nr_employed` string,
  `y` string,
  `source` string)
PARTITIONED BY (
  `job` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  's3://data-eng-bc-midterm-output/output/'
TBLPROPERTIES (
  'CrawlerSchemaDeserializerVersion'='1.0',
  'CrawlerSchemaSerializerVersion'='1.0',
  'UPDATED_BY_CRAWLER'='data_eng_bc_crawl_banking_data',
  'averageRecordSize'='10',
  'classification'='parquet',
  'compressionType'='none',
  'objectCount'='25',
  'recordCount'='41188',
  'sizeKey'='573155',
  'typeOfData'='file')'''

def retrieve_s3_files(**kwargs):
    s3_location = kwargs['dag_run'].conf['s3_location']
    kwargs['ti'].xcom_push(key='s3_location', value = s3_location)

def run_crawler():
    client=boto3.client('glue')
    response=client.start_crawler(
            Name=CRAWLER_NAME
    )

def create_table_if_not_exists():
    client=boto3.client('athena')
    response=client.start_query_execution(
            QueryString=QUERY,
            ResultConfiguration={'OutputLocation': 's3://de-bc-athena/output/'}
    )

def get_crawler_status():
    client = boto3.client('glue')
    response = client.get_crawler(
            Name=CRAWLER_NAME
    )
    crawler_state = response["Crawler"]['State']
   
    while crawler_state != 'READY':
        time.sleep(10)
        response = client.get_crawler(
            Name=CRAWLER_NAME
        )
        crawler_state = response["Crawler"]['State']        
    
    crawler_status = response["Crawler"]["LastCrawl"]["Status"]
    if crawler_status == 'SUCCEEDED':
        return True
    else:
        raise AirflowException(f"Status: {crawler_status}")


dag = DAG(
    'process_data_dag',
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    schedule_interval=None # not a CRON job - an event job
)

cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_cluster',
        job_flow_overrides=JOB_FLOW_OVERRIDES,
        aws_conn_id='aws_default',
        emr_conn_id='emr_default',
        dag=dag
)

parse_request = PythonOperator(
    task_id = 'parse_request',
    provide_context = True,
    python_callable = retrieve_s3_files,
    dag=dag
)


step_adder = EmrAddStepsOperator(
    task_id = 'add_steps',
    job_flow_id = "{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id = "aws_default",
    steps = SPARK_STEPS,
    dag = dag
)

step_checker = EmrStepSensor(
    task_id = 'watch_step',
    job_flow_id =  "{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    step_id = "{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id = 'aws_default',
    dag = dag
)

terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag
)

run_crawler_task = PythonOperator(
    task_id='run_crawler',
    python_callable=run_crawler,
    dag=dag
)

create_athena_table = PythonOperator(
    task_id='create_athena_table_if_not_exists',
    python_callable=create_table_if_not_exists,
    dag=dag
)

crawler_status = PythonOperator(
    task_id='get_crawler_status',
    python_callable=get_crawler_status,
    dag=dag
)

end_pipeline = DummyOperator(
    task_id='end_pipeline',
    trigger_rule='all_success',
    dag=dag
)

parse_request.set_upstream(cluster_creator)
step_adder.set_upstream(parse_request)
step_checker.set_upstream(step_adder)
terminate_emr_cluster.set_upstream(step_checker)
run_crawler_task.set_upstream(step_checker)
crawler_status.set_upstream(run_crawler_task)
create_athena_table.set_upstream(crawler_status)
end_pipeline.set_upstream([terminate_emr_cluster, create_athena_table])