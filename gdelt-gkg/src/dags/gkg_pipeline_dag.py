from airflow.models import DAG
# from airflow.operators.bash import BashOperator
# from airflow.operators.python import PythonOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta 
from airflow.models import Variable
import sys
import os


GDELT_HOME = os.environ.get('GDELT_HOME')

scraper_entry_point = os.path.join(os.environ['GDELT_HOME'], 'gdelt/scrapers/gkg_scraper.py')
etl_entry_point = os.path.join(os.environ['GDELT_HOME'], 'gdelt/etl/execute_etl.py')
dependency_path = os.path.join(os.environ['GDELT_HOME'], 'gdelt/dependencies/gdelt_dependencies.zip')


########################################################################
########################          DAG           ########################
########################################################################

custom_args = {
    'owner': 'Jack Wittbold',
    'email': ['airflow.job.status@gmail.com'],
    'email_on_failure': True,
    'email_on_success': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


gkg_pipeline = DAG(
    dag_id='gkg_etl',
    start_date=datetime(2022, 1, 1),
    default_args=custom_args,
    description='Download and perfrom ETL on GDELT GKG files',
    catchup=False,
    schedule_interval='*/12 * * * *' # run every 12 minutes (5 times per hour)
)


########################################################################
########################         TASKS          ########################
########################################################################


t0 = SparkSubmitOperator(
    task_id='spark_submit_gkg_scraper',
    application=scraper_entry_point,
    conn_id='spark_default',
    py_files=dependency_path,
    dag=gkg_pipeline
)

t1 = SparkSubmitOperator(
    task_id='spark_submit_execute_etl',
    application=etl_entry_point,
    py_files=dependency_path,
    conn_id='spark_default',
    dag=gkg_pipeline
)

t0 >> t1 