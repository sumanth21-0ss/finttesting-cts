from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'test-scheduler',
    'depends_on_past': False,
    'start_date': datetime(2021, 4, 28),
    'email': ['hemachandrarao.kuppuswamynaidu@directlinegroup.co.uk'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'test_premium_fsh_fdp_comparison', default_args=default_args, schedule_interval=None)

t1 = BashOperator(
    task_id='premium_fsh_fdp_comparison_test',
    bash_command='git clone https://github.com/Direct-Line-Group/fint-testing.git; '
                 'source /home/airflow/test-scheduler/env/bin/activate;'
                 'cd fint-testing'
                 'aws s3 cp s3://fdp-test-automation-reports-test/premium/fsh_fdp_comparison . --recursive;'
                 'python utils/FSH_FDP_Comparison.py;'
                 'aws s3 cp . s3://fdp-test-automation-reports-test/premium/fsh_fdp_comparison --recursive --exclude "*" --include "*.xlsx"',
    retries=0,
    dag=dag)